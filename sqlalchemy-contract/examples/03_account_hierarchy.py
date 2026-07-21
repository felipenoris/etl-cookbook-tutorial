"""Exemplo 3 — A árvore do plano de contas resolvida no DuckDB (sem ORM, sem Postgres).

No padrão antigo, filtrar lançamentos "da conta X para baixo" exigia navegar
`RelacionamentoContaHierarquia` no Postgres efêmero (ou pior: em loops Python
sobre objetos ORM). Aqui a mesma estrutura — arestas parent->child por
hierarquia — é resolvida com `WITH RECURSIVE` no DuckDB in-process:

1. as DIMENSÕES (contas, arestas) nascem como Tables Arrow pequenas e são
   consultadas direto pelo DuckDB (replacement scan — sem CREATE/INSERT);
2. o FATO (lançamentos) chega como parquet, como chegaria do S3;
3. a CTE recursiva materializa a árvore achatada 1x (id_conta -> caminho,
   nível, raiz), e "conta X e descendentes" vira um JOIN comum;
4. as agregações de valor somam DECIMAL(12,2) — exatas, sem float.

Compare com `DuckDB/examples/09_advanced_sql_transforms.py`, que introduz a
CTE recursiva; aqui ela opera o modelo real de arestas por hierarquia (com
uma hierarquia alternativa provando por que o desenho suporta N visões).

Rode com: `uv run examples/03_account_hierarchy.py`
"""

import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from _common import gerar_lancamentos, section

# plano de contas: (id, nome, numero, permite_lancamentos)
CONTAS = [
    (1, "Resultado", "0", False),
    (2, "Receitas", "1", False),
    (3, "Vendas", "1.1", True),
    (4, "Servicos", "1.2", True),
    (5, "Despesas", "2", False),
    (6, "Pessoal", "2.1", False),
    (7, "Salarios", "2.1.1", True),
    (8, "Encargos", "2.1.2", True),
    (9, "Ocupacao", "2.2", False),
    (10, "Aluguel", "2.2.1", True),
    (11, "Energia", "2.2.2", True),
]
# arestas (id_hierarquia, id_parent, id_child) — hierarquia 1: a visão contábil
ARESTAS = [
    (1, 1, 2), (1, 1, 5),
    (1, 2, 3), (1, 2, 4),
    (1, 5, 6), (1, 5, 9),
    (1, 6, 7), (1, 6, 8),
    (1, 9, 10), (1, 9, 11),
    # hierarquia 2: visão "custo fixo" — OUTRA árvore sobre as MESMAS contas
    (2, 1, 10), (2, 1, 11), (2, 1, 7),
]
CONTAS_FOLHA = [3, 4, 7, 8, 10, 11]

FLATTEN_SQL = """
WITH RECURSIVE arvore AS (
    -- âncora: raízes da hierarquia escolhida (contas que não são child de ninguém)
    SELECT c.id_conta, c.nome, 0 AS nivel, c.nome AS caminho
    FROM contas c
    WHERE c.id_conta NOT IN (
        SELECT id_child FROM arestas WHERE id_hierarquia = $hierarquia
    )
    AND c.id_conta IN (
        SELECT id_parent FROM arestas WHERE id_hierarquia = $hierarquia
    )
    UNION ALL
    -- passo: desce uma geração pelas arestas da hierarquia
    SELECT c.id_conta, c.nome, a.nivel + 1, a.caminho || ' > ' || c.nome
    FROM arestas r
    JOIN arvore a ON r.id_parent = a.id_conta
    JOIN contas c ON c.id_conta = r.id_child
    WHERE r.id_hierarquia = $hierarquia
)
SELECT * FROM arvore
"""

if __name__ == "__main__":
    workdir = Path(tempfile.mkdtemp(prefix="hierarquia_"))
    con = duckdb.connect()

    # dimensões como Tables Arrow: o DuckDB as enxerga pelo nome da variável
    contas = pa.table(
        {
            "id_conta": pa.array([c[0] for c in CONTAS], pa.int64()),
            "nome": pa.array([c[1] for c in CONTAS], pa.string()),
            "numero": pa.array([c[2] for c in CONTAS], pa.string()),
            "permite_lancamentos": pa.array([c[3] for c in CONTAS]),
        }
    )
    arestas = pa.table(
        {
            "id_hierarquia": pa.array([a[0] for a in ARESTAS], pa.int64()),
            "id_parent": pa.array([a[1] for a in ARESTAS], pa.int64()),
            "id_child": pa.array([a[2] for a in ARESTAS], pa.int64()),
        }
    )

    # o fato chega como parquet (como chegaria do S3 pós-UNLOAD)
    fato = workdir / "lancamentos.parquet"
    pq.write_table(gerar_lancamentos(200_000, CONTAS_FOLHA), fato)

    section("Árvore achatada (hierarquia 1): caminho e nível por conta")
    con.execute(f"CREATE TABLE arvore AS {FLATTEN_SQL}", {"hierarquia": 1})
    con.sql("SELECT nivel, caminho FROM arvore ORDER BY caminho").show(max_width=100)

    section("Filtro por subárvore: lançamentos de 'Pessoal' (id 6) e descendentes")
    con.sql(
        f"""
        SELECT a.caminho, COUNT(*) AS lancamentos, SUM(l.valor) AS total
        FROM read_parquet('{fato}') l
        JOIN arvore a ON a.id_conta = l.id_conta
        WHERE a.caminho LIKE (SELECT caminho FROM arvore WHERE id_conta = 6) || '%'
        GROUP BY a.caminho ORDER BY a.caminho
        """
    ).show(max_width=100)

    section("Demonstração de N visões: a MESMA consulta na hierarquia 2 (custo fixo)")
    con.execute("CREATE TABLE arvore2 AS " + FLATTEN_SQL, {"hierarquia": 2})
    con.sql(
        f"""
        SELECT a.caminho, SUM(l.valor) AS total
        FROM read_parquet('{fato}') l
        JOIN arvore2 a ON a.id_conta = l.id_conta
        GROUP BY a.caminho ORDER BY a.caminho
        """
    ).show(max_width=100)

    section("A soma é DECIMAL, não float")
    tipo = con.sql(
        f"SELECT typeof(SUM(valor)) FROM read_parquet('{fato}')"
    ).fetchone()[0]
    print(f"typeof(SUM(valor)) = {tipo}  (o valor DECIMAL(12,2) do contrato, exato)")

    section("Validação de integridade sem FK: anti-join encontra lançamentos órfãos")
    orfaos = con.sql(
        f"""
        SELECT COUNT(*) FROM read_parquet('{fato}') l
        ANTI JOIN contas c ON l.id_conta = c.id_conta
        """
    ).fetchone()[0]
    print(f"lançamentos com conta inexistente: {orfaos}")
    print("(o papel das FKs DEFERRED do modelo antigo, como query de qualidade)")
