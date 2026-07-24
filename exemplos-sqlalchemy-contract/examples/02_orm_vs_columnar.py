"""Exemplo 2 — O custo do ORM no caminho de dados: INSERT massivo vs colunar.

Mede, com o mesmo dado, os três jeitos de materializar o fato `Lancamento`
num banco local:

1. **ORM (o padrão antigo)**: instanciar um objeto ``Lancamento`` por linha e
   ``session.add_all`` + ``commit``. O custo não é do banco — é do Python:
   milhões de objetos, rastreamento de estado na session (unit of work) e
   serialização linha a linha.
2. **SQLAlchemy Core**: ``insert()`` com lista de dicts (executemany). Sem o
   overhead do unit of work, mas ainda linha a linha — o "teto" do mundo
   orientado a linhas.
3. **Colunar (a stack nova)**: a mesma massa como Table Arrow -> parquet ->
   ``CREATE TABLE AS SELECT * FROM read_parquet(...)`` no DuckDB. Nenhum
   objeto Python por linha; os dados nunca saem do domínio colunar.

O banco dos caminhos 1 e 2 é SQLite em memória — o cenário MAIS favorável ao
ORM (sem rede, sem disco, sem fsync). Mesmo assim a diferença é de ordens de
grandeza; contra um Postgres real, só piora.

Nota sobre PRODUTIVIDADE: o argumento mais comum para adotar um ORM não é
performance, e sim produtividade (não escrever SQL à mão, não codificar a
serialização banco <-> objeto). Este exemplo mede só o custo; o balanço de
produtividade da troca — o que se ganha, o que se perde e o que simplesmente
deixa de ser necessário — está no README do projeto, na seção
"Produtividade: o que se ganha e o que se perde na troca".

Rode com: `uv run examples/02_orm_vs_columnar.py [n_linhas]` (default 100000)
"""

import sys
import tempfile
import time
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import Session

from _common import gerar_lancamentos, section
from models import Base, Lancamento

CONTAS_FOLHA = list(range(1, 51))


def caminho_orm(linhas: list[dict]) -> float:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    inicio = time.perf_counter()
    # o padrão antigo: uma instância ORM por lançamento
    objetos = [Lancamento(**linha) for linha in linhas]
    with Session(engine) as session:
        session.add_all(objetos)
        session.commit()
    return time.perf_counter() - inicio


def caminho_core(linhas: list[dict]) -> float:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    inicio = time.perf_counter()
    with engine.begin() as conn:
        conn.execute(insert(Lancamento), linhas)  # executemany, sem unit of work
    return time.perf_counter() - inicio


def caminho_colunar(tabela, workdir: Path) -> float:
    inicio = time.perf_counter()
    destino = workdir / "lancamentos.parquet"
    pq.write_table(tabela, destino)
    con = duckdb.connect()
    con.execute(f"CREATE TABLE cad_lancamentos AS SELECT * FROM read_parquet('{destino}')")
    total = con.sql("SELECT COUNT(*) FROM cad_lancamentos").fetchone()[0]
    con.close()
    assert total == tabela.num_rows
    return time.perf_counter() - inicio


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
    workdir = Path(tempfile.mkdtemp(prefix="orm_vs_colunar_"))

    section(f"Preparando a massa: {n:,} lançamentos")
    tabela = gerar_lancamentos(n, CONTAS_FOLHA)
    # a MESMA massa como lista de dicts, para os caminhos orientados a linha —
    # to_pylist() já devolve os tipos nativos (datetime.date, Decimal, ...);
    # a conversão não entra no cronômetro de ninguém
    linhas = tabela.to_pylist()
    print(f"schema (do contrato): {[f.name for f in tabela.schema]}")

    section("Caminho 1 — ORM: objetos + session.add_all + commit")
    t_orm = caminho_orm(linhas)
    print(f"{t_orm:6.2f}s  ({n / t_orm:,.0f} linhas/s)")

    section("Caminho 2 — SQLAlchemy Core: insert() executemany")
    t_core = caminho_core(linhas)
    print(f"{t_core:6.2f}s  ({n / t_core:,.0f} linhas/s)")

    section("Caminho 3 — colunar: Arrow -> parquet -> DuckDB CTAS")
    t_col = caminho_colunar(tabela, workdir)
    print(f"{t_col:6.2f}s  ({n / t_col:,.0f} linhas/s)")

    section("Placar")
    print(f"ORM     : {t_orm:6.2f}s   (1x)")
    print(f"Core    : {t_core:6.2f}s   ({t_orm / t_core:.1f}x mais rápido que o ORM)")
    print(f"Colunar : {t_col:6.2f}s   ({t_orm / t_col:.0f}x mais rápido que o ORM)")
    print("\nA lição: o ORM não é lento por má configuração — ele paga um objeto")
    print("Python + rastreamento de estado POR LINHA. No caminho colunar, o fato")
    print("nunca vira objeto: só buffers Arrow do início ao fim.")
