"""Exemplo 22 — Consultas parametrizadas: segurança, corretude e reuso de plano.

Todos os exemplos anteriores montam SQL com literais fixos ou f-strings sobre
**caminhos de arquivo** (constantes do projeto, seguras). Mas assim que um valor
vem de **fora** — um filtro escolhido pelo usuário, um id de uma fila, uma data
de parâmetro — interpolar esse valor direto na string de SQL é errado por três
motivos, e a correção é a mesma: **passar o valor como parâmetro**, separado do
texto da query.

As formas de placeholder no client Python do DuckDB (todas exercitadas abaixo):

- **`?`** posicional — os valores vão numa lista, na ordem;
- **`$1`, `$2`** numerados — permitem **repetir** o mesmo parâmetro sem duplicar
  o valor na lista;
- **`$nome`** nomeados — os valores vão num dict `{"nome": valor}`, legível em
  queries com muitos parâmetros;
- **`PREPARE ... AS` / `EXECUTE`** — a forma no próprio SQL, útil para preparar
  uma vez e executar muitas.

Os três motivos para nunca interpolar valor externo em SQL:

1. **Segurança (injeção de SQL)**: um valor parametrizado é tratado como
   **dado**, nunca reinterpretado como SQL. O exemplo mede o ataque clássico
   `' OR '1'='1` — parametrizado ele casa 0 linhas (é uma região literal que
   não existe); interpolado numa f-string, ele **burla o filtro** e retorna a
   tabela inteira.
2. **Corretude/tipos**: o driver serializa `date`, `Decimal`, `bool`, `None`
   com o tipo certo — sem `str()` manual, sem aspas erradas, sem `NULL` virando
   a string `'None'`.
3. **Reuso de plano**: uma prepared statement é planejada uma vez; executá-la
   com valores diferentes pula o parsing/binding a cada chamada.

A ressalva importante: **parâmetro substitui VALOR, não IDENTIFICADOR**. Nome de
tabela/coluna não pode ser parâmetro (`SELECT ? FROM t` devolve o literal, não a
coluna). Para isso, valide o identificador contra uma lista permitida e só então
o interpole — nunca aceite um nome de coluna cru do usuário.

Rode com: `uv run examples/22_parameterized_queries.py`
"""

import datetime
from decimal import Decimal

import duckdb

from _common import CUSTOMERS_GLOB, section

if __name__ == "__main__":
    con = duckdb.connect()
    con.execute(
        f"CREATE VIEW customers AS SELECT * FROM read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true)"
    )

    section("As três notações de placeholder devolvem o mesmo tipo de resultado")
    # ? posicional: valores numa lista, na ordem
    n1 = con.execute(
        "SELECT COUNT(*) FROM customers WHERE region = ? AND is_active = ?",
        ["sul", True],
    ).fetchone()[0]
    print(f"?  posicional  (region='sul' AND ativo)          -> {n1:,} clientes")

    # $1 numerado: permite REPETIR o mesmo parâmetro sem duplicar o valor
    n2 = con.execute(
        "SELECT COUNT(*) FROM customers WHERE region = $1 OR upper(region) = upper($1)",
        ["norte"],
    ).fetchone()[0]
    print(f"$1 numerado    (usa o mesmo valor duas vezes)     -> {n2:,} clientes")

    # $nome nomeado: valores num dict — legível com muitos parâmetros
    n3 = con.execute(
        "SELECT COUNT(*) FROM customers WHERE region = $reg AND signup_date >= $desde",
        {"reg": "sudeste", "desde": "2024-01-01"},
    ).fetchone()[0]
    print(f"$nome nomeado  (region='sudeste' AND desde 2024)  -> {n3:,} clientes")

    section("Motivo 1 — SEGURANÇA: o ataque clássico ' OR '1'='1 medido nas duas versões")
    malicioso = "sul' OR '1'='1"
    seguro = con.execute(
        "SELECT COUNT(*) FROM customers WHERE region = ?", [malicioso]
    ).fetchone()[0]
    # a f-string interpola o texto no SQL: o OR '1'='1' passa a valer e casa TUDO
    vulneravel = con.execute(
        f"SELECT COUNT(*) FROM customers WHERE region = '{malicioso}'"
    ).fetchone()[0]
    total = con.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    print(f"parametrizado (?)   -> {seguro:,} linhas  (o valor é uma região literal inexistente)")
    print(f"f-string (VULNERÁVEL) -> {vulneravel:,} linhas  (de {total:,} — o filtro foi BURLADO)")
    print("-> parametrizado, o valor nunca é reinterpretado como SQL; na f-string, o")
    print("   trecho ' OR '1'='1 vira código e escancara a tabela. (Aqui é só COUNT; num")
    print("   DELETE/UPDATE, seria destrutivo.)")

    section("Motivo 2 — TIPOS: date/Decimal/bool/None serializados corretamente pelo driver")
    linhas = con.execute(
        """
        SELECT COUNT(*) FILTER (WHERE signup_date >= ?) AS recentes,
               COUNT(*) FILTER (WHERE is_active = ?)     AS ativos
        FROM customers
        """,
        [datetime.date(2024, 6, 1), True],
    ).fetchone()
    print(f"date e bool nativos (sem str() manual): recentes={linhas[0]:,}, ativos={linhas[1]:,}")
    # None vira NULL de verdade (não a string 'None'); Decimal mantém a escala
    valor = con.execute("SELECT ?::DECIMAL(12,2) AS d, ? IS NULL AS eh_nulo", [Decimal("19.90"), None]).fetchone()
    print(f"Decimal preservado -> {valor[0]!r}; None -> NULL de verdade? {valor[1]}")

    section("Motivo 3 — REUSO: a MESMA query parametrizada, executada por região")
    consulta = "SELECT COUNT(*) FROM customers WHERE region = ?"
    for reg in ["norte", "nordeste", "centro_oeste", "sudeste", "sul"]:
        qtd = con.execute(consulta, [reg]).fetchone()[0]
        print(f"  {reg:<13} {qtd:>6,}")
    print("-> reexecutar o mesmo texto de SQL com valores diferentes reaproveita o plano;")
    print("   nenhuma concatenação de string acontece no laço.")

    # a forma equivalente no próprio SQL: PREPARE uma vez, EXECUTE com o valor literal
    con.execute("PREPARE por_regiao AS SELECT COUNT(*) FROM customers WHERE region = $1")
    via_prepare = con.execute("EXECUTE por_regiao('sul')").fetchone()[0]
    print(f"via PREPARE/EXECUTE (equivalente em SQL): região 'sul' -> {via_prepare:,}")

    section("Carga em lote: executemany aplica o MESMO SQL a vários conjuntos de valores")
    con.execute("CREATE TABLE dim_regiao (regiao VARCHAR, rotulo VARCHAR)")
    con.executemany(
        "INSERT INTO dim_regiao VALUES (?, ?)",
        [("norte", "N"), ("sul", "S"), ("sudeste", "SE")],
    )
    con.sql("SELECT * FROM dim_regiao ORDER BY regiao").show()

    section("A ressalva: parâmetro é VALOR, não IDENTIFICADOR")
    literal = con.execute("SELECT ? AS resultado FROM customers LIMIT 1", ["region"]).fetchone()[0]
    print(f"SELECT ? com o valor 'region' -> {literal!r} (a STRING, não a coluna region)")
    # para escolher a coluna dinamicamente, valide contra uma lista permitida e só aí interpole
    colunas_ok = {"region", "customer_name", "signup_date"}
    escolha = "region"  # viria do usuário
    assert escolha in colunas_ok, "coluna não permitida"
    top = con.execute(
        f"SELECT {escolha}, COUNT(*) AS n FROM customers GROUP BY {escolha} ORDER BY n DESC LIMIT 3"
    ).fetchall()
    print(f"identificador escolhido com allowlist ({escolha!r}): {top}")

    section("Resumo")
    print("- valor externo -> SEMPRE parâmetro (?, $1, $nome), nunca f-string;")
    print("- protege contra injeção, serializa tipos corretamente e permite reuso de plano;")
    print("- PREPARE/EXECUTE e executemany para repetição; parâmetro NÃO troca identificador")
    print("  (use allowlist para nome de coluna/tabela dinâmico).")
