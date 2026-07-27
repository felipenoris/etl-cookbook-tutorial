"""Exemplo 27 — `con.register`: dar nome a objetos Python, e por que isso é uma VIEW.

O exemplo 26 mostrou que `con.read_parquet(glob)` devolve uma *query* ainda não
executada. Falta o último passo: **dar um nome a ela dentro do banco**. É o que
`con.register(nome, objeto)` faz — e a tese deste exemplo é que o resultado não
é "parecido com" uma view, é **uma view**, literalmente:

```python
con.register("orders", con.read_parquet(GLOB, hive_partitioning=True))
#  ==  CREATE TEMP VIEW orders AS SELECT * FROM read_parquet(GLOB, ...)
```

A prova está no catálogo (`duckdb_views()` lista o nome registrado ao lado das
views criadas por DDL, e `information_schema.tables` classifica as duas como
`VIEW`), no plano de execução (mesmos pushdowns, mesmo partition pruning — e
até a *mesma* patologia quando o cast atrapalha) e no comportamento: nenhuma das
duas materializa nada, as duas releem os arquivos a cada consulta, e as duas
enxergam arquivos que apareceram no diretório depois de criadas.

## O que `register` aceita, e o que muda

Não só relations: **`pandas.DataFrame`, `pyarrow.Table`, `polars.DataFrame`,
`RecordBatchReader`** — qualquer objeto colunar que o DuckDB saiba ler
zero-copy. Aí o nome vira uma view sobre *memória Python* em vez de sobre
arquivos, e o SQL passa a operar sobre o DataFrame como se fosse tabela:
`SELECT`, `JOIN` com parquet, `GROUP BY`, tudo. Nada é copiado — o DuckDB lê os
buffers do próprio DataFrame.

## `register` vs. o replacement scan

O exemplo 5 mostrou que `SELECT * FROM meu_df` já funciona sem registrar nada:
é o *replacement scan*, que procura o nome entre as variáveis do escopo de quem
chamou. `register` é a versão explícita, e resolve o que o replacement scan não
resolve:

| | replacement scan | `con.register` |
| --- | --- | --- |
| de onde vem o nome | variável no escopo do chamador | você escolhe |
| sobrevive ao fim da função | não | **sim** (o DuckDB guarda a referência) |
| aparece no catálogo | não | **sim**, como view temporária |
| dá para remover | — | `con.unregister(nome)` |

## As três diferenças em relação a `CREATE VIEW`

1. É **temporária** (`temporary = true`): morre com a conexão.
2. Não tem **texto SQL** no catálogo (`sql` vem vazio) — logo, `EXPORT DATABASE`
   a ignora silenciosamente, e ela não é reconstruível a partir do dump.
3. Vale só **naquela conexão**, como toda view temporária.

Rode com: `uv run examples/27_register_relations_and_dataframes.py`
"""

from __future__ import annotations

import shutil
import tempfile
import time
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa

from _common import CUSTOMERS_GLOB, ORDERS_GLOB, PRODUCTS_GLOB, section


def diagnostico_do_plano(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[str, bool]:
    """Devolve `(arquivos_lidos, tem_operador_FILTER)` para um `EXPLAIN` da query.

    `Scanning Files: N/M` só aparece no plano quando o DuckDB conseguiu
    descartar arquivos pelo valor da partição; sua ausência significa que todos
    foram abertos e o filtro virou um operador `FILTER` acima do scan.
    """
    plano = con.sql(f"EXPLAIN {sql}").fetchall()[0][1]
    linha = next(
        (l.strip(" │").strip() for l in plano.splitlines() if "Scanning Files" in l),
        "todos os arquivos",
    )
    return linha, "FILTER" in plano


def menor_tempo(fn, repeticoes: int = 3) -> float:
    """Menor tempo de N execuções, em milissegundos — o menos contaminado por ruído."""
    melhor = float("inf")
    for _ in range(repeticoes):
        inicio = time.perf_counter()
        fn()
        melhor = min(melhor, time.perf_counter() - inicio)
    return melhor * 1000


if __name__ == "__main__":
    con = duckdb.connect()

    section("Os dois caminhos: register(relation) e CREATE VIEW")
    con.register("orders_reg", con.read_parquet(ORDERS_GLOB, hive_partitioning=True))
    con.execute(
        f"""
        CREATE VIEW orders_view AS
        SELECT * FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
        """
    )
    print("Para o SQL, os dois nomes são indistinguíveis:")
    con.sql(
        """
        SELECT 'orders_reg' AS fonte, count(*) AS linhas FROM orders_reg
        UNION ALL
        SELECT 'orders_view', count(*) FROM orders_view
        """
    ).show()

    section("O catálogo: os dois estão na MESMA lista, duckdb_views()")
    con.sql(
        """
        SELECT view_name, temporary, column_count,
               CASE WHEN sql = '' THEN '(vazio)' ELSE left(sql, 34) || '...' END AS sql_guardado
        FROM duckdb_views
        ORDER BY view_name
        """
    ).show()
    print("information_schema classifica os dois como VIEW — não como BASE TABLE:")
    con.sql(
        """
        SELECT table_name, table_type FROM information_schema.tables
        WHERE table_name LIKE 'orders_%' ORDER BY table_name
        """
    ).show()

    section("O plano: mesmos pushdowns, e a MESMA patologia do cast")
    for fonte in ("orders_reg", "orders_view"):
        for literal, tipo in (("1", "INTEGER"), ("'01'", "VARCHAR")):
            consulta = f"SELECT order_id FROM {fonte} WHERE order_month = {literal}"
            arquivos, tem_filter = diagnostico_do_plano(con, consulta)
            tempo = menor_tempo(
                lambda f=fonte, l=literal: con.sql(
                    f"SELECT count(*) FROM {f} WHERE order_month = {l}"
                ).fetchone()
            )
            print(
                f"{fonte:12s} order_month = {literal:5s} ({tipo:7s}) "
                f"{tempo:6.1f}ms  {arquivos:20s} FILTER: {tem_filter}"
            )
    print("\nLinha a linha, register e CREATE VIEW dão o mesmo número. Inclusive na")
    print("pegadinha do exemplo 26: comparar a partição VARCHAR com um literal INTEGER")
    print("insere um CAST que o descarte por arquivo não sabe avaliar — nos DOIS.")
    print("Não é coincidência: é o mesmo objeto de catálogo, com a mesma resolução.")

    section("Nenhum dos dois é snapshot: arquivo novo aparece nos dois")
    temporario = Path(tempfile.mkdtemp(prefix="duckdb_register_"))
    glob_movel = str(temporario / "*.parquet")
    con.execute(f"COPY (SELECT 1 AS x) TO '{temporario / 'a.parquet'}' (FORMAT parquet)")
    con.register("movel_reg", con.read_parquet(glob_movel))
    con.execute(f"CREATE VIEW movel_view AS SELECT * FROM read_parquet('{glob_movel}')")
    con.execute(f"CREATE TABLE movel_tabela AS SELECT * FROM read_parquet('{glob_movel}')")

    def contagens() -> str:
        """Conta as linhas vistas por cada um dos três nomes, na mesma ordem."""
        return "  ".join(
            f"{nome.removeprefix('movel_')}={con.sql(f'SELECT count(*) FROM {nome}').fetchone()[0]}"
            for nome in ("movel_reg", "movel_view", "movel_tabela")
        )

    print(f"1 arquivo no diretório -> {contagens()}")
    con.execute(f"COPY (SELECT 2 AS x) TO '{temporario / 'b.parquet'}' (FORMAT parquet)")
    print(f"2 arquivos             -> {contagens()}")
    print("O register e a view reexpandiram o glob e viram o arquivo novo; a tabela")
    print("(CTAS) não — ela copiou os dados uma vez. É a diferença do exemplo 11.")

    section("A 1ª diferença: register é TEMP, e EXPORT DATABASE o ignora")
    dump = temporario / "dump"
    con.execute(f"EXPORT DATABASE '{dump}' (FORMAT parquet)")
    schema_sql = dump.joinpath("schema.sql").read_text()
    print("nomes que sobreviveram no schema.sql do dump:")
    for nome in ("orders_reg", "orders_view", "movel_reg", "movel_view", "movel_tabela"):
        print(f"  {nome:14s} {'sim' if nome in schema_sql else 'NÃO'}")
    print("\nO dump reconstrói o que tem DDL em texto. Um nome registrado não tem —")
    print("o catálogo guarda um ponteiro para um objeto Python, e isso não se serializa.")
    print("Consequência prática: register é para a SESSÃO; contrato durável é CREATE VIEW.")

    section("A 2ª e 3ª diferenças: escopo de conexão")
    outra = duckdb.connect()
    for nome in ("orders_reg", "orders_view"):
        try:
            outra.sql(f"SELECT count(*) FROM {nome}").fetchone()
            print(f"{nome:12s}: visível na outra conexão")
        except duckdb.Error as erro:
            print(f"{nome:12s}: {type(erro).__name__} — {str(erro).splitlines()[0][:60]}")
    print("(a view aqui também não atravessa porque o banco é in-memory e privado;")
    print(" num arquivo .duckdb compartilhado, a view persistiria e o register não)")
    outra.close()

    section("Registrando um pandas DataFrame: SQL sobre memória Python")
    # uma tabela de negócio que só existe no processo: metas por região,
    # digitadas/carregadas de uma planilha, sem arquivo parquet nenhum
    metas = pd.DataFrame(
        {
            "region": ["norte", "sul", "sudeste", "nordeste", "centro_oeste"],
            "meta_itens": [1_200_000, 3_500_000, 9_000_000, 2_800_000, 1_100_000],
            "responsavel": ["ana", "bruno", "carla", "diego", "elisa"],
        }
    )
    con.register("metas", metas)
    print(f"objeto Python: {type(metas).__name__} com {len(metas)} linhas")
    print("depois do register, o SQL enxerga um schema tipado:")
    con.sql("DESCRIBE metas").show()

    section("O DataFrame como tabela de primeira classe: JOIN com o parquet")
    con.sql(
        f"""
        WITH realizado AS (
            SELECT c.region, sum(o.quantity) AS itens
            FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true) o
            JOIN read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true) c USING (customer_id)
            WHERE o.order_month = '01'
            GROUP BY c.region
        )
        SELECT m.region, m.responsavel, r.itens, m.meta_itens,
               round(100.0 * r.itens / m.meta_itens, 1) AS pct_da_meta
        FROM metas m
        JOIN realizado r USING (region)
        ORDER BY pct_da_meta DESC
        """
    ).show()
    print("33M de linhas de parquet cruzadas com 5 linhas que só existem na RAM do")
    print("Python — sem CREATE TABLE, sem INSERT, sem escrever nada em disco.")

    section("Zero-copy, mas snapshot lógico: o efeito do copy-on-write do pandas")
    numeros = pd.DataFrame({"valor": [1, 2, 3]})
    con.register("numeros", numeros)
    antes = con.sql("SELECT sum(valor) FROM numeros").fetchone()[0]
    numeros.iloc[0, 0] = 100  # pandas >= 3.0: copy-on-write sempre ligado
    depois = con.sql("SELECT sum(valor) FROM numeros").fetchone()[0]
    print(f"soma vista pelo DuckDB antes da escrita:  {antes}")
    print(f"DataFrame em Python depois da escrita:    {numeros['valor'].tolist()}")
    print(f"soma vista pelo DuckDB depois:            {depois}")
    print("O DuckDB não copiou nada — ele guarda uma referência aos buffers. Mas o")
    print("copy-on-write do pandas 3 faz a escrita alocar buffers NOVOS para o")
    print("DataFrame, deixando os antigos intactos para quem os referencia. Efeito")
    print("prático: o nome registrado é um snapshot lógico, de graça. Para publicar")
    print("os dados novos, registre de novo — o mesmo nome é substituído:")
    con.register("numeros", numeros)
    print(f"depois de re-registrar:                   {con.sql('SELECT sum(valor) FROM numeros').fetchone()[0]}")

    section("pyarrow.Table também: é o formato nativo, sem conversão alguma")
    tabela_arrow = pa.table({"product_id": [1, 2, 3], "promocao": [True, False, True]})
    con.register("promocoes", tabela_arrow)
    con.sql(
        f"""
        SELECT p.category, count(*) AS produtos_em_promocao
        FROM promocoes pr
        JOIN read_parquet('{PRODUCTS_GLOB}') p USING (product_id)
        WHERE pr.promocao
        GROUP BY p.category ORDER BY 1
        """
    ).show()

    section("register vs. replacement scan: o que só o register faz")

    def prepara_com_register(conexao: duckdb.DuckDBPyConnection) -> None:
        """Publica o DataFrame local no catálogo. A variável morre no `return`."""
        local = pd.DataFrame({"origem": ["com register"], "n": [42]})
        conexao.register("do_escopo_local", local)

    def prepara_sem_register(conexao: duckdb.DuckDBPyConnection) -> None:
        """Aqui `sem_register` só existe enquanto esta função executa."""
        sem_register = pd.DataFrame({"origem": ["sem register"], "n": [42]})  # noqa: F841
        # dentro da função o replacement scan até acha (o frame do chamador de
        # .sql() é ESTE), mas o nome não sobrevive ao return
        conexao.sql("SELECT * FROM sem_register").fetchall()

    prepara_com_register(con)
    prepara_sem_register(con)
    print(f"registrado, lido depois do return: {con.sql('SELECT * FROM do_escopo_local').fetchall()}")
    try:
        con.sql("SELECT * FROM sem_register").fetchall()
    except duckdb.Error as erro:
        print(f"não registrado, lido depois:       {type(erro).__name__}: {str(erro).splitlines()[0]}")
    print("O replacement scan resolve o nome no frame de quem chamou `.sql()` — some")
    print("junto com o frame. Por isso bibliotecas e funções que preparam dados para")
    print("consultas posteriores usam register: é o que dura além da chamada.")

    section("unregister: tirar o nome do catálogo")
    con.unregister("promocoes")
    try:
        con.sql("SELECT count(*) FROM promocoes").fetchone()
    except duckdb.Error as erro:
        print(f"{type(erro).__name__}: {str(erro).splitlines()[0]}")
    print("(o objeto Python continua vivo; o que saiu foi só a entrada no catálogo)")

    section("E materializar, compensa? A medição desmente o palpite")
    con.register(
        "orders_jan",
        con.read_parquet(ORDERS_GLOB, hive_partitioning=True).filter("order_month = '01'"),
    )
    con.execute("CREATE TABLE jan_completa AS SELECT * FROM orders_jan")
    con.execute("CREATE TABLE jan_enxuta AS SELECT status, quantity FROM orders_jan")
    consulta = "SELECT status, sum(quantity) FROM {fonte} GROUP BY status"
    for fonte, descricao in (
        ("orders_jan", "nome registrado (view sobre parquet)"),
        ("jan_completa", "tabela materializada, 8 colunas"),
        ("jan_enxuta", "tabela materializada, 2 colunas"),
    ):
        tempo = menor_tempo(lambda f=fonte: con.sql(consulta.format(fonte=f)).fetchall())
        print(f"{descricao:38s} {tempo:6.0f}ms/consulta")
    print("\nA view GANHA das duas tabelas — o oposto da intuição. Duas razões, ambas")
    print("do exemplo 25: o parquet guarda `status` em RLE_DICTIONARY + snappy, então")
    print("a coluna lida é minúscula; e o pruning restringe a leitura a 1 arquivo de 6.")
    print("A tabela do banco EM MEMÓRIA não é comprimida — o scan move mais bytes.")
    print("Materializar compensa quando a query encapsulada é cara (join, sort, UDF)")
    print("ou quando a fonte é remota (S3); não pelo simples fato de 'estar no banco'.")
    print("É a mesma ressalva do exemplo 11, agora com o sinal invertido e medido.")

    section("A lição")
    print("1. con.register(nome, obj) cria uma VIEW TEMPORÁRIA de verdade: ela aparece")
    print("   em duckdb_views(), é VIEW no information_schema e gera o mesmo plano que")
    print("   o CREATE VIEW equivalente — pushdown, pruning e patologias inclusive;")
    print("2. logo, ela também não materializa nada: relê a fonte a cada consulta e")
    print("   enxerga arquivos novos (ao contrário do CTAS, que é snapshot);")
    print("3. as diferenças são de CICLO DE VIDA, não de semântica: é temporária, não")
    print("   guarda texto SQL (EXPORT DATABASE a ignora) e vale só naquela conexão;")
    print("4. register aceita DataFrame/Table/reader — o SQL passa a operar sobre a")
    print("   memória do Python, e o JOIN com parquet é direto, sem cópia;")
    print("5. com o copy-on-write do pandas 3, o nome registrado é snapshot lógico:")
    print("   escrever no DataFrame não muda o que o SQL vê; re-registre para publicar;")
    print("6. register é o replacement scan explícito — só ele sobrevive ao escopo da")
    print("   função, aparece no catálogo e pode ser desfeito com unregister.")

    shutil.rmtree(temporario, ignore_errors=True)
