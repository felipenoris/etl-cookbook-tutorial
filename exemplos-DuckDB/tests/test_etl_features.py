"""Testes das funcionalidades de ETL dos exemplos 06-10.

Cada teste reproduz, em miniatura e com dados controlados, o mecanismo que o
exemplo correspondente demonstra: COPY TO particionado idempotente, UPSERT em
banco persistente, quarentena de linhas rejeitadas, ASOF JOIN e UDFs.
"""

from pathlib import Path

import duckdb
import pytest

from _common import CUSTOMERS_GLOB, ORDERS_GLOB, PRODUCTS_GLOB


@pytest.fixture
def con():
    connection = duckdb.connect()
    yield connection
    connection.close()


def test_copy_to_partitioned_is_idempotent(tmp_path: Path, con):
    out = tmp_path / "saida"
    copy_sql = f"""
        COPY (
            SELECT order_month, status, COUNT(*) AS pedidos
            FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
            WHERE CAST(order_month AS INTEGER) <= 2
            GROUP BY ALL
        ) TO '{out}' (FORMAT parquet, PARTITION_BY (order_month), OVERWRITE_OR_IGNORE)
    """
    con.execute(copy_sql)
    particoes = sorted(p.name for p in out.iterdir())
    assert particoes == ["order_month=01", "order_month=02"]

    antes = con.sql(f"SELECT COUNT(*) FROM read_parquet('{out}/**/*.parquet')").fetchone()[0]
    con.execute(copy_sql)  # segunda rodada: sobrescreve, não duplica
    depois = con.sql(f"SELECT COUNT(*) FROM read_parquet('{out}/**/*.parquet')").fetchone()[0]
    assert antes == depois


def test_persistent_db_upsert(tmp_path: Path):
    db_path = tmp_path / "dim.db"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE dim (id BIGINT PRIMARY KEY, nome VARCHAR)")
    con.execute("INSERT INTO dim VALUES (1, 'original'), (2, 'b')")
    con.execute(
        """
        INSERT INTO dim VALUES (1, 'atualizado'), (3, 'novo')
        ON CONFLICT (id) DO UPDATE SET nome = EXCLUDED.nome
        """
    )
    con.close()

    # reabre o arquivo: upsert persistiu — 3 linhas, id=1 atualizado
    con2 = duckdb.connect(str(db_path), read_only=True)
    linhas = dict(con2.sql("SELECT id, nome FROM dim ORDER BY id").fetchall())
    con2.close()
    assert linhas == {1: "atualizado", 2: "b", 3: "novo"}


def test_read_csv_store_rejects_quarantines_bad_rows(tmp_path: Path, con):
    csv = tmp_path / "sujo.csv"
    csv.write_text("id,valor\n1,10\n2,abc\n3,30\n")
    # SELECT * de propósito: um COUNT(*) puro não parseia as colunas
    # (projection pushdown), então nenhuma linha seria rejeitada
    boas = len(
        con.sql(
            f"""
            SELECT * FROM read_csv(
                '{csv}', types = {{'valor': 'INTEGER'}}, store_rejects = true
            )
            """
        ).fetchall()
    )
    rejeitadas = con.sql("SELECT COUNT(*) FROM reject_errors").fetchone()[0]
    coluna = con.sql("SELECT column_name FROM reject_errors").fetchone()[0]
    assert boas == 2
    assert rejeitadas == 1
    assert coluna == "valor"


def test_asof_join_picks_latest_valid_price(con):
    con.execute(
        """
        CREATE TABLE precos (pid INT, desde DATE, preco DOUBLE);
        INSERT INTO precos VALUES
            (1, DATE '2025-01-01', 100.0),
            (1, DATE '2025-01-10', 110.0);
        CREATE TABLE pedidos (pid INT, dia DATE);
        INSERT INTO pedidos VALUES
            (1, DATE '2025-01-05'),
            (1, DATE '2025-01-10'),
            (1, DATE '2025-01-25');
        """
    )
    resultado = con.sql(
        """
        SELECT o.dia, p.preco FROM pedidos o
        ASOF JOIN precos p ON o.pid = p.pid AND o.dia >= p.desde
        ORDER BY o.dia
        """
    ).fetchall()
    # dia 05 pega o preço de 01/01; dia 10 (inclusive) e 25 pegam o reajuste
    assert [preco for _, preco in resultado] == [100.0, 110.0, 110.0]


def test_recursive_cte_flattens_hierarchy(con):
    con.execute(
        """
        CREATE TABLE h (id INT, nome VARCHAR, pai INT);
        INSERT INTO h VALUES (1, 'raiz', NULL), (2, 'filho', 1), (3, 'neto', 2);
        """
    )
    caminhos = con.sql(
        """
        WITH RECURSIVE arvore AS (
            SELECT id, nome AS caminho, 0 AS nivel FROM h WHERE pai IS NULL
            UNION ALL
            SELECT h.id, arvore.caminho || '/' || h.nome, arvore.nivel + 1
            FROM h JOIN arvore ON h.pai = arvore.id
        )
        SELECT caminho, nivel FROM arvore ORDER BY nivel
        """
    ).fetchall()
    assert caminhos == [("raiz", 0), ("raiz/filho", 1), ("raiz/filho/neto", 2)]


def test_export_database_writes_one_parquet_per_table(tmp_path: Path, con):
    con.execute(
        """
        CREATE TABLE t1 (id INT, nome VARCHAR);
        INSERT INTO t1 VALUES (1, 'a'), (2, 'b');
        CREATE TABLE t2 (id INT);
        INSERT INTO t2 VALUES (10);
        CREATE VIEW v1 AS SELECT id FROM t1;
        """
    )
    dump = tmp_path / "dump"
    con.execute(f"EXPORT DATABASE '{dump}' (FORMAT parquet)")

    parquets = sorted(p.name for p in dump.glob("*.parquet"))
    assert len(parquets) == 2  # um arquivo por TABELA (views vão só no schema.sql)
    assert (dump / "schema.sql").exists()
    assert "CREATE VIEW v1" in (dump / "schema.sql").read_text()

    # IMPORT reconstrói tabelas E views numa conexão nova
    con2 = duckdb.connect()
    con2.execute(f"IMPORT DATABASE '{dump}'")
    assert con2.sql("SELECT COUNT(*) FROM t1").fetchone()[0] == 2
    assert con2.sql("SELECT COUNT(*) FROM v1").fetchone()[0] == 2
    con2.close()


def test_view_reads_parquet_and_table_reads_internal_storage(con):
    con.execute(
        f"""
        CREATE VIEW v_orders AS
            SELECT * FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true);
        CREATE TABLE t_amostra AS
            SELECT * FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
            LIMIT 1000;
        """
    )
    plano_view = con.sql("EXPLAIN SELECT SUM(quantity) FROM v_orders").fetchall()[0][1]
    plano_tabela = con.sql("EXPLAIN SELECT SUM(quantity) FROM t_amostra").fetchall()[0][1]
    assert "READ_PARQUET" in plano_view  # view relê os arquivos externos
    assert "SEQ_SCAN" in plano_tabela  # tabela lê o storage interno
    assert "READ_PARQUET" not in plano_tabela


def test_sorted_parquet_has_selective_zonemaps(tmp_path: Path, con):
    # mesmo dado, dois layouts: valores ciclando 0..99 (cada row group vê o
    # range inteiro) vs ordenado (cada row group cobre faixa estreita)
    con.execute("CREATE TABLE base AS SELECT range % 100 AS chave, range AS valor FROM range(100000)")
    espalhado = tmp_path / "espalhado.parquet"
    ordenado = tmp_path / "ordenado.parquet"
    con.execute(f"COPY base TO '{espalhado}' (FORMAT parquet, ROW_GROUP_SIZE 10000)")
    con.execute(f"COPY (SELECT * FROM base ORDER BY chave) TO '{ordenado}' (FORMAT parquet, ROW_GROUP_SIZE 10000)")

    def row_groups_candidatos(arquivo: Path, alvo: int) -> tuple[int, int]:
        return con.sql(
            f"""
            SELECT COUNT(*) FILTER (
                       WHERE CAST(stats_min_value AS BIGINT) <= {alvo}
                         AND CAST(stats_max_value AS BIGINT) >= {alvo}
                   ),
                   COUNT(*)
            FROM parquet_metadata('{arquivo}')
            WHERE path_in_schema = 'chave'
            """
        ).fetchone()

    candidatos_espalhado, total = row_groups_candidatos(espalhado, 42)
    candidatos_ordenado, _ = row_groups_candidatos(ordenado, 42)
    assert total == 10
    assert candidatos_espalhado == total  # zonemaps inúteis: todo grupo cobre 0..99
    assert candidatos_ordenado <= 2  # zonemaps seletivas: faixa estreita por grupo

    # o layout não muda o resultado, só o custo
    for arquivo in (espalhado, ordenado):
        linhas = con.sql(
            f"SELECT COUNT(*) FROM read_parquet('{arquivo}') WHERE chave = 42"
        ).fetchone()[0]
        assert linhas == 1000


def test_duckdb_maps_all_stack_types(con):
    """Exemplo 14: cada tipo lógico do parquet vira o tipo SQL esperado."""
    tipos_c = dict(
        (r[0], r[1])
        for r in con.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true)"
        ).fetchall()
    )
    tipos_p = dict(
        (r[0], r[1])
        for r in con.sql(f"DESCRIBE SELECT * FROM read_parquet('{PRODUCTS_GLOB}')").fetchall()
    )
    assert tipos_c["is_active"] == "BOOLEAN"
    assert tipos_c["signup_ts"] == "TIMESTAMP"
    assert tipos_c["address"].startswith("STRUCT(")
    assert tipos_c["tags"] == "VARCHAR[]"
    assert tipos_c["preferences"] == "MAP(VARCHAR, VARCHAR)"
    assert tipos_p["unit_cost"] == "DECIMAL(12,2)"  # 2 casas: padrão do projeto
    assert tipos_p["sku"] == "BLOB"


def test_decimal_arithmetic_stays_decimal(con):
    tipo_soma, tipo_produto = con.sql(
        f"""
        SELECT typeof(SUM(unit_cost)), ANY_VALUE(typeof(unit_cost * 2))
        FROM read_parquet('{PRODUCTS_GLOB}')
        """
    ).fetchone()
    assert tipo_soma.startswith("DECIMAL")
    assert tipo_produto.startswith("DECIMAL")
    assert tipo_soma.endswith(",2)") and tipo_produto.endswith(",2)")


@pytest.mark.network
def test_remote_parquet_over_https(con):
    # exemplo 13: leitura remota via httpfs — schema e contagem estáveis
    url = "https://blobs.duckdb.org/stations.parquet"
    colunas = {r[0] for r in con.sql(f"DESCRIBE SELECT * FROM read_parquet('{url}')").fetchall()}
    assert {"code", "name_long", "country"} <= colunas
    assert con.sql(f"SELECT COUNT(*) FROM read_parquet('{url}')").fetchone()[0] == 578


@pytest.mark.network
def test_remote_parquet_over_s3_anonymous(con):
    con.execute("CREATE SECRET pub (TYPE s3, PROVIDER config, REGION 'us-east-1')")
    total = con.sql(
        """
        SELECT COUNT(*)
        FROM read_parquet('s3://noaa-ghcn-pds/parquet/by_year/YEAR=1763/*/*.parquet',
                          hive_partitioning=true)
        """
    ).fetchone()[0]
    assert total == 730  # ano de 1763: 365 TMAX + 365 TMIN (dado histórico, estável)


def test_python_udf_native_and_arrow(con):
    import pyarrow.compute as pc
    from duckdb.sqltypes import DOUBLE, VARCHAR

    con.create_function("upper_py", lambda s: s.upper(), [VARCHAR], VARCHAR)
    con.create_function("dobro", lambda v: pc.multiply(v, 2.0), [DOUBLE], DOUBLE, type="arrow")

    linha = con.sql("SELECT upper_py('abc'), dobro(21.0)").fetchone()
    assert linha == ("ABC", 42.0)
