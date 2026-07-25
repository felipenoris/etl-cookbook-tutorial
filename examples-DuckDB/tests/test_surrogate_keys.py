"""Testes de contrato do exemplo 23 (surrogate keys + RETURNING).

Valida que o DuckDB não tem IDENTITY, que sequence + DEFAULT nextval gera as
chaves, que RETURNING resgata o mapa natural→surrogate de um lote e que a
sequência é global (não reinicia) entre cargas.
"""

import duckdb
import pytest

from _common import CUSTOMERS_GLOB


@pytest.fixture
def con():
    connection = duckdb.connect()
    connection.execute(
        f"CREATE VIEW customers AS SELECT * FROM read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true)"
    )
    connection.execute("CREATE SEQUENCE seq START 1")
    connection.execute(
        """
        CREATE TABLE dim_cliente (
            sk_cliente  BIGINT DEFAULT nextval('seq') PRIMARY KEY,
            customer_id BIGINT UNIQUE,
            nome        VARCHAR
        )
        """
    )
    yield connection
    connection.close()


def test_identity_is_not_supported(con):
    with pytest.raises(duckdb.NotImplementedException):
        con.execute("CREATE TABLE t (id INTEGER GENERATED ALWAYS AS IDENTITY)")


def test_returning_recovers_generated_keys_paired_with_natural_key(con):
    mapa = con.execute(
        """
        INSERT INTO dim_cliente (customer_id, nome)
        SELECT customer_id, customer_name FROM customers WHERE customer_id <= 6
        RETURNING sk_cliente, customer_id
        """
    ).fetchall()
    # 6 pares devolvidos; surrogate keys 1..6 distintas; natural keys preservadas
    assert len(mapa) == 6
    assert {sk for sk, _ in mapa} == {1, 2, 3, 4, 5, 6}
    assert {cid for _, cid in mapa} == {1, 2, 3, 4, 5, 6}
    # o par (sk, natural) é a fonte confiável do mapa — a posição não é
    mapa_dict = dict((cid, sk) for sk, cid in mapa)
    for cid, sk in mapa_dict.items():
        na_tabela = con.execute(
            "SELECT sk_cliente FROM dim_cliente WHERE customer_id = ?", [cid]
        ).fetchone()[0]
        assert na_tabela == sk


def test_default_nextval_fills_the_key_when_omitted(con):
    con.execute("INSERT INTO dim_cliente (customer_id, nome) VALUES (100, 'x'), (200, 'y')")
    sks = con.sql("SELECT sk_cliente FROM dim_cliente ORDER BY sk_cliente").fetchall()
    assert [row[0] for row in sks] == [1, 2]  # preenchidas em sequência


def test_incremental_anti_join_inserts_only_missing_and_sequence_is_global(con):
    con.execute(
        "INSERT INTO dim_cliente (customer_id, nome) SELECT customer_id, customer_name FROM customers WHERE customer_id <= 3"
    )  # sk 1..3
    # novo lote: 3 já existe, 999 é novo — só o novo deve ganhar sk, continuando a sequência
    delta = con.execute(
        """
        INSERT INTO dim_cliente (customer_id, nome)
        SELECT * FROM (VALUES (3, 'existe'), (999, 'novo')) AS src(customer_id, nome)
        WHERE NOT EXISTS (SELECT 1 FROM dim_cliente d WHERE d.customer_id = src.customer_id)
        RETURNING sk_cliente, customer_id
        """
    ).fetchall()
    assert delta == [(4, 999)]  # sk continua de 4 (global), só o 999 entrou
    total = con.sql("SELECT COUNT(*) FROM dim_cliente").fetchone()[0]
    assert total == 4


def test_reinserting_existing_natural_key_violates_unique(con):
    con.execute("INSERT INTO dim_cliente (customer_id, nome) VALUES (1, 'a')")
    with pytest.raises(duckdb.ConstraintException):
        con.execute("INSERT INTO dim_cliente (customer_id, nome) VALUES (1, 'b')")
