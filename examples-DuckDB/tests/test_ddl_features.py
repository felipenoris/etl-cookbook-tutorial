"""Testes da paleta de DDL do exemplo 07 (constraints, defaults, gerada,
sequence, índice, ALTER) e da fronteira com os parâmetros estilo Hive."""

import duckdb
import pytest


@pytest.fixture
def con():
    connection = duckdb.connect()  # em memória basta para exercitar o DDL
    yield connection
    connection.close()


def criar_dim(con):
    con.execute(
        """
        CREATE TABLE dim (
            id BIGINT PRIMARY KEY,
            nome VARCHAR NOT NULL,
            region VARCHAR,
            carregado_em TIMESTAMP DEFAULT now(),
            regiao_norm VARCHAR GENERATED ALWAYS AS (upper(region)) VIRTUAL,
            CHECK (id > 0)
        )
        """
    )


def test_constraints_aparecem_no_catalogo(con):
    criar_dim(con)
    tipos = {
        r[0]
        for r in con.sql(
            "SELECT constraint_type FROM duckdb_constraints() WHERE table_name='dim'"
        ).fetchall()
    }
    assert {"PRIMARY KEY", "NOT NULL", "CHECK"} <= tipos


def test_default_e_coluna_gerada_sao_preenchidos(con):
    criar_dim(con)
    con.execute("INSERT INTO dim (id, nome, region) VALUES (1, 'a', 'sul')")
    linha = con.sql("SELECT regiao_norm, carregado_em IS NOT NULL FROM dim").fetchone()
    assert linha[0] == "SUL"  # coluna gerada = upper(region)
    assert linha[1] is True  # DEFAULT now() preencheu


def test_check_constraint_rejeita(con):
    criar_dim(con)
    with pytest.raises(duckdb.ConstraintException, match="CHECK"):
        con.execute("INSERT INTO dim (id, nome) VALUES (-1, 'x')")


def test_not_null_rejeita(con):
    criar_dim(con)
    with pytest.raises(duckdb.ConstraintException):
        con.execute("INSERT INTO dim (id, nome) VALUES (1, NULL)")


def test_primary_key_rejeita_duplicata(con):
    criar_dim(con)
    con.execute("INSERT INTO dim (id, nome) VALUES (1, 'a')")
    with pytest.raises(duckdb.ConstraintException):
        con.execute("INSERT INTO dim (id, nome) VALUES (1, 'b')")


def test_sequence_gera_surrogate_key(con):
    con.execute("CREATE SEQUENCE seq START 1000")
    con.execute("CREATE TABLE t (sk BIGINT DEFAULT nextval('seq'), cod VARCHAR)")
    con.execute("INSERT INTO t (cod) VALUES ('a'), ('b')")
    assert con.sql("SELECT sk FROM t ORDER BY sk").fetchall() == [(1000,), (1001,)]


def test_create_index_e_alter_add_column(con):
    criar_dim(con)
    con.execute("CREATE INDEX ix ON dim (region)")
    assert con.sql("SELECT COUNT(*) FROM duckdb_indexes() WHERE table_name='dim'").fetchone()[0] == 1
    con.execute("ALTER TABLE dim ADD COLUMN score INTEGER DEFAULT 0")
    assert "score" in con.sql("SELECT * FROM dim LIMIT 0").columns


def test_generated_stored_nao_suportado(con):
    # o DuckDB só aceita colunas geradas VIRTUAL, não STORED
    with pytest.raises(duckdb.Error, match="STORED"):
        con.execute("CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a) STORED)")


def test_partitioned_by_nao_suportado_em_tabela(con):
    # o parâmetro estilo Hive não existe no CREATE TABLE do DuckDB
    with pytest.raises(duckdb.CatalogException, match="PARTITIONED BY"):
        con.execute("CREATE TABLE t (id INT, dt DATE) PARTITIONED BY (dt)")


@pytest.mark.parametrize("clausula", ["CLUSTERED BY (id) INTO 4 BUCKETS", "STORED AS PARQUET", "LOCATION '/tmp/x'"])
def test_clausulas_hive_nao_existem(con, clausula):
    # CLUSTERED BY / STORED AS / LOCATION nem parseiam
    with pytest.raises(duckdb.ParserException):
        con.execute(f"CREATE TABLE t (id INT) {clausula}")
