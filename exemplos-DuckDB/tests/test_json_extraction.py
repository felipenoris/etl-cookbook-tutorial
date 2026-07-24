"""Testes de contrato do exemplo 19 (JSON): extração por caminho e sniff de schema.

Valida que o extension `json` está embutido (sem instalar nada), que os
operadores de caminho toleram documentos heterogêneos e que `read_json_auto`
reconstrói tipos aninhados a partir de um NDJSON.
"""

import duckdb
import pytest

from _common import CUSTOMERS_GLOB


@pytest.fixture
def con():
    connection = duckdb.connect()
    yield connection
    connection.close()


@pytest.fixture
def eventos(con):
    con.execute(
        """
        CREATE TABLE eventos (id INTEGER, payload JSON);
        INSERT INTO eventos VALUES
            (1, '{"tipo":"login","user":{"id":7,"plano":"pro"},"tags":["web","novo"]}'),
            (2, '{"tipo":"compra","valor":19.90,"itens":[{"sku":"X1"},{"sku":"X2"}]}'),
            (3, '{"tipo":"login","user":{"id":9}}');
        """
    )
    return con


def test_arrow_operator_extracts_text_and_missing_becomes_null(eventos):
    linhas = eventos.sql(
        """
        SELECT id, payload->>'$.tipo' AS tipo, payload->>'$.user.plano' AS plano
        FROM eventos ORDER BY id
        """
    ).fetchall()
    assert linhas == [(1, "login", "pro"), (2, "compra", None), (3, "login", None)]


def test_json_array_length_and_keys(eventos):
    n_itens = eventos.sql("SELECT json_array_length(payload->'$.itens') FROM eventos WHERE id = 2").fetchone()[0]
    assert n_itens == 2
    chaves = eventos.sql("SELECT json_keys(payload) FROM eventos WHERE id = 1").fetchone()[0]
    assert chaves == ["tipo", "user", "tags"]


def test_wildcard_path_explodes_with_unnest(eventos):
    skus = eventos.sql(
        """
        SELECT item.sku
        FROM eventos e, UNNEST(json_extract_string(e.payload, '$.itens[*].sku')) AS item(sku)
        WHERE e.id = 2 ORDER BY item.sku
        """
    ).fetchall()
    assert [row[0] for row in skus] == ["X1", "X2"]


def test_read_json_auto_infers_struct_and_list(con, tmp_path):
    json_file = tmp_path / "clientes.json"
    con.execute(
        f"""
        COPY (
            SELECT customer_id, address AS endereco, tags, preferences AS prefs
            FROM read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true)
            WHERE customer_id <= 50
        ) TO '{json_file}' (FORMAT json)
        """
    )
    tipos = dict(
        (nome, tipo)
        for nome, tipo, *_ in con.sql(f"DESCRIBE SELECT * FROM read_json_auto('{json_file}')").fetchall()
    )
    # objeto vira STRUCT, array vira LIST — o sniff reconstrói os tipos aninhados
    assert tipos["endereco"].startswith("STRUCT")
    assert tipos["tags"].endswith("[]")
    # e depois de inferido, dot-notation funciona como em parquet
    cidade = con.sql(
        f"SELECT endereco.city FROM read_json_auto('{json_file}') WHERE customer_id = 1"
    ).fetchone()[0]
    assert isinstance(cidade, str) and cidade
