"""Testes de contrato do exemplo 22 (consultas parametrizadas).

Valida as três notações de placeholder, a proteção contra injeção (o ponto
central), a serialização de tipos e a ressalva "parâmetro é valor, não
identificador".
"""

import datetime
from decimal import Decimal

import duckdb
import pytest

from _common import CUSTOMERS_GLOB


@pytest.fixture
def con():
    connection = duckdb.connect()
    connection.execute(
        f"CREATE VIEW customers AS SELECT * FROM read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true)"
    )
    yield connection
    connection.close()


def test_placeholder_notations_agree(con):
    posicional = con.execute("SELECT COUNT(*) FROM customers WHERE region = ?", ["sul"]).fetchone()[0]
    numerado = con.execute("SELECT COUNT(*) FROM customers WHERE region = $1", ["sul"]).fetchone()[0]
    nomeado = con.execute("SELECT COUNT(*) FROM customers WHERE region = $reg", {"reg": "sul"}).fetchone()[0]
    assert posicional == numerado == nomeado > 0


def test_parameter_blocks_injection_but_fstring_does_not(con):
    ataque = "sul' OR '1'='1"
    total = con.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    seguro = con.execute("SELECT COUNT(*) FROM customers WHERE region = ?", [ataque]).fetchone()[0]
    vulneravel = con.execute(
        f"SELECT COUNT(*) FROM customers WHERE region = '{ataque}'"
    ).fetchone()[0]
    assert seguro == 0            # o valor é uma região literal inexistente
    assert vulneravel == total    # a f-string deixou o OR '1'='1' burlar o filtro


def test_driver_serializes_native_types(con):
    d, eh_nulo = con.execute(
        "SELECT ?::DECIMAL(12,2) AS d, ? IS NULL AS eh_nulo", [Decimal("19.90"), None]
    ).fetchone()
    assert d == Decimal("19.90")  # escala preservada
    assert eh_nulo is True         # None vira NULL de verdade, não a string 'None'
    recentes = con.execute(
        "SELECT COUNT(*) FROM customers WHERE signup_date >= ?", [datetime.date(2024, 6, 1)]
    ).fetchone()[0]
    assert recentes >= 0


def test_parameter_is_value_not_identifier(con):
    # SELECT ? com 'region' devolve a STRING literal, não a coluna
    resultado = con.execute("SELECT ? AS r FROM customers LIMIT 1", ["region"]).fetchone()[0]
    assert resultado == "region"


def test_prepare_execute_matches_parameterized(con):
    con.execute("PREPARE por_regiao AS SELECT COUNT(*) FROM customers WHERE region = $1")
    via_prepare = con.execute("EXECUTE por_regiao('norte')").fetchone()[0]
    via_param = con.execute("SELECT COUNT(*) FROM customers WHERE region = ?", ["norte"]).fetchone()[0]
    assert via_prepare == via_param
