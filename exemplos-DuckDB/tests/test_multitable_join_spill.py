"""Testes das afirmações do exemplo 17 (JOIN de 5 tabelas + spill).

Valida os CONTRATOS determinísticos da consulta — o rateio N:N pelo `fator`, os
dois filtros (data e saldo) e o uso de hash join — sobre um dataset minúsculo,
feito à mão, cujo resultado dá para conferir na ponta do lápis. O smoke test
(`test_examples_run.py`) já cobre a execução do exemplo inteiro sobre os
milhões de linhas; aqui isolamos a semântica.
"""

import importlib
import re
from decimal import Decimal

import duckdb
import pytest

exemplo = importlib.import_module("17_multitable_join_spill")


@pytest.fixture
def con():
    connection = duckdb.connect()
    # area: duas áreas
    connection.execute("CREATE TABLE area AS SELECT * FROM (VALUES (0, 'area_0'), (1, 'area_1')) t(id_area, nome_area)")
    # operacao: oper 10 -> area 0; oper 11 -> area 1
    connection.execute(
        "CREATE TABLE operacao AS SELECT * FROM (VALUES "
        "(10, 100.00::DECIMAL(12,2), 0), (11, 100.00::DECIMAL(12,2), 1)) t(id_oper, valor_operacao, id_area)"
    )
    # contrato: c1 com saldo > 0 (entra); c2 com saldo < 0 (sai)
    connection.execute(
        "CREATE TABLE contrato AS SELECT * FROM (VALUES "
        "(1, 100.00::DECIMAL(12,2)), (2, (-5.00)::DECIMAL(12,2))) t(id_contrato, saldo_em_aberto)"
    )
    # fluxo: um fluxo válido de c1; um de c1 fora da data; um de c2 (saldo sai)
    connection.execute(
        "CREATE TABLE fluxo AS SELECT * FROM (VALUES "
        "(1, DATE '2026-02-01', 100.00::DECIMAL(12,2)), "  # entra
        "(1, DATE '2025-12-01', 999.00::DECIMAL(12,2)), "  # sai: data <= 2026-01-01
        "(2, DATE '2026-03-01', 50.00::DECIMAL(12,2)) "    # sai: saldo do contrato <= 0
        ") t(id_contrato, data_fluxo, valor_fluxo)"
    )
    # ponte N:N: c1 rateado 0.3 para oper 10 (area 0) e 0.7 para oper 11 (area 1)
    connection.execute(
        "CREATE TABLE rel AS SELECT * FROM (VALUES "
        "(10, 1, 0.3000::DECIMAL(5,4)), (11, 1, 0.7000::DECIMAL(5,4))) t(id_oper, id_contrato, fator)"
    )
    yield connection
    connection.close()


QUERY = """
    SELECT a.nome_area, SUM(fl.valor_fluxo * r.fator) AS soma_ponderada
    FROM fluxo fl
    JOIN contrato c ON fl.id_contrato = c.id_contrato
    JOIN rel r ON r.id_contrato = c.id_contrato
    JOIN operacao o ON o.id_oper = r.id_oper
    JOIN area a ON a.id_area = o.id_area
    WHERE fl.data_fluxo > DATE '2026-01-01' AND c.saldo_em_aberto > 0
    GROUP BY a.nome_area
    ORDER BY a.nome_area
"""


def test_rateio_ponderado_e_filtros(con):
    # único fluxo válido: valor 100 de c1, rateado 0.3/0.7 -> 30 e 70
    resultado = dict(con.sql(QUERY).fetchall())
    assert resultado == {"area_0": Decimal("30.0000"), "area_1": Decimal("70.0000")}


def test_soma_permanece_decimal(con):
    # dinheiro nunca vira float: DECIMAL * DECIMAL -> DECIMAL exato
    tipo = con.sql(QUERY).types[1]
    assert "DECIMAL" in str(tipo).upper()


def test_plano_usa_apenas_hash_join(con):
    plano = con.sql("EXPLAIN " + QUERY).fetchall()[0][1]
    joins = re.findall(r"[A-Z_]*JOIN", plano)
    assert joins == ["HASH_JOIN"] * 4  # 4 joins, todos hash join, nenhum index/nested-loop


def test_exemplo_expoe_a_query_com_os_dois_filtros():
    # o exemplo aplica os dois filtros da pergunta de negócio
    assert "data_fluxo > DATE '2026-01-01'" in exemplo.QUERY
    assert "saldo_em_aberto > 0" in exemplo.QUERY
    assert "valor_fluxo * r.fator" in exemplo.QUERY
