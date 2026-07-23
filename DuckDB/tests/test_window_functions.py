"""Testes de contrato do exemplo 20 (window functions avançadas).

Valida os comportamentos sutis: NTILE reparte em baldes de tamanho ~igual, a
diferença ROWS vs RANGE só aparece com empates (peers), e a pegadinha do frame
padrão do LAST_VALUE.
"""

import duckdb
import pytest


@pytest.fixture
def con():
    connection = duckdb.connect()
    yield connection
    connection.close()


def test_ntile_splits_into_equal_buckets(con):
    # 200 valores em NTILE(4) => 4 baldes de 50
    tamanhos = con.sql(
        """
        WITH q AS (
            SELECT NTILE(4) OVER (ORDER BY i) AS quartil FROM range(200) t(i)
        )
        SELECT quartil, COUNT(*) FROM q GROUP BY quartil ORDER BY quartil
        """
    ).fetchall()
    assert tamanhos == [(1, 50), (2, 50), (3, 50), (4, 50)]


def test_rows_vs_range_differ_only_on_ties(con):
    linhas = con.sql(
        """
        WITH vendas(dia, valor) AS (VALUES (1, 10), (1, 20), (2, 5), (3, 7), (3, 8))
        SELECT
            SUM(valor) OVER (ORDER BY dia ROWS  BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS por_linha,
            SUM(valor) OVER (ORDER BY dia RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS por_faixa
        FROM vendas ORDER BY dia, valor
        """
    ).fetchall()
    por_linha = [r[0] for r in linhas]
    por_faixa = [r[1] for r in linhas]
    # ROWS conta linhas físicas; RANGE inclui todos os peers do mesmo dia
    assert por_linha == [10, 30, 35, 42, 50]
    assert por_faixa == [30, 30, 35, 50, 50]


def test_last_value_needs_explicit_frame(con):
    linhas = con.sql(
        """
        WITH t(g, v) AS (VALUES ('a', 1), ('a', 2), ('a', 3))
        SELECT
            LAST_VALUE(v) OVER (PARTITION BY g ORDER BY v) AS ingenuo,
            LAST_VALUE(v) OVER (
                PARTITION BY g ORDER BY v
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS completo
        FROM t ORDER BY v
        """
    ).fetchall()
    # frame padrão para na linha atual (1,2,3); com frame completo, sempre o último (3)
    assert [r[0] for r in linhas] == [1, 2, 3]
    assert [r[1] for r in linhas] == [3, 3, 3]


def test_lag_lead_are_null_at_the_edges(con):
    linhas = con.sql(
        """
        SELECT i,
               LAG(i)  OVER (ORDER BY i) AS anterior,
               LEAD(i) OVER (ORDER BY i) AS proximo
        FROM range(3) t(i) ORDER BY i
        """
    ).fetchall()
    assert linhas[0][1] is None   # o primeiro não tem anterior
    assert linhas[-1][2] is None  # o último não tem próximo


def test_group_by_over_window_is_rejected(con):
    # window não reduz linhas; agrupar por cima na mesma consulta é erro de binder
    with pytest.raises(duckdb.BinderException):
        con.sql(
            "SELECT ROW_NUMBER() OVER (ORDER BY i) AS r FROM range(5) t(i) GROUP BY r"
        ).fetchall()
