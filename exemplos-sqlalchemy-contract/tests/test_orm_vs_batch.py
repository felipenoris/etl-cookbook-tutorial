"""Testes das quatro estratégias do exemplo 04 (ORM vs lote, em Python puro).

As quatro devem ser **indistinguíveis pelo resultado** — só o custo muda.
Os testes usam dados pequenos e determinísticos (sem geração aleatória), para
que a igualdade seja verificável contra um valor calculado à mão.
"""

import importlib
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pytest
from sqlalchemy import create_engine, insert

from models import Base, Conta, Lancamento

exemplo04 = importlib.import_module("04_orm_vs_batch")

VIAS_SQL = [exemplo04.via_orm_lazy, exemplo04.via_orm_eager, exemplo04.via_linhas]
IDS_SQL = ["orm_lazy", "orm_eager", "linhas"]


def montar_cenario():
    """Cenário determinístico com saldo que sobe, desce e volta a subir.

    conta 1: +100, -30, +50  -> saldos 100, 70, 120  -> pico 120
    conta 2: +10, +5         -> saldos 10, 15        -> pico 15
    conta 3: -20, +5         -> saldos -20, -15      -> pico 0 (nunca positivo)
    """
    contas = [
        {"id_conta": i, "nome": f"c{i}", "numero": str(i), "permite_lancamentos": True}
        for i in (1, 2, 3)
    ]
    valores = [
        (1, 1, "100.00", date(2025, 1, 1)),
        (2, 1, "-30.00", date(2025, 1, 2)),
        (3, 1, "50.00", date(2025, 1, 3)),
        (4, 2, "10.00", date(2025, 1, 1)),
        (5, 2, "5.00", date(2025, 1, 2)),
        (6, 3, "-20.00", date(2025, 1, 1)),
        (7, 3, "5.00", date(2025, 1, 2)),
    ]
    lancamentos = [
        {
            "id_lancamento": lid,
            "id_veiculo": 1,
            "id_conta": cid,
            "data": dt,
            "valor": Decimal(v),
            "meta": None,
            "timestamp": datetime(2026, 1, 1, 12, 0),
        }
        for lid, cid, v, dt in valores
    ]
    return contas, lancamentos


ESPERADO = {1: Decimal("120.00"), 2: Decimal("15.00"), 3: Decimal("0")}


@pytest.fixture
def engine():
    contas, lancamentos = montar_cenario()
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    with eng.begin() as conn:
        conn.execute(insert(Conta), contas)
        conn.execute(insert(Lancamento), lancamentos)
    return eng


@pytest.fixture
def tabela_lancamentos():
    _, lancamentos = montar_cenario()
    return pa.Table.from_pylist(
        lancamentos,
        schema=pa.schema(
            [
                ("id_lancamento", pa.int64()),
                ("id_veiculo", pa.int64()),
                ("id_conta", pa.int64()),
                ("data", pa.date32()),
                ("valor", pa.decimal128(12, 2)),
                ("meta", pa.string()),
                ("timestamp", pa.timestamp("us")),
            ]
        ),
    )


@pytest.mark.parametrize("via", VIAS_SQL, ids=IDS_SQL)
def test_vias_sql_batem_com_o_esperado(via, engine):
    assert via(engine) == ESPERADO


def test_via_lote_bate_com_o_esperado(tabela_lancamentos):
    assert exemplo04.via_lote(tabela_lancamentos) == ESPERADO


def test_as_quatro_concordam(engine, tabela_lancamentos):
    resultados = [via(engine) for via in VIAS_SQL]
    resultados.append(exemplo04.via_lote(tabela_lancamentos))
    assert all(r == resultados[0] for r in resultados)


def test_resultado_preserva_decimal_exato(tabela_lancamentos):
    # o saldo atravessa DuckDB e volta como Decimal, sem virar float
    valores = exemplo04.via_lote(tabela_lancamentos).values()
    assert all(isinstance(v, Decimal) for v in valores)


def test_nucleo_maior_saldo():
    # o pico é do saldo ACUMULADO, não o maior lançamento isolado
    assert exemplo04.maior_saldo([Decimal("100"), Decimal("-30"), Decimal("50")]) == Decimal("120")
    # série sempre negativa: o pico é 0 (o saldo nunca fica positivo)
    assert exemplo04.maior_saldo([Decimal("-5"), Decimal("-1")]) == Decimal("0")
    assert exemplo04.maior_saldo([]) == Decimal("0")


def test_contas_sem_lancamentos_ficam_fora(engine, tabela_lancamentos):
    # a conta 4 existe no cadastro mas não tem lançamentos: nenhuma via a inclui
    with engine.begin() as conn:
        conn.execute(
            insert(Conta),
            [{"id_conta": 4, "nome": "c4", "numero": "4", "permite_lancamentos": True}],
        )
    for via in VIAS_SQL:
        assert 4 not in via(engine)
    assert 4 not in exemplo04.via_lote(tabela_lancamentos)
