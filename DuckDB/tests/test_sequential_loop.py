"""Testes da lógica sequencial com estado do exemplo 15 (DuckDB).

Valida o núcleo: o gasto acumulado por cliente carregado entre lotes deve
bater com a soma agrupada, e o estado deve sobreviver à fronteira de lote.
"""

import importlib

import duckdb
import pyarrow as pa

exemplo = importlib.import_module("15_sequential_stateful_loop")


def processar(tabela: pa.Table, batch_size: int) -> dict[int, float]:
    """Roda o padrão do exemplo (estado entre lotes) sobre uma Table dada."""
    con = duckdb.connect()
    con.register("entrada", tabela)
    reader = con.sql(
        "SELECT customer_id, amount FROM entrada ORDER BY customer_id, ordem"
    ).to_arrow_reader(batch_size)
    running: dict[int, float] = {}
    for batch in reader:
        for cid, amt in zip(
            batch.column("customer_id").to_pylist(), batch.column("amount").to_pylist()
        ):
            running[cid] = running.get(cid, 0.0) + amt
    return running


def test_tier_thresholds():
    assert exemplo.tier(499.99) == "bronze"
    assert exemplo.tier(500.0) == "prata"
    assert exemplo.tier(1999.99) == "prata"
    assert exemplo.tier(2000.0) == "ouro"


def test_estado_sobrevive_entre_lotes():
    # 3 clientes, valores escolhidos; batch_size=2 força fronteiras de lote
    # no meio dos dados de um cliente
    tabela = pa.table(
        {
            "customer_id": pa.array([1, 1, 2, 1, 3, 2], pa.int64()),
            "ordem": pa.array([1, 2, 1, 3, 1, 2], pa.int64()),
            "amount": pa.array([100.0, 200.0, 50.0, 300.0, 10.0, 25.0], pa.float64()),
        }
    )
    running = processar(tabela, batch_size=2)
    assert running[1] == 600.0  # 100+200+300, apesar de cair em 2 lotes
    assert running[2] == 75.0
    assert running[3] == 10.0


def test_bate_com_soma_agrupada():
    import numpy as np

    rng = np.random.default_rng(0)
    n = 5000
    tabela = pa.table(
        {
            "customer_id": pa.array(rng.integers(1, 20, n), pa.int64()),
            "ordem": pa.array(np.arange(n), pa.int64()),
            "amount": pa.array(rng.uniform(1, 100, n), pa.float64()),
        }
    )
    running = processar(tabela, batch_size=512)
    con = duckdb.connect()
    con.register("entrada", tabela)
    gabarito = dict(
        con.sql("SELECT customer_id, SUM(amount) FROM entrada GROUP BY customer_id").fetchall()
    )
    assert running.keys() == gabarito.keys()
    assert all(abs(running[c] - gabarito[c]) < 1e-6 for c in running)
