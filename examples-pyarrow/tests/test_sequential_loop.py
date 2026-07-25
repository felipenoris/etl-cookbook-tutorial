"""Testes da lógica sequencial com estado do exemplo 11 (pyarrow)."""

import importlib

import numpy as np
import pyarrow as pa

exemplo = importlib.import_module("11_sequential_stateful_loop")


def processar(tabela: pa.Table, batch_size: int) -> dict[int, float]:
    running: dict[int, float] = {}
    for batch in tabela.to_batches(max_chunksize=batch_size):
        for cid, amt in zip(
            batch.column("customer_id").to_pylist(), batch.column("amount").to_pylist()
        ):
            running[cid] = running.get(cid, 0.0) + amt
    return running


def test_tier_thresholds():
    assert exemplo.tier(499.99) == "bronze"
    assert exemplo.tier(500.0) == "prata"
    assert exemplo.tier(2000.0) == "ouro"


def test_estado_sobrevive_entre_lotes():
    tabela = pa.table(
        {
            "customer_id": pa.array([1, 1, 2, 1], pa.int64()),
            "amount": pa.array([100.0, 200.0, 50.0, 300.0], pa.float64()),
        }
    )
    running = processar(tabela, batch_size=2)  # cliente 1 cruza a fronteira
    assert running[1] == 600.0
    assert running[2] == 50.0


def test_bate_com_group_by_sum():
    rng = np.random.default_rng(1)
    n = 5000
    tabela = pa.table(
        {
            "customer_id": pa.array(rng.integers(1, 20, n), pa.int64()),
            "amount": pa.array(rng.uniform(1, 100, n), pa.float64()),
        }
    )
    running = processar(tabela, batch_size=512)
    import pyarrow.compute as pc  # noqa: F401

    agrupado = tabela.group_by("customer_id").aggregate([("amount", "sum")])
    gabarito = dict(
        zip(agrupado.column("customer_id").to_pylist(), agrupado.column("amount_sum").to_pylist())
    )
    assert running.keys() == gabarito.keys()
    assert all(abs(running[c] - gabarito[c]) < 1e-6 for c in running)
