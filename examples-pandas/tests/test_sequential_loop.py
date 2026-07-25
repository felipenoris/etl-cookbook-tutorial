"""Testes da lógica sequencial com estado do exemplo 10 (pandas)."""

import importlib

import numpy as np
import pandas as pd

exemplo = importlib.import_module("10_sequential_stateful_loop")

BATCH = 512


def processar(df: pd.DataFrame) -> dict[int, float]:
    running: dict[int, float] = {}
    for inicio in range(0, len(df), BATCH):
        for linha in df.iloc[inicio : inicio + BATCH].itertuples(index=False):
            running[linha.customer_id] = running.get(linha.customer_id, 0.0) + linha.amount
    return running


def test_tier_thresholds():
    assert exemplo.tier(499.99) == "bronze"
    assert exemplo.tier(500.0) == "prata"
    assert exemplo.tier(2000.0) == "ouro"


def test_estado_sobrevive_entre_lotes():
    df = pd.DataFrame({"customer_id": [1, 1, 2, 1], "amount": [100.0, 200.0, 50.0, 300.0]})
    running = processar(df)
    assert running[1] == 600.0
    assert running[2] == 50.0


def test_bate_com_groupby_cumsum():
    rng = np.random.default_rng(2)
    n = 5000
    df = pd.DataFrame(
        {
            "customer_id": rng.integers(1, 20, n),
            "amount": rng.uniform(1, 100, n),
        }
    )
    running = processar(df)
    # o total final por cliente == o último valor do cumsum vetorizado
    gabarito = df.groupby("customer_id")["amount"].sum().to_dict()
    assert running.keys() == gabarito.keys()
    assert all(abs(running[c] - gabarito[c]) < 1e-6 for c in running)
