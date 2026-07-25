"""Helpers compartilhados pelos exemplos: caminhos para os dados fictícios em data/raw."""

from __future__ import annotations

from pathlib import Path

import pyarrow.dataset as ds

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw"


def customers_dataset() -> ds.Dataset:
    return ds.dataset(RAW_DIR / "customers", format="parquet", partitioning="hive")


def products_dataset() -> ds.Dataset:
    return ds.dataset(RAW_DIR / "products", format="parquet")


def orders_dataset() -> ds.Dataset:
    """Dataset particionado (order_year=/order_month=) de orders, ~5.6M linhas/mês."""
    return ds.dataset(RAW_DIR / "orders", format="parquet", partitioning="hive")


def section(title: str) -> None:
    print(f"\n{'=' * 10} {title} {'=' * 10}")
