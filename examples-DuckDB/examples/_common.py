"""Helpers compartilhados: caminhos e glob patterns para os dados em data/raw."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw"
RICH_DIR = REPO_ROOT / "data" / "rich"

ORDERS_GLOB = str(RAW_DIR / "orders" / "**" / "*.parquet")
CUSTOMERS_GLOB = str(RAW_DIR / "customers" / "**" / "*.parquet")
PRODUCTS_GLOB = str(RAW_DIR / "products" / "*.parquet")


def section(title: str) -> None:
    print(f"\n{'=' * 10} {title} {'=' * 10}")
