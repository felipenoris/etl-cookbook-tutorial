"""Helpers compartilhados pelos exemplos: caminhos e leitura com backend Arrow."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw"


def read_parquet_arrow(path: Path, **kwargs) -> pd.DataFrame:
    """Le um parquet (arquivo ou dataset particionado) com backend Arrow ponta a ponta.

    `engine="pyarrow"` faz a leitura via Arrow, e `dtype_backend="pyarrow"` faz o
    DataFrame resultante manter as colunas como `ArrowDtype` (ao inves de converter
    para numpy), evitando copias e preservando nulos/tipos com mais fidelidade.
    """
    return pd.read_parquet(path, engine="pyarrow", dtype_backend="pyarrow", **kwargs)


def load_customers() -> pd.DataFrame:
    return read_parquet_arrow(RAW_DIR / "customers")


def load_products() -> pd.DataFrame:
    return read_parquet_arrow(RAW_DIR / "products")


def load_orders(months: list[int] | None = None) -> pd.DataFrame:
    """Carrega orders. Por padrao só o primeiro mes (dataset é grande: ~5.6M linhas/mes)."""
    if months is None:
        months = [1]
    frames = [
        read_parquet_arrow(RAW_DIR / "orders" / "order_year=2025" / f"order_month={m:02d}")
        for m in months
    ]
    return pd.concat(frames, ignore_index=True)


def section(title: str) -> None:
    print(f"\n{'=' * 10} {title} {'=' * 10}")
