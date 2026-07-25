"""Testes das etapas do pipeline `run_etl.py`.

As funções puras (`project_for_rust`, `enrich_with_rust`, `summarize_with_pandas`)
são testadas com dados pequenos em memória. O pipeline completo sobre
`data/raw` (~33.7M linhas) roda em um único teste marcado como `slow` —
deselecione com `uv run pytest -m "not slow"` para uma rodada rápida.
"""

import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import run_etl


def make_joined_table() -> pa.Table:
    """Tabela mínima com o mesmo schema que sai de `extract_and_join_with_duckdb`."""
    return pa.table(
        {
            "order_id": pa.array([1, 2, 3], type=pa.int64()),
            "customer_id": pa.array([10, 10, 20], type=pa.int64()),
            "order_date": pa.array(["2025-01-01", "2025-01-02", "2025-01-01"]).cast("date32"),
            "quantity": pa.array([1, 2, 3], type=pa.int32()),
            "unit_price": pa.array([100.0, 300.0, 50.0], type=pa.float64()),
            "amount": pa.array([100.0, 600.0, 150.0], type=pa.float64()),
            "region": pa.array(["sul", "sul", "norte"], type=pa.string()),
            "category": pa.array(["livros", "casa", "livros"], type=pa.string()),
        }
    )


def test_project_for_rust_selects_and_combines():
    batch = run_etl.project_for_rust(make_joined_table())
    assert isinstance(batch, pa.RecordBatch)
    assert batch.schema.names == [
        "order_id",
        "customer_id",
        "order_date",
        "amount",
        "region",
        "category",
    ]
    assert batch.num_rows == 3


def test_enrich_with_rust_appends_metric_columns():
    batch = run_etl.project_for_rust(make_joined_table())
    enriched = run_etl.enrich_with_rust(batch)
    assert isinstance(enriched, pa.Table)
    assert "cumulative_spend" in enriched.schema.names
    assert "customer_tier" in enriched.schema.names
    assert enriched["cumulative_spend"].to_pylist() == [100.0, 700.0, 150.0]
    assert enriched["customer_tier"].to_pylist() == ["bronze", "prata", "bronze"]


def test_summarize_with_pandas_groups_by_tier():
    enriched = run_etl.enrich_with_rust(run_etl.project_for_rust(make_joined_table()))
    summary = run_etl.summarize_with_pandas(enriched)
    assert isinstance(summary, pd.DataFrame)
    assert set(summary.index) == {"bronze", "prata"}
    assert summary.loc["bronze", "total_pedidos"] == 2
    assert summary.loc["bronze", "clientes_distintos"] == 2
    assert summary["receita_total"].sum() == 850.0


def test_write_rich_output_partitions_by_tier(tmp_path, monkeypatch):
    monkeypatch.setattr(run_etl, "RICH_DIR", tmp_path)
    enriched = run_etl.enrich_with_rust(run_etl.project_for_rust(make_joined_table()))
    run_etl.write_rich_output(enriched)
    partitions = sorted(p.name for p in (tmp_path / "order_metrics").iterdir())
    assert partitions == ["customer_tier=bronze", "customer_tier=prata"]


@pytest.mark.slow
def test_full_pipeline_against_raw_data(tmp_path, monkeypatch):
    """Roda o ETL inteiro sobre data/raw e valida o resultado gravado."""
    monkeypatch.setattr(run_etl, "RICH_DIR", tmp_path)
    run_etl.main()

    import pyarrow.dataset as ds

    result = ds.dataset(tmp_path / "order_metrics", format="parquet", partitioning="hive")
    assert result.count_rows() == 33_769_710
    assert "cumulative_spend" in result.schema.names
    assert "customer_tier" in result.schema.names

    # max_rows_per_file divide partições grandes em múltiplos part-{i}.parquet:
    # "ouro" (~33.7M linhas) precisa de vários arquivos, as demais cabem em um
    ouro_parts = list((tmp_path / "order_metrics" / "customer_tier=ouro").glob("*.parquet"))
    assert len(ouro_parts) >= 2
