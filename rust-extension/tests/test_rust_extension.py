"""Testes unitários das funções da extensão Rust (`etl_rust_ext`).

Exercita as duas funções expostas por `src/lib.rs` com RecordBatches pequenos
construídos em memória, validando o contrato de entrada/saída (schema, tipos,
valores calculados, propagação de nulos e erros para colunas ausentes).
"""

import pyarrow as pa
import pytest

from etl_rust_ext import add_line_total, compute_customer_running_spend


def make_batch(**columns) -> pa.RecordBatch:
    return pa.record_batch(columns)


class TestAddLineTotal:
    def test_computes_quantity_times_unit_price(self):
        batch = make_batch(
            quantity=pa.array([1, 2, 3], type=pa.int32()),
            unit_price=pa.array([10.0, 20.0, 5.0], type=pa.float64()),
        )
        out = add_line_total(batch)
        assert isinstance(out, pa.RecordBatch)
        assert out.column("line_total").to_pylist() == [10.0, 40.0, 15.0]

    def test_preserves_input_columns(self):
        batch = make_batch(
            quantity=pa.array([4], type=pa.int32()),
            unit_price=pa.array([2.5], type=pa.float64()),
            extra=pa.array(["mantida"], type=pa.string()),
        )
        out = add_line_total(batch)
        assert out.schema.names == ["quantity", "unit_price", "extra", "line_total"]
        assert out.column("extra").to_pylist() == ["mantida"]

    def test_null_input_propagates_to_null_output(self):
        batch = make_batch(
            quantity=pa.array([1, None], type=pa.int32()),
            unit_price=pa.array([10.0, 20.0], type=pa.float64()),
        )
        out = add_line_total(batch)
        assert out.column("line_total").to_pylist() == [10.0, None]

    def test_missing_column_raises(self):
        batch = make_batch(quantity=pa.array([1], type=pa.int32()))
        with pytest.raises(ValueError, match="unit_price"):
            add_line_total(batch)


class TestComputeCustomerRunningSpend:
    def test_accumulates_per_customer(self):
        batch = make_batch(
            customer_id=pa.array([1, 1, 2, 1], type=pa.int64()),
            amount=pa.array([100.0, 450.0, 900.0, 1200.0], type=pa.float64()),
        )
        out = compute_customer_running_spend(batch)
        assert out.column("cumulative_spend").to_pylist() == [100.0, 550.0, 900.0, 1750.0]

    def test_tier_thresholds(self):
        # bronze < 500, prata < 2000, ouro >= 2000 (sobre o acumulado)
        batch = make_batch(
            customer_id=pa.array([1, 1, 1], type=pa.int64()),
            amount=pa.array([499.0, 1500.0, 1.0], type=pa.float64()),
        )
        out = compute_customer_running_spend(batch)
        assert out.column("customer_tier").to_pylist() == ["bronze", "prata", "ouro"]

    def test_customers_are_independent(self):
        batch = make_batch(
            customer_id=pa.array([1, 2, 1, 2], type=pa.int64()),
            amount=pa.array([100.0, 300.0, 100.0, 300.0], type=pa.float64()),
        )
        out = compute_customer_running_spend(batch)
        assert out.column("cumulative_spend").to_pylist() == [100.0, 300.0, 200.0, 600.0]

    def test_null_rows_yield_null_outputs(self):
        batch = make_batch(
            customer_id=pa.array([1, None, 1], type=pa.int64()),
            amount=pa.array([100.0, 50.0, 100.0], type=pa.float64()),
        )
        out = compute_customer_running_spend(batch)
        assert out.column("cumulative_spend").to_pylist() == [100.0, None, 200.0]
        assert out.column("customer_tier").to_pylist() == ["bronze", None, "bronze"]

    def test_missing_column_raises(self):
        batch = make_batch(customer_id=pa.array([1], type=pa.int64()))
        with pytest.raises(ValueError, match="amount"):
            compute_customer_running_spend(batch)

    def test_zero_copy_roundtrip_returns_real_pyarrow_batch(self):
        batch = make_batch(
            customer_id=pa.array([7], type=pa.int64()),
            amount=pa.array([10.0], type=pa.float64()),
        )
        out = compute_customer_running_spend(batch)
        # a saída é um pyarrow.RecordBatch de verdade, utilizável no restante do pipeline
        assert isinstance(out, pa.RecordBatch)
        assert pa.Table.from_batches([out]).num_rows == 1
