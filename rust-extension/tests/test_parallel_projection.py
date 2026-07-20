"""Testes da projeção paralela de receita (`ParallelRevenueProjector`).

O cálculo de referência usa a forma fechada da tabela Price
(``receita = n * parcela - principal``), que o loop mês a mês do Rust deve
reproduzir a menos de erro de ponto flutuante.
"""

import pyarrow as pa
import pytest

from etl_rust_ext import ParallelRevenueProjector, project_revenue_batch


def make_contracts(ids, principals, rates, months) -> pa.RecordBatch:
    return pa.record_batch(
        {
            "id_contrato": pa.array(ids, type=pa.int64()),
            "principal": pa.array(principals, type=pa.float64()),
            "taxa_mensal": pa.array(rates, type=pa.float64()),
            "prazo_meses": pa.array(months, type=pa.int32()),
        }
    )


def receita_forma_fechada(principal: float, taxa: float, prazo: int) -> float:
    fator = (1.0 + taxa) ** prazo
    parcela = principal * taxa * fator / (fator - 1.0)
    return prazo * parcela - principal


class TestProjectRevenueBatch:
    def test_matches_closed_form(self):
        batch = make_contracts([1, 2], [100_000.0, 250_000.0], [0.01, 0.015], [120, 240])
        out = project_revenue_batch(batch)
        esperado = [
            receita_forma_fechada(100_000.0, 0.01, 120),
            receita_forma_fechada(250_000.0, 0.015, 240),
        ]
        assert out.column("id_contrato").to_pylist() == [1, 2]
        for calculado, referencia in zip(out.column("receita_projetada").to_pylist(), esperado):
            assert calculado == pytest.approx(referencia, rel=1e-9)

    def test_degenerate_contracts_yield_zero(self):
        batch = make_contracts([1, 2], [100_000.0, 100_000.0], [0.0, 0.01], [120, 0])
        out = project_revenue_batch(batch)
        assert out.column("receita_projetada").to_pylist() == [0.0, 0.0]

    def test_missing_column_raises(self):
        batch = pa.record_batch({"id_contrato": pa.array([1], type=pa.int64())})
        with pytest.raises(ValueError, match="principal"):
            project_revenue_batch(batch)

    def test_wrong_type_raises(self):
        batch = pa.record_batch(
            {
                "id_contrato": pa.array([1], type=pa.int64()),
                "principal": pa.array([1], type=pa.int32()),  # deveria ser float64
                "taxa_mensal": pa.array([0.01], type=pa.float64()),
                "prazo_meses": pa.array([12], type=pa.int32()),
            }
        )
        with pytest.raises(ValueError, match="principal"):
            project_revenue_batch(batch)

    def test_null_raises(self):
        batch = make_contracts([1, 2], [100_000.0, None], [0.01, 0.01], [12, 12])
        with pytest.raises(ValueError, match="nulo"):
            project_revenue_batch(batch)


class TestParallelRevenueProjector:
    def test_consolidates_batches_in_submission_order(self):
        projetor = ParallelRevenueProjector()
        assert projetor.submit_batch(make_contracts([1], [100_000.0], [0.01], [12])) == 1
        assert projetor.submit_batch(make_contracts([2, 3], [50_000.0, 80_000.0], [0.02, 0.01], [24, 36])) == 2
        out = projetor.collect()
        assert out.num_rows == 3
        assert out.column("id_contrato").to_pylist() == [1, 2, 3]

    def test_matches_serial_function_exactly(self):
        lotes = [
            make_contracts(range(i * 100, (i + 1) * 100), [120_000.0] * 100, [0.012] * 100, [180] * 100)
            for i in range(4)
        ]
        projetor = ParallelRevenueProjector()
        for lote in lotes:
            projetor.submit_batch(lote)
        paralelo = pa.Table.from_batches([projetor.collect()])
        serial = pa.Table.from_batches([project_revenue_batch(lote) for lote in lotes])
        # mesmo código de cálculo nos dois caminhos: igualdade exata, não aproximada
        assert paralelo.equals(serial)

    def test_collect_without_batches_returns_empty(self):
        out = ParallelRevenueProjector().collect()
        assert out.num_rows == 0
        assert out.schema.names == ["id_contrato", "receita_projetada"]

    def test_submit_after_collect_raises(self):
        projetor = ParallelRevenueProjector()
        projetor.collect()
        with pytest.raises(ValueError, match="collect"):
            projetor.submit_batch(make_contracts([1], [1000.0], [0.01], [12]))

    def test_invalid_batch_fails_on_submit_not_on_collect(self):
        projetor = ParallelRevenueProjector()
        with pytest.raises(ValueError, match="taxa_mensal"):
            projetor.submit_batch(
                pa.record_batch(
                    {
                        "id_contrato": pa.array([1], type=pa.int64()),
                        "principal": pa.array([1.0], type=pa.float64()),
                    }
                )
            )
        assert projetor.batches_submitted() == 0

    def test_batches_submitted_survives_collect(self):
        projetor = ParallelRevenueProjector()
        projetor.submit_batch(make_contracts([1], [1000.0], [0.01], [12]))
        projetor.collect()
        assert projetor.batches_submitted() == 1
