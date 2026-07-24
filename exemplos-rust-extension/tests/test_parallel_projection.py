"""Testes da projeção paralela de receita (`ParallelRevenueProjector` e
`BoundedRevenueProjector`).

O cálculo de referência usa a forma fechada da tabela Price
(``receita = n * parcela - principal``), que o loop mês a mês do Rust deve
reproduzir a menos de erro de ponto flutuante.
"""

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from etl_rust_ext import (
    BoundedRevenueProjector,
    ParallelRevenueProjector,
    project_revenue_batch,
)


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


class TestBoundedRevenueProjector:
    def test_writes_parquet_matching_serial(self, tmp_path):
        lotes = [
            make_contracts(range(i * 50, (i + 1) * 50), [90_000.0] * 50, [0.013] * 50, [200] * 50)
            for i in range(6)
        ]
        saida = tmp_path / "out.parquet"
        projetor = BoundedRevenueProjector(str(saida), num_workers=3, queue_depth=2)
        for lote in lotes:
            projetor.submit_batch(lote)
        caminho, linhas = projetor.finish()

        assert caminho == str(saida)
        assert linhas == 300
        # a saída é um parquet; relendo e ordenando, bate com o serial
        bounded = pq.read_table(saida).sort_by("id_contrato")
        serial = pa.concat_tables(
            [pa.table(project_revenue_batch(lote)) for lote in lotes]
        ).sort_by("id_contrato")
        assert bounded.num_rows == serial.num_rows
        assert bounded.equals(serial)

    def test_tiny_queue_does_not_deadlock(self, tmp_path):
        # fila de 1 com muitos lotes: o backpressure bloqueia e libera, mas
        # nunca trava (os workers drenam) — todas as linhas chegam ao parquet
        saida = tmp_path / "out.parquet"
        projetor = BoundedRevenueProjector(str(saida), num_workers=2, queue_depth=1)
        for i in range(20):
            projetor.submit_batch(make_contracts([i], [10_000.0], [0.01], [12]))
        _, linhas = projetor.finish()
        assert linhas == 20

    def test_config_getters(self, tmp_path):
        projetor = BoundedRevenueProjector(str(tmp_path / "o.parquet"), num_workers=4, queue_depth=8)
        assert projetor.num_workers == 4
        assert projetor.queue_depth == 8
        projetor.finish()

    def test_default_workers_and_queue(self, tmp_path):
        projetor = BoundedRevenueProjector(str(tmp_path / "o.parquet"))
        assert projetor.num_workers >= 1
        assert projetor.queue_depth == 2 * projetor.num_workers
        projetor.finish()

    def test_submit_after_finish_raises(self, tmp_path):
        projetor = BoundedRevenueProjector(str(tmp_path / "o.parquet"))
        projetor.finish()
        with pytest.raises(ValueError, match="finish"):
            projetor.submit_batch(make_contracts([1], [1000.0], [0.01], [12]))

    def test_finish_twice_raises(self, tmp_path):
        projetor = BoundedRevenueProjector(str(tmp_path / "o.parquet"))
        projetor.finish()
        with pytest.raises(ValueError, match="finish"):
            projetor.finish()

    def test_invalid_output_path_raises(self):
        with pytest.raises(RuntimeError, match="criar"):
            BoundedRevenueProjector("/caminho/inexistente/xyz/o.parquet")

    def test_invalid_batch_fails_on_submit(self, tmp_path):
        projetor = BoundedRevenueProjector(str(tmp_path / "o.parquet"))
        with pytest.raises(ValueError, match="prazo_meses"):
            projetor.submit_batch(
                pa.record_batch(
                    {
                        "id_contrato": pa.array([1], type=pa.int64()),
                        "principal": pa.array([1.0], type=pa.float64()),
                        "taxa_mensal": pa.array([0.01], type=pa.float64()),
                    }
                )
            )
        projetor.finish()
