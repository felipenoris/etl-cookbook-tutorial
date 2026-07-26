"""Testes do pipeline em RecordBatch do exemplo 24 (DuckDB).

Valida os contratos que o exemplo assume: o lote de saída estende o de entrada
sem copiar os buffers originais, o estado sobrevive à fronteira entre lotes, e
o reader enriquecido é preguiçoso e consumível pelo DuckDB.
"""

import importlib

import duckdb
import pyarrow as pa

exemplo = importlib.import_module("24_record_batch_pipeline")


def lote_linhas(quantity, unit_price) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        [
            pa.array(list(range(len(quantity))), pa.int64()),
            pa.array(quantity, pa.int32()),
            pa.array(unit_price, pa.float64()),
        ],
        names=["order_id", "quantity", "unit_price"],
    )


# --- add_line_total: o gêmeo do `add_line_total` do Rust --------------------


def test_line_total_estende_o_schema():
    entrada = lote_linhas([2, 3], [10.0, 4.5])
    saida = exemplo.add_line_total(entrada)
    assert saida.schema.names == ["order_id", "quantity", "unit_price", "line_total"]
    assert saida.schema.field("line_total").type == pa.float64()
    assert saida.num_rows == entrada.num_rows
    assert saida.column("line_total").to_pylist() == [20.0, 13.5]


def test_line_total_reaproveita_os_buffers_de_entrada():
    """O ponto do exemplo: só a coluna nova é alocada (clone de Arc no Rust)."""
    entrada = lote_linhas([2, 3], [10.0, 4.5])
    saida = exemplo.add_line_total(entrada)
    for coluna in ("order_id", "quantity", "unit_price"):
        assert (
            entrada.column(coluna).buffers()[1].address
            == saida.column(coluna).buffers()[1].address
        )


def test_line_total_propaga_nulos():
    entrada = lote_linhas([2, None], [10.0, 4.5])
    saida = exemplo.add_line_total(entrada)
    assert saida.column("line_total").to_pylist() == [20.0, None]


# --- add_running_spend: o gêmeo do `compute_customer_running_spend` ---------


def lote_acumulado(customer_id, amount) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        [pa.array(customer_id, pa.int64()), pa.array(amount, pa.float64())],
        names=["customer_id", "amount"],
    )


def test_running_spend_acumula_por_cliente():
    running: dict[int, float] = {}
    saida = exemplo.add_running_spend(
        lote_acumulado([1, 1, 2], [100.0, 200.0, 50.0]), running, prata=150.0, ouro=250.0
    )
    assert saida.column("cumulative_spend").to_pylist() == [100.0, 300.0, 50.0]
    assert saida.column("customer_tier").to_pylist() == ["bronze", "ouro", "bronze"]
    assert running == {1: 300.0, 2: 50.0}


def test_running_spend_atravessa_a_fronteira_de_lote():
    """O dicionário vem de fora justamente para sobreviver entre os lotes."""
    running: dict[int, float] = {}
    exemplo.add_running_spend(lote_acumulado([1, 2], [100.0, 10.0]), running)
    segundo = exemplo.add_running_spend(lote_acumulado([1, 2], [400.0, 5.0]), running)
    # o cliente 1 continua de onde parou no lote anterior
    assert segundo.column("cumulative_spend").to_pylist() == [500.0, 15.0]
    assert running == {1: 500.0, 2: 15.0}


def test_running_spend_thresholds():
    running: dict[int, float] = {}
    saida = exemplo.add_running_spend(
        lote_acumulado([1, 2, 3], [499.99, 500.0, 2000.0]), running, prata=500.0, ouro=2000.0
    )
    assert saida.column("customer_tier").to_pylist() == ["bronze", "prata", "ouro"]


def test_running_spend_nulo_nao_avanca_o_acumulado():
    running: dict[int, float] = {}
    saida = exemplo.add_running_spend(lote_acumulado([1, None, 1], [100.0, 5.0, 50.0]), running)
    assert saida.column("cumulative_spend").to_pylist() == [100.0, None, 150.0]
    assert saida.column("customer_tier").to_pylist()[1] is None
    assert running == {1: 150.0}  # a linha nula não entrou na conta


# --- stream_enriquecido: o reader de saída ---------------------------------


def reader_de(batches: list[pa.RecordBatch]) -> pa.RecordBatchReader:
    return pa.RecordBatchReader.from_batches(batches[0].schema, iter(batches))


def test_stream_expoe_o_schema_ja_enriquecido():
    entrada = reader_de([lote_linhas([1], [2.0]), lote_linhas([3], [4.0])])
    saida = exemplo.stream_enriquecido(entrada, exemplo.add_line_total)
    assert saida.schema.names == ["order_id", "quantity", "unit_price", "line_total"]


def test_stream_e_preguicoso():
    """Só o primeiro lote é processado na chamada; o resto sai sob demanda."""
    chamadas = []

    def contar(batch):
        chamadas.append(batch.num_rows)
        return exemplo.add_line_total(batch)

    entrada = reader_de([lote_linhas([1], [2.0]), lote_linhas([3], [4.0]), lote_linhas([5], [6.0])])
    saida = exemplo.stream_enriquecido(entrada, contar)
    # o primeiro lote é consumido para descobrir o schema de saída; os outros não
    assert len(chamadas) == 1
    lotes = list(saida)
    assert len(chamadas) == 3
    assert len(lotes) == 3


def test_stream_vazio_preserva_o_schema_de_entrada():
    entrada = pa.RecordBatchReader.from_batches(lote_linhas([], []).schema, iter(()))
    saida = exemplo.stream_enriquecido(entrada, exemplo.add_line_total)
    assert list(saida) == []
    assert saida.schema.names == ["order_id", "quantity", "unit_price"]


def test_duckdb_consome_o_reader_enriquecido():
    """O fecho do exemplo: o reader do Python vira tabela para o DuckDB."""
    entrada = reader_de([lote_linhas([2, 3], [10.0, 4.5]), lote_linhas([4], [1.0])])
    # a variável local `enriquecido` é o que o replacement scan enxerga no SQL
    enriquecido = exemplo.stream_enriquecido(entrada, exemplo.add_line_total)
    con = duckdb.connect()
    linhas, receita = con.sql(
        "SELECT count(*), sum(line_total) FROM enriquecido"
    ).fetchone()
    assert linhas == 3
    assert receita == 20.0 + 13.5 + 4.0
