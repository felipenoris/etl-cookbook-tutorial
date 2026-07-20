"""Testes do interop pyarrow <-> pandas (exemplos 08 e 09).

Valida os contratos que o padrão híbrido assume: conversão zero-copy com
backend Arrow, roundtrip fiel, degradação do backend numpy, streaming por
lotes com escrita incremental e recarga idempotente de partição.
"""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from _common import orders_dataset


@pytest.fixture(scope="module")
def table() -> pa.Table:
    return orders_dataset().to_table(filter=pc.field("order_month") == 1).combine_chunks()


def test_to_pandas_arrow_backend_is_zero_copy(table):
    df = table.to_pandas(types_mapper=pd.ArrowDtype)
    buffer_tabela = table["quantity"].chunks[0].buffers()[1].address
    buffer_df = df["quantity"].array._pa_array.chunks[0].buffers()[1].address
    assert buffer_tabela == buffer_df


def test_roundtrip_preserves_schema_and_data(table):
    df = table.to_pandas(types_mapper=pd.ArrowDtype)
    reconvertida = pa.Table.from_pandas(df, preserve_index=False)
    assert reconvertida.schema.equals(table.schema)
    assert reconvertida.equals(table)


def test_numpy_backend_degrades_nullable_int():
    com_nulo = pa.table({"qtd": pa.array([1, None, 3], type=pa.int32())})
    assert str(com_nulo.to_pandas()["qtd"].dtype) == "float64"  # int virou float
    assert str(com_nulo.to_pandas(types_mapper=pd.ArrowDtype)["qtd"].dtype) == "int32[pyarrow]"


def test_preserve_index_false_drops_phantom_column():
    df = pd.DataFrame({"a": [1, 2]}, index=pd.Index([10, 20], name="idx"))
    assert "idx" in pa.Table.from_pandas(df).column_names
    assert pa.Table.from_pandas(df, preserve_index=False).column_names == ["a"]


def test_streaming_batches_keep_memory_bounded_and_count_all_rows(tmp_path: Path):
    dataset = orders_dataset()
    janeiro = pc.field("order_month") == 1
    esperado = dataset.count_rows(filter=janeiro)

    writer = None
    total = 0
    for batch in dataset.to_batches(filter=janeiro, batch_size=500_000):
        assert batch.num_rows <= 500_000  # memória limitada pelo tamanho do lote
        df = batch.to_pandas(types_mapper=pd.ArrowDtype)
        out = pa.Table.from_pandas(df.assign(dobro=df["quantity"] * 2), preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(tmp_path / "saida.parquet", out.schema)
        writer.write_table(out)
        total += batch.num_rows
    writer.close()

    assert total == esperado
    assert pq.read_metadata(tmp_path / "saida.parquet").num_rows == esperado


def test_write_dataset_delete_matching_is_idempotent(tmp_path: Path):
    tabela = pa.table({"grupo": ["a", "a", "b"], "valor": [1, 2, 3]})
    particao = ds.partitioning(pa.schema([("grupo", pa.string())]), flavor="hive")
    for _ in range(2):  # duas rodadas: substitui, não duplica
        ds.write_dataset(
            tabela,
            tmp_path / "out",
            format="parquet",
            partitioning=particao,
            existing_data_behavior="delete_matching",
        )
    assert ds.dataset(tmp_path / "out", partitioning="hive").count_rows() == 3
