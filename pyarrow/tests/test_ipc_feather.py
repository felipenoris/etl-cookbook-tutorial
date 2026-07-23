"""Testes de contrato do exemplo 13 (Arrow IPC/Feather vs Parquet).

Valida o roundtrip fiel do IPC, o zero-copy do memory_map (a leitura não aloca)
em contraste com o Parquet (que materializa), e que comprimir o IPC anula o
zero-copy.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from _common import orders_dataset
import pyarrow.compute as pc


@pytest.fixture(scope="module")
def tabela():
    return orders_dataset().to_table(
        columns=["order_id", "customer_id", "product_id", "quantity"],
        filter=(pc.field("order_month") == 1),
    ).slice(0, 500_000)


def _escreve_ipc(caminho, tabela, compression=None):
    opts = pa.ipc.IpcWriteOptions(compression=compression) if compression else None
    with pa.ipc.new_file(caminho, tabela.schema, options=opts) as w:
        w.write_table(tabela)


def _le_ipc_mmap(caminho):
    with pa.ipc.open_file(pa.memory_map(str(caminho), "r")) as r:
        return r.read_all()


def test_ipc_roundtrip_is_faithful(tabela, tmp_path):
    f = tmp_path / "d.arrow"
    _escreve_ipc(f, tabela)
    assert _le_ipc_mmap(f).equals(tabela)


def test_uncompressed_ipc_mmap_read_is_zero_copy(tabela, tmp_path):
    f = tmp_path / "d.arrow"
    _escreve_ipc(f, tabela)
    pa.default_memory_pool().release_unused()
    base = pa.total_allocated_bytes()
    t = _le_ipc_mmap(f)                       # manter a referência viva
    delta = pa.total_allocated_bytes() - base
    assert t.num_rows == tabela.num_rows
    assert delta == 0                          # a Table aponta para o mmap; nada foi alocado


def test_parquet_read_allocates(tabela, tmp_path):
    f = tmp_path / "d.parquet"
    pq.write_table(tabela, f, compression="zstd")
    pa.default_memory_pool().release_unused()
    base = pa.total_allocated_bytes()
    t = pq.read_table(f)                       # manter a referência viva
    delta = pa.total_allocated_bytes() - base
    assert t.num_rows == tabela.num_rows
    assert delta > 0                           # ler parquet materializa/decodifica a tabela


def test_compressing_ipc_breaks_zero_copy(tabela, tmp_path):
    f = tmp_path / "dz.arrow"
    _escreve_ipc(f, tabela, compression="zstd")
    pa.default_memory_pool().release_unused()
    base = pa.total_allocated_bytes()
    t = _le_ipc_mmap(f)                         # manter a referência viva
    delta = pa.total_allocated_bytes() - base
    assert t.num_rows == tabela.num_rows
    assert delta > 0                           # comprimido: precisa descomprimir para a RAM


def test_parquet_is_smaller_than_uncompressed_ipc(tabela, tmp_path):
    f_pq = tmp_path / "d.parquet"
    f_ipc = tmp_path / "d.arrow"
    pq.write_table(tabela, f_pq, compression="zstd")
    _escreve_ipc(f_ipc, tabela)
    assert f_pq.stat().st_size < f_ipc.stat().st_size  # Parquet comprime; IPC é a memória crua
