"""Exemplo 13 — Arrow IPC (Feather) vs Parquet: dois jeitos de pôr Arrow em disco.

Todo o resto do projeto grava em **Parquet**. Mas o Arrow tem um segundo
formato de arquivo, o **IPC** (*Inter-Process Communication*) — o mesmo que o
`Feather V2`. Os dois guardam uma tabela Arrow em disco, com filosofias
opostas, e a pergunta prática ("qual usar?") quase nunca é respondida. Este
exemplo mede a diferença sobre os dados reais.

A distinção em uma frase: **Parquet é um formato de _armazenamento_; IPC é o
formato de _memória_ do Arrow, salvo como está.**

- **Parquet** codifica e comprime (dictionary, RLE, zstd...), guarda
  estatísticas por row group e permite *pushdown* de projeção/predicado e
  particionamento. Fica **pequeno** no disco e é o padrão de intercâmbio e de
  lago de dados — mas **ler exige decodificar**: materializa (aloca) a tabela
  inteira na RAM.
- **IPC/Feather** grava os buffers Arrow **exatamente** como estão na memória.
  Não há decodificação: com `memory_map`, a "leitura" é **zero-copy** — a
  `Table` aponta direto para as páginas do arquivo mapeadas pelo SO, sem alocar
  nem copiar. Em troca, o arquivo é **maior** (sem a compressão do Parquet).

O eixo do trade-off, então, é **tamanho em disco × velocidade/zero-copy de
carga**. Este exemplo comprova os dois lados:

1. **Tamanho**: Parquet (zstd) vs IPC sem compressão vs IPC com zstd;
2. **Velocidade de carga** para `Table`, cronometrada nas quatro formas;
3. **Prova do zero-copy**: `pa.total_allocated_bytes()` não se mexe ao mapear
   um IPC sem compressão (a tabela referencia o `mmap`), mas salta ao ler o
   Parquet (que aloca tudo);
4. **A nuance da compressão**: comprimir o IPC recupera espaço mas **destrói o
   zero-copy** — volta a decodificar e alocar, como o Parquet;
5. **Arquivo vs stream** de IPC (acesso aleatório com rodapé × sequencial).

Fonte: um subconjunto real de `orders` (mês 1). Tudo em diretório temporário.

Rode com: `uv run examples/13_ipc_feather_vs_parquet.py`
"""

import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from _common import orders_dataset, section

N_LINHAS = 2_000_000  # subconjunto para o exemplo rodar rápido


def cronometrar(fn, rodadas=7) -> float:
    """Melhor de N execuções, em ms (o melhor tempo é o menos contaminado por ruído)."""
    melhor = float("inf")
    for _ in range(rodadas):
        inicio = time.perf_counter()
        fn()
        melhor = min(melhor, time.perf_counter() - inicio)
    return melhor * 1000


def ler_ipc_mmap(caminho: Path) -> pa.Table:
    """Abre o IPC via memory_map: a Table referencia as páginas do arquivo (zero-copy)."""
    with pa.ipc.open_file(pa.memory_map(str(caminho), "r")) as reader:
        return reader.read_all()


if __name__ == "__main__":
    tabela = orders_dataset().to_table(
        columns=["order_id", "customer_id", "product_id", "quantity", "order_date", "status"],
        filter=(pc.field("order_month") == 1),
    ).slice(0, N_LINHAS)
    tmp = Path(tempfile.mkdtemp(prefix="ipc_vs_parquet_"))
    print(f"{tabela.num_rows:,} linhas · {tabela.num_columns} colunas")

    f_pq = tmp / "orders.parquet"
    f_ipc = tmp / "orders.arrow"          # IPC file, SEM compressão (para o zero-copy)
    f_ipc_z = tmp / "orders_zstd.arrow"   # IPC file, COM compressão zstd

    pq.write_table(tabela, f_pq, compression="zstd")
    # pa.ipc.new_file: o formato de ARQUIVO IPC (com rodapé/índice = acesso aleatório).
    # options=None => sem compressão, que é o que preserva o mmap zero-copy.
    with pa.ipc.new_file(f_ipc, tabela.schema) as w:
        w.write_table(tabela)
    with pa.ipc.new_file(
        f_ipc_z, tabela.schema, options=pa.ipc.IpcWriteOptions(compression="zstd")
    ) as w:
        w.write_table(tabela)

    # ========================================================================
    section("1) Tamanho em disco: Parquet comprime; IPC guarda a memória crua")
    tam = {
        "Parquet (zstd)": f_pq.stat().st_size,
        "IPC sem compressão": f_ipc.stat().st_size,
        "IPC (zstd)": f_ipc_z.stat().st_size,
    }
    menor = min(tam.values())
    for rotulo, bytes_ in tam.items():
        print(f"  {rotulo:<20} {bytes_ / 1e6:6.1f} MB   ({bytes_ / menor:.1f}x o menor)")
    print("-> Parquet é o menor (codificação colunar + compressão); o IPC sem compressão")
    print("   é a memória Arrow byte-a-byte — maior, mas é o que viabiliza o zero-copy.")

    # ========================================================================
    section("2) Velocidade de carga para Table (melhor de 7)")
    t_pq = cronometrar(lambda: pq.read_table(f_pq))
    t_ipc_mmap = cronometrar(lambda: ler_ipc_mmap(f_ipc))
    t_ipc_read = cronometrar(lambda: pa.ipc.open_file(str(f_ipc)).read_all())
    t_ipc_zmmap = cronometrar(lambda: ler_ipc_mmap(f_ipc_z))
    print(f"  Parquet (zstd) read_table        : {t_pq:6.1f} ms")
    print(f"  IPC sem compressão + memory_map  : {t_ipc_mmap:6.1f} ms   <- zero-copy")
    print(f"  IPC sem compressão (sem mmap)    : {t_ipc_read:6.1f} ms")
    print(f"  IPC zstd + memory_map            : {t_ipc_zmmap:6.1f} ms   (comprimido: precisa decodificar)")
    print(f"-> o mmap do IPC sem compressão é ~{t_pq / t_ipc_mmap:.0f}x mais rápido que o Parquet:")
    print("   não decodifica nada, só mapeia o arquivo.")

    # ========================================================================
    section("3) Prova do zero-copy: quanto cada leitura ALOCA (pa.total_allocated_bytes)")
    pa.default_memory_pool().release_unused()
    base = pa.total_allocated_bytes()
    t_mmap = ler_ipc_mmap(f_ipc)          # manter a referência viva durante a medição
    delta_ipc = pa.total_allocated_bytes() - base

    pa.default_memory_pool().release_unused()
    base = pa.total_allocated_bytes()
    t_parq = pq.read_table(f_pq)          # manter a referência viva
    delta_pq = pa.total_allocated_bytes() - base

    print(f"  IPC sem compressão + mmap : {delta_ipc:>14,} bytes alocados  (a Table aponta para o mmap)")
    print(f"  Parquet (zstd) read_table : {delta_pq:>14,} bytes alocados  (materializa a tabela inteira)")
    print(f"  (as duas Tables têm {t_mmap.num_rows:,} linhas idênticas: {t_mmap.num_rows == t_parq.num_rows})")
    print("-> ~0 byte no IPC mapeado: a leitura não copiou dados. É o mesmo princípio do")
    print("   interop zero-copy entre pandas/pyarrow (exemplo 08), agora contra o DISCO.")

    # ========================================================================
    section("4) A nuance: comprimir o IPC recupera espaço, mas ANULA o zero-copy")
    pa.default_memory_pool().release_unused()
    base = pa.total_allocated_bytes()
    t_z = ler_ipc_mmap(f_ipc_z)
    delta_z = pa.total_allocated_bytes() - base
    print(f"  IPC zstd + mmap : {delta_z:>14,} bytes alocados (>0: descomprimiu para a RAM)")
    print("-> comprimir aproxima o IPC do tamanho do Parquet, mas a leitura volta a")
    print("   decodificar e alocar. Zero-copy exige IPC SEM compressão: é o preço do disco.")

    # ========================================================================
    section("5) IPC arquivo vs stream: acesso aleatório (rodapé) vs sequencial")
    f_stream = tmp / "orders.arrows"
    with pa.ipc.new_stream(f_stream, tabela.schema) as w:  # formato STREAM
        w.write_table(tabela)
    with pa.ipc.open_file(str(f_ipc)) as r:  # o formato FILE tem rodapé/índice
        print(f"  IPC file  : {r.num_record_batches} record batch(es), acesso aleatório por rodapé")
    with pa.ipc.open_stream(str(f_stream)) as r:  # o STREAM é lido do início ao fim
        total = r.read_all().num_rows
    print(f"  IPC stream: lido sequencialmente, {total:,} linhas (sem rodapé; ideal para pipe/socket)")
    print("-> use o FILE (.arrow) para ler de disco com memory_map; o STREAM (.arrows) para")
    print("   transmitir entre processos (o handoff entre etapas de um pipeline).")

    section("Quando usar cada um")
    print("- Parquet: armazenamento/intercâmbio e lago de dados — pequeno no disco, portável,")
    print("  com pushdown de projeção/predicado e particionamento (exemplos 01, 02, 12).")
    print("- IPC/Feather SEM compressão: dado intermediário 'quente', cache local, handoff")
    print("  entre processos — carga quase instantânea e zero-copy via memory_map.")
    print("- Regra: Parquet quando o gargalo é DISCO/rede; IPC quando o gargalo é")
    print("  RECARREGAR rápido a mesma tabela muitas vezes (e cabe sem compressão).")
