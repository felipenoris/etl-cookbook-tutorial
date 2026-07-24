r"""Exemplo de multithreading: projeção de receita de contratos em paralelo no Rust.

Duas estratégias de paralelismo, com uma diferença crucial de **memória**. O
cálculo por contrato (juros da tabela Price, simulados mês a mês) é
independente entre contratos — o caso ideal para paralelizar — e as duas
estratégias produzem o mesmo resultado que o serial (``project_revenue_batch``).

## Bibliotecas usadas e o papel de cada uma

**Camada Python (leitura e escrita colunar):**

- [`pyarrow.dataset`](https://arrow.apache.org/docs/python/dataset.html) —
  ``Dataset.to_batches(batch_size=...)``
  ([API](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html))
  lê o parquet em *streaming*: entrega um ``RecordBatch`` por vez, mantendo o
  uso de memória do lado Python constante independentemente do tamanho do
  arquivo. É o produtor dos lotes.
- [`pyarrow.parquet`](https://arrow.apache.org/docs/python/parquet.html) —
  grava/relê os parquets de apoio.

**Camada Rust (o trabalho pesado, fora do GIL):**

- [PyO3](https://pyo3.rs/) — a ponte Python↔Rust. O
  [capítulo de paralelismo](https://pyo3.rs/latest/parallelism.html) explica o
  ``py.detach`` (antigo ``allow_threads``): **solta o GIL** enquanto o Rust
  calcula, permitindo paralelismo real de CPU — impossível com threads de
  Python puro.
- [pyo3-arrow](https://docs.rs/pyo3-arrow) — transporta os ``RecordBatch``
  entre Python e Rust **zero-copy** (via Arrow C Data Interface); passar um
  lote a um worker é mover um ``Arc`` (ponteiro), não copiar dados.
- [`std::thread`](https://doc.rust-lang.org/std/thread/fn.spawn.html) — as
  threads do SO. São threads **num único processo** (mesma memória
  compartilhada), não processos separados.
- [crossbeam-channel](https://docs.rs/crossbeam-channel) — a fila **MPMC
  limitada** (multi-produtor/multi-consumidor) que conecta as threads no
  ``BoundedRevenueProjector``. A capacidade limitada é o que dá o
  *backpressure* (ver adiante).
- [`parquet::arrow::ArrowWriter`](https://docs.rs/parquet/latest/parquet/arrow/arrow_writer/struct.ArrowWriter.html)
  — escreve o parquet de saída **incrementalmente**, um lote por vez, direto
  do Rust.

## A) ``ParallelRevenueProjector`` — simples, sem limite de memória

```mermaid
flowchart LR
    P["Python\nto_batches (streaming)"]
    subgraph MEM["residente em memória (SEM limite)"]
        direction TB
        T1["thread lote 1"]
        T2["thread lote 2"]
        T3["thread lote 3"]
        Tn["thread lote N..."]
    end
    C["collect()\nconcatena TUDO\nem 1 RecordBatch"]
    P -->|"submit_batch\n(retorna na hora,\nNUNCA bloqueia)"| T1
    P --> T2
    P --> T3
    P --> Tn
    T1 --> C
    T2 --> C
    T3 --> C
    Tn --> C
    C --> R["RecordBatch consolidado\n(saída inteira na RAM)"]
```

O ``submit_batch`` dispara uma thread por lote e **retorna imediatamente, sem
nunca bloquear**. Se o Python lê mais rápido que os workers processam (o caso
comum), os lotes "em voo" se acumulam **sem limite** — no pior caso, todos os
lotes da base ficam residentes ao mesmo tempo. Além disso, o ``collect()``
concatena a saída inteira num único ``RecordBatch`` na RAM. Funciona para
bases pequenas (o subgraph fica pequeno); para bases massivas, **estoura a
memória**.

## B) ``BoundedRevenueProjector`` — memória constante

```mermaid
flowchart LR
    P["Python\nto_batches (streaming)"]
    subgraph MEM["residente em memória (LIMITADO: ~queue_depth + N lotes)"]
        direction LR
        Q(["fila de entrada\nLIMITADA\ncap = queue_depth"])
        subgraph POOL["pool FIXO de N workers"]
            direction TB
            W1["worker 1"]
            W2["worker 2"]
            Wn["worker N"]
        end
        RQ(["fila de resultados\nLIMITADA"])
    end
    WR["thread escritora\nArrowWriter"]
    D[("parquet em DISCO\ncresce fora da RAM")]
    P -->|"submit_batch\nBLOQUEIA se a fila\nestiver cheia (backpressure)"| Q
    Q --> W1
    Q --> W2
    Q --> Wn
    W1 --> RQ
    W2 --> RQ
    Wn --> RQ
    RQ --> WR
    WR -->|"escreve 1 lote por vez\n(incremental)"| D
    Q -. "fila cheia -> send bloqueia\n-> Python espera" .-> P
```

**O aspecto do diagrama que garante memória constante** é o retângulo
``MEM``: tudo que fica na RAM está *dentro* dele, e o seu tamanho é fixo,
não proporcional à base. Três elementos o mantêm limitado:

1. **A fila de entrada é LIMITADA** (`cap = queue_depth`). Quando enche, o
   ``send`` **bloqueia** — a aresta pontilhada de volta ao Python é o
   *backpressure*: o produtor é forçado a esperar os workers consumirem. Sem
   esse limite (fila ilimitada), o Python despejaria a base inteira na fila e
   o retângulo cresceria sem controle.
2. **O pool de workers é FIXO** (N threads, não uma por lote). No máximo N
   lotes estão em processamento a qualquer instante.
3. **A saída sai da RAM continuamente**: a thread escritora grava cada
   resultado no parquet em disco (``ArrowWriter``, incremental). O banco de
   dados de saída — o cilindro ``D`` — está **fora** do retângulo ``MEM``:
   ele cresce em disco, não na memória. ``finish()`` devolve só
   ``(caminho, linhas)``, nunca a base.

Resultado: o pico de memória é ``≈ queue_depth lotes de entrada + N em
processamento + buffer de resultados``, **constante independentemente do
tamanho da base** — é o que evita o estouro de memória do caminho A.

Rode com: ``uv run run_contracts_parallel.py`` (a partir de ``rust-extension``).
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from etl_rust_ext import (
    BoundedRevenueProjector,
    ParallelRevenueProjector,
    project_revenue_batch,
)

NUM_CONTRATOS = 1_600_000
BATCH_SIZE = 200_000
RNG_SEED = 7


def gerar_contratos_parquet(destino: Path) -> None:
    """Gera a fonte de dados fictícia: um parquet com os contratos.

    Colunas: ``id_contrato`` (int64), ``principal`` (float64),
    ``taxa_mensal`` (float64) e ``prazo_meses`` (int32).
    """
    rng = np.random.default_rng(RNG_SEED)
    tabela = pa.table(
        {
            "id_contrato": np.arange(1, NUM_CONTRATOS + 1, dtype=np.int64),
            "principal": np.round(rng.uniform(10_000, 500_000, NUM_CONTRATOS), 2),
            "taxa_mensal": np.round(rng.uniform(0.008, 0.025, NUM_CONTRATOS), 5),
            "prazo_meses": rng.integers(60, 361, NUM_CONTRATOS, dtype=np.int32),
        }
    )
    pq.write_table(tabela, destino, row_group_size=BATCH_SIZE)
    print(f"[fonte] {NUM_CONTRATOS:,} contratos em {destino.name} "
          f"({destino.stat().st_size / (1024 * 1024):.1f}MB)")


def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="contratos_"))
    fonte = workdir / "contratos.parquet"
    gerar_contratos_parquet(fonte)
    dataset = ds.dataset(fonte, format="parquet")

    # --- linha de base: mesmo cálculo, um lote por vez, sequencial ----------
    print("\n[serial] processando lote a lote com project_revenue_batch...")
    inicio = time.perf_counter()
    resultados_serial = [
        project_revenue_batch(batch)
        for batch in dataset.to_batches(batch_size=BATCH_SIZE)
    ]
    t_serial = time.perf_counter() - inicio
    serial = pa.Table.from_batches(resultados_serial)
    print(f"[serial] {serial.num_rows:,} contratos em {t_serial:.2f}s")

    # --- paralelo: submissão serial, processamento concorrente --------------
    print("\n[paralelo] submetendo lotes ao ParallelRevenueProjector...")
    projetor = ParallelRevenueProjector()
    inicio = time.perf_counter()
    for batch in dataset.to_batches(batch_size=BATCH_SIZE):
        t_submit = time.perf_counter()
        n = projetor.submit_batch(batch)
        print(f"  lote {n} submetido em {(time.perf_counter() - t_submit) * 1000:.1f}ms "
              "(retorna na hora; o cálculo segue em background)")
    t_submissao = time.perf_counter() - inicio

    consolidado = projetor.collect()
    t_paralelo = time.perf_counter() - inicio
    print(f"[paralelo] submissão dos {projetor.batches_submitted()} lotes: {t_submissao:.2f}s")
    print(f"[paralelo] total (submissão + collect): {t_paralelo:.2f}s")

    # --- conferência e placar ------------------------------------------------
    paralelo = pa.Table.from_batches([consolidado])
    print(f"\n[check] resultados idênticos ao serial: {paralelo.equals(serial)}")
    cores = os.cpu_count()
    print(f"[placar] serial {t_serial:.2f}s vs paralelo {t_paralelo:.2f}s "
          f"-> speedup {t_serial / t_paralelo:.1f}x ({cores} CPUs na máquina)")

    # --- bounded: memória constante, saída direto em parquet -----------------
    print("\n[bounded] mesmo trabalho com BoundedRevenueProjector (fila limitada)...")
    saida = workdir / "receitas.parquet"
    projetor_b = BoundedRevenueProjector(str(saida), queue_depth=3)
    print(f"[bounded] {projetor_b.num_workers} workers | fila de {projetor_b.queue_depth} lotes "
          "(submit BLOQUEIA quando cheia -> memória limitada)")
    inicio = time.perf_counter()
    for batch in dataset.to_batches(batch_size=BATCH_SIZE):
        projetor_b.submit_batch(batch)  # bloqueia se a fila estiver cheia
    caminho, linhas = projetor_b.finish()  # devolve resumo, não a base
    t_bounded = time.perf_counter() - inicio
    print(f"[bounded] {linhas:,} linhas gravadas em {t_bounded:.2f}s -> {Path(caminho).name}")

    # a saída do bounded é um parquet; relemos e conferimos contra o serial
    bounded = pq.read_table(saida).sort_by("id_contrato")
    soma_bounded = pc.sum(bounded["receita_projetada"]).as_py()
    soma_serial = pc.sum(serial["receita_projetada"]).as_py()
    print(f"[check] linhas e soma batem com o serial: "
          f"{bounded.num_rows == serial.num_rows and abs(soma_bounded - soma_serial) < 1e-3}")

    print("\n[amostra] id_contrato -> receita_projetada:")
    print(paralelo.slice(0, 5).to_pandas().to_string(index=False))


if __name__ == "__main__":
    main()
