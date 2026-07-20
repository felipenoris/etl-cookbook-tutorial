"""Exemplo de multithreading: projeção de receita de contratos em paralelo no Rust.

O padrão exercitado aqui:

1. o Python lê a fonte de dados (um parquet de contratos) **em lotes**;
2. cada lote é passado **serialmente** para o Rust via
   ``ParallelRevenueProjector.submit_batch(batch)`` — a chamada valida o lote,
   dispara uma thread Rust para processá-lo e retorna imediatamente;
3. enquanto o Python lê o próximo lote, os anteriores já estão sendo
   calculados em paralelo (as threads são Rust puro, fora do GIL);
4. ``collect()`` espera todas as threads e devolve UM ``pyarrow.RecordBatch``
   consolidado: ``id_contrato -> receita_projetada``.

O cálculo por contrato (juros totais da tabela Price, simulando o saldo
devedor mês a mês) é independente entre contratos — o caso ideal para
paralelizar. Para comparação, ``project_revenue_batch`` roda o mesmo cálculo
de forma sequencial.

Rode com: ``uv run run_contracts_parallel.py`` (a partir de ``rust-extension``).
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from etl_rust_ext import ParallelRevenueProjector, project_revenue_batch

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

    print("\n[amostra] id_contrato -> receita_projetada:")
    print(paralelo.slice(0, 5).to_pandas().to_string(index=False))


if __name__ == "__main__":
    main()
