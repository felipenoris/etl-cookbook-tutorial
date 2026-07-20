"""Exemplo 9 — Padrão híbrido: pyarrow nas bordas, pandas no miolo (streaming).

Este é o desenho recomendado quando a equipe é proficiente em pandas mas os
dados não cabem (ou não deveriam passar inteiros) pela memória:

    pyarrow.dataset  ->  RecordBatch  ->  pandas (lógica de negócio)  ->  pyarrow  ->  parquet
      (leitura            (um lote         (zero-copy, API familiar)      (escrita
       particionada,       por vez)                                        incremental)
       pruning)

Conceitos:
- `dataset.to_batches(batch_size=...)`: streaming — itera o dataset em lotes
  de tamanho controlado, mantendo o uso de memória constante, independente do
  tamanho total do dataset.
- Cada lote vira DataFrame com `types_mapper=pd.ArrowDtype` (zero-copy), a
  transformação é escrita em pandas puro, e o resultado volta a Arrow com
  `Table.from_pandas`.
- `pq.ParquetWriter`: escrita incremental — os lotes transformados são
  anexados a um mesmo arquivo parquet, lote a lote.
- `ds.write_dataset(..., existing_data_behavior="delete_matching")`: recarga
  idempotente de partição no pyarrow (equivalente do `OVERWRITE_OR_IGNORE`
  do DuckDB, exemplo `../DuckDB/examples/06`).

Rode com: `uv run examples/09_hybrid_pandas_etl.py`
"""

import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from _common import REPO_ROOT, orders_dataset, section

OUT_DIR = REPO_ROOT / "data" / "rich" / "pyarrow_hybrid_demo"


def regra_de_negocio(df: pd.DataFrame) -> pd.DataFrame:
    """A transformação que a equipe escreve — pandas puro, sem pyarrow à vista.

    Marca pedidos urgentes (quantidade alta ainda não enviada) e deriva a
    semana do mês; qualquer lógica pandas funciona aqui.
    """
    return df.assign(
        urgente=(df["quantity"] >= 8) & df["status"].isin(["novo"]),
        semana_do_mes=((df["order_date"].dt.day - 1) // 7 + 1).astype("int8[pyarrow]"),
    )


if __name__ == "__main__":
    shutil.rmtree(OUT_DIR, ignore_errors=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    dataset = orders_dataset()
    janeiro = pc.field("order_month") == 1

    section("Streaming: processando o mês 1 em lotes de 1M de linhas")
    writer: pq.ParquetWriter | None = None
    total, lotes, maior_lote = 0, 0, 0
    for batch in dataset.to_batches(filter=janeiro, batch_size=1_000_000):
        # lote Arrow -> DataFrame (zero-copy) -> lógica pandas -> Table Arrow
        df = batch.to_pandas(types_mapper=pd.ArrowDtype)
        transformado = pa.Table.from_pandas(regra_de_negocio(df), preserve_index=False)

        if writer is None:  # o schema de saída só é conhecido após o 1º lote
            writer = pq.ParquetWriter(OUT_DIR / "orders_enriquecido.parquet", transformado.schema)
        writer.write_table(transformado)

        total += len(df)
        lotes += 1
        maior_lote = max(maior_lote, len(df))
    assert writer is not None
    writer.close()
    print(f"{total:,} linhas em {lotes} lotes (maior lote: {maior_lote:,} linhas)")
    print("memória usada ~= 1 lote por vez, qualquer que seja o tamanho do dataset")

    section("O arquivo incremental é um parquet normal")
    resultado = pq.read_table(OUT_DIR / "orders_enriquecido.parquet")
    print(f"{resultado.num_rows:,} linhas | urgentes: {pc.sum(resultado['urgente']).as_py():,}")

    section("Escrita particionada idempotente: delete_matching")
    # Reprocessar uma partição substitui SÓ os arquivos dela — rodar 2x não duplica.
    por_semana = resultado.select(["order_id", "quantity", "urgente", "semana_do_mes"])
    for rodada in (1, 2):
        ds.write_dataset(
            por_semana,
            OUT_DIR / "por_semana",
            format="parquet",
            partitioning=ds.partitioning(
                pa.schema([("semana_do_mes", pa.int8())]), flavor="hive"
            ),
            existing_data_behavior="delete_matching",
        )
        n = ds.dataset(OUT_DIR / "por_semana", partitioning="hive").count_rows()
        print(f"rodada {rodada}: {n:,} linhas no destino")

    section("Partições geradas")
    for path in sorted((OUT_DIR / "por_semana").iterdir()):
        print(path.name)
