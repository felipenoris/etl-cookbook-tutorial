"""Pipeline de ETL completo: DuckDB + pyarrow + pandas + extensão Rust (PyO3/pyo3-arrow).

Este script fecha o ciclo dos exemplos das etapas anteriores do tutorial em um
único ETL de ponta a ponta:

1. **DuckDB** (`../exemplos-DuckDB`) lê e junta `orders` + `customers` + `products` de
   ``data/raw`` diretamente dos parquets particionados, com ``memory_limit``/
   ``temp_directory`` configurados para exercitar spill em disco, e devolve o
   resultado ordenado como uma única ``pyarrow.RecordBatch``.
2. **pyarrow** (`../exemplos-pyarrow`) faz uma pequena projeção/cast antes de repassar
   o batch para a camada Rust.
3. A extensão **Rust** (`etl_rust_ext`, ver ``src/lib.rs``) recebe esse
   ``RecordBatch`` via ``pyo3-arrow`` — sem copiar os buffers de dados — e
   calcula, num único loop sequencial, o gasto acumulado por cliente e um
   tier de fidelidade (algo lento em Python puro e trivial em Rust).
4. **pandas** (`../exemplos-pandas`) com backend Arrow resume o resultado final por
   tier, para inspeção humana.
5. O resultado enriquecido é gravado em ``data/rich/order_metrics/`` como
   parquet particionado por ``customer_tier``.

Rode com: ``uv run run_etl.py`` (a partir da pasta ``rust-extension``). A
extensão Rust é compilada automaticamente pelo ``uv sync``/``uv run`` via o
build backend ``maturin`` configurado em ``pyproject.toml``.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from etl_rust_ext import compute_customer_running_spend

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw"
RICH_DIR = REPO_ROOT / "data" / "rich"
SPILL_DIR = Path(__file__).resolve().parent / "_tmp_spill"

ORDERS_GLOB = str(RAW_DIR / "orders" / "**" / "*.parquet")
CUSTOMERS_GLOB = str(RAW_DIR / "customers" / "**" / "*.parquet")
PRODUCTS_GLOB = str(RAW_DIR / "products" / "*.parquet")


def extract_and_join_with_duckdb() -> pa.Table:
    """Lê os 3 datasets parquet de ``data/raw`` e devolve o join já ordenado.

    Configura um teto de memória propositalmente apertado (``memory_limit``)
    com spill habilitado (``temp_directory``), igual ao exemplo
    ``exemplos-DuckDB/examples/04_memory_limit_and_spill.py``, para mostrar que o join
    e o ``ORDER BY`` sobre as ~33.7M linhas de ``orders`` funcionam mesmo sem
    RAM suficiente para manter tudo em memória de uma vez.

    A ordenação por ``customer_id, order_date`` é o que torna o "gasto
    acumulado por cliente" (calculado depois em Rust) coerente: cada cliente
    tem suas linhas agrupadas e em ordem cronológica. O ``order_id`` no final
    desempata pedidos do mesmo cliente na mesma data — sem ele, com
    ``preserve_insertion_order=false``, a ordem dos empates (e portanto o tier
    das primeiras linhas de cada cliente) variaria entre execuções.
    """
    SPILL_DIR.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='512MB'")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")

    query = f"""
        SELECT
            o.order_id,
            o.customer_id,
            o.order_date,
            o.quantity,
            p.unit_price,
            (o.quantity * p.unit_price) AS amount,
            c.region,
            p.category
        FROM read_parquet('{ORDERS_GLOB}') o
        JOIN read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true) c USING (customer_id)
        JOIN read_parquet('{PRODUCTS_GLOB}') p USING (product_id)
        ORDER BY o.customer_id, o.order_date, o.order_id
    """
    table = con.sql(query).to_arrow_table()
    con.close()
    shutil.rmtree(SPILL_DIR, ignore_errors=True)
    return table


def project_for_rust(table: pa.Table) -> pa.RecordBatch:
    """Projeta só as colunas que a extensão Rust precisa e materializa em um único batch.

    ``pyo3-arrow`` espera um ``RecordBatch`` (não uma ``Table`` com múltiplos
    chunks), então combinamos os chunks internos em um só antes de repassar.
    """
    projected = table.select(["order_id", "customer_id", "order_date", "amount", "region", "category"])
    combined = projected.combine_chunks()
    (batch,) = combined.to_batches()
    return batch


def enrich_with_rust(batch: pa.RecordBatch) -> pa.Table:
    """Chama a extensão Rust para calcular gasto acumulado e tier por cliente.

    ``compute_customer_running_spend`` roda em Rust um único loop sequencial
    mantendo um ``HashMap<customer_id, total>`` — o tipo de computação com
    estado que é custosa em pandas/pyarrow/SQL vetorizados, mas trivial e
    rápida em Rust. A troca de dados é zero-copy nos dois sentidos via a
    Arrow C Data Interface.
    """
    enriched_batch = compute_customer_running_spend(batch)
    return pa.Table.from_batches([enriched_batch])


def summarize_with_pandas(table: pa.Table) -> pd.DataFrame:
    """Resumo final com pandas usando backend Arrow (mesmo padrão de ``../exemplos-pandas``)."""
    df = table.to_pandas(types_mapper=pd.ArrowDtype)
    return (
        df.groupby("customer_tier")
        .agg(
            total_pedidos=("order_id", "count"),
            clientes_distintos=("customer_id", "nunique"),
            receita_total=("amount", "sum"),
        )
        .sort_values("receita_total", ascending=False)
    )


# Sem limite, o write_dataset gera um único arquivo por partição, por maior que
# ela seja (a partição "ouro" sairia com ~590MB num part-0.parquet só). Com
# ~18 bytes/linha no parquet final, 3M linhas dão arquivos de ~50MB — partições
# grandes viram múltiplos parts (part-0.parquet, part-1.parquet, ...).
MAX_ROWS_PER_FILE = 3_000_000


def write_rich_output(table: pa.Table) -> None:
    """Grava o resultado final em ``data/rich/order_metrics``, particionado por tier.

    ``max_rows_per_file`` limita o tamanho de cada arquivo: partições maiores
    que o limite são divididas em vários ``part-{i}.parquet`` (~50MB cada) —
    o layout típico de saída de jobs distribuídos, e mais amigável para leitura
    paralela e object stores do que um arquivo único gigante.
    """
    out_dir = RICH_DIR / "order_metrics"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    ds.write_dataset(
        table,
        out_dir,
        format="parquet",
        partitioning=ds.partitioning(pa.schema([("customer_tier", pa.string())]), flavor="hive"),
        existing_data_behavior="overwrite_or_ignore",
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_rows_per_group=1_000_000,
    )
    num_files = len(list(out_dir.rglob("*.parquet")))
    print(f"[rich] {table.num_rows:,} linhas em {num_files} arquivos gravadas em {out_dir}")


def main() -> None:
    print("[1/5] extraindo e juntando orders+customers+products via DuckDB (com spill)...")
    joined = extract_and_join_with_duckdb()
    print(f"      {joined.num_rows:,} linhas após o join")

    print("[2/5] projetando colunas para a extensão Rust (pyarrow)...")
    batch = project_for_rust(joined)

    print("[3/5] calculando gasto acumulado e tier por cliente (Rust + pyo3-arrow)...")
    enriched = enrich_with_rust(batch)

    print("[4/5] resumindo por tier (pandas, backend Arrow):")
    print(summarize_with_pandas(enriched))

    print("[5/5] gravando resultado em data/rich/order_metrics/...")
    write_rich_output(enriched)


if __name__ == "__main__":
    main()
