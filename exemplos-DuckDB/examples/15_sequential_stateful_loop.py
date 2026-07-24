"""Exemplo 15 — Lógica sequencial com estado, em lotes, no lado Python.

Este exemplo faz **em Python puro** o que o `rust-extension` faz nativamente
(`compute_customer_running_spend`): o gasto ACUMULADO por cliente e um tier de
fidelidade. É o tipo de cálculo que **não vetoriza** — cada linha depende do
resultado da anterior (uma soma corrente por cliente, com estado carregado de
uma linha para a seguinte) — e por isso é o caso em que uma extensão nativa
compensa. Aqui, o objetivo é o OPOSTO: exercitar a API de streaming do DuckDB
no Python, mesmo sabendo que o laço linha a linha não performa bem.

O padrão exercitado (o mesmo dos três exemplos irmãos em `../exemplos-pandas` e
`../exemplos-pyarrow`):

1. **entrada em streaming**: o DuckDB entrega o resultado em LOTES via
   `relation.to_arrow_reader(n)` — um `RecordBatch` por iteração, sem
   materializar tudo de uma vez;
2. **estado entre lotes**: um `dict` `{customer_id -> total_corrente}` vive
   FORA do laço de lotes, então sobrevive à fronteira entre eles — essencial,
   porque as linhas de um cliente podem cair em lotes diferentes;
3. **laço sequencial**: dentro de cada lote, uma passada linha a linha
   atualiza o total do cliente e classifica o tier pelo acumulado do momento.

Como o acumulado precisa ser cronológico por cliente, a consulta ordena por
`customer_id, order_date` — e o DuckDB **transmite o resultado já ordenado**
do motor (ele ordena e depois faz stream), sem o Python segurar tudo.

Rode com: `uv run examples/15_sequential_stateful_loop.py`
"""

import duckdb
import pyarrow as pa

from _common import ORDERS_GLOB, PRODUCTS_GLOB, section

BATCH = 20_000
THRESHOLD_PRATA = 500.0
THRESHOLD_OURO = 2000.0


def tier(total: float) -> str:
    if total < THRESHOLD_PRATA:
        return "bronze"
    if total < THRESHOLD_OURO:
        return "prata"
    return "ouro"


if __name__ == "__main__":
    con = duckdb.connect()

    # a fonte: orders join products -> amount, filtrada a um subconjunto para o
    # laço Python terminar rápido, e ORDENADA por cliente/data (o acumulado
    # precisa ser cronológico por cliente)
    relation = con.sql(
        f"""
        SELECT o.order_id, o.customer_id, o.quantity * p.unit_price AS amount
        FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true) o
        JOIN read_parquet('{PRODUCTS_GLOB}') p USING (product_id)
        WHERE o.order_month = 1 AND o.customer_id <= 40
        ORDER BY o.customer_id, o.order_date, o.order_id
        """
    )

    section("Streaming em lotes com to_arrow_reader (1 RecordBatch por vez)")
    reader = relation.to_arrow_reader(BATCH)  # RecordBatchReader, lazy

    running: dict[int, float] = {}  # ESTADO que sobrevive entre os lotes
    out_ids: list[int] = []
    out_cumulative: list[float] = []
    out_tier: list[str] = []
    n_lotes = 0

    for batch in reader:  # cada iteração puxa um lote do DuckDB
        n_lotes += 1
        # colunas do lote -> listas Python; o zip percorre as linhas
        ids = batch.column("order_id").to_pylist()
        custs = batch.column("customer_id").to_pylist()
        amounts = batch.column("amount").to_pylist()
        for order_id, customer_id, amount in zip(ids, custs, amounts):
            total = running.get(customer_id, 0.0) + amount  # lê o estado...
            running[customer_id] = total  # ...e escreve de volta
            out_ids.append(order_id)
            out_cumulative.append(total)
            out_tier.append(tier(total))

    print(f"{len(out_ids):,} linhas processadas em {n_lotes} lotes de até {BATCH:,}")

    section("Resultado: gasto acumulado e tier (amostra)")
    # as listas do laço viram uma Table Arrow — o resultado do processamento
    # volta a ser um dado colunar, consultável pelo DuckDB ou gravável em parquet
    resultado = pa.table(
        {
            "order_id": pa.array(out_ids, pa.int64()),
            "cumulative_spend": pa.array(out_cumulative, pa.float64()),
            "customer_tier": pa.array(out_tier, pa.string()),
        }
    )
    con.sql("SELECT * FROM resultado LIMIT 5").show()  # replacement scan sobre a Table

    section("Distribuição de tiers (pelo total FINAL de cada cliente)")
    tiers_finais = [tier(t) for t in running.values()]
    for t in ("bronze", "prata", "ouro"):
        print(f"  {t:7s}: {tiers_finais.count(t)} clientes")

    section("Verificação: o total corrente final == soma vetorizada por cliente")
    # o mesmo cálculo, mas VETORIZADO em SQL (o jeito rápido) — serve de gabarito
    gabarito = dict(
        con.sql(
            f"""
            SELECT o.customer_id, SUM(o.quantity * p.unit_price) AS total
            FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true) o
            JOIN read_parquet('{PRODUCTS_GLOB}') p USING (product_id)
            WHERE o.order_month = 1 AND o.customer_id <= 40
            GROUP BY o.customer_id
            """
        ).fetchall()
    )
    bate = all(abs(running[c] - gabarito[c]) < 1e-6 for c in running)
    print(f"o último acumulado de cada cliente bate com o SUM agrupado: {bate}")
    print("(o SUM é ~milhares de vezes mais rápido — o laço só se justifica")
    print(" quando a lógica é sequencial DE VERDADE e cabe melhor no Rust)")
