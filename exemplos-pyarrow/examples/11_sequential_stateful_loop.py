"""Exemplo 11 — Lógica sequencial com estado, em lotes, no lado Python.

O análogo em pyarrow puro do que o `rust-extension` faz nativamente
(`compute_customer_running_spend`): gasto ACUMULADO por cliente + um tier. É o
tipo de cálculo que **não vetoriza** — cada linha depende do resultado da
anterior (soma corrente por cliente, com estado carregado adiante) — e por
isso é o caso em que uma extensão nativa compensa. Aqui o objetivo é o
oposto: exercitar a API do pyarrow para percorrer dados em lotes, mesmo
sabendo que o laço linha a linha é lento.

O padrão (o mesmo dos irmãos em `../exemplos-pandas` e `../exemplos-DuckDB`):

1. **entrada em lotes**: `Table.to_batches(max_chunksize=n)` fatia a tabela em
   `RecordBatch`es — a unidade natural de iteração do Arrow;
2. **estado entre lotes**: um `dict` `{customer_id -> total}` vive FORA do laço
   de lotes, sobrevivendo à fronteira entre eles;
3. **laço sequencial**: dentro de cada lote, uma passada linha a linha.

Diferente do DuckDB (que transmite o resultado já ordenado do motor), o
pyarrow **não ordena durante o scan**: materializamos a tabela filtrada e
chamamos `sort_by(...)` antes de fatiar em lotes. O acumulado precisa ser
cronológico por cliente, daí a ordenação.

Ponto de API a observar: o Arrow é colunar e **não tem uma boa API de
iteração por LINHA** — ele é feito para operar colunas inteiras de uma vez.
Iterar linha a linha (via `to_pylist()` ou `zip` de colunas) é ir contra a
natureza da biblioteca; funciona, mas é o "jeito errado" de propósito, para
mostrar quando você deveria sair do pyarrow (para SQL vetorizado ou para o
Rust).

Rode com: `uv run examples/11_sequential_stateful_loop.py`
"""

import pyarrow as pa
import pyarrow.compute as pc

from _common import orders_dataset, products_dataset, section

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
    section("Preparando a fonte: join com products, filtro e ordenação")
    # products é uma dimensão pequena: um lookup {product_id -> unit_price}
    products = products_dataset().to_table()
    preco = dict(
        zip(products.column("product_id").to_pylist(), products.column("unit_price").to_pylist())
    )

    # subconjunto (para o laço Python terminar rápido) das orders do mês 1
    orders = orders_dataset().to_table(
        columns=["order_id", "customer_id", "product_id", "order_date", "quantity"],
        filter=(pc.field("order_month") == 1) & (pc.field("customer_id") <= 40),
    )
    # amount = quantity * preço do produto — enriquecido via pc.multiply +
    # o preço mapeado (index_in + take faria o join vetorizado; aqui um
    # dict basta e é mais legível)
    precos_por_linha = pa.array([preco[p] for p in orders.column("product_id").to_pylist()])
    amount = pc.multiply(pc.cast(orders.column("quantity"), pa.float64()), precos_por_linha)
    orders = orders.append_column("amount", amount)

    # ordena por cliente/data: o pyarrow ordena a TABELA inteira em memória
    # (não há sort durante o scan, ao contrário do DuckDB)
    orders = orders.sort_by(
        [("customer_id", "ascending"), ("order_date", "ascending"), ("order_id", "ascending")]
    )
    print(f"{orders.num_rows:,} linhas prontas, ordenadas por cliente/data")

    section("Laço sequencial sobre os lotes de Table.to_batches")
    running: dict[int, float] = {}  # ESTADO entre lotes
    out_ids: list[int] = []
    out_cumulative: list[float] = []
    out_tier: list[str] = []
    n_lotes = 0

    for batch in orders.to_batches(max_chunksize=BATCH):
        n_lotes += 1
        ids = batch.column("order_id").to_pylist()
        custs = batch.column("customer_id").to_pylist()
        amounts = batch.column("amount").to_pylist()
        for order_id, customer_id, amt in zip(ids, custs, amounts):
            total = running.get(customer_id, 0.0) + amt
            running[customer_id] = total
            out_ids.append(order_id)
            out_cumulative.append(total)
            out_tier.append(tier(total))

    print(f"{len(out_ids):,} linhas processadas em {n_lotes} lotes de até {BATCH:,}")

    section("Resultado como Table Arrow (amostra)")
    resultado = pa.table(
        {
            "order_id": pa.array(out_ids, pa.int64()),
            "cumulative_spend": pa.array(out_cumulative, pa.float64()),
            "customer_tier": pa.array(out_tier, pa.string()),
        }
    )
    print(resultado.slice(0, 5))

    section("Verificação: total corrente final == group_by/sum vetorizado")
    # o mesmo cálculo, mas do jeito CERTO no pyarrow: uma agregação vetorizada
    agrupado = orders.group_by("customer_id").aggregate([("amount", "sum")])
    gabarito = dict(
        zip(
            agrupado.column("customer_id").to_pylist(),
            agrupado.column("amount_sum").to_pylist(),
        )
    )
    bate = all(abs(running[c] - gabarito[c]) < 1e-6 for c in running)
    print(f"o último acumulado de cada cliente bate com o group_by/sum: {bate}")
    print("(a agregação vetorizada é ~milhares de vezes mais rápida; o laço só")
    print(" se justifica quando a lógica é sequencial DE VERDADE — aí, use Rust)")
