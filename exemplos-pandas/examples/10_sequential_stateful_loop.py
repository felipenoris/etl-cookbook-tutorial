"""Exemplo 10 — Lógica sequencial com estado, em lotes, no lado Python.

O análogo em pandas puro do que o `rust-extension` faz nativamente
(`compute_customer_running_spend`): gasto ACUMULADO por cliente + um tier. É o
tipo de cálculo que **não vetoriza** — cada linha depende do resultado da
anterior (soma corrente por cliente, com estado carregado adiante) — e por
isso é o caso em que uma extensão nativa compensa. Aqui o objetivo é o
oposto: exercitar a API do pandas para percorrer linhas em lotes, mesmo
sabendo que iterar linha a linha é o anti-padrão de performance do pandas.

O padrão (o mesmo dos irmãos em `../exemplos-pyarrow` e `../exemplos-DuckDB`):

1. **entrada em lotes**: o pandas não tem um leitor streaming nativo, então
   fatiamos o DataFrame ordenado em blocos com `range`/`iloc` — simulando o
   fluxo de lotes que o DuckDB e o pyarrow entregam de forma nativa;
2. **estado entre lotes**: um `dict` `{customer_id -> total}` vive FORA do laço
   de lotes, sobrevivendo à fronteira entre eles;
3. **laço sequencial**: dentro de cada bloco, `itertuples()` percorre as
   linhas — a forma mais rápida de iterar um DataFrame linha a linha (bem
   melhor que `iterrows()`), mas ainda ordens de grandeza mais lenta que uma
   operação vetorizada.

Como o acumulado precisa ser cronológico por cliente, o DataFrame é ordenado
por `customer_id, order_date` antes do laço. Note o contraste didático no fim:
o mesmo resultado sai de um `groupby().cumsum()` **vetorizado** em uma linha —
o jeito certo em pandas. O laço só existe aqui para exercitar a API do caso em
que a lógica sequencial não caberia num `cumsum` (dependências mais
complexas entre linhas), cenário que na prática migraria para o Rust.

Rode com: `uv run examples/10_sequential_stateful_loop.py`
"""

import pandas as pd

from _common import load_orders, load_products, section

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
    section("Preparando a fonte: merge com products, filtro e ordenação")
    orders = load_orders([1])
    orders = orders[orders["customer_id"] <= 40].copy()
    products = load_products()[["product_id", "unit_price"]]

    # merge (join) fato+dimensão e o cálculo de amount, vetorizados
    enriquecido = orders.merge(products, on="product_id", how="inner")
    enriquecido["amount"] = enriquecido["quantity"].astype("float64[pyarrow]") * enriquecido["unit_price"]

    # ordena por cliente/data: o acumulado precisa ser cronológico por cliente
    enriquecido = enriquecido.sort_values(
        ["customer_id", "order_date", "order_id"]
    ).reset_index(drop=True)
    print(f"{len(enriquecido):,} linhas prontas, ordenadas por cliente/data")

    section("Laço sequencial sobre blocos do DataFrame (itertuples)")
    running: dict[int, float] = {}  # ESTADO entre lotes
    out_ids: list[int] = []
    out_cumulative: list[float] = []
    out_tier: list[str] = []
    n_lotes = 0

    # o pandas não streama; fatiamos em blocos com iloc para simular os lotes
    for inicio in range(0, len(enriquecido), BATCH):
        bloco = enriquecido.iloc[inicio : inicio + BATCH]
        n_lotes += 1
        # itertuples é a iteração linha a linha mais rápida do pandas
        for linha in bloco.itertuples(index=False):
            total = running.get(linha.customer_id, 0.0) + linha.amount
            running[linha.customer_id] = total
            out_ids.append(linha.order_id)
            out_cumulative.append(total)
            out_tier.append(tier(total))

    print(f"{len(out_ids):,} linhas processadas em {n_lotes} blocos de até {BATCH:,}")

    section("Resultado como DataFrame (backend Arrow, amostra)")
    resultado = pd.DataFrame(
        {
            "order_id": pd.array(out_ids, dtype="int64[pyarrow]"),
            "cumulative_spend": pd.array(out_cumulative, dtype="float64[pyarrow]"),
            "customer_tier": pd.array(out_tier, dtype="string[pyarrow]"),
        }
    )
    print(resultado.head(5).to_string(index=False))

    section("O jeito CERTO em pandas: groupby().cumsum() vetorizado, em 1 linha")
    # a mesma soma corrente por cliente, vetorizada — sem laço nenhum
    enriquecido["cumsum_vetorizado"] = enriquecido.groupby("customer_id")["amount"].cumsum()
    # o resultado do laço, alinhado à mesma ordem, deve bater
    bate = bool(
        (
            enriquecido["cumsum_vetorizado"].astype("float64").round(6).to_numpy()
            == pd.Series(out_cumulative).round(6).to_numpy()
        ).all()
    )
    print(f"o acumulado do laço bate com o groupby().cumsum() vetorizado: {bate}")
    print("(o cumsum vetorizado é ~centenas de vezes mais rápido; o laço só se")
    print(" justifica quando a dependência entre linhas não cabe num cumsum —")
    print(" e aí o caso costuma migrar para uma extensão nativa em Rust)")
