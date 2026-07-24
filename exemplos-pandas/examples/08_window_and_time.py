"""Exemplo 8 — Séries temporais: resample, rolling window e acumulados.

Conceitos:
- `resample` agrega por período de tempo (ex.: diário), exigindo um índice
  do tipo datetime.
- `rolling` calcula estatísticas em janela móvel (ex.: média móvel de 3 dias).
- `cumsum`/`cumcount` para acumulados ao longo de uma série (ou por grupo).

Nota: `resample`/`rolling` precisam de um índice datetime64 "de verdade", então
convertemos a coluna `order_date` (Arrow date32) para o dtype datetime nativo do
pandas antes dessa etapa — um bom exemplo de quando vale sair do backend Arrow.

Rode com: `uv run examples/08_window_and_time.py`
"""

from _common import load_orders, section

if __name__ == "__main__":
    orders = load_orders([1])

    section("Quantidade vendida por dia (convertendo order_date para datetime64[ns])")
    orders = orders.copy()
    orders["order_date_ts"] = orders["order_date"].astype("date32[pyarrow]").astype("datetime64[ns]")
    qtd_diaria = (
        orders.groupby("order_date_ts")["quantity"]
        .sum()
        .sort_index()
    )
    print(qtd_diaria.head(5))

    section("resample: agregando por período de 3 dias")
    por_3_dias = qtd_diaria.resample("3D").sum()
    print(por_3_dias.head(5))

    section("rolling: média móvel de 3 dias sobre a quantidade diária")
    media_movel = qtd_diaria.rolling(window=3).mean()
    print(media_movel.head(6))

    section("cumsum: quantidade acumulada ao longo do tempo")
    print(qtd_diaria.cumsum().head(5))

    section("cumcount por grupo: número sequencial do pedido de cada cliente")
    orders_sorted = orders.sort_values(["customer_id", "order_date_ts"])
    orders_sorted["pedido_num_do_cliente"] = orders_sorted.groupby("customer_id").cumcount() + 1
    print(
        orders_sorted[["customer_id", "order_date_ts", "pedido_num_do_cliente"]]
        .head(6)
    )
