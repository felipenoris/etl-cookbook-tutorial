"""Exemplo 4 — Agrupamento e agregação com groupby.

Conceitos:
- `groupby(...).agg(...)` com "named aggregation" (nomes de coluna de saída
  explícitos), a forma recomendada desde o pandas 0.25+.
- `.transform()` para devolver um resultado com o mesmo índice do DataFrame
  original (útil para criar colunas derivadas do grupo, ex.: "% do total do
  grupo").
- `.apply()` para lógica de grupo mais livre (mais lento, usar com moderação).

Rode com: `uv run examples/04_groupby_aggregation.py`
"""

from _common import load_orders, section

if __name__ == "__main__":
    orders = load_orders([1])

    section("groupby + named aggregation")
    resumo = orders.groupby("status").agg(
        total_pedidos=("order_id", "count"),
        quantidade_media=("quantity", "mean"),
        quantidade_total=("quantity", "sum"),
    )
    print(resumo)

    section("groupby por múltiplas chaves")
    por_cliente_status = (
        orders.groupby(["customer_id", "status"])
        .agg(total=("order_id", "count"))
        .reset_index()
        .sort_values("total", ascending=False)
    )
    print(por_cliente_status.head(5))

    section("transform: participação de cada pedido na quantidade total do status")
    orders = orders.copy()
    orders["total_do_status"] = orders.groupby("status")["quantity"].transform("sum")
    orders["pct_do_status"] = orders["quantity"] / orders["total_do_status"]
    print(orders[["status", "quantity", "total_do_status", "pct_do_status"]].head(5))

    section("apply: lógica de grupo livre (top-1 pedido por quantidade em cada status)")
    top_por_status = orders.groupby("status", group_keys=False).apply(
        lambda g: g.nlargest(1, "quantity"), include_groups=False
    )
    print(top_por_status[["order_id", "quantity"]])

    section("agregação múltipla por coluna com dicionário")
    multi_agg = orders.groupby("status").agg({"quantity": ["mean", "min", "max"]})
    print(multi_agg)
