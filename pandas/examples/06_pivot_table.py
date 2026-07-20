"""Exemplo 6 — Pivot table e melt (reshape "largo" <-> "longo").

Conceitos:
- `pivot_table` para tabular valores por duas dimensões, com `aggfunc` e
  `margins` (totais).
- `melt` para desfazer o pivot (formato longo), útil antes de plotar ou de
  gravar em formatos que preferem "long format".
- `pivot` (sem agregação) vs `pivot_table` (com agregação) — usar `pivot`
  exige chave única por combinação de índice/coluna.

Rode com: `uv run examples/06_pivot_table.py`
"""

from _common import load_orders, load_products, section

if __name__ == "__main__":
    orders = load_orders([1])
    products = load_products()
    full = orders.merge(products, on="product_id", how="inner")
    full["receita"] = full["quantity"] * full["unit_price"]

    section("pivot_table: receita média por categoria x status")
    pivot = full.pivot_table(
        index="category",
        columns="status",
        values="receita",
        aggfunc="mean",
    )
    print(pivot.round(2))

    section("pivot_table com múltiplas agregações e margins (totais)")
    pivot_margins = full.pivot_table(
        index="category",
        columns="status",
        values="quantity",
        aggfunc="sum",
        margins=True,
        margins_name="total",
    )
    print(pivot_margins)

    section("melt: voltando do formato largo para o longo")
    pivot_reset = pivot.reset_index()
    longo = pivot_reset.melt(id_vars="category", var_name="status", value_name="receita_media")
    print(longo.head(6))

    section("pivot (sem agregação) exige uma linha por combinação índice/coluna")
    receita_por_dia_categoria = (
        full.groupby(["order_date", "category"])["receita"].sum().reset_index()
    )
    pivot_simples = receita_por_dia_categoria.pivot(
        index="order_date", columns="category", values="receita"
    )
    print(pivot_simples.head(3))
