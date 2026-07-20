"""Exemplo 5 — Joins entre orders, customers e products com Table.join.

Conceitos:
- `Table.join(other, keys=..., join_type=...)` — suporta "inner", "left outer",
  "right outer", "full outer", entre outros.
- Join encadeado (fato + 2 dimensões), igual ao exemplo de merge do pandas.
- Diferente do pandas, o resultado do join no Arrow não garante a ordem das
  linhas — se a ordem importa, usar `.sort_by(...)` no final.

Rode com: `uv run examples/05_joins.py`
"""

import pyarrow.compute as pc

from _common import customers_dataset, orders_dataset, products_dataset, section

if __name__ == "__main__":
    orders = orders_dataset().to_table(filter=pc.field("order_month") == 1)
    customers = customers_dataset().to_table()
    products = products_dataset().to_table()

    section("Join orders -> customers (inner)")
    enriched = orders.join(customers, keys="customer_id", join_type="inner")
    print(f"{orders.num_rows:,} orders -> {enriched.num_rows:,} linhas após o join")
    print(enriched.select(["order_id", "customer_name", "region"]).slice(0, 3))

    section("Join encadeado: (orders join customers) join products")
    full = enriched.join(products, keys="product_id", join_type="inner")
    print(
        full.select(["order_id", "customer_name", "product_name", "category", "unit_price"]).slice(
            0, 3
        )
    )

    section("left outer join: mantém pedidos sem cliente correspondente")
    orders_com_invalido = orders.set_column(
        orders.schema.get_field_index("customer_id"),
        "customer_id",
        pc.if_else(
            pc.equal(orders["order_id"], orders["order_id"][0]),
            -1,
            orders["customer_id"],
        ),
    )
    left = orders_com_invalido.join(customers, keys="customer_id", join_type="left outer")
    sem_match = left.filter(pc.is_null(left["customer_name"]))
    print(f"{sem_match.num_rows} pedido(s) sem cliente correspondente")

    section("Receita total por categoria (join + group_by)")
    full = full.append_column("receita", pc.multiply(pc.cast(full["quantity"], "float64"), full["unit_price"]))
    receita_categoria = full.group_by("category").aggregate([("receita", "sum")]).sort_by(
        [("receita_sum", "descending")]
    )
    print(receita_categoria)
