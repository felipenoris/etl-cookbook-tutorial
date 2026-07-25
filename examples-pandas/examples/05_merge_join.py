"""Exemplo 5 — Merge/join entre orders, customers e products.

Conceitos:
- `merge` com `how="inner"` e `how="left"`.
- Join encadeado de 3 tabelas (fato + 2 dimensões), o padrão clássico de
  data warehouse em estrela.
- `validate=` para checar a cardinalidade esperada do relacionamento (falha
  cedo se a suposição de 1:1/1:N/N:1 estiver errada).
- Alternativa via índice com `.join()`.

Rode com: `uv run examples/05_merge_join.py`
"""

from _common import load_customers, load_orders, load_products, section

if __name__ == "__main__":
    orders = load_orders([1])
    customers = load_customers()
    products = load_products()

    section("Join orders -> customers (N:1, valida cardinalidade)")
    enriched = orders.merge(
        customers,
        on="customer_id",
        how="inner",
        validate="many_to_one",
    )
    print(f"{len(orders)} orders -> {len(enriched)} linhas após o join (deve ser igual)")
    print(enriched[["order_id", "customer_name", "region"]].head(3))

    section("Join encadeado: orders -> customers -> products")
    full = enriched.merge(products, on="product_id", how="inner", validate="many_to_one")
    print(full[["order_id", "customer_name", "product_name", "category", "unit_price"]].head(3))

    section("left join: mantém pedidos mesmo sem cliente correspondente")
    orders_com_cliente_invalido = orders.copy()
    orders_com_cliente_invalido.loc[0, "customer_id"] = -1  # id inexistente
    left = orders_com_cliente_invalido.merge(customers, on="customer_id", how="left")
    print(f"clientes nulos após left join: {left['customer_name'].isna().sum()}")

    section("Join via índice com .join() (equivalente a merge on='customer_id')")
    customers_idx = customers.set_index("customer_id")
    orders_idx = orders.set_index("customer_id")
    joined = orders_idx.join(customers_idx, how="inner", rsuffix="_cliente")
    print(joined.reset_index()[["customer_id", "order_id", "region"]].head(3))

    section("Receita total por categoria de produto (join + groupby)")
    full["receita"] = full["quantity"] * full["unit_price"]
    receita_categoria = full.groupby("category")["receita"].sum().sort_values(ascending=False)
    print(receita_categoria)
