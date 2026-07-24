"""Exemplo 4 — Agrupamento e agregação com Table.group_by.

Conceitos:
- `Table.group_by([...]).aggregate([...])` é a API nativa de groupby do Arrow
  — vetorizada e sem sair do domínio Arrow (zero conversão para Python).
- Agregações múltiplas na mesma chamada, incluindo `count`, `sum`, `mean`,
  `min`, `max`, `stddev`.
- Agrupar por mais de uma coluna.

Rode com: `uv run examples/04_groupby_aggregation.py`
"""

import pyarrow.compute as pc

from _common import orders_dataset, section

if __name__ == "__main__":
    orders = orders_dataset().to_table(filter=pc.field("order_month") == 1)

    section("group_by(status).aggregate: contagem e estatísticas de quantity")
    resumo = orders.group_by("status").aggregate(
        [
            ("order_id", "count"),
            ("quantity", "mean"),
            ("quantity", "sum"),
            ("quantity", "stddev"),
        ]
    )
    print(resumo.sort_by("quantity_sum"))

    section("group_by com múltiplas chaves (customer_id, status)")
    por_cliente_status = (
        orders.group_by(["customer_id", "status"])
        .aggregate([("order_id", "count")])
        .sort_by([("order_id_count", "descending")])
    )
    print(por_cliente_status.slice(0, 5))

    section("Filtrando depois de agregar (equivalente a HAVING)")
    clientes_recorrentes = por_cliente_status.filter(pc.field("order_id_count") >= 5)
    print(f"{clientes_recorrentes.num_rows} combinações cliente/status com 5+ pedidos")

    section("count_distinct dentro de um group_by")
    produtos_distintos_por_status = orders.group_by("status").aggregate(
        [("product_id", "count_distinct")]
    )
    print(produtos_distintos_por_status)
