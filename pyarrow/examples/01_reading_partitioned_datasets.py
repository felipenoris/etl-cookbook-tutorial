"""Exemplo 1 — Lendo datasets parquet particionados com pyarrow.dataset.

Conceitos:
- `pyarrow.dataset.dataset(...)` mapeia um diretório inteiro (com subpastas
  `chave=valor` no estilo Hive) sem carregar nada em memória ainda — só
  descobre o schema e os arquivos.
- `.to_table(filter=...)` aplica *predicate pushdown*: quando o filtro usa uma
  coluna de partição, pyarrow pode descartar arquivos inteiros sem sequer
  abri-los (partition pruning).
- `.schema` para inspecionar tipos sem ler dados.

Rode com: `uv run examples/01_reading_partitioned_datasets.py`
"""

import pyarrow.compute as pc

from _common import customers_dataset, orders_dataset, products_dataset, section

if __name__ == "__main__":
    orders = orders_dataset()

    section("Schema do dataset particionado de orders")
    print(orders.schema)

    section("Arquivos físicos descobertos (um por partição order_year/order_month)")
    for f in orders.files:
        print(f)

    section("Partition pruning: filtrando por order_month (coluna de partição)")
    only_march = orders.to_table(filter=pc.field("order_month") == 3)
    print(f"{only_march.num_rows:,} linhas lidas (só a partição de março)")

    section("Filtro combinando coluna de partição + coluna normal")
    filtrado = orders.to_table(
        filter=(pc.field("order_month") == 1) & (pc.field("quantity") >= 8)
    )
    print(f"{filtrado.num_rows:,} linhas com quantity >= 8 em janeiro")

    section("Dataset de customers particionado por region")
    customers = customers_dataset()
    print(customers.schema)
    norte = customers.to_table(filter=pc.field("region") == "norte")
    print(f"{norte.num_rows} clientes na região norte")

    section("Dataset de products (arquivo único, sem partições)")
    products = products_dataset()
    print(products.schema)
    print(f"{products.count_rows()} produtos")
