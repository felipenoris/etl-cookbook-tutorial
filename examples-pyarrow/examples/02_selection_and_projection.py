"""Exemplo 2 — Seleção/projeção de colunas e casts com pyarrow.compute.

Conceitos:
- Projeção de colunas já na leitura (`columns=[...]`), evitando materializar
  colunas que não serão usadas — mais barato que ler tudo e descartar depois.
- `Table.select()` para projeção após a leitura.
- `Table.slice()` para fatiar linhas (equivalente a `.iloc` do pandas).
- Casts explícitos com `pyarrow.compute.cast` / `Array.cast`.

Rode com: `uv run examples/02_selection_and_projection.py`
"""

import pyarrow as pa
import pyarrow.compute as pc

from _common import orders_dataset, section

if __name__ == "__main__":
    orders_ds = orders_dataset()

    section("Projeção já na leitura (columns=)")
    tabela = orders_ds.to_table(
        columns=["order_id", "customer_id", "quantity", "status"],
        filter=pc.field("order_month") == 1,
    )
    print(tabela.schema)
    print(f"{tabela.num_rows:,} linhas, {tabela.num_columns} colunas")

    section("Table.select(): projeção depois de já ter a tabela em mãos")
    so_status = tabela.select(["order_id", "status"])
    print(so_status.slice(0, 3))

    section("Table.slice(): fatiar linhas por posição (como .iloc)")
    print(tabela.slice(offset=10, length=3))

    section("Cast de coluna com pyarrow.compute.cast")
    quantity_float = pc.cast(tabela["quantity"], pa.float64())
    print(quantity_float.type, quantity_float[:3])

    section("Adicionando uma coluna calculada com append_column")
    dobro = pc.multiply(tabela["quantity"], pa.scalar(2, type=pa.int32()))
    com_dobro = tabela.append_column("quantity_x2", dobro)
    print(com_dobro.select(["quantity", "quantity_x2"]).slice(0, 3))

    section("Renomeando colunas")
    renomeado = tabela.rename_columns(
        {"order_id": "id_pedido", "customer_id": "id_cliente", "quantity": "qtd", "status": "situacao"}
    )
    print(renomeado.column_names)
