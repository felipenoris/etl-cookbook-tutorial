"""Exemplo 3 — Limpeza de dados com pyarrow.compute: nulos e dictionary encoding.

Conceitos:
- `pc.is_null`/`pc.is_valid` para máscaras de nulos; `pc.fill_null` para
  substituir; `Table.drop_null` para remover linhas com qualquer nulo.
- `pc.unique`/`pc.value_counts` para inspecionar cardinalidade de uma coluna.
- `Array.dictionary_encode()` para compactar colunas de baixa cardinalidade
  (equivalente ao `category` do pandas).

Rode com: `uv run examples/03_data_cleaning.py`
"""

import pyarrow as pa
import pyarrow.compute as pc

from _common import orders_dataset, section

if __name__ == "__main__":
    orders = orders_dataset().to_table(filter=pc.field("order_month") == 1)
    orders = orders.slice(0, 20_000)

    section("Introduzindo nulos artificiais em quantity para o exemplo")
    quantity = orders["quantity"].to_pylist()
    for i in range(0, len(quantity), 20):
        quantity[i] = None
    orders = orders.set_column(
        orders.schema.get_field_index("quantity"), "quantity", pa.array(quantity, type=pa.int32())
    )
    print(f"nulos em quantity: {pc.sum(pc.is_null(orders['quantity'])).as_py()}")

    section("fill_null: substitui nulos por um valor fixo")
    preenchido = pc.fill_null(orders["quantity"], 0)
    print(f"nulos após fill_null: {pc.sum(pc.is_null(preenchido)).as_py()}")

    section("drop_null: remove linhas com qualquer nulo")
    sem_nulos = orders.drop_null()
    print(f"{orders.num_rows:,} -> {sem_nulos.num_rows:,} linhas")

    section("value_counts: cardinalidade de status")
    print(pc.value_counts(orders["status"]))

    section("dictionary_encode: comprime coluna de baixa cardinalidade")
    status_encoded = orders["status"].combine_chunks().dictionary_encode()
    print(status_encoded.type)
    print(f"bytes (string): {orders['status'].nbytes}, bytes (dictionary): {status_encoded.nbytes}")
