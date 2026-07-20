"""Exemplo 7 — Ordenação e um "pivot" manual (Arrow não tem pivot nativo).

Conceitos:
- `Table.sort_by([(coluna, "ascending"|"descending"), ...])`.
- `pyarrow.compute.rank` para ranking dentro de um array.
- Arrow não tem uma operação de pivot embutida (diferente do pandas); o jeito
  idiomático é: `group_by` para agregar, depois reformatar manualmente
  construindo uma coluna por valor distinto — bom contraste didático com o
  `pivot_table` visto em `pandas/examples/06_pivot_table.py`.

Rode com: `uv run examples/07_sorting_and_pivot_like.py`
"""

import pyarrow as pa
import pyarrow.compute as pc

from _common import orders_dataset, products_dataset, section

if __name__ == "__main__":
    orders = orders_dataset().to_table(filter=pc.field("order_month") == 1)
    products = products_dataset().to_table()
    full = orders.join(products, keys="product_id", join_type="inner")
    full = full.append_column(
        "receita", pc.multiply(pc.cast(full["quantity"], "float64"), full["unit_price"])
    )

    section("sort_by: top 5 pedidos por receita")
    print(full.select(["order_id", "category", "receita"]).sort_by([("receita", "descending")]).slice(0, 5))

    section("pc.rank: ranking de receita dentro da tabela inteira")
    ranks = pc.rank(full["receita"], sort_keys="descending")
    com_rank = full.append_column("rank_receita", ranks)
    print(com_rank.select(["order_id", "receita", "rank_receita"]).sort_by([("rank_receita", "ascending")]).slice(0, 5))

    section("Pivot manual: receita por categoria (linhas) x status (colunas)")
    agregado = full.group_by(["category", "status"]).aggregate([("receita", "sum")])

    categorias = sorted(set(agregado["category"].to_pylist()))
    status_distintos = sorted(set(agregado["status"].to_pylist()))

    colunas = {"category": categorias}
    for status in status_distintos:
        valores = []
        for categoria in categorias:
            mask = pc.and_(
                pc.equal(agregado["category"], categoria), pc.equal(agregado["status"], status)
            )
            filtrado = agregado.filter(mask)
            valores.append(filtrado["receita_sum"][0].as_py() if filtrado.num_rows else 0.0)
        colunas[status] = valores

    pivot_manual = pa.table(colunas)
    print(pivot_manual)
