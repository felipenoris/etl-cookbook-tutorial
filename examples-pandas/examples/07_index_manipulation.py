"""Exemplo 7 — Manipulação de índices: set/reset, MultiIndex, sort, stack/unstack.

Conceitos:
- `set_index`/`reset_index` para promover colunas a índice e vice-versa.
- `MultiIndex` (índice composto) criado por `set_index` com várias colunas ou
  por `groupby`.
- `sort_index` vs `sort_values`.
- `stack`/`unstack` para mover um nível do índice de linhas para colunas (e
  vice-versa) — a base do pivot_table por baixo dos panos.
- `reindex` para alinhar um DataFrame a um conjunto de rótulos específico.

Rode com: `uv run examples/07_index_manipulation.py`
"""

from _common import load_orders, section

if __name__ == "__main__":
    orders = load_orders([1]).head(10_000).copy()

    section("set_index / reset_index")
    by_order_id = orders.set_index("order_id")
    print(by_order_id.head(2))
    voltou = by_order_id.reset_index()
    print(f"colunas após reset_index: {voltou.columns.tolist()}")

    section("MultiIndex a partir de groupby (customer_id, status)")
    agrupado = orders.groupby(["customer_id", "status"])["quantity"].sum()
    print(type(agrupado.index))
    print(agrupado.head(4))

    section("Selecionando um nível específico do MultiIndex com .xs()")
    primeiro_cliente = orders["customer_id"].iloc[0]
    print(agrupado.xs(primeiro_cliente, level="customer_id"))

    section("sort_index vs sort_values")
    print(agrupado.sort_index().head(3))
    print(agrupado.sort_values(ascending=False).head(3))

    section("unstack: leva o nível 'status' do índice para colunas")
    largo = agrupado.unstack("status", fill_value=0)
    print(largo.head(3))

    section("stack: operação inversa, volta 'status' para o índice")
    de_volta = largo.stack()
    print(de_volta.head(4))

    section("reindex: alinhar a uma lista fixa de status (preenchendo faltantes com 0)")
    todos_status = ["novo", "enviado", "entregue", "cancelado", "devolvido"]
    print(largo.reindex(columns=todos_status, fill_value=0).head(3))
