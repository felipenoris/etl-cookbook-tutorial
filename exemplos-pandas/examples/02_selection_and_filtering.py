"""Exemplo 2 — Seleção de colunas e filtragem de linhas.

Conceitos:
- Seleção de colunas por lista (`df[[...]]`), por posição (`.iloc`) e por
  rótulo (`.loc`).
- Filtragem por máscara booleana e pelo método `.query()` (mais legível para
  expressões compostas).
- Combinação de múltiplas condições com `&`/`|` (não usar `and`/`or` — atuam
  elemento a elemento, não em série inteira).

Rode com: `uv run examples/02_selection_and_filtering.py`
"""

from _common import load_orders, section

if __name__ == "__main__":
    orders = load_orders([1])

    section("Seleção de colunas por lista")
    subset = orders[["order_id", "customer_id", "quantity", "status"]]
    print(subset.head(3))

    section("Seleção por posição com .iloc (linhas 0-2, colunas 0-1)")
    print(orders.iloc[0:3, 0:2])

    section("Seleção por rótulo com .loc (linhas 0-2, colunas nomeadas)")
    print(orders.loc[0:2, ["order_id", "status"]])

    section("Filtro booleano: pedidos entregues com quantidade >= 5")
    mask = (orders["status"] == "entregue") & (orders["quantity"] >= 5)
    filtered = orders[mask]
    print(f"{len(filtered)} de {len(orders)} pedidos atendem ao filtro")
    print(filtered.head(3))

    section("O mesmo filtro com .query() (mais legível para regras compostas)")
    filtered_query = orders.query("status == 'entregue' and quantity >= 5")
    print(f"{len(filtered_query)} linhas (deve ser igual ao filtro booleano acima)")

    section("Filtro com .isin() para múltiplos valores")
    urgent_statuses = orders[orders["status"].isin(["novo", "enviado"])]
    print(urgent_statuses["status"].value_counts())

    section("Selecionando colunas por dtype (select_dtypes)")
    print(orders.select_dtypes(include="number").columns.tolist())
