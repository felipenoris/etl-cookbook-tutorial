"""Exemplo 3 — Limpeza de dados: nulos, duplicatas, strings e categorias.

Conceitos:
- Introduzimos nulos artificialmente para exercitar `isna`/`fillna`/`dropna`
  (o dataset gerado não tem nulos "de fábrica").
- O accessor `.str` funciona normalmente em colunas `string[pyarrow]`.
- `drop_duplicates` e conversão para `category` para colunas de baixa
  cardinalidade (economiza memória e acelera groupby).

Rode com: `uv run examples/03_data_cleaning.py`
"""

import pandas as pd

from _common import load_orders, load_products, section

if __name__ == "__main__":
    orders = load_orders([1]).head(20_000).copy()

    section("Introduzindo nulos artificiais para o exemplo")
    orders.loc[orders.sample(frac=0.05, random_state=0).index, "quantity"] = pd.NA
    print(f"nulos em quantity: {orders['quantity'].isna().sum()}")

    section("dropna: remove linhas com nulo em quantity")
    sem_nulos = orders.dropna(subset=["quantity"])
    print(f"{len(orders)} -> {len(sem_nulos)} linhas")

    section("fillna: preenche com a mediana")
    mediana = orders["quantity"].median()
    preenchido = orders["quantity"].fillna(mediana)
    print(f"mediana usada para preencher: {mediana}")
    print(f"nulos restantes: {preenchido.isna().sum()}")

    section("Normalizando strings com o accessor .str")
    products = load_products()
    products["category_upper"] = products["category"].str.upper()
    products["product_slug"] = products["product_name"].str.replace("_", "-", regex=False)
    print(products[["product_name", "product_slug", "category_upper"]].head(3))

    section("Detectando e removendo duplicatas")
    with_dupes = pd.concat([orders.head(5), orders.head(5)], ignore_index=True)
    print(f"linhas: {len(with_dupes)}, duplicadas: {with_dupes.duplicated().sum()}")
    deduped = with_dupes.drop_duplicates()
    print(f"após drop_duplicates: {len(deduped)} linhas")

    section("Convertendo coluna de baixa cardinalidade para category")
    print(f"memória como string[pyarrow]: {orders['status'].memory_usage(deep=True)} bytes")
    as_category = orders["status"].astype("category")
    print(f"memória como category: {as_category.memory_usage(deep=True)} bytes")
