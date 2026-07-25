"""Exemplo 9 — Tipos Arrow no pandas: bool, timestamp, decimal, list, struct, map e binary.

Com `dtype_backend="pyarrow"`, o pandas carrega TODOS os tipos do Arrow — até
os que não existem no pandas clássico (decimal exato, list, struct, map,
binary). Este exemplo mostra como cada um aparece num DataFrame e como
manipulá-los:

- acessores dedicados: `.list` (tamanho, indexação) e `.struct` (campos);
- **map** não tem acessor no pandas — a saída é usar a "escotilha" para o
  pyarrow (`pc.map_lookup` sobre a coluna) e voltar, sem cópia;
- **decimal128(12,2)**: agregações preservam o tipo exato — dinheiro não
  vira float64 no meio do caminho;
- escrita: `to_parquet` preserva todos esses tipos (roundtrip fiel).

Rode com: `uv run examples/09_arrow_data_types.py`
"""

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from _common import RAW_DIR, load_customers, load_products, section

OUT_DIR = RAW_DIR.parent / "rich" / "pandas_types_demo"

if __name__ == "__main__":
    customers = load_customers()
    products = load_products()

    section("Como cada tipo Arrow aparece nos dtypes do DataFrame")
    print(customers.dtypes)
    print()
    print(products.dtypes[["unit_cost", "sku"]])

    section("bool[pyarrow]: filtros e agregações diretas")
    print(f"clientes ativos: {customers['is_active'].sum()} "
          f"({customers['is_active'].mean():.1%})")

    section("timestamp[us][pyarrow]: acessor .dt completo")
    print(customers["signup_ts"].dt.hour.value_counts().head(3))

    section("list<string>: acessor .list (tamanho e indexação)")
    customers["num_tags"] = customers["tags"].list.len()
    print(customers[["customer_id", "tags", "num_tags"]].head(3).to_string(index=False))
    # pegadinha: .list[0] lança erro se alguma lista for VAZIA (indexação não
    # devolve nulo para fora dos limites) — filtre pelo tamanho antes
    com_tags = customers[customers["num_tags"] > 0]
    print("\nprimeira tag (só de quem tem tags):", com_tags["tags"].list[0].head(3).tolist())

    section("struct: acessor .struct.field p/ achatar campos aninhados")
    customers["city"] = customers["address"].struct.field("city")
    print(customers.groupby("city", observed=True)["customer_id"].count().head(5))

    section("map: sem acessor no pandas — escotilha p/ pyarrow (zero-copy)")
    prefs_arrow = pa.chunked_array(customers["preferences"].array._pa_array)
    canal = pc.map_lookup(prefs_arrow, pa.scalar("canal"), "first")
    customers["canal"] = pd.Series(canal.to_pandas(types_mapper=pd.ArrowDtype))
    print(customers["canal"].value_counts(dropna=False))

    section("decimal128(12,2): agregações permanecem decimais exatos")
    total = products["unit_cost"].sum()
    print(f"soma dos custos: {total} (tipo Python: {type(total).__name__})")
    por_categoria = products.groupby("category", observed=True)["unit_cost"].sum()
    print(por_categoria.head(3))
    print(f"dtype da agregação: {por_categoria.dtype}")

    section("binary[pyarrow]: bytes opacos (hex para exibir)")
    print(products["sku"].head(3).map(lambda b: b.hex()).to_string(index=False))

    section("Roundtrip: to_parquet preserva todos os tipos")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    destino = OUT_DIR / "customers_enriquecido.parquet"
    enriquecido = customers[["customer_id", "is_active", "signup_ts", "address", "tags",
                             "preferences", "num_tags", "city", "canal"]]
    enriquecido.to_parquet(destino, index=False)
    relido = pd.read_parquet(destino, dtype_backend="pyarrow")
    print(f"dtypes preservados: {relido.dtypes.equals(enriquecido.dtypes)}")
    print(relido.dtypes)
