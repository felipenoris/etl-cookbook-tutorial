"""Exemplo 10 — Os tipos de dados da stack Arrow: do básico ao aninhado.

As dimensões de `data/raw` foram geradas cobrindo os principais tipos do
Arrow: string, int64, float64, bool, date32, timestamp[us], decimal128(12,2),
list<string>, struct, map<string,string> e binary. Este exemplo mostra como
LER, MANIPULAR (via `pyarrow.compute`) e ESCREVER cada um, com atenção a:

- **decimal128(12,2)**: valores monetários exatos com 2 casas decimais — a
  aritmética permanece decimal (sem os erros de arredondamento do float64);
- **tipos aninhados** (list/struct/map): kernels dedicados
  (`list_value_length`, `struct_field`, `map_lookup`) em vez de loops Python;
- **roundtrip parquet**: todos esses tipos sobrevivem a escrita+leitura com
  schema idêntico — o parquet tem tipos lógicos para cada um.

Rode com: `uv run examples/10_data_types.py`
"""

from decimal import Decimal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from _common import REPO_ROOT, customers_dataset, products_dataset, section

OUT_DIR = REPO_ROOT / "data" / "rich" / "pyarrow_types_demo"

if __name__ == "__main__":
    customers = customers_dataset().to_table()
    products = products_dataset().to_table()

    section("O schema conta tudo: tipos lógicos de cada coluna")
    print(customers.schema)
    print(products.schema)

    section("bool: contagem e filtro vetorizados")
    ativos = pc.sum(pc.cast(customers["is_active"], "int32")).as_py()
    print(f"clientes ativos: {ativos} de {customers.num_rows}")

    section("date32: a ponte com o Python é o datetime.date da stdlib")
    primeira = customers["signup_date"][0].as_py()
    print(f".as_py() devolve: {primeira!r} (tipo: {type(primeira).__module__}.{type(primeira).__name__})")
    # na construção, pa.array aceita datetime.date diretamente
    from datetime import date

    datas = pa.array([date(2026, 1, 1), date(2026, 12, 25)], type=pa.date32())
    print(f"construído de datetime.date: {datas.to_pylist()}")

    section("timestamp[us]: extração de componentes com pc.hour/pc.day_of_week")
    horas = pc.hour(customers["signup_ts"])
    print("cadastros por madrugada (0-5h):", pc.sum(pc.cast(pc.less(horas, 6), "int32")).as_py())

    section("struct: acessando campos com pc.struct_field (sem loop Python)")
    cidades = pc.struct_field(customers["address"], "city")
    print(pa.table({"city": cidades}).group_by("city").aggregate([("city", "count")]).slice(0, 5))

    section("list<string>: tamanho, flatten e filtro por conteúdo")
    print("nº de tags do 1º cliente:", pc.list_value_length(customers["tags"])[0].as_py())
    todas_as_tags = pc.list_flatten(customers["tags"])
    print("tags no total:", len(todas_as_tags), "| distintas:", pc.unique(todas_as_tags).to_pylist())

    section("map<string,string>: lookup por chave com pc.map_lookup")
    canais = pc.map_lookup(customers["preferences"], pa.scalar("canal"), "first")
    print(pa.table({"canal": canais}).group_by("canal").aggregate([("canal", "count")]))

    section("decimal128(12,2): aritmética exata com 2 casas decimais")
    custo_total = pc.sum(products["unit_cost"])
    print(f"soma dos custos: {custo_total.as_py()} (tipo Arrow: {custo_total.type})")
    # a ponte com o Python é o decimal.Decimal da stdlib: .as_py() devolve um,
    # e pa.array(...) aceita Decimals na construção (ver bloco manual abaixo)
    print(f"em Python, .as_py() devolve: {type(custo_total.as_py()).__module__}."
          f"{type(custo_total.as_py()).__name__}")
    # multiplicar decimal por inteiro preserva decimal; note a escala do resultado
    dobro = pc.multiply(products["unit_cost"], pa.scalar(2, type=pa.int32()))
    print(f"custo em dobro (1º produto): {dobro[0].as_py()} (tipo: {dobro.type})")
    # armadilha do float64 para dinheiro: 0.10 + 0.20 não é 0.30 em binário
    dec = pa.array([Decimal("0.10"), Decimal("0.20")], type=pa.decimal128(12, 2))
    flt = pc.cast(dec, "float64")
    print(f"decimal: 0.10 + 0.20 = {pc.sum(dec).as_py()}")
    print(f"float64: 0.10 + 0.20 = {pc.sum(flt).as_py()!r}  <- por isso decimal p/ dinheiro")

    section("binary: bytes opacos, com kernels próprios")
    print("tamanho do sku (bytes):", pc.binary_length(products["sku"])[0].as_py())
    print("sku do 1º produto (hex):", products["sku"][0].as_py().hex())

    section("Construindo arrays de cada tipo do zero (o caminho da escrita)")
    manual = pa.table(
        {
            "d": pa.array([Decimal("19.90"), Decimal("5.00")], type=pa.decimal128(12, 2)),
            "l": pa.array([["a", "b"], []], type=pa.list_(pa.string())),
            "s": pa.array([{"x": 1}, {"x": 2}], type=pa.struct([("x", pa.int32())])),
            "m": pa.array([[("k", "v")], []], type=pa.map_(pa.string(), pa.string())),
            "b": pa.array([b"\x01\x02", b"\xff"], type=pa.binary()),
        }
    )
    print(manual.schema)

    section("Roundtrip parquet: schema idêntico após escrita+leitura")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    destino = OUT_DIR / "customers_roundtrip.parquet"
    pq.write_table(customers, destino)
    relido = pq.read_table(destino)
    print(f"schema preservado: {relido.schema.equals(customers.schema)}")
    print(f"dados idênticos:   {relido.equals(customers)}")
