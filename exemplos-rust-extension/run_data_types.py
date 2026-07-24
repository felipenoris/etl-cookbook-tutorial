"""Tipos de dados Arrow atravessando a fronteira Python -> Rust -> Python.

As dimensões de ``data/raw`` cobrem os tipos da stack (bool, timestamp,
struct, list, map, decimal128(12,2), binary), e as funções Rust deste exemplo
os manipulam DO LADO NATIVO, via pyo3-arrow, sem criar objetos Python por
linha.

Vale nomear as camadas do DECIMAL e das DATAS, porque cada uma tem um tipo:

- **na coluna** (Arrow/Parquet): ``decimal128(12,2)`` e ``date32`` — é o que
  os schemas mostram e o que viaja nos RecordBatches;
- **no cálculo, em Rust**: ``rust_decimal::Decimal`` e ``chrono::NaiveDate``
  — as células são convertidas para esses tipos para a aritmética
  (exata/calendário) e voltam para a coluna Arrow na saída;
- **no escalar, em Python**: ``decimal.Decimal`` e ``datetime.date`` da
  stdlib — o que ``.as_py()`` devolve, o que as funções aceitam como
  parâmetro, e o que as features ``rust_decimal``/``chrono`` do pyo3
  convertem automaticamente na fronteira.

As funções exercitadas:

- ``roundtrip_all_types(batch)``: o teste integral — um batch com UMA coluna
  de cada um dos 11 tipos entra no Rust, e um batch com o MESMO schema volta,
  com cada coluna derivada da entrada (leitura E escrita de todos os tipos).
- ``flatten_customer_profile(batch, reference_date)``: struct, list, map,
  timestamp e bool sobre os dados reais. O ``reference_date`` atravessa a
  fronteira como **``datetime.date`` -> ``chrono::NaiveDate``** (feature
  ``chrono`` do pyo3), a aritmética de dias usa o calendário do chrono, e a
  coluna ``signup_date`` volta como date32.
- ``compute_product_margin(batch, desconto)``: aritmética em
  ``rust_decimal::Decimal``; o ``desconto`` atravessa como
  **``decimal.Decimal`` -> ``rust_decimal::Decimal``** (feature
  ``rust_decimal``) — um float no lugar é rejeitado.
- ``sum_decimal_column(batch, coluna)``: a volta escalar — o total sai do
  Rust como ``rust_decimal::Decimal`` e chega como ``decimal.Decimal``.

Rode com: ``uv run run_data_types.py`` (a partir de ``rust-extension``).
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds

from etl_rust_ext import (
    compute_product_margin,
    flatten_customer_profile,
    roundtrip_all_types,
    sum_decimal_column,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw"


def make_all_types_batch() -> pa.RecordBatch:
    """Constrói, em Python, um batch com uma coluna de cada tipo da stack.

    Note os tipos Python usados na construção: ``str``, ``int``, ``float``,
    ``bool``, **``datetime.date``**, ``datetime.datetime``,
    **``decimal.Decimal``**, ``list``, ``dict`` (struct), pares (map) e
    ``bytes`` — o pyarrow converte cada um para o tipo Arrow declarado.
    """
    return pa.record_batch(
        {
            "texto": pa.array(["ana", "bia"], type=pa.string()),
            "inteiro": pa.array([1, 2], type=pa.int64()),
            "flutuante": pa.array([1.5, 2.5], type=pa.float64()),
            "logico": pa.array([True, False]),
            "data": pa.array([date(2026, 1, 1), date(2026, 2, 28)], type=pa.date32()),
            "instante": pa.array(
                [datetime(2026, 1, 1, 23, 30), datetime(2026, 6, 15, 12, 0)],
                type=pa.timestamp("us"),
            ),
            "valor": pa.array([Decimal("19.90"), Decimal("0.05")], type=pa.decimal128(12, 2)),
            "lista": pa.array([["a", "b"], ["c"]], type=pa.list_(pa.string())),
            "estrutura": pa.array(
                [{"nome": "caixa", "quantidade": 3}, {"nome": "mesa", "quantidade": 5}],
                type=pa.struct([("nome", pa.string()), ("quantidade", pa.int32())]),
            ),
            "mapa": pa.array(
                [[("k1", "v1"), ("k2", "v2")], [("k3", "v3")]],
                type=pa.map_(pa.string(), pa.string()),
            ),
            "binario": pa.array([b"\x01\x02\x03", b"\xff\x00"], type=pa.binary()),
        }
    )


def main() -> None:
    customers = ds.dataset(RAW_DIR / "customers", format="parquet", partitioning="hive")
    products = ds.dataset(RAW_DIR / "products", format="parquet")

    print("[1/3] roundtrip_all_types: os 11 tipos, ida E volta pelo Rust")
    entrada = make_all_types_batch()
    saida = roundtrip_all_types(entrada)
    for nome in entrada.schema.names:
        antes = entrada.column(nome).to_pylist()[0]
        depois = saida.column(nome).to_pylist()[0]
        tipo = entrada.schema.field(nome).type
        print(f"  {nome:10s} {str(tipo):24s} {antes!r:42s} -> {depois!r}")
    mesmos_tipos = all(
        entrada.schema.field(c).type == saida.schema.field(c).type for c in entrada.schema.names
    )
    print(f"  tipos preservados em todas as colunas: {mesmos_tipos}\n")

    print("[2/3] flatten_customer_profile: datetime.date -> chrono::NaiveDate")
    batch = customers.to_table().combine_chunks().to_batches()[0]
    perfil = flatten_customer_profile(batch, date(2026, 1, 1))  # datetime.date puro
    tabela = pa.Table.from_batches([perfil])
    print(tabela.schema)
    print(tabela.slice(0, 5).to_pandas().to_string(index=False))
    primeira_data = tabela["signup_date"][0].as_py()
    print(f"signup_date[0] no Python: {primeira_data!r} "
          f"(tipo: {type(primeira_data).__module__}.{type(primeira_data).__name__})\n")

    print("[3/3] compute_product_margin: decimal.Decimal -> rust_decimal::Decimal")
    batch = products.to_table().combine_chunks().to_batches()[0]
    margens = pa.Table.from_batches([compute_product_margin(batch, desconto=Decimal("0.10"))])
    print(margens.slice(0, 3).to_pandas().to_string(index=False))
    total = sum_decimal_column(margens.to_batches()[0], "margin")
    print(f"\nsoma das margens (10% desc.): {total} "
          f"(tipo Python: {type(total).__name__} — Rust devolveu rust_decimal::Decimal)")
    try:
        compute_product_margin(batch, desconto=0.10)  # float: proibido
    except TypeError as exc:
        print(f"desconto como float -> TypeError: {exc}")


if __name__ == "__main__":
    main()
