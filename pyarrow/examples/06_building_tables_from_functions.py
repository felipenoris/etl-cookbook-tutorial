"""Exemplo 6 — Construindo Tables/RecordBatches a partir de lógica Python.

Conceitos:
- `pa.table({...})` a partir de dict de listas/arrays — o jeito mais direto de
  criar uma tabela derivada de uma computação qualquer.
- Construção explícita de um `RecordBatch` com schema definido manualmente
  (útil quando se quer controlar tipos com precisão, ex.: antes de repassar
  para uma extensão Rust, como na etapa `rust-extension/`).
- UDF em Python puro aplicada com list comprehension + `pa.array(...)` —
  mais lento que `pyarrow.compute`, mas às vezes é a única opção para lógica
  arbitrária que não existe como função vetorizada pronta.
- `Table.from_batches` / `Table.from_pylist` como outras formas de construção.

Rode com: `uv run examples/06_building_tables_from_functions.py`
"""

import pyarrow as pa
import pyarrow.compute as pc

from _common import orders_dataset, section


def classificar_faixa(quantidade: int) -> str:
    """UDF arbitrária: não existe como função vetorizada pronta no pyarrow.compute."""
    if quantidade <= 2:
        return "baixa"
    if quantidade <= 6:
        return "media"
    return "alta"


if __name__ == "__main__":
    section("pa.table a partir de um dict de listas Python")
    resumo_manual = pa.table(
        {
            "regiao": ["norte", "sul", "sudeste"],
            "meta_receita": [100_000.0, 250_000.0, 500_000.0],
        }
    )
    print(resumo_manual)

    section("RecordBatch construído explicitamente com schema definido à mão")
    schema = pa.schema([("id", pa.int64()), ("score", pa.float64())])
    batch = pa.record_batch(
        [pa.array([1, 2, 3], type=pa.int64()), pa.array([0.1, 0.5, 0.9], type=pa.float64())],
        schema=schema,
    )
    print(batch)

    section("UDF em Python puro: classificando faixa de quantidade")
    orders = orders_dataset().to_table(filter=pc.field("order_month") == 1).slice(0, 5_000)
    faixas = pa.array(
        [classificar_faixa(q) for q in orders["quantity"].to_pylist()], type=pa.string()
    )
    com_faixa = orders.append_column("faixa_quantidade", faixas)
    print(com_faixa.select(["quantity", "faixa_quantidade"]).slice(0, 5))
    print(pc.value_counts(com_faixa["faixa_quantidade"]))

    section("Table.from_pylist: construção a partir de uma lista de dicts (linha a linha)")
    linhas = [{"produto": "A", "preco": 9.9}, {"produto": "B", "preco": 19.9}]
    tabela_de_linhas = pa.Table.from_pylist(linhas)
    print(tabela_de_linhas)

    section("Table.from_batches: juntando vários RecordBatch em uma Table")
    outro_batch = pa.record_batch(
        [pa.array([4], type=pa.int64()), pa.array([0.7], type=pa.float64())], schema=schema
    )
    junto = pa.Table.from_batches([batch, outro_batch])
    print(junto)
