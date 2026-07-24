"""Exemplo 8 — Interop pyarrow <-> pandas: Table -> DataFrame -> Table.

Conceitos:
- `table.to_pandas(types_mapper=pd.ArrowDtype)` converte uma Table Arrow em
  DataFrame **sem copiar os buffers**: as colunas viram `ArrowDtype`, apoiadas
  nos mesmos arrays Arrow. É a ponte que permite usar a API do pandas
  sobre dados carregados pelo pyarrow.
- `table.to_pandas()` sem `types_mapper` usa o backend clássico (numpy): copia
  os dados e degrada tipos — int com nulo vira float64+NaN, string vira
  `object`, date vira `object`. Evite em ETL.
- `pa.Table.from_pandas(df)` faz o caminho de volta; com DataFrame de backend
  Arrow, também é zero-copy. `preserve_index=False` evita a coluna
  `__index_level_0__` fantasma no parquet.
- O roundtrip Table -> DataFrame (ArrowDtype) -> Table preserva schema e
  valores fielmente.

Rode com: `uv run examples/08_pandas_interop.py`
"""

import time

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from _common import orders_dataset, section

if __name__ == "__main__":
    # ~5.6M linhas do mês 1, materializadas em um chunk único
    table = orders_dataset().to_table(filter=pc.field("order_month") == 1).combine_chunks()

    section("Table -> DataFrame com backend Arrow (types_mapper=pd.ArrowDtype)")
    inicio = time.perf_counter()
    df_arrow = table.to_pandas(types_mapper=pd.ArrowDtype)
    t_arrow = time.perf_counter() - inicio
    print(df_arrow.dtypes)
    print(f"\nconversão de {len(df_arrow):,} linhas: {t_arrow * 1000:.0f}ms (zero-copy)")

    section("Prova do zero-copy: DataFrame e Table apontam para o MESMO buffer")
    buffer_tabela = table["quantity"].chunks[0].buffers()[1].address
    buffer_df = df_arrow["quantity"].array._pa_array.chunks[0].buffers()[1].address
    print(f"endereço do buffer na Table:     {buffer_tabela:#x}")
    print(f"endereço do buffer no DataFrame: {buffer_df:#x}")
    print(f"mesmo bloco de memória: {buffer_tabela == buffer_df}")

    section("Comparando com o backend clássico (numpy): cópia e degradação de tipos")
    inicio = time.perf_counter()
    df_numpy = table.to_pandas()  # sem types_mapper
    t_numpy = time.perf_counter() - inicio
    print(df_numpy.dtypes)
    print(f"\nconversão: {t_numpy * 1000:.0f}ms (com cópia)")
    print("note: order_date virou 'object' e status 'object' — strings/datas Python")

    section("Degradação clássica: int com nulo vira float64 + NaN no backend numpy")
    com_nulo = pa.table({"qtd": pa.array([1, None, 3], type=pa.int32())})
    print("backend numpy:", dict(com_nulo.to_pandas().dtypes))
    print("backend Arrow:", dict(com_nulo.to_pandas(types_mapper=pd.ArrowDtype).dtypes))

    section("Manipulando com a API do pandas (terreno familiar)")
    resumo = (
        df_arrow.assign(faixa=pd.cut(df_arrow["quantity"], bins=[0, 3, 7, 10], labels=["baixa", "media", "alta"]))
        .groupby(["status", "faixa"], observed=True)
        .agg(pedidos=("order_id", "count"), qtd_media=("quantity", "mean"))
        .reset_index()
    )
    print(resumo.head(8))

    section("DataFrame -> Table: pa.Table.from_pandas (de volta ao mundo Arrow)")
    resumo["faixa"] = resumo["faixa"].astype(str)  # categorical -> string p/ schema simples
    tabela_resumo = pa.Table.from_pandas(resumo, preserve_index=False)
    print(tabela_resumo.schema)

    section("Roundtrip fiel: Table -> DataFrame (ArrowDtype) -> Table")
    reconvertida = pa.Table.from_pandas(
        table.to_pandas(types_mapper=pd.ArrowDtype), preserve_index=False
    )
    print(f"schema igual ao original: {reconvertida.schema.equals(table.schema)}")
    print(f"dados iguais ao original: {reconvertida.equals(table)}")

    section("Cuidado com o índice: preserve_index=True gera coluna fantasma")
    df_indexado = resumo.set_index("status")
    com_indice = pa.Table.from_pandas(df_indexado)  # default preserva o índice
    sem_indice = pa.Table.from_pandas(df_indexado, preserve_index=False)
    print("com preserve_index (default):", com_indice.column_names)
    print("com preserve_index=False:    ", sem_indice.column_names)
