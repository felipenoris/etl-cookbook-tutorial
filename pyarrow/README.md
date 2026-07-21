# pyarrow — exemplos com a API nativa do Arrow

Projeto Python isolado (gerenciado com `uv`) que exercita a API do pyarrow
diretamente (datasets, compute, joins, construção de tabelas), lendo os dados
fictícios em [`../data/raw`](../data/raw).

## Setup

```bash
cd pyarrow
uv sync
```

## Exemplos

| Script | Conceitos |
| --- | --- |
| `01_reading_partitioned_datasets.py` | `pyarrow.dataset`, hive partitioning, partition pruning via `filter=` |
| `02_selection_and_projection.py` | projeção na leitura, `Table.select`/`.slice`, casts com `pyarrow.compute` |
| `03_data_cleaning.py` | `is_null`/`fill_null`/`drop_null`, `value_counts`, `dictionary_encode` |
| `04_groupby_aggregation.py` | `Table.group_by(...).aggregate(...)`, múltiplas chaves, `count_distinct` |
| `05_joins.py` | `Table.join` (inner/left outer), join encadeado de 3 tabelas |
| `06_building_tables_from_functions.py` | `pa.table`, `RecordBatch` manual, UDF Python + `pa.array`, `from_pylist`/`from_batches` |
| `07_sorting_and_pivot_like.py` | `sort_by`, `pc.rank`, "pivot" manual via `group_by` + reshape |
| `08_pandas_interop.py` | Table -> DataFrame (`types_mapper=pd.ArrowDtype`, zero-copy provado por endereço de buffer) e DataFrame -> Table (`from_pandas`, `preserve_index`), roundtrip fiel |
| `09_hybrid_pandas_etl.py` | padrão híbrido: `to_batches` (streaming), lógica de negócio em pandas puro, `ParquetWriter` incremental, `delete_matching` (recarga idempotente) |
| `10_data_types.py` | todos os tipos da stack: decimal(12,2) exato, list/struct/map (kernels), binary, construção manual e roundtrip parquet |

```bash
uv run examples/01_reading_partitioned_datasets.py
```

## Estratégia para equipes proficientes em pandas

O interop pyarrow <-> pandas com backend Arrow é **zero-copy nos dois
sentidos** — um DataFrame `ArrowDtype` e uma `Table` compartilham os mesmos
buffers (o exemplo 08 prova comparando endereços de memória). Isso viabiliza
o desenho do exemplo 09, recomendado quando pandas é a ferramenta dominante:

- **pyarrow nas bordas**: leitura de datasets particionados (pruning),
  streaming em lotes (`to_batches`) e escrita parquet (incremental ou
  particionada idempotente);
- **pandas no miolo**: a lógica de negócio numa API já dominada,
  recebendo cada lote como DataFrame sem custo de conversão.

Não é preciso migrar para `pyarrow.compute` — bastam as conversões
(`to_pandas(types_mapper=pd.ArrowDtype)` / `Table.from_pandas`) e manter o
backend Arrow ponta a ponta (sem ele, cada conversão copia os dados e degrada
tipos — int com nulo vira float64, string vira object).

## Nota sobre tipos inferidos em partições Hive

Ao ler `data/raw/orders` (particionado por `order_year=2025/order_month=01`),
o pyarrow infere `order_month` como `int32` a partir do nome do diretório —
por isso os filtros usam `pc.field("order_month") == 1`, não `"01"` (comparar
tipos diferentes lança `ArrowNotImplementedError`). Já `region`, em
`data/raw/customers`, vira `dictionary<string>` por não ser numérico.

## Testes

```bash
uv run pytest
```

Os testes em `tests/` fazem duas coisas: rodam cada script de `examples/` num
subprocesso (smoke test — o exemplo inteiro deve executar sem erro) e validam
os contratos que os exemplos assumem (schema/dtypes dos dados, integridade das
chaves de join, comportamento das operações principais).

## Referências

- [pyarrow — documentação oficial](https://arrow.apache.org/docs/python/index.html) — ponto de entrada da documentação Python do Arrow.
- [API reference do pyarrow](https://arrow.apache.org/docs/python/api.html) — referência completa (`Table`, `RecordBatch`, `Array`, `Schema`...).
- [Compute Functions](https://arrow.apache.org/docs/python/compute.html) — o módulo `pyarrow.compute` usado nos exemplos de filtro, agregação e transformação.
- [Tabular Datasets](https://arrow.apache.org/docs/python/dataset.html) — leitura de datasets particionados (Hive), filtros com partition pruning e escrita com `write_dataset`.
- [Reading and Writing Parquet](https://arrow.apache.org/docs/python/parquet.html) — detalhes do `pyarrow.parquet` (row groups, compressão, metadados).
- [Apache Arrow Cookbook (Python)](https://arrow.apache.org/cookbook/py/) — receitas oficiais de casos práticos.
- [Formato colunar Arrow](https://arrow.apache.org/docs/format/Columnar.html) — a especificação da representação em memória; leitura recomendada para entender por que o interop com pandas/DuckDB/Rust é zero-copy.
