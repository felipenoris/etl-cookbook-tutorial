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

```bash
uv run examples/01_reading_partitioned_datasets.py
```

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
