# pandas — exemplos com backend Arrow

Projeto Python isolado (gerenciado com `uv`) que exercita a API do pandas sempre
usando o **Arrow como backend** de armazenamento das colunas (`dtype_backend="pyarrow"`
+ `engine="pyarrow"` na leitura de parquet), lendo os dados fictícios em
[`../data/raw`](../data/raw).

## Setup

```bash
cd pandas
uv sync
```

## Exemplos

Cada arquivo em `examples/` é independente e roda com `uv run`:

| Script | Conceitos |
| --- | --- |
| `01_loading_and_dtypes.py` | leitura com backend Arrow, `ArrowDtype`, conversão de tipos, accessor `.dt` |
| `02_selection_and_filtering.py` | `.loc`/`.iloc`, máscaras booleanas, `.query()`, `.isin()` |
| `03_data_cleaning.py` | nulos (`fillna`/`dropna`), `.str`, duplicatas, `category` |
| `04_groupby_aggregation.py` | `groupby().agg()` (named aggregation), `.transform()`, `.apply()` |
| `05_merge_join.py` | `merge` (inner/left, `validate=`), join encadeado, `.join()` por índice |
| `06_pivot_table.py` | `pivot_table` (com `aggfunc`/`margins`), `melt`, `pivot` |
| `07_index_manipulation.py` | `set_index`/`reset_index`, `MultiIndex`, `sort_index`, `stack`/`unstack`, `reindex` |
| `08_window_and_time.py` | `resample`, `rolling`, `cumsum`/`cumcount` |

```bash
uv run examples/01_loading_and_dtypes.py
```

## Por que backend Arrow?

Por padrão, o pandas guarda strings como `object` (ponteiros Python) e não tem um
jeito nativo de representar inteiros/nulos sem promover tudo para `float64`. Com
`dtype_backend="pyarrow"`, as colunas passam a ser `ArrowDtype`, apoiadas
diretamente em arrays Arrow — mais compactas, com suporte nativo a nulos em
qualquer tipo, e compatíveis (sem cópia) com pyarrow/DuckDB, que são explorados
nas pastas [`../pyarrow`](../pyarrow) e [`../DuckDB`](../DuckDB).

## Testes

```bash
uv run pytest
```

Os testes em `tests/` fazem duas coisas: rodam cada script de `examples/` num
subprocesso (smoke test — o exemplo inteiro deve executar sem erro) e validam
os contratos que os exemplos assumem (schema/dtypes dos dados, integridade das
chaves de join, comportamento das operações principais).
