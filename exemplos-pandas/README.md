# pandas — exemplos com backend Arrow

Projeto Python isolado (gerenciado com `uv`) que exercita a API do pandas sempre
usando o **Arrow como backend** de armazenamento das colunas (`dtype_backend="pyarrow"`
+ `engine="pyarrow"` na leitura de parquet), lendo os dados fictícios em
[`../data/raw`](../data/raw).

## Setup

```bash
cd exemplos-pandas
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
| `09_arrow_data_types.py` | tipos Arrow no pandas: bool, timestamp, decimal(12,2), list/struct (acessores), map (escotilha pyarrow), binary, roundtrip |
| `10_sequential_stateful_loop.py` | lógica sequencial com estado, em lotes (streaming), no lado Python — o análogo do `compute_customer_running_spend` do Rust, exercitando a API mesmo sem performar (via `itertuples` + estado; contraste com `groupby().cumsum()`) |

```bash
uv run examples/01_loading_and_dtypes.py
```

## Por que backend Arrow?

Por padrão, o pandas guarda strings como `object` (ponteiros Python) e não tem um
jeito nativo de representar inteiros/nulos sem promover tudo para `float64`. Com
`dtype_backend="pyarrow"`, as colunas passam a ser `ArrowDtype`, apoiadas
diretamente em arrays Arrow — mais compactas, com suporte nativo a nulos em
qualquer tipo, e compatíveis (sem cópia) com pyarrow/DuckDB, que são explorados
nas pastas [`../exemplos-pyarrow`](../exemplos-pyarrow) e [`../exemplos-DuckDB`](../exemplos-DuckDB).

## Testes

```bash
uv run pytest
```

Os testes em `tests/` fazem duas coisas: rodam cada script de `examples/` num
subprocesso (smoke test — o exemplo inteiro deve executar sem erro) e validam
os contratos que os exemplos assumem (schema/dtypes dos dados, integridade das
chaves de join, comportamento das operações principais).

## Referências

- [User Guide do pandas](https://pandas.pydata.org/docs/user_guide/index.html) — o tutorial oficial, organizado por tema (seleção, groupby, merge, reshape...); espelha bem a sequência dos exemplos desta pasta.
- [10 minutes to pandas](https://pandas.pydata.org/docs/user_guide/10min.html) — visão geral rápida para quem está começando.
- [API reference do pandas](https://pandas.pydata.org/docs/reference/index.html) — referência completa de todas as funções e métodos.
- [PyArrow functionality](https://pandas.pydata.org/docs/user_guide/pyarrow.html) — capítulo do User Guide sobre o backend Arrow (`dtype_backend="pyarrow"` / `ArrowDtype`), o modo usado em todos os exemplos daqui.
- [Cookbook do pandas](https://pandas.pydata.org/docs/user_guide/cookbook.html) — receitas curtas de casos práticos.
- [pyarrow — documentação oficial](https://arrow.apache.org/docs/python/index.html) — útil para entender o que existe por baixo dos `ArrowDtype`; explorado a fundo na pasta [`../exemplos-pyarrow`](../exemplos-pyarrow).
