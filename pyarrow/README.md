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
| `06_building_tables_from_functions.py` | `pa.table`, `RecordBatch` manual, função Python (UDF) + `pa.array`, `from_pylist`/`from_batches` |
| `07_sorting_and_pivot_like.py` | `sort_by`, `pc.rank`, "pivot" manual via `group_by` + reshape |
| `08_pandas_interop.py` | Table -> DataFrame (`types_mapper=pd.ArrowDtype`, zero-copy provado por endereço de buffer) e DataFrame -> Table (`from_pandas`, `preserve_index`), roundtrip fiel |
| `09_hybrid_pandas_etl.py` | padrão híbrido: `to_batches` (streaming), lógica de negócio em pandas puro, `ParquetWriter` incremental, `delete_matching` (recarga idempotente) |
| `10_data_types.py` | todos os tipos da stack: decimal(12,2) exato, list/struct/map (kernels), binary, construção manual e roundtrip parquet |
| `11_sequential_stateful_loop.py` | lógica sequencial com estado, em lotes (streaming), no lado Python — o análogo do `compute_customer_running_spend` do Rust, exercitando a API mesmo sem performar (via `Table.to_batches` + estado; contraste com `group_by/sum`) |
| `12_predicate_pushdown_and_bloom.py` | predicate pushdown por `min`/`max` de row group (ordenado vs embaralhado, medido) e bloom filters (`bloom_filter_options` na escrita, provado pelos metadados) |

```bash
uv run examples/01_reading_partitioned_datasets.py
```

## As três formas de ler menos do Parquet

Ler um parquet rápido é, antes de tudo, **não ler o que não interessa**. São
três mecanismos independentes, que agem em níveis diferentes e se somam — cada
um corresponde a um exemplo deste projeto:

| Técnica | Elimina o quê | Usa o quê | Exemplo |
| --- | --- | --- | --- |
| **Partition pruning** | pastas/arquivos inteiros | o caminho Hive `coluna=valor/` | `01_reading_partitioned_datasets.py` |
| **Predicate pushdown** | *row groups*/páginas dentro do arquivo | estatísticas `min`/`max` (e bloom filter) | `12_predicate_pushdown_and_bloom.py` |
| **Projection pushdown** | colunas que você não pediu | o layout colunar (lê só os *column chunks* do `select`) | `02_selection_and_projection.py` |

Num scan de `orders` com `filter=(pc.field("order_month") == 1) & (pc.field("amount") > 1000)`
e `columns=["order_id", "amount"]`: o *pruning* descarta as pastas dos outros
meses, o *predicate pushdown* pula os row groups de janeiro cujo `max(amount) <= 1000`,
e a *projeção* lê só as duas colunas pedidas.

Dois detalhes que o exemplo 12 mede sobre os dados reais:

- **Ordenar não é requisito do predicate pushdown, mas é o que o torna eficaz.**
  Todo row group carrega `min`/`max` de cada coluna; o filtro pula um bloco
  quando prova que nenhuma linha dele passa. Com os dados ordenados pela coluna
  do filtro, as faixas `[min, max]` ficam estreitas e sem sobreposição e quase
  todos os blocos são descartados; embaralhados, cada faixa é larga e nada é
  pulado (o exemplo mede 9/10 vs 0/10 row groups puláveis).
- **Bloom filter é a exceção que cobre a igualdade em alta cardinalidade** — o
  caso em que `min`/`max` não ajuda (todo intervalo contém o valor buscado). Ele
  **precisa ser ligado na escrita** (`bloom_filter_options` do `write_table`) e
  fica gravado dentro do arquivo, a um custo em disco que o exemplo quantifica.
  Aqui os dois writers da stack divergem: no **pyarrow** o bloom é *opt-in* por
  coluna, enquanto o **DuckDB** (`COPY … FORMAT parquet`) o grava
  automaticamente, mas só quando os distintos por row group são ≤ 20% das linhas
  — logo, pula colunas quase-únicas (medições na Parte B do exemplo 12).
- **As estatísticas `min`/`max`/`null_count` vêm ligadas por padrão**
  (`write_statistics=True`), então o predicate pushdown funciona "de graça" na
  maioria dos parquets — só se perde se alguém escrever com
  `write_statistics=False` (o bloco some inteiro, `null_count` incluído). Numa
  coluna toda nula, `min`/`max` ficam ausentes, mas o `null_count` permanece.

Isso vale para toda a stack Arrow: o **DuckDB** aplica os mesmos três mecanismos
sobre os mesmos parquets (veja `../DuckDB`, exemplos 02 e 12).

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
