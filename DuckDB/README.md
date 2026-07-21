# DuckDB — conceitos e manipulação de parquet particionado

Projeto Python isolado (gerenciado com `uv`) que exercita a API Python do
DuckDB sobre os dados fictícios em [`../data/raw`](../data/raw).

## Setup

```bash
cd DuckDB
uv sync
```

## Conceitos centrais do DuckDB

- **In-process / embutido**: DuckDB roda dentro do processo Python (como o
  SQLite), sem servidor separado. `duckdb.connect()` (sem argumento, ou
  `":memory:"`) abre um banco em memória; passar um caminho de arquivo `.duckdb`
  persiste o estado entre execuções.
- **Relation API vs SQL puro**: `con.sql("SELECT ...")` roda uma query e
  devolve uma `DuckDBPyRelation` (lazy, só executa de fato quando você pede o
  resultado com `.fetchall()`/`.df()`/`.arrow()`). `con.execute(...)` é mais
  parecido com DB-API (cursor), útil para comandos sem retorno tabular
  (`INSERT`, `SET`, `PRAGMA`).
- **Leitura direta de parquet**: `read_parquet('caminho/**/*.parquet',
  hive_partitioning=true)` lê arquivos particionados sem precisar declarar
  tabelas antes — o DuckDB infere o schema e reconstrói as colunas de
  partição a partir do path.
- **Predicate/partition pruning**: ao filtrar por uma coluna de partição, o
  DuckDB evita abrir arquivos que não podem satisfazer o filtro — visível no
  plano de `EXPLAIN`.
- **Memória e spill em disco**: por padrão o DuckDB usa até ~80% da RAM
  detectada. `SET memory_limit='256MB'` reduz esse teto; `SET
  temp_directory='...'` define onde ele grava buffers temporários quando uma
  operação (sort, join, aggregate) não cabe no limite configurado — o
  chamado *spill to disk*. Isso é o que permite processar arquivos maiores
  que a RAM disponível sem estourar memória.
- **Threads**: `SET threads=N` controla o paralelismo interno do motor
  vetorizado.

## Exemplos

| Script | Conceitos |
| --- | --- |
| `01_connecting_and_querying.py` | `connect()`, `con.sql()` vs `con.execute()`, SELECT sobre glob |
| `02_reading_partitioned_parquet.py` | `hive_partitioning=true`, partition pruning via `EXPLAIN` |
| `03_joins_and_aggregations.py` | join de 3 tabelas, agregações, window functions (`ROW_NUMBER`, `QUALIFY`) |
| `04_memory_limit_and_spill.py` | `memory_limit`, `temp_directory`, forçando spill num sort/aggregate grande |
| `05_pandas_arrow_interop.py` | `.arrow()`/`.df()`, handoff zero-copy com pyarrow e pandas (backend Arrow) |
| `06_copy_to_partitioned.py` | `COPY TO` com `PARTITION_BY`, recarga idempotente de partição, `FILE_SIZE_BYTES` |
| `07_persistent_staging_upsert.py` | banco persistente (`.db`), CTAS, `ATTACH` entre bancos, UPSERT (`ON CONFLICT`) |
| `08_ingestion_and_quality.py` | `read_csv` com sniffer, quarentena (`store_rejects`/`reject_errors`), `SUMMARIZE`, `USING SAMPLE` |
| `09_advanced_sql_transforms.py` | `WITH RECURSIVE` (hierarquia), `PIVOT`/`UNPIVOT`, `ASOF JOIN`, `LIST`/`UNNEST` |
| `10_macros_and_python_udfs.py` | `CREATE MACRO` (escalar e de tabela), UDF Python nativa vs. vetorizada (`type="arrow"`) |
| `11_export_import_and_views_vs_tables.py` | `EXPORT`/`IMPORT DATABASE` (um parquet por tabela + `schema.sql`), view vs. tabela materializada (timing e `EXPLAIN`) |
| `12_performance_without_indexes.py` | o "índice" do mundo parquet: partition pruning + `ORDER BY` na escrita (zonemaps/`parquet_metadata`), leitura colunar, hash join sem índice |
| `13_reading_public_s3.py` | parquet remoto via httpfs: `https://` e `s3://` anônimo (`CREATE SECRET`), range requests, join remoto, glob hive no S3 — **exige internet** (~2MB) |
| `14_data_types.py` | BOOLEAN/TIMESTAMP/DECIMAL(12,2)/STRUCT/LIST/MAP/BLOB: notação de ponto, `[1]`, `map['chave']`, `typeof`, roundtrip COPY |

## Performance sem índices (exemplo 12)

A dúvida clássica de quem vem de bases transacionais: "onde crio o índice?".
Em parquet, não cria — o paralelo é o **layout dos dados**, decidido na
escrita:

- **particionamento** (diretórios) faz o papel do índice na coluna de filtro
  principal (partition pruning, exemplo 02);
- **ordenar na escrita** (`COPY ... ORDER BY coluna`) faz o papel do índice
  nas colunas secundárias: cada row group do parquet guarda min/max por
  coluna (zonemaps), e com os dados clusterizados o DuckDB pula os row
  groups fora da faixa — no exemplo, a consulta pontual abre 1 de 275 row
  groups e fica ~8x mais rápida, com o mesmíssimo dado;
- **JOINs não precisam de índice**: o DuckDB usa hash join (a dimensão vira
  hash table em memória na hora);
- o custo é pago 1x no ETL que grava; toda leitura posterior aproveita.

```bash
uv run examples/01_connecting_and_querying.py
```

## Testes

```bash
uv run pytest                # suíte completa (3 testes exigem internet)
uv run pytest --no-network   # pula os testes marcados com 'network'
```

Os testes em `tests/` fazem duas coisas: rodam cada script de `examples/` num
subprocesso (smoke test — o exemplo inteiro deve executar sem erro) e validam
os contratos que os exemplos assumem (schema/dtypes dos dados, integridade das
chaves de join, comportamento das operações principais).

Os testes do exemplo 13 (leitura de buckets S3/HTTP públicos) são marcados
com `@pytest.mark.network`; a flag `--no-network` (definida em
`tests/conftest.py`) os pula em ambientes sem acesso à internet.

## Referências

- [DuckDB Python API](https://duckdb.org/docs/stable/clients/python/overview) — o client Python usado em todos os exemplos (`duckdb.connect`, `.sql()`, relações).
- [SQL Introduction](https://duckdb.org/docs/stable/sql/introduction) — introdução ao dialeto SQL do DuckDB.
- [Reading Parquet](https://duckdb.org/docs/stable/data/parquet/overview) — `read_parquet`, globs e projeção/filter pushdown.
- [Hive Partitioning](https://duckdb.org/docs/stable/data/partitioning/hive_partitioning) — `hive_partitioning=true` e partition pruning, exercitados no exemplo 02.
- [Configuration](https://duckdb.org/docs/stable/configuration/overview) — referência de `SET`, incluindo `memory_limit`, `temp_directory` e `preserve_insertion_order` usados no exemplo 04 (spill).
- [Tuning Workloads](https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads) — guia de performance: memória, paralelismo e operadores que fazem spill.
- [SQL on Arrow](https://duckdb.org/docs/stable/guides/python/sql_on_arrow) — consulta direta sobre objetos pyarrow e retorno via `.to_arrow_table()`, exercitados no exemplo 05.
