# DuckDB — conceitos e manipulação de parquet particionado

Projeto Python isolado (gerenciado com `uv`) que exercita a API Python do
DuckDB sobre os dados fictícios em [`../data/raw`](../data/raw).

## Setup

```bash
cd exemplos-DuckDB
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
| `07_persistent_staging_upsert.py` | banco persistente (`.db`), CTAS, `ATTACH` entre bancos, UPSERT (`ON CONFLICT`); paleta de DDL (constraints, `DEFAULT`, coluna gerada, `SEQUENCE`, `CREATE INDEX`, `ALTER`) e a comparação com os parâmetros estilo Hive (`PARTITIONED BY`/`LOCATION`) |
| `08_ingestion_and_quality.py` | `read_csv` com sniffer, quarentena (`store_rejects`/`reject_errors`), `SUMMARIZE`, `USING SAMPLE` |
| `09_advanced_sql_transforms.py` | `WITH RECURSIVE` (hierarquia), `PIVOT`/`UNPIVOT`, `ASOF JOIN`, `LIST`/`UNNEST` |
| `10_macros_and_python_udfs.py` | `CREATE MACRO` (escalar e de tabela), UDF Python (função definida pelo usuário) nativa vs. vetorizada (`type="arrow"`) |
| `11_export_import_and_views_vs_tables.py` | `EXPORT`/`IMPORT DATABASE` (um parquet por tabela + `schema.sql`), view vs. tabela materializada (timing e `EXPLAIN`) |
| `12_performance_without_indexes.py` | o "índice" do mundo parquet: partition pruning + `ORDER BY` na escrita (zonemaps/`parquet_metadata`), leitura colunar, hash join sem índice |
| `13_reading_public_s3.py` | parquet remoto via httpfs: `https://` e `s3://` anônimo (`CREATE SECRET`), range requests, join remoto, glob hive no S3 — **exige internet** (~2MB) |
| `14_data_types.py` | BOOLEAN/TIMESTAMP/DECIMAL(12,2)/STRUCT/LIST/MAP/BLOB: notação de ponto, `[1]`, `map['chave']`, `typeof`, roundtrip COPY |
| `15_sequential_stateful_loop.py` | lógica sequencial com estado, em lotes (streaming), no lado Python — o análogo do `compute_customer_running_spend` do Rust, exercitando a API mesmo sem performar (via `to_arrow_reader` + estado; contraste com `SUM` agrupado) |
| `16_join_performance.py` | JOIN sem agregação: por que índice ART NÃO acelera join (é hash join), e o que acelera de fato — pushdown do filtro até o fato + zonemaps do fato ordenado (medido) |
| `17_multitable_join_spill.py` | JOIN de 5 tabelas (estrela + ponte N:N ponderada por `fator`) sob `memory_limit='100MB'`: `SUM(valor_fluxo * fator)` por área com spill para disco medido; `SET threads=2` para caber no teto |
| `18_explain_analyze_profiling.py` | profiling como ferramenta: `EXPLAIN` (plano estimado) vs `EXPLAIN ANALYZE`, `PRAGMA enable_profiling='json'`, operador dominante, cardinalidade real vs estimada, `Dynamic Filters` no scan |
| `19_json_ingestion_and_extraction.py` | JSON opaco de texto (`->`, `->>`, `json_extract`, caminhos `$.a.b`/`[*]`, `json_keys`) vs `read_json_auto` que sniffa o schema (objeto→STRUCT); contraste com os tipos nativos STRUCT/LIST/MAP do parquet |
| `20_window_functions_advanced.py` | `LAG`/`LEAD` (navegação), `NTILE` (quartis), frames `ROWS` vs `RANGE` (empates/peers), `FIRST_VALUE`/`LAST_VALUE` e a pegadinha do frame padrão |
| `21_transactions_and_mvcc.py` | `BEGIN`/`COMMIT`/`ROLLBACK`, atomicidade sob erro (transação abortada), MVCC/isolamento por snapshot entre conexões, concorrência otimista (conflito na mesma linha) |
| `22_parameterized_queries.py` | placeholders `?`/`$1`/`$nome`, injeção de SQL medida (0 vs 2000 linhas), tipos serializados pelo driver, `PREPARE`/`EXECUTE`, `executemany`, e a ressalva "parâmetro é valor, não identificador" |
| `23_surrogate_keys_returning.py` | chaves primárias sequenciais (surrogate keys): `CREATE SEQUENCE` + `DEFAULT nextval` (não há `IDENTITY`), `RETURNING` para resgatar as chaves geradas em lote, tradução natural→surrogate no fato, carga incremental por anti-join |

## Glossário: comandos além do SQL transacional básico

Vários exemplos usam construções que quem vem de SQL de aplicação (CRUD em
Postgres/MySQL) raramente encontrou. Algumas são **SQL padrão, mas avançadas**
(as *window functions*, `UNNEST`, `CREATE VIEW`); outras são **atalhos ou
extensões do DuckDB** (`QUALIFY`, o cast `::tipo`, `RETURNING`, sequências).
Abaixo, cada uma em uma frase, com o exemplo onde ela aparece medida na prática.

### Casts e views

- **`expr::DECIMAL(18, 2)`** — o operador `::` é um **atalho para `CAST(expr AS
  DECIMAL(18, 2))`** (herdado do Postgres). Converte o tipo de `expr`; aqui fixa
  uma soma como decimal exato de 18 dígitos e 2 casas. Padrão/DuckDB.
  *(exemplos 17, 20, 22)*
- **`CREATE VIEW nome AS SELECT ...`** — registra um **nome reutilizável para uma
  query** (um "atalho salvo"). As consultas seguintes usam `FROM nome` como se
  fosse tabela, mas nada é materializado: a view reexecuta o `SELECT` a cada uso
  (contraste com tabela materializada no exemplo 11). SQL padrão.
  *(exemplos 01, 03, 14, 20, 22, 23)*
- **`UNNEST(lista)`** — o **inverso de agregar numa lista**: transforma uma linha
  cuja coluna é uma lista de N itens em N linhas (uma por item). Usado para
  "explodir" `LIST`/arrays (inclusive de JSON) e então agregar por cima. SQL
  padrão. *(exemplos 09, 14, 19)*

### Window functions (funções de janela)

Todas têm a forma `FUNCAO(...) OVER (PARTITION BY ... ORDER BY ... <frame>)` e,
ao contrário do `GROUP BY`, **preservam as linhas**, anexando um valor calculado
sobre as "vizinhas". São SQL padrão (exceto `QUALIFY`). A anatomia completa está
no cabeçalho do exemplo 20.

- **`OVER (...)`** — o que torna uma função uma *window function*. `SUM(x)`
  agrega tudo; `SUM(x) OVER (...)` calcula um valor por linha.
- **`OVER (PARTITION BY coluna)`** — divide as linhas em **janelas independentes**
  (uma por valor da coluna), como um `GROUP BY` que não colapsa. Sem
  `PARTITION BY`, a janela é a tabela inteira.
- **`OVER (ORDER BY coluna)`** — ordena as linhas **dentro** da janela (≠ do
  `ORDER BY` final da query). É o que dá sentido a "anterior/seguinte", posição e
  acumulado.
- **`OVER (ORDER BY ... ROWS BETWEEN ...)`** — *frame* por **linhas físicas**:
  para cada linha, agrega as N anteriores até a atual (ex.: média móvel de 3 dias
  com `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`). *(exemplos 03, 20)*
- **`OVER (ORDER BY ... RANGE BETWEEN ...)`** — *frame* por **valor**: inclui
  todos os *peers* (linhas com o mesmo valor de `ORDER BY`). Só difere de `ROWS`
  quando há **empates** — e é o **default** quando o frame é omitido. *(exemplo 20)*
- **`ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`** — numera 1, 2, 3... as
  linhas de cada janela na ordem dada, reiniciando a cada partição. Base do
  "top-N por grupo". *(exemplos 03, 20)*
- **`LAG(coluna)` / `LEAD(coluna)`** — leem a linha **anterior** / **seguinte**
  dentro da janela, sem *self-join*. Servem para variação período-a-período e
  distância ao vizinho num ranking (`NULL` nas bordas). *(exemplo 20)*
- **`NTILE(n)`** — reparte as linhas ordenadas em **n baldes de tamanho quase
  igual** — quartis (`NTILE(4)`), decis (`NTILE(10)`), etc. *(exemplo 20)*
- **`FIRST_VALUE` / `LAST_VALUE`** — o **primeiro/último** valor da janela. Cuidado
  com o `LAST_VALUE`: o frame padrão para na linha atual, então é preciso abrir o
  frame até `UNBOUNDED FOLLOWING` para pegar o último de fato. *(exemplo 20)*
- **`QUALIFY cond`** — o **"`WHERE` das window functions"**: filtra pelo resultado
  de uma função de janela sem exigir uma subquery (o `WHERE` normal roda antes das
  janelas). **Extensão** do DuckDB/Snowflake/BigQuery; não existe em Postgres/MySQL.
  *(exemplos 03, 20)*

### Chaves geradas pelo banco (exemplo 23)

- **`CREATE SEQUENCE seq` + `coluna BIGINT DEFAULT nextval('seq')`** — como o
  DuckDB **não tem `AUTO_INCREMENT`/`IDENTITY`**, essa dupla é o idioma para uma
  chave primária que o banco preenche em sequência (o `SERIAL` do Postgres feito à
  mão). *(exemplo 23)*
- **`INSERT ... RETURNING col, ...`** — faz o `INSERT` (ou `UPDATE`/`DELETE`)
  **devolver as linhas afetadas** já com as colunas preenchidas pelo banco — o
  jeito de resgatar as surrogate keys geradas por um lote inteiro numa só ida ao
  banco. *(exemplo 23)*

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
  hash table em memória na hora) — ver o exemplo 16, que mede que `CREATE
  INDEX` não muda o plano nem o tempo de um join;
- o custo é pago 1x no ETL que grava; toda leitura posterior aproveita.

## Performance de JOIN e índices (exemplo 16)

Aprofunda a preocupação mais comum ao adotar DuckDB — *"joins complexos serão
rápidos sem os índices que eu criaria no Postgres?"*. Medições no exemplo 16:

- **índice ART NÃO acelera JOIN**: o plano é sempre `HASH_JOIN`; criar um
  índice na chave do join não muda o plano nem o tempo (ele serve a
  point-lookup por `WHERE` direto e a constraints, não a joins);
- **join que varre tudo**: o hash join já é rápido, paralelo e vetorizado —
  não precisa de tuning;
- **join seletivo**: são duas camadas, medidas separadamente — (1) *pushdown*,
  o predicado seletivo precisa **alcançar o fato** (o filtro na dimensão não
  vira filtro do fato sozinho; replique-o na chave do join), e (2) *zonemaps*,
  o fato precisa estar **ordenado na escrita** pela coluna filtrada para o
  scan pular row groups (o mecanismo do exemplo 12).

## JOIN de muitas tabelas com RAM limitada (exemplo 17)

Junta os exemplos 16 (hash join sem índice) e 04 (spill) num cenário realista:
cinco tabelas — uma dimensão pequena (`area`), dois fatos volumosos
(`operacao`, `contrato`), os `fluxo` de cada contrato e uma **ponte N:N**
(`rel_operacao_contrato`) ponderada pela coluna `fator`. A pergunta de negócio
soma `valor_fluxo` dos fluxos com `data_fluxo > 2026-01-01`, só de contratos com
`saldo_em_aberto > 0`, **agrupado por área** — e como a relação
operação↔contrato é N:N, o valor de cada fluxo é **rateado** pela área na
proporção do `fator`, isto é, `SUM(valor_fluxo * fator)`. Como no resto do
tutorial, dinheiro é `DECIMAL` de ponta a ponta (o `fator` é `DECIMAL(5,4)`),
então produto e soma ficam exatos, nunca `float`.

- **quatro hash joins encadeados, sem índice**: o plano é `HASH_JOIN` em todos
  os cruzamentos, inclusive na ponte N:N que multiplica linhas;
- **`memory_limit='100MB'` força spill**: os ~160MB de parquet de origem já não
  cabem no teto, e o join intermediário muito menos — o DuckDB derrama as hash
  tables para `temp_directory` e ainda assim conclui. O exemplo **mede o pico**
  de bytes derramados (amostrando o diretório durante a query, pois o DuckDB
  apaga os arquivos ao terminar) para provar que o spill aconteceu (~200MB), e
  contrasta com `memory_limit='8GB'`, onde nada vai para disco;
- **`SET threads=2`**: sob um teto apertado, cada thread mantém partições de
  hash próprias; menos threads = menos memória concorrente, o que faz a query
  caber nos 100MB de forma reprodutível em qualquer máquina (o próprio erro de
  OOM do DuckDB sugere reduzir threads). É sobre caber no orçamento, não sobre
  velocidade.

```bash
uv run examples/17_multitable_join_spill.py
```

## Transações, MVCC e concorrência (exemplo 21)

Como o DuckDB é *embutido* (roda dentro do processo, sem servidor separado — ver
"Conceitos centrais"), o processo que abre a conexão manipula o arquivo `.duckdb`
diretamente, sem um processo central mediando os acessos. Isso levanta uma dúvida
natural para quem vem de um SGBD cliente-servidor (Postgres, MySQL): como ficam as
transações? A resposta tem **dois níveis** bem distintos.

### Dentro de um processo: transações completas (MVCC)

No mesmo processo, o DuckDB oferece transações **ACID** de verdade, com
`BEGIN`/`COMMIT`/`ROLLBACK`:

- **Atomicidade**: um erro no meio da transação a aborta por inteiro; nada é
  gravado pela metade (`ROLLBACK` implícito).
- **MVCC** (*multi-version concurrency control*) com **isolamento por snapshot**:
  cada transação enxerga um instantâneo consistente do banco no momento em que
  começou; leitores não bloqueiam escritores e vice-versa.
- **Concorrência otimista**: várias conexões e várias threads do mesmo processo
  podem escrever ao mesmo tempo. O DuckDB não trava linhas antecipadamente — ele
  detecta conflito **no commit**. Se duas transações alteram o mesmo dado, uma
  commita e a outra recebe um erro de conflito (`TransactionContext Error`) e
  precisa refazer.
- **Durabilidade via WAL** (*write-ahead log*): as mudanças vão primeiro para um
  arquivo `.wal`, consolidado no arquivo principal em um `CHECKPOINT`.

Ou seja: **todo o controle transacional seguro é coordenado pela instância viva do
banco dentro de um processo.** Múltiplas conexões desse mesmo processo compartilham
o mesmo gerenciador MVCC e se coordenam com segurança total — é isso que o exemplo
21 exercita.

### Entre processos independentes: lock de arquivo, não coordenação

O que o DuckDB **não** faz é coordenar transações entre processos distintos que
abrem o mesmo arquivo. Sem um servidor para arbitrar, ele recorre a um **lock de
arquivo**, e o modelo é:

> **ou um único processo leitor-escritor, ou vários processos somente-leitura —
> nunca os dois ao mesmo tempo.**

- Abrir em **read-write** (o padrão) pega um **lock exclusivo**: enquanto esse
  processo segura o arquivo, nenhum outro consegue abri-lo, nem para ler.
- Vários processos podem abrir o **mesmo** arquivo em `access_mode = 'READ_ONLY'`
  simultaneamente, desde que **nenhum** o tenha em read-write.

Não há, portanto, escrita concorrente entre processos nem coordenação transacional
que atravesse a fronteira do processo. O lock é a única proteção, e ele exclui o
arquivo inteiro.

### Comparação com o SQLite

O SQLite é o parente próximo (também embutido, também um arquivo por banco), mas o
trade-off de concorrência é quase **invertido**:

| | SQLite (modo WAL) | DuckDB |
| --- | --- | --- |
| Escrita entre processos | 1 escritor **+** leitores concorrentes | escritor é **exclusivo** (trava o arquivo todo) |
| Leitura entre processos | concorrente | concorrente **só se não houver escritor** |
| Concorrência dentro do processo | serializada, granularidade grossa | **MVCC rico, multi-thread, vetorizado** |

O SQLite em modo WAL é **mais** permissivo *entre* processos (permite ler enquanto
alguém escreve). O DuckDB abre mão disso para ser muito mais forte *dentro* do
processo, o que combina com o caso de uso dele — cargas analíticas (OLAP),
multi-thread, um processo grande fazendo ETL — em vez de muitas conexões
transacionais concorrentes (OLTP), o terreno do SQLite/Postgres.

### Consequências práticas

- Precisa de **vários processos escrevendo** no mesmo banco? Esse não é o caso de
  uso do DuckDB embutido. Prefira **um único processo escritor** (serializando as
  escritas por uma fila/serviço) com *fan-out* de leitores em `READ_ONLY`.
- Precisa do modelo **cliente-servidor** clássico (um processo central mediando
  conexões)? Use o **MotherDuck** (serviço gerenciado sobre DuckDB) ou embrulhe o
  DuckDB em um servidor próprio.
- `ATTACH` de vários arquivos numa conexão **não** contorna o lock: cada arquivo
  aberto em read-write continua exclusivo.

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
