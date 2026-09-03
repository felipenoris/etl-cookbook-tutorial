# DuckDB — conceitos e manipulação de parquet particionado

Projeto Python isolado (gerenciado com `uv`) que exercita a API Python do
DuckDB sobre os dados fictícios em [`../data/raw`](../data/raw).

## Setup

```bash
cd examples-DuckDB
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
  vetorizado. O default é uma thread por núcleo; sob um `memory_limit` apertado,
  reduzir as threads é essencial para não estourar a memória (ver *Tuning de
  workloads: memória, threads e spill* abaixo).

## Exemplos

| Script | Conceitos |
| --- | --- |
| `01_connecting_and_querying.py` | `connect()`, `con.sql()` vs `con.execute()`, SELECT sobre glob |
| `02_reading_partitioned_parquet.py` | `hive_partitioning=true`, partition pruning via `EXPLAIN` |
| `03_joins_and_aggregations.py` | join de 3 tabelas, agregações, window functions (`ROW_NUMBER`, `QUALIFY`) |
| `04_memory_limit_and_spill.py` | `memory_limit`, `temp_directory`, `SET threads` para caber no teto, forçando spill num sort/aggregate grande |
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
| `24_record_batch_pipeline.py` | `to_arrow_reader` (o antigo `fetch_record_batch`): lote entra / lote sai sem sair do Arrow, no mesmo formato das funções do Rust; buffers reaproveitados por referência (prova por endereço), estado entre lotes, e o reader do Python devolvido ao DuckDB por replacement scan |
| `25_metadata_introspection.py` | introspecção sem ler dados: `parquet_schema` (tipo físico vs lógico), `parquet_file_metadata` (footer), `parquet_metadata` (row group × coluna: compressão, encoding, nulos), `parquet_kv_metadata`; o lado catálogo (`DESCRIBE`, `duckdb_columns`, `information_schema`, `.description`) e um detector de schema drift entre partições, com `union_by_name` reconciliando o caso benigno |
| `26_relational_api_read_parquet.py` | `con.read_parquet()` devolve uma `DuckDBPyRelation` **lazy**: montagem condicional da query em Python (`.filter`/`.project`/`.aggregate`/`.join`), `.sql_query()` e `.query()` como pontes com o SQL, ausência de cache, e a pegadinha medida do **cast implícito na coluna de partição** matando o pruning (lê os 6 arquivos em vez de 1) |
| `27_register_relations_and_dataframes.py` | `con.register()` **é** `CREATE TEMP VIEW`, provado pelo catálogo (`duckdb_views`, `information_schema`), pelo plano e pelo comportamento; as 3 diferenças (temporária, sem texto SQL, escopo de conexão); registrar `pandas.DataFrame`/`pyarrow.Table` e consultá-los por SQL; snapshot lógico via copy-on-write; `unregister`; `register` vs replacement scan |
| `28_new_closing_date_routine.py` | rotina de **nova data-base** sobre uma base parquet particionada por data de fechamento: `.duckdb` vazio, DDL, dimensão carregada inteira + fatos como **view** (`hive_types` fixando a partição como `VARCHAR`) + última partição em `CREATE TEMP TABLE` de staging (pruning 1/12), o mês novo **derivado do staging em pandas** (backend pyarrow) e devolvido por **bulk insert** (`INSERT ... BY NAME SELECT * FROM lote`) com `id_lancamento` vindo de `SEQUENCE` no servidor, validações por `ANTI JOIN` dentro da transação (`COMMIT`/`ROLLBACK`), e `COPY ... PARTITION_BY` de volta para a base de origem (recarga idempotente; `OVERWRITE` vs `OVERWRITE_OR_IGNORE` vs `APPEND`) |

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
- **`CREATE SEQUENCE seq START WITH n`** — o **único momento** de escolher onde a
  numeração começa: não há `setval()` nem `ALTER SEQUENCE ... RESTART`, e
  `CREATE OR REPLACE`/`DROP` são barrados enquanto uma tabela usar a sequence em
  `DEFAULT`. Para continuar a numeração de uma base existente, crie a sequence
  com `max(id) + 1` **depois** de ler a base. As demais restrições (ordem dos
  valores, `ROLLBACK`, `currval`, persistência) estão na seção "Chaves
  sequenciais (`SEQUENCE`): as restrições". *(exemplo 28)*

### Carga em lote (exemplo 28)

- **`INSERT INTO t BY NAME SELECT ...`** — casa as colunas do `SELECT` com as da
  tabela **pelo nome**, não pela posição; as ausentes recebem o `DEFAULT`. É o que
  permite `SELECT * FROM df` quando o DataFrame não traz a chave que o banco gera
  (o `SELECT *` posicional falha: "5 columns but 4 values"). Extensão do DuckDB.
  *(exemplo 28)*
- **`a ANTI JOIN b USING (col)`** — as linhas de `a` **sem** correspondente em
  `b`: a forma direta do `WHERE NOT EXISTS (...)`. É a checagem de chave órfã em
  lote quando não há FK declarada (há também `SEMI JOIN`, o inverso). Extensão
  do DuckDB. *(exemplo 28)*
- **`COPY (query) TO 'dir' (FORMAT parquet, PARTITION_BY (col), <modo>)`** — grava
  um dataset hive-particionado; num diretório que já tem partições é preciso
  escolher o modo: `OVERWRITE_OR_IGNORE` (substitui arquivos de mesmo nome só na
  partição gravada), `OVERWRITE` (**apaga todos os arquivos do diretório-alvo**
  antes de gravar) ou `APPEND` (arquivo novo de nome aleatório a cada execução).
  Extensão do DuckDB. *(exemplos 06, 28)*

## Recursos OLTP e eficiência do DuckDB

Uma dúvida recorrente de quem chega vindo de Postgres/MySQL: *o DuckDB tem chave
primária, foreign key, verificação de integridade referencial e chave sequencial
automática? Se a origem é um parquet, ele copia tudo para o `.duckdb`? E como
tudo isso se mantém eficiente sem um serviço dedicado rodando atrás?* As três
respostas se conectam — e a conclusão muda como você modela a carga.

### 1. As constraints existem e são verificadas de verdade

`PRIMARY KEY`, `UNIQUE`, `NOT NULL`, `CHECK` e `FOREIGN KEY` são todos
**aplicados no momento do `INSERT`/`UPDATE`** — não são decorativos. Para
verificá-los de forma eficiente, o DuckDB cria **automaticamente um índice ART**
(*Adaptive Radix Tree*) para cada constraint de PK, `UNIQUE` e FK. Um `INSERT`
que referencie um valor inexistente na tabela pai é rejeitado.

Chave sequencial automática **não** existe como `AUTO_INCREMENT`/`SERIAL`/
`IDENTITY`; o idioma é `DEFAULT nextval('seq')`, exatamente o que o exemplo 23
exercita (ver "Chaves geradas pelo banco", acima). A sequence tem restrições
próprias — não se reposiciona, não segue a ordem do lote, não volta no
`ROLLBACK` e nem sempre é persistida —, reunidas na seção "Chaves sequenciais
(`SEQUENCE`): as restrições", logo abaixo.

**As pegadinhas**, que aparecem cedo:

- **`ON DELETE CASCADE` é aceito pelo parser mas não executa o cascade** — apagar
  uma linha pai ainda referenciada continua dando erro de constraint. `INSERT` em
  tabela com FK auto-referenciada também não é suportado.
- **FK não atravessa bancos** (`ATTACH`): referência entre arquivos diferentes
  não funciona.
- **`UPDATE` é reescrito internamente como `DELETE` + `INSERT`**, processado em
  chunks de 2048 linhas. Isso faz um `UPDATE` legítimo (ex.: `SET i = i + 1` numa
  coluna com PK) estourar violação de constraint quando a tabela passa do tamanho
  do vetor — o motor ainda não "viu" o chunk seguinte.
- **O índice ART precisa caber em memória** durante a criação; em tabela grande
  isso vira OOM (ver "Checklist para não cair no OOM", adiante).

> As constraints funcionam bem como **guarda de qualidade** em tabelas
> dimensionais e cargas controladas. Elas não foram pensadas para sustentar um
> modelo transacional com muitos updates pontuais.

### 2. Parquet: copia ou não? Depende do comando

Não existe cópia implícita:

| Comando | O que acontece |
| --- | --- |
| `SELECT * FROM 'arq.parquet'` | lê direto do arquivo; **zero cópia**, zero estado no `.duckdb` |
| `CREATE VIEW v AS SELECT * FROM 'arq.parquet'` | guarda só o **texto da consulta** no catálogo; o parquet é relido a cada query |
| `con.register('v', con.read_parquet('arq.parquet'))` | o mesmo que a linha acima, como **view temporária** — sem cópia (exemplo 27) |
| `CREATE TABLE t AS SELECT * FROM 'arq.parquet'` | **copia**, convertendo para o formato colunar nativo dentro do `.duckdb` |
| `COPY t FROM 'arq.parquet'` | idem — ingere de fato |

A consequência amarra as duas primeiras perguntas: **não dá para colocar PK/FK
sobre um parquet.** Constraints só existem em tabelas nativas. Se o parquet é a
fonte e você quer integridade referencial, ou materializa
(`CREATE TABLE ... AS SELECT`), ou valida por query (`ANTI JOIN`,
`COUNT(DISTINCT)`) sem constraint declarada — o caminho que o exemplo 08 usa
para checagem de qualidade.

### 3. Por que é eficiente sem um serviço dedicado

A premissa merece ser invertida: **"sem servidor" não significa "sem engine".** O
engine completo — parser, otimizador, executor vetorizado, buffer manager, MVCC,
WAL — está todo lá, compilado como biblioteca **dentro do seu processo**. O
processo da sua aplicação *é* o servidor. O que foi eliminado não é a máquina de
banco: é o processo separado.

E é daí que vem o ganho. Boa parte do custo de um SGBD OLTP tradicional não está
em verificar uma PK — está no **protocolo de rede, na serialização, no
gerenciamento de pool de conexões e na coordenação entre processos**. Verificar
uma PK contra um ART em memória é operação de nanossegundos; o `INSERT` no
Postgres custa caro por causa de tudo *ao redor*.

O preço dessa escolha é o modelo de concorrência — lock de arquivo entre
processos, MVCC rico dentro do processo —, detalhado na seção "Transações, MVCC e
concorrência (exemplo 21)". A durabilidade continua garantida por WAL +
`CHECKPOINT` executados dentro do próprio processo: não há daemon de vacuum nem
de recuperação porque não há nada além do seu processo para coordenar.

### A conclusão que importa para arquitetura

**O custo real das constraints no DuckDB não é o de manutenção em runtime — é o
de carga.** A checagem linha a linha de PK/FK derruba o throughput de bulk load
em ordens de magnitude, justamente porque **quebra o padrão vetorizado** que dá
velocidade ao motor.

O padrão que funciona bem:

1. **carregar sem constraint** (bulk load vetorizado, no talo);
2. **validar em batch com SQL** (`ANTI JOIN`, `GROUP BY ... HAVING COUNT(*) > 1`);
3. **reservar PK/FK para tabelas pequenas de referência**, onde a garantia
   declarativa vale o custo.

## Chaves sequenciais (`SEQUENCE`): as restrições (exemplos 23 e 28)

`CREATE SEQUENCE seq` + `coluna DEFAULT nextval('seq')` é o único idioma de
chave gerada pelo banco (não há `IDENTITY`/`SERIAL`/`AUTO_INCREMENT`). Ele
funciona, mas com restrições que não existem no Postgres — todas medidas no
DuckDB 1.5.5, no exemplo 28 e em `tests/test_new_closing_date_routine.py`:

| Você quer | O que o DuckDB faz | Faça assim |
| --- | --- | --- |
| **reposicionar** a sequence (`setval()`, `ALTER SEQUENCE ... RESTART`) | não existem: `CatalogException` / `NotImplementedException` | o `START WITH` é definido **só na criação**; para continuar a numeração de dados existentes, leia `max(id)` primeiro e crie a sequence com `START WITH max(id) + 1` |
| **recriar** (`CREATE OR REPLACE SEQUENCE`) ou **apagar** (`DROP SEQUENCE`) uma sequence usada num `DEFAULT` | `DependencyException` quando tabela e sequence estão no mesmo catálogo; **entre catálogos** (tabela `TEMP`, sequence em `main`) a dependência não é rastreada: os dois comandos passam, e o `DROP` deixa o `DEFAULT` apontando para o nada (`CatalogException` no próximo `INSERT`) | crie a sequence antes da tabela e já com o `START WITH` certo; não use a lacuna entre catálogos como se fosse um `RESTART` |
| inserir um **id explícito** numa coluna com `DEFAULT nextval` | a sequence **não avança**: o próximo `nextval` devolve o mesmo valor e, havendo PK, `ConstraintException` (`Duplicate key`) | ou todos os ids vêm da sequence, ou o `START WITH` fica acima do maior id gravado (o caso do staging do exemplo 28, que recebe a última partição com ids explícitos) |
| desfazer um lote (`ROLLBACK`) | as linhas voltam; os valores consumidos, **não** — ficam buracos na numeração (como no Postgres). Entre conexões não há bloqueio nem reuso | aceite os buracos: são inofensivos |
| ids **na ordem das linhas de origem** num `INSERT ... SELECT` | **sem garantia**: a carga é paralela por chunk/row group e os valores saem embaralhados (medido: 10 mil inversões num lote Arrow de 1M linhas em 16 batches; 90 mil em 5,6M linhas de um parquet com 36 row groups; um lote de um único batch sai ordenado apenas por não ser paralelizado) | ids determinísticos: `max_id + row_number() OVER (ORDER BY ...)`; e, no `RETURNING`, traga a natural key junto — a posição não diz nada (exemplo 23) |
| saber o **último valor usado** (`currval('seq')`) | só vale na sessão que chamou `nextval` (`SequenceException` antes disso); **numa sessão nova sobre um arquivo**, `currval` devolve o contador persistido — o *próximo* valor, ainda não usado — e `last_value` em `duckdb_sequences()` idem | o último id gravado é `max(id)` na tabela, não `currval` |
| contar com o contador **depois de reabrir** o `.duckdb` | ele só é gravado junto com uma **transação de escrita**: um `nextval` num `SELECT` puro em auto-commit é **perdido** ao fechar — reabrindo, o mesmo valor é entregue de novo, e nem `CHECKPOINT`/`FORCE CHECKPOINT` salvam. Dentro de `BEGIN ... COMMIT`, ou numa transação com qualquer `INSERT` (mesmo em tabela `TEMP`), persiste | não use a sequence como autoridade de ids que vivem fora do banco (parquet): derive o `START WITH` dos dados a cada execução, como a rotina do exemplo 28 faz |
| tipos, limites e opções | `nextval` devolve `BIGINT` (cast implícito para `INTEGER` no `DEFAULT`); `MAXVALUE` sem `CYCLE` estoura com `SequenceException`; `START WITH 0` exige `MINVALUE 0` (`ParserException`); há `INCREMENT BY`, `MINVALUE`, `MAXVALUE`, `CYCLE` e `CREATE TEMP SEQUENCE` | `duckdb_sequences()` mostra `start_value`, `last_value`, `increment_by`, `min_value`, `max_value`, `cycle`, `temporary` |
| **custo** em bulk load | `DEFAULT nextval` roda linha a linha, mas é barato perto de uma `PRIMARY KEY` (índice ART): no epílogo do exemplo 28 (1M linhas), staging com PK + `nextval` custou ~2,5× o mesmo staging sem PK, e `row_number()` ficou ~25% abaixo do `nextval` | em staging, dispense a PK e valide a unicidade em lote (`ANTI JOIN`/`count(*)`) |

## Rotina de nova data-base: DataFrame → staging → partição parquet (exemplo 28)

O exemplo 28 junta as peças acima numa rotina de ETL completa sobre uma base
contábil fictícia particionada por data de fechamento
(`cad_lancamentos/data_base_str=yyyy-mm-dd/`): `.duckdb` vazio → DDL → carga da
base atual (dimensão inteira, fatos como **view**, só a última partição num
`CREATE TEMP TABLE` de staging) → o mês novo derivado do staging em pandas
(backend pyarrow) e devolvido por bulk insert → validações → `COPY` da partição
nova de volta para a base de origem. A regra de derivação do exemplo é fictícia
(saldos de abertura + recorrentes); o caminho do dado é o real. Cinco fatos
medidos definem a forma da rotina:

**A sequence nasce com o `START WITH` certo, ou não nasce.** Não há `setval()`
nem `ALTER SEQUENCE ... RESTART` (`NotImplementedException`), e `CREATE OR
REPLACE SEQUENCE`/`DROP SEQUENCE` são barrados pela dependência da tabela que a
usa no `DEFAULT` (`DependencyException`). Como o valor certo é
`max(id_lancamento) + 1`, a sequence e o staging (a única tabela que a usa) só
podem ser criados **depois** da view sobre a base — no passo de carga, não no de
DDL. A dependência é rastreada por catálogo: com o staging `TEMP` e a sequence
em `main`, o replace passa (e o `DEFAULT`, resolvido pelo nome a cada `INSERT`,
passa a usar a sequence nova) — mas um `DROP SEQUENCE` também passa e deixa o
`DEFAULT` apontando para o nada (`CatalogException` no próximo `INSERT`). É uma
lacuna do rastreamento, não uma API.

**`INSERT INTO t BY NAME SELECT * FROM df`.** O DataFrame não tem
`id_lancamento`, então o `SELECT *` posicional falha ("table has 5 columns but 4
values were supplied"). `BY NAME` casa as colunas pelo nome, ignora a ordem do
DataFrame e deixa o `DEFAULT nextval(...)` preencher a chave — o "servidor gera
a chave" de outros bancos, sem `executemany`. Os tipos do DataFrame com backend
pyarrow chegam intactos: `decimal128(12,2)` → `DECIMAL(12,2)`, `date32` →
`DATE`, `int32` → `INTEGER`.

**A coluna de partição é tipada por autodetecção.** `hive_partitioning=true`
tenta tipar o valor lido do caminho: `2024-12-31` vira `DATE`, `1`/`10` viram
`BIGINT`, `01` fica `VARCHAR` (não faz o round-trip). Se o contrato diz
`VARCHAR`, fixe com `hive_types={'data_base_str': 'VARCHAR'}` — o texto da view
guarda a escolha. O pruning por partição funciona igual nos dois casos.

**Validar em lote, dentro da transação.** FK do staging para a dimensão não
existe: tabela temporária não referencia tabela do catálogo `main` ("Creating
foreign keys across different schemas or catalogs is not supported"). A rotina
faz o que a seção anterior recomenda: `BEGIN`, bulk insert, checagens por `ANTI
JOIN`/`count(*)` sobre o mês inteiro, `COMMIT` se passam e `ROLLBACK` se não. O
`ROLLBACK` desfaz o lote mas **não a sequence**: os ids consumidos viram um
buraco na numeração, como no Postgres.

**Os três modos de escrita do `COPY ... PARTITION_BY`**, medidos no exemplo e
nos testes, num diretório que já tem partições:

| Modo | Efeito |
| --- | --- |
| *(nenhum)* | recusa: `Directory "..." is not empty` |
| `OVERWRITE_OR_IGNORE` | grava a partição do resultado; arquivos de mesmo nome (`data_0.parquet`) são substituídos e as outras partições ficam intactas |
| `OVERWRITE` | **apaga todos os arquivos abaixo do diretório-alvo** — todas as partições, e qualquer outro arquivo — e só então grava (sobram as pastas vazias) |
| `APPEND` | grava um arquivo de nome aleatório (UUID) na partição; rodar 2x duplica o mês |

A recarga idempotente da rotina é remover a pasta da partição-alvo e gravar com
`OVERWRITE_OR_IGNORE` (necessário só porque a pasta-raiz já existe). A view
sobre o glob não precisa de ajuste — no `SELECT` seguinte ela já lista a
partição nova —, e o `.duckdb` que sobra guarda só o catálogo (a dimensão e o
texto da view, com o caminho absoluto); os fatos continuam nos parquet.

### O que custa cada peça — e a versão enxuta

O epílogo do exemplo mede seis mecânicas para levar o mesmo lote Arrow de 1M
de linhas (16 batches, como um DataFrame montado por `concat`) até a partição
parquet, cada uma numa conexão nova e terminando no mesmo `COPY`:

| Mecânica | Tempo | ids fora da ordem do lote |
| --- | --- | --- |
| staging com `PRIMARY KEY` + `DEFAULT nextval`, `.duckdb` em arquivo (a rotina como especificada) | 243ms | ~10.500 |
| idem, conexão em memória | 243ms | ~10.600 |
| staging sem PK + `DEFAULT nextval` | 92ms | ~10.800 |
| staging sem PK + `row_number() OVER ()` | 68ms | 0 |
| sem staging: `row_number() OVER ()` dentro do `COPY` | 91ms | — |
| sem staging: id já vem no lote, `COPY` direto do Arrow | 22ms | — |

Três conclusões, que valem para qualquer rotina de carga no DuckDB:

1. **A `PRIMARY KEY` do staging é o custo dominante** (o índice ART é
   construído linha a linha) — não a sequence, não o staging e não o arquivo
   `.duckdb`: o staging é `TEMP` e não toca o arquivo (as duas primeiras
   linhas empatam). Em staging, dispense a PK e valide a unicidade em lote,
   que a rotina já faz.
2. **`nextval` não segue a ordem do lote** numa carga paralela (a coluna da
   direita), e custa um pouco mais que `row_number()`. Para ids determinísticos
   e reproduzíveis: `max_id + row_number() OVER (ORDER BY ...)`.
3. **O caminho mais curto não tem staging**: com o id já calculado no lote, o
   `COPY` lê os batches do Arrow em paralelo e grava a partição direto — 10×
   mais rápido que a rotina. Uma window function dentro do `COPY` desfaz o
   ganho (sem `PARTITION BY`, ela roda numa thread só).

A rotina do exemplo mantém staging, `SEQUENCE` e transação porque é isso que
ela ensina — e porque, num fechamento mensal, 245ms por milhão de linhas não é
o gargalo. A versão enxuta da mesma rotina, para quando o volume importar:
conexão em memória, `dom_veiculos` e `cad_lancamentos` como views sobre o
parquet, o lote validado por `ANTI JOIN` direto contra a view (sem inserir
nada), ids atribuídos no próprio lote (`max(id)` da última partição +
posição) e um único `COPY ... PARTITION_BY` do Arrow para a partição nova —
gravada numa pasta temporária e renomeada no fim, já que o `COPY` não é
atômico e a transação do DuckDB não cobre arquivos parquet.

## Streaming em lotes: RecordBatch entrando e saindo (exemplos 15 e 24)

Nem todo resultado cabe na RAM, e nem todo cálculo cabe em SQL. Para esses
casos o DuckDB entrega o resultado em **lotes Arrow**, um `RecordBatch` por
vez, em vez de materializar a tabela inteira.

### As duas portas (a mesma classe)

```python
con.execute(sql).to_arrow_reader(n)   # a partir do result set (estilo DBAPI)
con.sql(sql).to_arrow_reader(n)       # a partir da relation (lazy)
```

Ambas devolvem um `pyarrow.RecordBatchReader`; muda só por onde se chega nele.
O nome **`fetch_record_batch(n)`**, comum em código e tutoriais mais antigos,
é este mesmo método com o nome da família `fetch*`: ainda funciona, mas desde
o DuckDB 1.5 emite `DeprecationWarning` apontando para `to_arrow_reader()`.

O reader é **lazy e de passada única**: cada `next()` puxa um lote do motor, e
depois de esgotado ele não rebobina.

### O que fazer com o lote — os dois caminhos

| | exemplo 15 | exemplo 24 |
| --- | --- | --- |
| o lote vira | listas Python (`to_pylist()`) | continua `RecordBatch` |
| a saída é | listas, remontadas em `pa.table` no fim | um `RecordBatch` com schema estendido |
| o custo | µs **por linha** | o de `pyarrow.compute` (C++) |
| quando usar | a lógica é sequencial de verdade | a lógica vetoriza |

O exemplo 24 segue a forma das funções do `rust-extension`
(`add_line_total`, `compute_customer_running_spend`): **lote entra, lote sai**,
com as colunas de entrada repassadas **por referência** e só a coluna nova
alocada. Em Rust isso é `columns().to_vec()` clonando `Arc`s; em Python é
`list(batch.columns)` guardando os mesmos `pa.Array` — e o exemplo comprova
comparando o **endereço do buffer** antes e depois.

### Devolvendo o resultado ao DuckDB

Com `pa.RecordBatchReader.from_batches(schema, gerador)`, o lado Python vira
mais um reader, que o DuckDB varre por replacement scan como se fosse tabela.
O pipeline inteiro fica sem materialização: o motor produz um lote, o Python
enriquece aquele lote, o motor consome, e só então o próximo é lido.

> **Use uma conexão separada para consumir.** Se o `SELECT` que lê o reader
> enriquecido rodar na MESMA conexão que produziu o reader de origem, o DuckDB
> reentra em si mesmo — o comportamento observado foi ora um silencioso
> `count = 0` (o stream de origem é invalidado pela nova query), ora um
> travamento. Duas conexões resolvem: uma só produz, a outra só consome.

## Introspecção de metadados (exemplo 25)

Antes de processar um diretório de parquet, a pergunta é sempre "o que tem aqui
dentro?". Nada disso exige ler os dados: cada arquivo carrega no fim um
**footer** que descreve o arquivo inteiro, e o DuckDB o expõe como quatro table
functions — uma por nível da árvore de metadados.

| Nível | Função | Uma linha por | Responde |
| --- | --- | --- | --- |
| schema | `parquet_schema(f)` | nó do schema | quais colunas, de que tipo, nullable |
| arquivo | `parquet_file_metadata(f)` | arquivo | `num_rows`, `num_row_groups`, `created_by`, tamanho |
| row group × coluna | `parquet_metadata(f)` | column chunk | `compression`, `encodings`, min/max, `stats_null_count`, bytes |
| chave/valor | `parquet_kv_metadata(f)` | par key/value | metadados livres da ferramenta que escreveu |

As quatro aceitam **glob** e devolvem `file_name` — é isso que torna
"comparar o schema de mil arquivos" uma query só, em vez de mil aberturas.

**Tipo físico vs tipo lógico.** O parquet só guarda meia dúzia de tipos
físicos; o resto é tipo físico + anotação. Uma data é `INT32` + `DateType()`,
um texto é `BYTE_ARRAY` + `StringType()`. `parquet_schema` traz as duas colunas
e ainda `duckdb_type`, já traduzido. `repetition_type = 'OPTIONAL'` é como o
formato codifica "aceita nulo".

**Arquivo em disco ≠ tabela no catálogo.** As funções `parquet_*` leem
arquivos; para tabelas dentro do DuckDB o caminho é `DESCRIBE`,
`duckdb_columns()`, `duckdb_tables()` e `information_schema.columns` (o
equivalente padrão SQL, portável). O `DESCRIBE` atravessa os dois mundos: ele
aceita tanto um nome de tabela quanto uma query — inclusive
`DESCRIBE SELECT * FROM read_parquet(glob)`. No lado Python, o schema do result
set vem de graça em `con.execute(sql).description` (DBAPI).

**A ordem de grandeza.** Sobre as 6 partições de `orders` (33,7M de linhas),
medido no exemplo: `DESCRIBE` do glob inteiro em ~1 ms e `sum(num_rows)` pelo
footer em ~1 ms, contra ~14 ms para varrer **uma só** coluna — uma ordem de
grandeza, e a distância cresce com o volume, porque o footer não depende dele.
Toda pergunta sobre estrutura — colunas, tipos, contagem, nulos, tamanho —
deve ser respondida pelo footer, nunca por uma varredura.

## API relacional e `register`: dar nome a objetos Python (exemplos 26 e 27)

Além da string SQL, o DuckDB expõe a leitura de parquet como **método da
conexão**: `con.read_parquet(glob, hive_partitioning=True)` devolve uma
`DuckDBPyRelation`. Ela **não é o resultado** — é a consulta ainda não
executada, a mesma coisa que `con.sql("SELECT ...")` devolve. Tanto que
`rel.sql_query()` imprime o `SELECT` equivalente.

O motivo de existir é a **montagem condicional**: em Python a query costuma
depender de argumentos (`--mes`, `--status`), e montá-la concatenando SQL é
frágil e abre porta para injeção (exemplo 22). Com a relation, cada passo é um
método que devolve uma nova relation, e nada executa até o `.fetchall()` do
fim — o otimizador ainda vê a consulta inteira de uma vez.

```python
rel = con.read_parquet(ORDERS_GLOB, hive_partitioning=True)
if mes:
    rel = rel.filter(f"order_month = '{mes:02d}'")
rel = rel.aggregate("status, count(*) AS n", "status")   # ainda nada foi lido
```

**A pegadinha que lê 6 arquivos em vez de 1 (medida no exemplo 26).** As colunas de partição vêm
do *nome do diretório*, então `order_month` é `VARCHAR` (`'01'`). Escrever
`filter("order_month = 1")` insere um `CAST(order_month AS INTEGER)`, e o
descarte de arquivos não sabe avaliar essa expressão: os 6 arquivos são abertos
e o filtro vira um operador `FILTER`. Com o literal no tipo nativo o plano
volta a mostrar `Scanning Files: 1/6`.

| Filtro | Tempo | Arquivos lidos |
| --- | --- | --- |
| `rel.filter("order_month = 1")` | ~4 ms | 6 de 6 |
| `rel.filter("order_month = '01'")` | ~2 ms | **1 de 6** |
| `... WHERE order_month = 1` (string SQL) | ~1 ms | 1 de 6 |

Os tempos são de uma máquina de referência e estão na casa do milissegundo —
cite a **coluna da direita**, que é o efeito estrutural e não varia: com o
cast, o descarte por arquivo não acontece. A penalidade em tempo depende do
que mais a consulta faz e cresce com a seletividade do filtro.

> **Regra:** na API relacional, compare coluna de partição com literal **do
> tipo dela**. Na string SQL o otimizador reescreve o cast sozinho; passando por
> uma relation (ou por uma view — vale para as duas), não.

**`con.register(nome, objeto)` é `CREATE TEMP VIEW`,** não algo parecido com
isso. O exemplo 27 prova pelos três lados:

- **catálogo** — o nome registrado aparece em `duckdb_views()` ao lado das views
  de DDL, e `information_schema.tables` classifica ambos como `VIEW`;
- **plano** — mesmos pushdowns, mesmo partition pruning e até a *mesma*
  patologia do cast acima, número por número;
- **comportamento** — nenhum dos dois materializa: os dois releem a fonte a cada
  consulta e enxergam arquivos que apareceram no diretório depois (o `CTAS`,
  não).

As diferenças são de **ciclo de vida**, não de semântica:

| | `con.register` | `CREATE VIEW` |
| --- | --- | --- |
| `temporary` no catálogo | `true` (morre com a conexão) | `false` |
| texto SQL guardado | vazio — é um ponteiro para objeto Python | o DDL completo |
| `EXPORT DATABASE` | **ignora silenciosamente** | exporta no `schema.sql` |
| fonte possível | relation, `DataFrame`, `pyarrow.Table`, reader | só SQL |

Registrar um `pandas.DataFrame`/`pyarrow.Table` transforma **memória Python em
tabela SQL**, sem cópia: `JOIN` com parquet, `GROUP BY`, tudo direto. E com o
copy-on-write do pandas 3 o nome registrado vira um **snapshot lógico de
graça** — escrever no DataFrame aloca buffers novos e não muda o que o SQL vê;
para publicar, basta registrar de novo com o mesmo nome.

**`register` vs. replacement scan.** `SELECT * FROM meu_df` já funciona sem
registrar nada (exemplo 5), mas o replacement scan resolve o nome no *frame de
quem chamou* `.sql()` — some junto com o frame. `register` é a versão explícita:
você escolhe o nome, ele sobrevive ao `return`, aparece no catálogo e sai com
`con.unregister(nome)`. É por isso que funções e bibliotecas usam `register`.

**Materializar nem sempre ganha.** Medido no exemplo 27, sobre `orders` de
janeiro: o nome registrado (view sobre parquet) roda a agregação em ~8 ms,
contra ~13 ms da mesma tabela materializada por `CTAS`. O parquet guarda
`status` em `RLE_DICTIONARY` + snappy e o pruning restringe a leitura a 1
arquivo; a tabela do banco **em memória** não é comprimida, então o scan move
mais bytes. Materializar compensa quando a query encapsulada é cara (join,
sort, UDF) ou quando a fonte é remota (S3) — não pelo fato de "estar no banco".

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

## Tuning de workloads: memória, threads e spill

Dois padrões do DuckDB surpreendem quem chega de um SGBD cliente-servidor e são
a causa mais comum de *"funcionava na minha máquina"*:

- **memória**: o teto default é ~80% da RAM detectada — generoso, mas é o *seu*
  processo que paga a conta.
- **paralelismo**: o default é **uma thread por núcleo** da máquina. Ótimo para
  velocidade, mas cada thread reserva um **piso de memória de trabalho** para
  suas partições de hash/sort.

Os dois interagem de um jeito que morde: ao **apertar o `memory_limit`** (para
caber num container, num teste, ou de propósito para forçar spill), o piso de
memória somado de *todas* as threads pode **estourar o teto antes de o motor
conseguir spillar** — e então, em vez de derramar para disco, ele aborta com
`OutOfMemoryException` (`failed to pin block of size ...`). Como o número de
threads acompanha a contagem de núcleos, o **mesmo código passa numa máquina de
poucos núcleos e falha numa de muitos** com o mesmíssimo `memory_limit`.
(Medido neste repo: `memory_limit='150MB'` + o `ORDER BY` de 33.7M linhas do
exemplo 04 spilla normalmente com ≤8 threads e estoura com ≥16.)

### Os botões (todos via `SET`, valem por conexão)

| Comando | O que faz |
| --- | --- |
| `SET memory_limit='512MB'` | teto de memória do motor — um orçamento **global**, dividido entre as threads. |
| `SET threads=4` | número de threads de execução. **Menos threads = menos memória concorrente**; é o primeiro ajuste que o próprio erro de OOM sugere. |
| `SET temp_directory='/caminho'` | onde gravar os blocos que não couberem no teto (o *spill*). **Sem ele, uma operação grande não tem para onde derramar** — só resta o OOM. |
| `SET preserve_insertion_order=false` | dispensa a garantia de ordem dos dados de origem em resultados sem `ORDER BY`, liberando memória e paralelismo. |

### A regra prática

O `memory_limit` é um **orçamento compartilhado por todas as threads**. Grosso
modo, é preciso que `piso_por_thread × threads` caiba no teto — caso contrário o
motor fica sem espaço nem para os buffers mínimos e falha antes de spillar. Logo:
ao **baixar o `memory_limit`, baixe também as `threads`**. Sob tetos apertados
(centenas de MB), 2–4 threads costumam bastar para concluir com spill; sob o teto
default (na casa dos GB), deixe o DuckDB usar todos os núcleos.

### Checklist para não cair no OOM

- Definiu um `memory_limit` baixo? **Fixe `SET threads` num valor pequeno (2–4)** —
  não confie no default, que muda de máquina para máquina.
- **Sempre** defina `temp_directory` quando a operação puder não caber: sem ele
  não há spill, só OOM.
- Precisa de ordem determinística? Use `ORDER BY` explícito com
  `preserve_insertion_order=false`; nunca dependa da ordem implícita de inserção.
- "Passou na minha máquina" não vale se ela tem contagem de núcleos diferente da
  de produção/CI — teste com o mesmo `threads` que vai rodar em produção.
- Sob teto apertado, **meça o spill** (o exemplo 17 amostra o `temp_directory`
  durante a query) para confirmar que a operação está derramando para disco, e
  não segurando tudo em RAM.

Os exemplos [`04_memory_limit_and_spill.py`](examples/04_memory_limit_and_spill.py)
e [`17_multitable_join_spill.py`](examples/17_multitable_join_spill.py) aplicam
exatamente esse cuidado: `memory_limit` baixo **+** `SET threads` fixo **+** spill
para `temp_directory`, de forma reprodutível em qualquer máquina.

Guia oficial completo: [DuckDB — How to Tune Workloads](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads).

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
- [Constraints](https://duckdb.org/docs/stable/sql/constraints) — `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, `CHECK` e `FOREIGN KEY`, incluindo o custo em bulk load discutido em "Recursos OLTP e eficiência do DuckDB".
- [Indexes](https://duckdb.org/docs/stable/sql/indexes) — o índice ART criado automaticamente por PK/`UNIQUE`/FK, e por que ele não acelera JOIN (exemplo 16).
