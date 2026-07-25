"""Exemplo 7 — Banco persistente como staging, ATTACH e UPSERT.

Este é o exemplo em que o DuckDB mais se parece com uma base transacional —
tabelas de verdade, PRIMARY KEY, transações — e a diferença está na
infraestrutura: o "banco" inteiro é UM arquivo local, sem servidor.

Comandos usados:

`duckdb.connect("arquivo.db")`
    Cria/abre um banco **persistente**: tabelas sobrevivem ao processo. Todas
    as tabelas do banco vivem dentro desse único arquivo (não há um arquivo
    por tabela), no formato colunar próprio do DuckDB — não é parquet.
    É a "área de staging" natural entre etapas de um ETL.

`CREATE TABLE ... AS SELECT` (CTAS)
    Materializa o resultado de uma query como tabela do banco — a forma
    idiomática de criar tabelas em ETL (em vez de `CREATE TABLE` + colunas +
    `INSERT`, o schema é inferido da própria query).

`CHECKPOINT`
    Força a escrita do WAL (write-ahead log) para o arquivo principal. O
    conceito é o mesmo das bases transacionais; a diferença é que aqui pode
    ser invocado explicitamente para garantir o arquivo compacto/completo
    antes de copiá-lo para outro lugar.

`ATTACH 'outro.db' AS nome`
    Conecta um segundo banco na MESMA sessão — as queries referenciam
    `nome.tabela`. Mover dados entre bancos vira um simples
    `INSERT INTO producao.dim SELECT ... FROM staging_tbl`. Em bases
    cliente-servidor isso exigiria dblink/foreign data wrapper/ETL externo;
    aqui é nativo e trivial.

`INSERT ... ON CONFLICT (chave) DO UPDATE SET ...` (UPSERT)
    Sintaxe idêntica à do Postgres: linha nova é inserida, linha existente
    (mesma PRIMARY KEY) é atualizada — `EXCLUDED.col` referencia o valor que
    tentou entrar. É o mecanismo para dimensões que mudam. Detalhe
    importante: isso só funciona em TABELAS do banco (que suportam PK);
    arquivos parquet são imutáveis — para eles, o padrão é a recarga de
    partição do exemplo 06.

## A paleta de DDL de uma tabela interna do DuckDB

Numa tabela DuckDB (dentro do `.db`) você tem quase o DDL de um banco
relacional. O que este exemplo exercita, na `dim_customer` e na seção
dedicada:

- **Constraints**: `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, `CHECK(expr)` e
  `FOREIGN KEY` — visíveis depois em `duckdb_constraints()`.
- **`DEFAULT expr`**: valor calculado quando o INSERT omite a coluna
  (`DEFAULT now()`, `DEFAULT nextval('seq')`).
- **Colunas geradas**: `GENERATED ALWAYS AS (expr) VIRTUAL` — coluna
  calculada a partir de outras, na leitura (o DuckDB só suporta `VIRTUAL`,
  não `STORED`).
- **Sequences**: `CREATE SEQUENCE` para chaves surrogate auto-incrementais.
- **Índices**: `CREATE INDEX` / `CREATE UNIQUE INDEX` (estrutura ART, para
  point lookups e constraints), em `duckdb_indexes()`. Além deles, cada
  coluna tem *zonemaps* (min/max por bloco) automáticas — não precisam de
  DDL.
- **`ALTER TABLE`**: `ADD`/`DROP`/`RENAME COLUMN`, `ALTER ... TYPE`,
  `SET DEFAULT`, `ADD PRIMARY KEY`.
- **`USING COMPRESSION 'codec'`** por coluna (zstd, rle, bitpacking, alp...);
  por padrão o DuckDB escolhe o melhor codec sozinho, então raramente se mexe.
- Ainda: `CREATE TYPE ... AS ENUM`, `CREATE SCHEMA`, `CREATE TEMP TABLE`,
  `CREATE OR REPLACE` / `IF NOT EXISTS`.

## E os parâmetros estilo Hive/Hadoop (`PARTITIONED BY`, `LOCATION`...)?

O `CREATE TABLE` do Hive descreve uma tabela EXTERNA sobre arquivos no HDFS —
por isso tem `PARTITIONED BY`, `CLUSTERED BY` (bucketing), `STORED AS` e
`LOCATION`. No DuckDB, esses parâmetros **não existem no `CREATE TABLE`**
(todos foram testados e dão erro): uma tabela interna vive no formato colunar
próprio do DuckDB, dentro do arquivo `.db`. O conceito não some — ele muda de
lugar, para o lado do **parquet**:

| Conceito Hive (no `CREATE TABLE`) | Onde vive no DuckDB |
|---|---|
| `PARTITIONED BY (col)` | na ESCRITA de parquet: `COPY ... TO 'dir' (PARTITION_BY (col))` — exemplo 06 |
| `CLUSTERED BY (col) INTO N BUCKETS` | não há bucketing; o análogo de "agrupar por chave" é `ORDER BY` na escrita, que ativa zonemaps — exemplo 12 |
| `STORED AS PARQUET/ORC` | tabela interna usa o formato do DuckDB; parquet é só import/export (`read_parquet` / `COPY TO`) |
| `LOCATION '/hdfs/...'` | não por tabela; o arquivo do banco é escolhido no `connect`/`ATTACH`, e dados externos vêm de `read_parquet('caminho')` |
| índice / `SORTED BY` | `CREATE INDEX` (ART) em tabela interna; para parquet, `ORDER BY` na escrita + zonemaps |

Resumindo: DDL de tabela DuckDB é DDL relacional (constraints, defaults,
índices, sequences); "particionamento e layout de storage" é decisão de
ESCRITA de parquet, não cláusula de `CREATE TABLE`.

Rode com: `uv run examples/07_persistent_staging_upsert.py`
"""

import tempfile
from pathlib import Path

import duckdb

from _common import CUSTOMERS_GLOB, section

if __name__ == "__main__":
    workdir = Path(tempfile.mkdtemp(prefix="duckdb_staging_"))
    staging_path = workdir / "staging.db"
    prod_path = workdir / "producao.db"

    section("Banco persistente + CTAS: materializando customers no staging")
    con = duckdb.connect(str(staging_path))
    con.execute(
        f"""
        CREATE OR REPLACE TABLE customers_stg AS
        SELECT customer_id, customer_name, region, signup_date
        FROM read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true)
        """
    )
    con.sql("SELECT COUNT(*) AS linhas_no_staging FROM customers_stg").show()

    section("O banco é um arquivo de verdade no disco")
    con.execute("CHECKPOINT")  # força a escrita do WAL para o arquivo principal
    print(f"{staging_path.name}: {staging_path.stat().st_size / 1024:.0f}KB")

    section("ATTACH: conectando o banco de 'produção' na mesma sessão")
    con.execute(f"ATTACH '{prod_path}' AS producao")
    # DDL rico: PK, NOT NULL, CHECK, DEFAULT e uma COLUNA GERADA
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS producao.dim_customer (
            customer_id BIGINT PRIMARY KEY,                 -- chave natural (cria índice ART)
            customer_name VARCHAR NOT NULL,                 -- constraint NOT NULL
            region VARCHAR,
            signup_date DATE,
            carregado_em TIMESTAMP DEFAULT now(),           -- DEFAULT: preenchido se omitido
            regiao_norm VARCHAR                             -- coluna GERADA (calculada na leitura)
                GENERATED ALWAYS AS (upper(region)) VIRTUAL,
            CHECK (customer_id > 0)                         -- CHECK no nível da tabela
        )
        """
    )
    # com colunas DEFAULT/geradas, o INSERT precisa listar as colunas explícitas
    # (as demais o DuckDB preenche sozinho)
    con.execute(
        """
        INSERT INTO producao.dim_customer (customer_id, customer_name, region, signup_date)
        SELECT * FROM customers_stg
        """
    )
    con.sql("SELECT COUNT(*) AS linhas_em_producao FROM producao.dim_customer").show()
    print("colunas preenchidas automaticamente (DEFAULT + gerada):")
    con.sql(
        """
        SELECT customer_id, region, regiao_norm, carregado_em
        FROM producao.dim_customer WHERE customer_id <= 2 ORDER BY customer_id
        """
    ).show()

    section("UPSERT: cliente 1 mudou de região; cliente 999999 é novo")
    con.execute(
        """
        INSERT INTO producao.dim_customer (customer_id, customer_name, region, signup_date) VALUES
            (1, 'cliente_00001', 'sul_ATUALIZADO', DATE '2023-01-15'),
            (999999, 'cliente_novo', 'norte', DATE '2026-07-20')
        ON CONFLICT (customer_id) DO UPDATE SET
            customer_name = EXCLUDED.customer_name,
            region = EXCLUDED.region
        """
    )
    con.sql(
        """
        SELECT customer_id, customer_name, region, regiao_norm FROM producao.dim_customer
        WHERE customer_id IN (1, 999999) ORDER BY customer_id
        """
    ).show()
    con.sql("SELECT COUNT(*) AS total_apos_upsert FROM producao.dim_customer").show()

    section("As constraints declaradas aparecem em duckdb_constraints()")
    con.sql(
        """
        SELECT constraint_type, constraint_text
        FROM duckdb_constraints() WHERE table_name = 'dim_customer'
        ORDER BY constraint_type
        """
    ).show(max_width=100)

    section("CHECK em ação: um customer_id inválido é REJEITADO")
    try:
        con.execute(
            "INSERT INTO producao.dim_customer (customer_id, customer_name) VALUES (-1, 'x')"
        )
        print("(não deveria chegar aqui)")
    except duckdb.ConstraintException as exc:
        print(f"rejeitado pela constraint: {str(exc).splitlines()[0]}")

    section("Chave surrogate via CREATE SEQUENCE + DEFAULT nextval")
    con.execute("CREATE SEQUENCE producao.seq_produto START 1000 INCREMENT 1")
    con.execute(
        """
        CREATE TABLE producao.dim_produto (
            sk_produto BIGINT DEFAULT nextval('producao.seq_produto'),  -- surrogate auto
            codigo VARCHAR UNIQUE,
            nome VARCHAR NOT NULL
        )
        """
    )
    con.execute(
        "INSERT INTO producao.dim_produto (codigo, nome) VALUES ('A1','Cadeira'), ('B2','Mesa')"
    )
    con.sql("SELECT sk_produto, codigo, nome FROM producao.dim_produto ORDER BY sk_produto").show()

    section("CREATE INDEX (ART) e ALTER TABLE; ambos visíveis no catálogo")
    con.execute("CREATE INDEX idx_regiao ON producao.dim_customer (region)")
    con.execute("ALTER TABLE producao.dim_customer ADD COLUMN score INTEGER DEFAULT 0")
    print("índices:", con.sql(
        "SELECT index_name FROM duckdb_indexes() WHERE table_name='dim_customer'"
    ).fetchall())
    print("coluna nova visível:", "score" in con.sql(
        "SELECT * FROM producao.dim_customer LIMIT 0"
    ).columns)

    section("O que NÃO existe: parâmetros estilo Hive no CREATE TABLE")
    try:
        con.execute("CREATE TABLE producao.t_part (id INT, dt DATE) PARTITIONED BY (dt)")
    except duckdb.CatalogException as exc:
        print(f"PARTITIONED BY rejeitado: {str(exc).splitlines()[0]}")
    print("-> particionamento/layout são decisão de ESCRITA de parquet:")
    print("   COPY ... (PARTITION_BY ...) no exemplo 06, ORDER BY na escrita no exemplo 12.")

    section("Os catálogos visíveis na sessão (memória + staging + producao)")
    con.sql("SELECT database_name, path FROM duckdb_databases()").show()

    con.close()

    section("Reabrindo o arquivo de produção: os dados persistiram")
    con2 = duckdb.connect(str(prod_path), read_only=True)
    con2.sql(
        "SELECT region, COUNT(*) AS clientes FROM dim_customer GROUP BY region ORDER BY clientes DESC"
    ).show()
    con2.close()
    print(f"(arquivos de exemplo em {workdir} — apague quando quiser)")
