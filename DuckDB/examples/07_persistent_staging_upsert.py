"""Exemplo 7 — Banco persistente como staging, ATTACH e UPSERT.

Conceitos:
- `duckdb.connect("arquivo.db")` cria/abre um banco **persistente**: tabelas
  sobrevivem ao processo — a "área de staging" entre etapas de um ETL.
- `CREATE TABLE ... AS SELECT` (CTAS) materializa o resultado de uma query
  (por exemplo, a leitura de um parquet) como tabela do banco.
- `ATTACH` conecta um segundo banco na mesma sessão; dá para mover dados entre
  eles com um simples `INSERT INTO db2.t SELECT ... FROM db1.t` — federação
  entre áreas (staging → produção) sem sair do SQL.
- `INSERT ... ON CONFLICT DO UPDATE` (UPSERT) atualiza dimensões que mudam:
  linha nova é inserida, linha existente é atualizada (exige PRIMARY KEY).

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
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS producao.dim_customer (
            customer_id BIGINT PRIMARY KEY,
            customer_name VARCHAR,
            region VARCHAR,
            signup_date DATE
        )
        """
    )
    # staging -> produção com um INSERT entre bancos
    con.execute("INSERT INTO producao.dim_customer SELECT * FROM customers_stg")
    con.sql("SELECT COUNT(*) AS linhas_em_producao FROM producao.dim_customer").show()

    section("UPSERT: cliente 1 mudou de região; cliente 999999 é novo")
    con.execute(
        """
        INSERT INTO producao.dim_customer VALUES
            (1, 'cliente_00001', 'sul_ATUALIZADO', DATE '2023-01-15'),
            (999999, 'cliente_novo', 'norte', DATE '2026-07-20')
        ON CONFLICT (customer_id) DO UPDATE SET
            customer_name = EXCLUDED.customer_name,
            region = EXCLUDED.region
        """
    )
    con.sql(
        """
        SELECT * FROM producao.dim_customer
        WHERE customer_id IN (1, 999999) ORDER BY customer_id
        """
    ).show()
    con.sql("SELECT COUNT(*) AS total_apos_upsert FROM producao.dim_customer").show()

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
