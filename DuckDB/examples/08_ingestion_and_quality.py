"""Exemplo 8 — Ingestão de CSV sujo, linhas rejeitadas e profiling.

Conceitos:
- `read_csv` com auto-detecção: o sniffer descobre delimitador, header e tipos
  sozinho — e aceita overrides pontuais (`types={...}`) quando você sabe mais
  que ele.
- `store_rejects=true`: em vez de abortar no primeiro erro, o DuckDB carrega as
  linhas boas e registra as ruins nas tabelas `reject_errors`/`reject_scans` —
  o padrão "quarentena" de ingestão: o ETL segue, e as rejeitadas viram
  relatório para correção na origem.
- `SUMMARIZE`: profiling instantâneo de qualquer tabela/parquet (min, max,
  nulos, cardinalidade aproximada) — um data quality check de uma linha.
- `USING SAMPLE`: desenvolver a transformação numa amostra antes de rodar no
  dataset inteiro.

Rode com: `uv run examples/08_ingestion_and_quality.py`
"""

import tempfile
from pathlib import Path

import duckdb

from _common import ORDERS_GLOB, section

CSV_SUJO = """order_id,customer_id,quantity,unit_price
1,100,2,10.50
2,101,cinco,20.00
3,102,1,abc
4,103,7,99.90
5,,3,15.00
6,104,4,7.25
"""

if __name__ == "__main__":
    con = duckdb.connect()

    workdir = Path(tempfile.mkdtemp(prefix="duckdb_ingest_"))
    csv_path = workdir / "pedidos_legado.csv"
    csv_path.write_text(CSV_SUJO)

    section("Auto-detecção do read_csv (sniffer): schema inferido")
    # Sem hints, o sniffer olha os dados: 'cinco' e 'abc' forçam as colunas
    # quantity/unit_price a VARCHAR — leitura funciona, mas os tipos ficam ruins.
    con.sql(f"DESCRIBE SELECT * FROM read_csv('{csv_path}')").show()

    section("Tipos explícitos + store_rejects: linhas boas entram, ruins vão p/ quarentena")
    con.sql(
        f"""
        SELECT * FROM read_csv(
            '{csv_path}',
            types = {{'quantity': 'INTEGER', 'unit_price': 'DOUBLE'}},
            store_rejects = true
        )
        ORDER BY order_id
        """
    ).show()

    section("A quarentena: reject_errors diz linha, coluna e motivo de cada rejeição")
    con.sql(
        """
        SELECT line, column_name, error_type, csv_line
        FROM reject_errors ORDER BY line
        """
    ).show()

    section("SUMMARIZE: profiling de um parquet inteiro em uma linha de SQL")
    con.sql(
        f"""
        SELECT column_name, column_type, min, max, approx_unique, null_percentage
        FROM (SUMMARIZE SELECT * FROM read_parquet('{ORDERS_GLOB}'))
        """
    ).show()

    section("USING SAMPLE: desenvolver na amostra, rodar no total")
    # o USING SAMPLE fecha a cláusula FROM — para agregar por cima, a amostra
    # entra numa subquery
    con.sql(
        f"""
        SELECT status, COUNT(*) AS pedidos_na_amostra
        FROM (
            SELECT * FROM read_parquet('{ORDERS_GLOB}') USING SAMPLE 1 PERCENT (system)
        )
        GROUP BY status ORDER BY pedidos_na_amostra DESC
        """
    ).show()
    print("(proporções próximas do dataset completo, com fração do custo de leitura)")
