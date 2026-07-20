"""Exemplo 11 — EXPORT/IMPORT DATABASE e o contraste view vs. tabela materializada.

Conceitos:
- `EXPORT DATABASE 'dir' (FORMAT parquet)`: dump do banco inteiro para um
  diretório — **um arquivo parquet por tabela**, mais `schema.sql` (DDLs de
  tabelas e views) e `load.sql` (COPYs de recarga). É a única forma em que
  "cada tabela vira um arquivo parquet individual" existe no DuckDB, e serve
  para backup, migração entre máquinas e publicação do catálogo completo.
- `IMPORT DATABASE 'dir'`: reconstrói o banco a partir do dump em qualquer
  outra conexão/máquina.
- **View vs. tabela materializada**: uma VIEW sobre parquet guarda só a query
  (relê os arquivos a cada consulta — sempre atualizada, custo repetido);
  um CTAS copia os dados para o storage interno do banco (leitura 1x — mais
  rápido para consultar de novo, mas é um snapshot). O `EXPLAIN` denuncia a
  diferença: `READ_PARQUET` num caso, `SEQ_SCAN` no outro.

Rode com: `uv run examples/11_export_import_and_views_vs_tables.py`
"""

import tempfile
import time
from pathlib import Path

import duckdb

from _common import ORDERS_GLOB, PRODUCTS_GLOB, section

if __name__ == "__main__":
    workdir = Path(tempfile.mkdtemp(prefix="duckdb_export_"))
    con = duckdb.connect()

    section("Montando um banco com 2 tabelas e 1 view")
    con.execute(
        f"""
        CREATE TABLE vendas_mes1 AS
            SELECT order_id, customer_id, product_id, quantity
            FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
            WHERE order_month = 1 AND customer_id <= 100;
        CREATE TABLE dim_produto AS
            SELECT * FROM read_parquet('{PRODUCTS_GLOB}');
        CREATE VIEW v_vendas_com_produto AS
            SELECT v.order_id, p.product_name, v.quantity
            FROM vendas_mes1 v JOIN dim_produto p USING (product_id);
        """
    )
    con.sql(
        """
        SELECT table_name, estimated_size AS linhas
        FROM duckdb_tables() ORDER BY table_name
        """
    ).show()

    section("EXPORT DATABASE: um parquet POR TABELA + schema.sql + load.sql")
    export_dir = workdir / "dump"
    con.execute(f"EXPORT DATABASE '{export_dir}' (FORMAT parquet)")
    for path in sorted(export_dir.iterdir()):
        print(f"{path.name:30s} {path.stat().st_size / 1024:8.1f}KB")
    print("\nschema.sql guarda os DDLs (inclusive o da view):")
    print(export_dir.joinpath("schema.sql").read_text().strip())

    section("IMPORT DATABASE: reconstruindo tudo numa conexão nova")
    con2 = duckdb.connect()  # outra sessão, banco vazio
    con2.execute(f"IMPORT DATABASE '{export_dir}'")
    con2.sql(
        """
        SELECT (SELECT COUNT(*) FROM vendas_mes1) AS vendas,
               (SELECT COUNT(*) FROM dim_produto) AS produtos,
               (SELECT COUNT(*) FROM v_vendas_com_produto) AS via_view
        """
    ).show()
    con2.close()

    section("View vs. tabela materializada: a MESMA consulta, custos diferentes")
    con.execute(
        f"""
        CREATE VIEW v_orders AS
            SELECT * FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true);
        CREATE TABLE t_orders AS
            SELECT * FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
            WHERE order_month = 1;
        """
    )
    consulta = "SELECT status, SUM(quantity) FROM {fonte} WHERE order_month = 1 GROUP BY status"

    for fonte in ("v_orders", "t_orders"):
        inicio = time.perf_counter()
        for _ in range(3):
            con.sql(consulta.format(fonte=fonte)).fetchall()
        media = (time.perf_counter() - inicio) / 3
        print(f"{fonte}: {media * 1000:6.0f}ms/consulta (média de 3)")
    print(
        "(a view relê o parquet a cada consulta; a tabela lê o storage interno.\n"
        " Com parquet local + partition pruning a diferença é pequena — ela cresce\n"
        " com storage remoto (S3/httpfs) ou quando a view encapsula query pesada)"
    )

    section("O EXPLAIN denuncia a diferença de fonte")
    plano_view = con.sql(f"EXPLAIN {consulta.format(fonte='v_orders')}").fetchall()[0][1]
    plano_tabela = con.sql(f"EXPLAIN {consulta.format(fonte='t_orders')}").fetchall()[0][1]
    print(f"consulta na view usa READ_PARQUET:  {'READ_PARQUET' in plano_view}")
    print(f"consulta na tabela usa SEQ_SCAN:    {'SEQ_SCAN' in plano_tabela}")

    section("Trade-off em uma linha")
    print("view  = sempre atualizada, paga leitura do parquet a cada consulta")
    print("tabela = snapshot no .db, leitura 1x — ideal p/ staging consultado N vezes")
    print(f"(dump de exemplo em {workdir} — apague quando quiser)")
