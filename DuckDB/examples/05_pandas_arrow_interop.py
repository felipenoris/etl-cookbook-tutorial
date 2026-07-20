"""Exemplo 5 — Interoperabilidade zero-copy: DuckDB <-> pyarrow <-> pandas (Arrow backend).

Num banco tradicional, mover o resultado de uma query para o Python passa por
um protocolo de rede linha a linha (cursor -> fetchall -> objetos Python) — o
custo cresce com o volume. No DuckDB o resultado já nasce colunar, no mesmo
layout do Arrow, então a "exportação" para pyarrow/pandas é basicamente
entregar ponteiros: **zero-copy**, custo constante.

Caminhos de saída (DuckDB -> Python):
- `.to_arrow_table()` materializa o resultado como `pyarrow.Table` — o meio
  de troca preferido. (`.arrow()` devolve um `RecordBatchReader` streaming,
  melhor para resultados maiores que a RAM.)
- `.df()` devolve pandas "clássico" (numpy). Para manter o DataFrame com
  backend Arrow — o padrão deste tutorial, ver `../pandas` — o caminho é
  `to_arrow_table().to_pandas(types_mapper=pd.ArrowDtype)`.

Caminho de entrada (Python -> DuckDB), o que mais surpreende quem vem de
bases cliente-servidor: **o SQL enxerga variáveis Python por nome**. Se
`tabela_python` é um `pyarrow.Table`/DataFrame no escopo, então
`SELECT ... FROM tabela_python` simplesmente funciona ("replacement scan") —
sem CREATE TABLE, sem INSERT, sem cópia. Isso permite alternar SQL e Python
livremente no meio de um pipeline, usando o melhor de cada um.

Rode com: `uv run examples/05_pandas_arrow_interop.py`
"""

import duckdb
import pandas as pd
import pyarrow as pa

from _common import CUSTOMERS_GLOB, ORDERS_GLOB, section

if __name__ == "__main__":
    con = duckdb.connect()

    section("DuckDB -> pyarrow.Table via .to_arrow_table()")
    tabela_arrow = con.sql(
        f"""
        SELECT region, COUNT(*) AS total_clientes
        FROM read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true)
        GROUP BY region
        """
    ).to_arrow_table()
    print(type(tabela_arrow))
    print(tabela_arrow)

    section("DuckDB -> pandas com backend Arrow (via pyarrow.Table.to_pandas)")
    df = con.sql(
        f"SELECT status, AVG(quantity) AS qtd_media FROM read_parquet('{ORDERS_GLOB}') WHERE order_month = 1 GROUP BY status"
    ).to_arrow_table().to_pandas(types_mapper=pd.ArrowDtype)
    print(df.dtypes)
    print(df)

    section("pyarrow.Table Python -> DuckDB: consultando um objeto em memória com SQL")
    tabela_python = pa.table({"customer_id": [1, 2, 3], "flag_vip": [True, False, True]})
    resultado = con.sql(
        "SELECT customer_id, flag_vip FROM tabela_python WHERE flag_vip"
    ).to_arrow_table()
    print(resultado)

    section("Combinando: agregação em SQL + resultado em Arrow repassado para outra query")
    receita_por_regiao = con.sql(
        f"""
        SELECT c.region, SUM(o.quantity) AS quantidade_total
        FROM read_parquet('{ORDERS_GLOB}') o
        JOIN read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true) c USING (customer_id)
        WHERE o.order_month = 1
        GROUP BY c.region
        """
    ).to_arrow_table()
    # `receita_por_regiao` já é uma Table Arrow; o DuckDB consegue rodar outra
    # query em cima dela sem reconverter nada.
    top_regiao = con.sql(
        "SELECT * FROM receita_por_regiao ORDER BY quantidade_total DESC LIMIT 1"
    ).fetchone()
    print(f"região com maior quantidade vendida em janeiro: {top_regiao}")
