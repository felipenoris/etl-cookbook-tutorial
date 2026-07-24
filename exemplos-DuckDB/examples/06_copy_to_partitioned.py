"""Exemplo 6 — Escrevendo parquet particionado com COPY TO e recarga idempotente.

Numa base transacional, o resultado de um processamento vira `INSERT INTO`
outra tabela. No mundo data lake, vira **arquivos parquet** que outros motores
lerão — e o `COPY ... TO` é o "INSERT em arquivo" do DuckDB.

Comandos usados:

`COPY (query) TO 'dir' (FORMAT parquet, PARTITION_BY (col), ...)`
    Grava o resultado de QUALQUER SELECT como dataset parquet particionado
    Hive-style (uma subpasta `col=valor/` por valor). É o equivalente SQL do
    `pyarrow.dataset.write_dataset`. Note que `COPY` aqui não tem relação com
    o `COPY` do Postgres (carga de CSV): é exportação estruturada.

`OVERWRITE_OR_IGNORE`
    Sobrescreve arquivos de mesmo nome no destino. Como os nomes gerados são
    determinísticos, rodar o mesmo COPY filtrado por UMA partição substitui
    só os arquivos daquela partição — o padrão de ETL mais importante na
    prática: **recarga idempotente** (reprocessar o mês que mudou sem tocar
    nos demais, e rodar 2x não duplica nada). Em bases transacionais o
    idempotente análogo seria `DELETE WHERE mes = X` + `INSERT`, dentro de
    uma transação.

`FILE_SIZE_BYTES '16MB'`
    Quebra a saída em múltiplos arquivos (`data_0.parquet`, `data_1...`) —
    mesmo assunto do `max_rows_per_file` do pyarrow em
    `exemplos-rust-extension/run_etl.py`. O limite vale para o tamanho ANTES da
    compressão e por thread, então os arquivos finais saem menores que o
    nominal.

`GROUP BY ALL`
    Conveniência do DuckDB (não é SQL padrão): agrupa por TODAS as colunas
    do SELECT que não são agregadas, dispensando repetir a lista. Elimina a
    classe de erro "column must appear in the GROUP BY clause".

Rode com: `uv run examples/06_copy_to_partitioned.py`
"""

import shutil

import duckdb

from _common import ORDERS_GLOB, RICH_DIR, section

OUT_DIR = RICH_DIR / "duckdb_copy_demo"

if __name__ == "__main__":
    con = duckdb.connect()

    # começa do zero para o exemplo ser reproduzível (o COPY cria o diretório
    # final, mas não os pais — garantimos a pasta base aqui)
    shutil.rmtree(OUT_DIR, ignore_errors=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    section("COPY TO com PARTITION_BY: agregado diário por status, particionado por mês")
    con.execute(
        f"""
        COPY (
            SELECT order_month, order_date, status,
                   COUNT(*) AS pedidos, SUM(quantity) AS unidades
            FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
            GROUP BY ALL
        ) TO '{OUT_DIR / "status_diario"}'
        (FORMAT parquet, PARTITION_BY (order_month), OVERWRITE_OR_IGNORE)
        """
    )
    for path in sorted((OUT_DIR / "status_diario").rglob("*.parquet")):
        print(path.relative_to(OUT_DIR))

    section("Recarga idempotente: reprocessar SÓ a partição do mês 1")
    # Num ETL de verdade, o mês 1 teria dados corrigidos na origem; aqui basta
    # rodar o mesmo COPY filtrado. OVERWRITE_OR_IGNORE substitui o arquivo da
    # partição (nomes determinísticos), sem tocar nos meses 2..6.
    antes = con.sql(
        f"SELECT COUNT(*) FROM read_parquet('{OUT_DIR}/status_diario/**/*.parquet')"
    ).fetchone()[0]
    con.execute(
        f"""
        COPY (
            SELECT order_month, order_date, status,
                   COUNT(*) AS pedidos, SUM(quantity) AS unidades
            FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
            WHERE order_month = 1
            GROUP BY ALL
        ) TO '{OUT_DIR / "status_diario"}'
        (FORMAT parquet, PARTITION_BY (order_month), OVERWRITE_OR_IGNORE)
        """
    )
    depois = con.sql(
        f"SELECT COUNT(*) FROM read_parquet('{OUT_DIR}/status_diario/**/*.parquet')"
    ).fetchone()[0]
    print(f"linhas antes da recarga: {antes} | depois: {depois} (idempotente: {antes == depois})")

    # o limite vale para o tamanho ANTES da compressão e é avaliado por thread,
    # então os arquivos finais saem menores que o valor nominal
    section("FILE_SIZE_BYTES: quebrando a saída em múltiplos parts")
    con.execute(
        f"""
        COPY (
            SELECT * FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
            WHERE order_month = 1
        ) TO '{OUT_DIR / "orders_mes1_parts"}'
        (FORMAT parquet, FILE_SIZE_BYTES '16MB')
        """
    )
    for path in sorted((OUT_DIR / "orders_mes1_parts").rglob("*.parquet")):
        size_mb = path.stat().st_size / (1024 * 1024)
        print(f"{path.name}: {size_mb:.1f}MB")

    section("O dataset gravado é um dataset parquet como outro qualquer")
    con.sql(
        f"""
        SELECT order_month, SUM(pedidos) AS pedidos
        FROM read_parquet('{OUT_DIR}/status_diario/**/*.parquet', hive_partitioning=true)
        GROUP BY order_month ORDER BY order_month
        """
    ).show()
