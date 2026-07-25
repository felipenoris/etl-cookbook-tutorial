"""Exemplo 2 — Lendo parquet particionado (hive) e observando partition pruning.

O particionamento "Hive-style" é a convenção do mundo data lake para o que,
numa base transacional, seria o particionamento de tabela (table partitioning)
do próprio banco: em vez de metadados internos, a partição vira **estrutura de
diretórios** — `order_year=2025/order_month=01/arquivo.parquet` — e o VALOR da
coluna fica codificado no nome da pasta, não dentro dos arquivos.

Comandos usados:

`read_parquet(glob, hive_partitioning=true)`
    O glob (`orders/**/*.parquet`) descobre os arquivos; a flag faz o DuckDB
    reconstruir `order_year`/`order_month` como colunas do resultado a partir
    do caminho. Sem a flag, essas colunas simplesmente não existem no schema.
    Atenção a um detalhe de tipos: o DuckDB tenta inferir o tipo do valor da
    partição, mas só quando isso não perde informação. `order_year=2025` vira
    BIGINT; já `order_month=01` fica VARCHAR — o zero à esquerda seria perdido
    num inteiro, então o DuckDB mantém a string. Consequência prática: filtrar
    `order_month` por igualdade com inteiro (`= 1`) funciona por cast
    implícito, mas comparações de faixa (`<=`) pedem CAST explícito.

**Partition pruning** — o equivalente data-lake do "index scan"
    Quando o filtro usa uma coluna de partição (`WHERE order_month = 1`), o
    DuckDB decide POR NOME DE DIRETÓRIO quais arquivos abrir: das 6
    partições, lê 1 — os outros arquivos nem são abertos. Numa base
    transacional, evitar a varredura completa é papel do índice; aqui é o
    desenho de diretórios que cumpre essa função (por isso a escolha da
    coluna de particionamento é A decisão de modelagem de um data lake).

`EXPLAIN SELECT ...`
    Mostra o plano de execução ANTES de rodar (sem executar a query). No
    plano, `Scanning Files: 1/6` é a prova do pruning. (O exemplo 12 usa a
    variante `EXPLAIN ANALYZE`, que executa e mede tempos reais.)

`glob('padrão')`
    Função de tabela do DuckDB que lista os arquivos que batem com o padrão —
    útil para inspecionar o que uma leitura enxergaria, sem ler nada.

Rode com: `uv run examples/02_reading_partitioned_parquet.py`
"""

import duckdb

from _common import ORDERS_GLOB, section

if __name__ == "__main__":
    con = duckdb.connect()

    section("Arquivos descobertos pelo glob")
    for row in con.sql(f"SELECT * FROM glob('{ORDERS_GLOB}')").fetchall():
        print(row[0])

    section("Leitura com hive_partitioning=true: colunas de partição aparecem no schema")
    con.sql(
        f"SELECT * FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true) LIMIT 3"
    ).show()

    section("EXPLAIN sem filtro: teria que varrer as 6 partições")
    plano_completo = con.sql(
        f"""
        EXPLAIN SELECT COUNT(*) FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
        """
    ).fetchall()
    print(plano_completo[0][1])

    section("EXPLAIN filtrando por order_month=1: só a partição de janeiro é lida")
    plano_filtrado = con.sql(
        f"""
        EXPLAIN SELECT COUNT(*)
        FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
        WHERE order_month = 1
        """
    ).fetchall()
    print(plano_filtrado[0][1])

    section("Confirmando o resultado do filtro por partição")
    total_janeiro = con.sql(
        f"""
        SELECT COUNT(*) FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
        WHERE order_month = 1
        """
    ).fetchone()
    print(f"pedidos em janeiro/2025: {total_janeiro[0]:,}")
