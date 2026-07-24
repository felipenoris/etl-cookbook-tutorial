"""Exemplo 8 — Ingestão de CSV sujo, linhas rejeitadas e profiling.

Ingestão é onde ETLs quebram na prática — e o instinto de quem vem de bases
transacionais ("carga aborta na primeira linha inválida, corrija e recarregue")
não escala para arquivos de terceiros. Este exemplo mostra o ferramental do
DuckDB para ingerir dados imperfeitos sem parar o pipeline.

Comandos usados:

`read_csv('arquivo.csv')` com auto-detecção
    O *sniffer* examina uma amostra e descobre sozinho delimitador, header e
    tipos de cada coluna. Cuidado com o efeito colateral didático mostrado
    aqui: se uma coluna numérica tem UM valor texto, o sniffer resolve o
    conflito rebaixando a coluna inteira para VARCHAR — a leitura funciona,
    mas os tipos degradam silenciosamente. Por isso, cargas recorrentes devem
    fixar `types={...}` para o sniffer não decidir sozinho.

`store_rejects=true` + tabelas `reject_errors`/`reject_scans`
    O padrão **quarentena**: em vez de abortar no primeiro erro (comportamento
    default, e o único disponível na maioria das bases), o DuckDB carrega as
    linhas boas e registra as ruins na tabela temporária `reject_errors` —
    com linha, coluna, valor e motivo da rejeição. O ETL segue com o que
    presta; as rejeitadas viram relatório para correção na origem.
    (Pegadinha coberta nos testes: um `COUNT(*)` puro não parseia as colunas
    e portanto não gera rejeições — materialize colunas de verdade.)

`SUMMARIZE tabela_ou_query`
    Profiling instantâneo: min, max, nulos, cardinalidade aproximada e
    percentis de TODAS as colunas em um comando. Não existe equivalente SQL
    padrão (seria um SELECT gigante de agregados por coluna); como primeiro
    contato com um parquet desconhecido, é a ferramenta certa.

`FROM ... USING SAMPLE 1 PERCENT (system)`
    Amostragem nativa na leitura. `system` sorteia blocos inteiros (rápido,
    granularidade grossa); `bernoulli`/`reservoir` sorteiam linha a linha
    (mais uniformes, mais caros). Padrão de trabalho: desenvolver a
    transformação na amostra, rodar no total só no final.

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
