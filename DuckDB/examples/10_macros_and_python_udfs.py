"""Exemplo 10 — Macros SQL e UDFs Python (linha a linha e vetorizada via Arrow).

Como encapsular lógica reutilizável — o papel que stored procedures e
functions cumprem numa base transacional. O DuckDB não tem stored procedures;
tem dois mecanismos mais simples, cada um com seu lugar:

`CREATE MACRO nome(args) AS expressão`
    Macro **escalar**: um nome para uma expressão SQL. Diferente de uma
    function de banco tradicional (que é chamada em runtime), a macro é
    expandida INLINE pelo otimizador antes de executar — custo zero, como um
    `#define`. Limitação correspondente: é uma expressão única, sem corpo
    procedural (sem loops/variáveis).

`CREATE MACRO nome(args) AS TABLE SELECT ...`
    Macro de **tabela**: uma "view com parâmetros" — `FROM pedidos_do_mes(3)`.
    Views não aceitam argumentos em nenhum banco; a macro de tabela resolve
    exatamente isso, e é ótima para padronizar leituras parametrizadas
    (mês de referência, cliente, cenário) entre etapas do ETL.

`con.create_function(nome, fn_python, [tipos], tipo_retorno)`
    Registra uma função **Python** chamável de dentro do SQL — o análogo de
    criar uma UDF no servidor, exceto que aqui "o servidor" é o seu processo.
    No modo default a função é chamada UMA VEZ POR LINHA, com o custo de
    atravessar a fronteira SQL->Python milhões de vezes: flexível, lento.

`con.create_function(..., type="arrow")`
    A variante que importa em ETL: a função recebe/devolve **vetores Arrow**
    (um chunk de milhares de linhas por chamada), e o corpo roda vetorizado
    (`pyarrow.compute`, C++). O placar do exemplo: 5.6M de linhas em ~0.5s —
    a versão linha a linha levaria minutos. É a mesma filosofia da extensão
    Rust do tutorial (`../rust-extension`): atravessar a fronteira POR LOTE,
    nunca por linha.

Rode com: `uv run examples/10_macros_and_python_udfs.py`
"""

import time
import unicodedata

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
# nas versões antigas do pacote python era `duckdb.typing`; desde a 1.5 é `duckdb.sqltypes`
from duckdb.sqltypes import DOUBLE, VARCHAR

from _common import ORDERS_GLOB, PRODUCTS_GLOB, section


def remover_acentos(texto: str) -> str:
    """UDF linha a linha: normaliza unicode e descarta os acentos."""
    if texto is None:
        return None
    decomposto = unicodedata.normalize("NFKD", texto)
    return "".join(ch for ch in decomposto if not unicodedata.combining(ch))


def desconto_progressivo(preco: pa.lib.ChunkedArray) -> pa.lib.ChunkedArray:
    """UDF vetorizada (type="arrow"): recebe um vetor Arrow, devolve um vetor.

    A lógica roda no pyarrow.compute (C++), não em loop Python: 10% de
    desconto acima de 100, 5% acima de 50, sem desconto abaixo.
    """
    return pc.if_else(
        pc.greater(preco, 100.0),
        pc.multiply(preco, 0.90),
        pc.if_else(pc.greater(preco, 50.0), pc.multiply(preco, 0.95), preco),
    )


if __name__ == "__main__":
    con = duckdb.connect()

    section("CREATE MACRO escalar: faixa de quantidade")
    con.execute(
        """
        CREATE MACRO faixa_qtd(q) AS
            CASE WHEN q >= 8 THEN 'alta' WHEN q >= 4 THEN 'media' ELSE 'baixa' END
        """
    )
    con.sql(
        f"""
        SELECT faixa_qtd(quantity) AS faixa, COUNT(*) AS pedidos
        FROM read_parquet('{ORDERS_GLOB}')
        GROUP BY faixa ORDER BY pedidos DESC
        """
    ).show()

    section("CREATE MACRO ... AS TABLE: 'view parametrizada' por mês de referência")
    con.execute(
        f"""
        CREATE MACRO pedidos_do_mes(mes) AS TABLE
        SELECT * FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
        WHERE order_month = mes
        """
    )
    con.sql(
        "SELECT COUNT(*) AS pedidos_mes_3 FROM pedidos_do_mes(3)"
    ).show()

    section('UDF Python linha a linha (type="native"): remover acentos')
    con.create_function("remover_acentos", remover_acentos, [VARCHAR], VARCHAR)
    con.sql(
        """
        SELECT nome, remover_acentos(nome) AS sem_acento
        FROM (VALUES ('Alimentação'), ('Eletrônicos'), ('Vestuário')) t(nome)
        """
    ).show()

    section('UDF Python vetorizada (type="arrow"): desconto progressivo')
    con.create_function(
        "desconto_progressivo", desconto_progressivo, [DOUBLE], DOUBLE, type="arrow"
    )
    con.sql(
        f"""
        SELECT product_name, unit_price,
               ROUND(desconto_progressivo(unit_price), 2) AS preco_final
        FROM read_parquet('{PRODUCTS_GLOB}')
        ORDER BY unit_price DESC LIMIT 5
        """
    ).show()

    section("Por que 'arrow' importa: mesma UDF nos 200 produtos x 5.6M de pedidos")
    inicio = time.perf_counter()
    con.sql(
        f"""
        SELECT SUM(desconto_progressivo(p.unit_price * o.quantity)) AS total_com_desconto
        FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true) o
        JOIN read_parquet('{PRODUCTS_GLOB}') p USING (product_id)
        WHERE o.order_month = 1
        """
    ).show()
    print(f"UDF arrow sobre 5.6M linhas: {time.perf_counter() - inicio:.2f}s")
    print("(a versão linha a linha levaria minutos: 5.6M chamadas Python individuais)")

    section("Macros e UDFs aparecem no catálogo como funções normais")
    con.sql(
        """
        SELECT function_name, function_type
        FROM duckdb_functions()
        WHERE function_name IN ('faixa_qtd', 'pedidos_do_mes', 'remover_acentos', 'desconto_progressivo')
        ORDER BY function_name
        """
    ).show()
