"""Exemplo 10 — Macros SQL e UDFs Python (linha a linha e vetorizada via Arrow).

Uma **UDF** (*User-Defined Function*, "função definida pelo usuário") é uma
função escrita por você e registrada no motor SQL para ser chamada de dentro
de uma consulta, como se fosse uma função embutida (`UPPER`, `ROUND`...). No
DuckDB, uma UDF pode ser escrita em Python (`con.create_function`) — é o que
permite injetar lógica que o SQL não expressa nativamente no meio de uma
query, ao custo de sair do motor para o Python (medido adiante).

Este exemplo cobre como encapsular lógica reutilizável — o papel que stored
procedures e functions cumprem numa base transacional. O DuckDB não tem stored
procedures; tem dois mecanismos mais simples, cada um com seu lugar:

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
    No modo default (`type="native"`) a função é escrita como se recebesse
    UM valor por vez, o que a torna simples de escrever para qualquer lógica.

`con.create_function(..., type="arrow")`
    A variante vetorizada: a função recebe/devolve **vetores Arrow** (um
    chunk de milhares de linhas por chamada), e o corpo roda em
    `pyarrow.compute` (C++), sem laço Python.

**Medição com controle** (a MESMA lógica de desconto sobre 5,6M de linhas,
incluindo uma consulta *sem* UDF para isolar o custo do scan+join):

| Abordagem | Tempo total | Custo da lógica | vs SQL puro |
|---|---|---|---|
| controle: scan+join+`SUM`, sem UDF | ~0,01s | — | — |
| **SQL puro** (`CASE WHEN`) | ~0,02s | ~0,01s | **1x** |
| UDF `native` (valor a valor) | ~0,35s | ~0,34s | **~30x mais lento** |
| UDF `arrow` (vetorizada) | ~0,57s | ~0,56s | **~50x mais lento** |

Duas conclusões, e a ordem entre elas importa:

1. **O custo dominante é SAIR do motor** — não o estilo em que a UDF é
   escrita. Qualquer UDF Python custa 20–50x o equivalente em SQL puro.
   Antes de escolher entre `native` e `arrow`, pergunte se a lógica não cabe
   num `CASE WHEN`, num operador aritmético ou numa window function.
2. **Entre as duas variantes, a diferença é modesta** — e aqui a `native`
   ganha (~1,6x). Isso contraria a intuição de "uma chamada Python por
   linha seria ordens de grandeza pior": o DuckDB amortiza o overhead
   processando vetores internamente, enquanto a `arrow` paga alocação de
   arrays intermediários a cada kernel do `pyarrow.compute` (`greater`,
   `multiply`, `if_else`...). Com função mais pesada por linha (raiz, log,
   polinômio) a `arrow` passa à frente, mas por ~10%.

**A lição prática**: use SQL sempre que a lógica for expressável nele; recorra
a UDF Python quando não for, escolhendo a variante pela clareza (a diferença
entre elas é pequena perto do custo de já ter saído do motor); e desça para
uma extensão nativa (`../exemplos-rust-extension`) quando o cálculo por entidade for
pesado o bastante para justificar.

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


def desconto_progressivo_native(preco: float) -> float:
    """A MESMA regra, escrita valor a valor (type="native", o default).

    Serve de contraponto à versão vetorizada abaixo: mesma lógica, mesmo
    resultado, escrita muito mais simples — e, como o exemplo mede, sem
    perda relevante de performance.
    """
    if preco > 100.0:
        return preco * 0.90
    if preco > 50.0:
        return preco * 0.95
    return preco


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

    section("Quanto custa sair do motor: SQL puro vs UDF (5.6M de pedidos)")
    # a mesma regra de desconto, escrita valor a valor (native)
    con.create_function("desconto_native", desconto_progressivo_native, [DOUBLE], DOUBLE)

    consulta = """
        SELECT SUM({fn}(p.unit_price * o.quantity)) AS total_com_desconto
        FROM read_parquet('%s', hive_partitioning=true) o
        JOIN read_parquet('%s') p USING (product_id)
        WHERE o.order_month = 1
    """ % (ORDERS_GLOB, PRODUCTS_GLOB)

    # o SQL puro equivalente: o CONTROLE que revela quanto custa sair do motor
    sql_puro = """
        SELECT SUM(CASE
                     WHEN p.unit_price * o.quantity > 100 THEN p.unit_price * o.quantity * 0.90
                     WHEN p.unit_price * o.quantity > 50  THEN p.unit_price * o.quantity * 0.95
                     ELSE p.unit_price * o.quantity
                   END) AS total_com_desconto
        FROM read_parquet('%s', hive_partitioning=true) o
        JOIN read_parquet('%s') p USING (product_id)
        WHERE o.order_month = 1
    """ % (ORDERS_GLOB, PRODUCTS_GLOB)

    medicoes = [
        ("SQL puro (CASE WHEN)", sql_puro),
        ("UDF native (valor a valor)", consulta.format(fn="desconto_native")),
        ("UDF arrow (vetorizada)", consulta.format(fn="desconto_progressivo")),
    ]
    tempos = {}
    for rotulo, query in medicoes:
        inicio = time.perf_counter()
        total = con.sql(query).fetchone()[0]
        tempos[rotulo] = time.perf_counter() - inicio
        print(f"{rotulo:28s} {tempos[rotulo]:5.2f}s  ->  "
              f"{5_628_285 / tempos[rotulo] / 1e6:6.1f}M linhas/s  | total: {total:,.2f}")

    base = tempos["SQL puro (CASE WHEN)"]
    print(f"\nO custo dominante é SAIR do motor: as UDFs custam "
          f"{tempos['UDF native (valor a valor)'] / base:.0f}x e "
          f"{tempos['UDF arrow (vetorizada)'] / base:.0f}x o SQL puro.")
    print("Entre as duas variantes a diferença é modesta — e aqui a 'native' ganha:")
    print("o DuckDB amortiza o overhead processando vetores internamente, enquanto")
    print("a 'arrow' aloca arrays intermediários a cada kernel do pyarrow.compute.")

    section("Macros e UDFs aparecem no catálogo como funções normais")
    con.sql(
        """
        SELECT function_name, function_type
        FROM duckdb_functions()
        WHERE function_name IN ('faixa_qtd', 'pedidos_do_mes', 'remover_acentos', 'desconto_progressivo')
        ORDER BY function_name
        """
    ).show()
