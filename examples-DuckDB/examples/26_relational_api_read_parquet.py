"""Exemplo 26 — `con.read_parquet`: a API relacional, ou SQL sem escrever SQL.

Todos os exemplos anteriores leem parquet do mesmo jeito: uma string SQL com
`read_parquet('...')` dentro. O DuckDB oferece um segundo caminho, que quase
não aparece nos tutoriais: **`con.read_parquet(glob)`**, um método da conexão
que devolve um objeto `DuckDBPyRelation`.

O ponto que importa entender: essa relation **não é o resultado**. Ela é a
*consulta* — uma árvore de operadores ainda não executada, exatamente como a
que `con.sql("SELECT ...")` devolve. Tanto que ela sabe se imprimir como SQL:
`rel.sql_query()` mostra o `SELECT` equivalente.

## Por que existir um segundo caminho

Porque em Python uma query costuma ser **montada por partes**: um filtro que só
existe se o usuário passou `--mes`, uma projeção que depende de configuração,
uma agregação escolhida em runtime. Fazer isso com string SQL vira concatenação
— frágil, e a porta de entrada da injeção de SQL (ver exemplo 22). Com a
relation, cada passo é uma chamada de método que devolve uma nova relation:

```python
rel = con.read_parquet(GLOB, hive_partitioning=True)
if mes:
    rel = rel.filter(f"order_month = '{mes:02d}'")
rel = rel.aggregate("status, count(*) AS n", "status")
```

Nada executou ainda. A leitura só acontece no `.fetchall()`/`.to_arrow_table()`
do fim — e o otimizador vê a query inteira de uma vez, então o filtro desce até
o leitor de parquet do mesmo jeito que desceria no SQL.

## O que a relation NÃO é

- **Não é um cache.** Cada consumo re-executa o plano do zero. Duas chamadas de
  `.fetchall()` na mesma relation leem o parquet duas vezes.
- **Não é portátil entre conexões.** A relation carrega a conexão que a criou;
  entregá-la a outra conexão não funciona (o fim deste exemplo mostra o erro).
- **Não é um snapshot.** O glob é reexpandido a cada execução: arquivo novo no
  diretório aparece na próxima leitura.

Esses três pontos são a mesma propriedade vista de ângulos diferentes — e são
exatamente os de uma `VIEW`. É o que o exemplo 27 formaliza, dando nome à
relation com `con.register`.

## A pegadinha que custa 6x (medida aqui)

`filter()` recebe **texto de expressão SQL**, e comparação entre tipos
diferentes vira um `CAST` implícito. Nas colunas de partição isso é caro: elas
vêm do *nome do diretório*, então `order_month` é `VARCHAR` (`'01'`), e escrever
`filter("order_month = 1")` gera `CAST(order_month AS INTEGER) = 1` — expressão
que o DuckDB **não consegue usar para descartar arquivos**. O plano ganha um
operador `FILTER` e os 6 arquivos são abertos. Com o literal no tipo nativo,
`filter("order_month = '01'")`, o plano mostra `Scanning Files: 1/6`.

Rode com: `uv run examples/26_relational_api_read_parquet.py`
"""

from __future__ import annotations

import time

import duckdb

from _common import ORDERS_GLOB, PRODUCTS_GLOB, section


def montar_consulta(
    con: duckdb.DuckDBPyConnection,
    mes: int | None = None,
    status: str | None = None,
) -> duckdb.DuckDBPyRelation:
    """Monta incrementalmente a consulta de orders, aplicando só os filtros pedidos.

    O ponto do exemplo: os `if` ficam em Python, não em concatenação de string
    SQL. Cada `.filter()` devolve uma **nova** relation — nada é mutado, nada é
    executado, e o otimizador ainda enxerga a query inteira no fim.

    Args:
        con: conexão que vai executar a consulta (a relation fica presa a ela).
        mes: se informado, restringe ao mês de partição. Formatado como `'01'`,
            o tipo nativo da coluna de partição, para preservar o pruning.
        status: se informado, restringe ao status do pedido.

    Returns:
        A relation com a agregação por status — **ainda não executada**.
    """
    rel = con.read_parquet(ORDERS_GLOB, hive_partitioning=True)
    if mes is not None:
        rel = rel.filter(f"order_month = '{mes:02d}'")
    if status is not None:
        # `filter` recebe texto de expressão SQL: escapar literais continua
        # sendo responsabilidade de quem monta (ver exemplo 22)
        rel = rel.filter("status = '{}'".format(status.replace("'", "''")))
    return rel.aggregate("status, count(*) AS pedidos, sum(quantity) AS itens", "status")


def resumo_do_plano(plano: str) -> list[str]:
    """Extrai do `EXPLAIN` só as linhas que interessam: operadores e pushdowns.

    O plano vem desenhado em caixas unicode; esta função descarta as bordas e
    devolve o conteúdo textual de cada linha, para os trechos poderem ser
    comparados entre si.
    """
    linhas = []
    for linha in plano.splitlines():
        texto = linha.strip(" │").strip()
        if texto and not set(texto) <= set("─┌┐└┘├┤┬┴┼"):
            linhas.append(texto)
    return linhas


if __name__ == "__main__":
    con = duckdb.connect()

    section("con.read_parquet devolve uma relation, não dados")
    inicio = time.perf_counter()
    orders = con.read_parquet(ORDERS_GLOB, hive_partitioning=True)
    t_lazy = time.perf_counter() - inicio

    inicio = time.perf_counter()
    (linhas,) = orders.aggregate("count(*)").fetchone()
    t_exec = time.perf_counter() - inicio

    print(f"tipo devolvido: {type(orders).__name__}")
    print(f"con.read_parquet(...):   {t_lazy * 1000:7.1f}ms  (só monta o plano)")
    print(f"primeiro .fetchone():    {t_exec * 1000:7.1f}ms  ({linhas:,} linhas)")
    print("O schema, porém, já é conhecido — vem do footer, sem varrer os dados:")
    for nome, tipo in zip(orders.columns, orders.types):
        print(f"  {nome:14s} {tipo}")
    print("(order_year/order_month não estão em arquivo nenhum: vêm do nome dos")
    print(" diretórios, via hive_partitioning=true — e order_month é VARCHAR)")

    section("A relation É uma query: ela sabe se imprimir como SQL")
    print(orders.limit(3).sql_query())

    section("Encadeamento: cada método devolve uma nova relation")
    consulta = (
        orders.filter("order_month = '01'")
        .project("status, quantity")
        .aggregate("status, count(*) AS pedidos, sum(quantity) AS itens", "status")
        .order("itens DESC")
    )
    print("montado, ainda não executado. Agora sim:")
    consulta.show()

    section("A montagem condicional, que é o motivo de a API existir")
    for kwargs in ({}, {"mes": 1}, {"mes": 1, "status": "entregue"}):
        descricao = ", ".join(f"{k}={v!r}" for k, v in kwargs.items()) or "sem filtro"
        resultado = montar_consulta(con, **kwargs).order("pedidos DESC").fetchall()
        total = sum(linha[1] for linha in resultado)
        print(f"{descricao:32s} -> {len(resultado)} status, {total:,} pedidos")

    section("Relation e string SQL convergem: o mesmo pushdown até o parquet")
    via_relation = orders.filter("quantity > 5").aggregate("count(*)")
    via_sql = con.sql(
        f"""
        SELECT count(*) FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)
        WHERE quantity > 5
        """
    )
    print(f"resultados iguais: {via_relation.fetchone() == via_sql.fetchone()}")
    for rotulo, relacao in (("relation", via_relation), ("SQL     ", via_sql)):
        plano = resumo_do_plano(relacao.explain())
        operadores = [linha for linha in dict.fromkeys(plano) if linha.isupper()]
        filtros = [linha for linha in plano if linha.startswith("Filters:")]
        print(f"{rotulo}: {' -> '.join(operadores)}  |  {filtros[0]}")
    print("O predicado desceu ATÉ o leitor de parquet nos dois casos: não há operador")
    print("FILTER separado. O otimizador não distingue de onde a query veio.")

    section("Onde a equivalência QUEBRA: cast implícito na coluna de partição")

    def medir(fabrica, repeticoes: int = 3) -> float:
        """Menor tempo de N execuções — o menor é o menos contaminado por ruído."""
        melhor = float("inf")
        for _ in range(repeticoes):
            inicio = time.perf_counter()
            fabrica()
            melhor = min(melhor, time.perf_counter() - inicio)
        return melhor

    variantes = {
        "relation, order_month = 1    (INTEGER)": lambda: con.read_parquet(
            ORDERS_GLOB, hive_partitioning=True
        ).filter("order_month = 1"),
        "relation, order_month = '01' (VARCHAR)": lambda: con.read_parquet(
            ORDERS_GLOB, hive_partitioning=True
        ).filter("order_month = '01'"),
        "SQL,      order_month = 1    (INTEGER)": lambda: con.sql(
            f"SELECT * FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true) "
            "WHERE order_month = 1"
        ),
    }
    for rotulo, fabrica in variantes.items():
        plano = resumo_do_plano(fabrica().project("order_id").explain())
        arquivos = next((linha for linha in plano if linha.startswith("Scanning Files")), "6/6")
        tem_filter = "FILTER" in plano
        tempo = medir(lambda f=fabrica: f().aggregate("count(*)").fetchone())
        print(f"{rotulo}: {tempo * 1000:6.1f}ms  {arquivos:20s} operador FILTER: {tem_filter}")
    print("\nNa relation, `order_month = 1` vira CAST(order_month AS INTEGER) = 1 — e o")
    print("pruning por arquivo não sabe avaliar isso, então abre os 6 e filtra depois.")
    print("Escrito no tipo nativo da partição ('01'), o descarte volta a acontecer.")
    print("No SQL o otimizador reescreve o cast sozinho; na relation, não. Regra:")
    print("**na API relacional, compare coluna de partição com literal do tipo dela.**")

    section("Voltando ao SQL quando o SQL é melhor: rel.query()")
    # `query(nome, sql)` dá um nome temporário à relation e roda SQL contra ele.
    # Serve para o que a API fluente não cobre bem: window functions, QUALIFY, CTEs.
    top_por_mes = orders.filter("order_month IN ('01', '02', '03')").query(
        "o",
        """
        SELECT order_month, status, count(*) AS pedidos
        FROM o
        GROUP BY order_month, status
        QUALIFY row_number() OVER (PARTITION BY order_month ORDER BY count(*) DESC) = 1
        ORDER BY order_month
        """,
    )
    top_por_mes.show()
    print("Os dois estilos se misturam livremente: a relation entra no SQL pelo nome,")
    print("e o SELECT resultante volta a ser uma relation.")

    section("Join entre duas relations, sem SQL")
    produtos = con.read_parquet(PRODUCTS_GLOB)
    junção = (
        orders.filter("order_month = '01'")
        .set_alias("o")
        .join(produtos.set_alias("p"), "o.product_id = p.product_id")
        .aggregate("p.category, count(*) AS pedidos, sum(o.quantity) AS itens", "p.category")
        .order("itens DESC")
        .limit(5)
    )
    junção.show()

    section("Lazy de verdade: consumir duas vezes lê duas vezes")
    pesada = orders.aggregate("status, sum(quantity) AS itens", "status")
    tempos = []
    for _ in range(2):
        inicio = time.perf_counter()
        pesada.fetchall()
        tempos.append(time.perf_counter() - inicio)
    print(f"1ª execução: {tempos[0] * 1000:6.0f}ms")
    print(f"2ª execução: {tempos[1] * 1000:6.0f}ms  (não há cache: o plano roda de novo)")
    print("Para pagar a leitura UMA vez, materialize: .to_table('nome') faz o CTAS,")
    print("e a partir daí as consultas leem o storage interno (ver exemplo 11).")

    section("Saídas: a relation entrega em qualquer formato")
    pequena = orders.filter("order_id % 1000000 = 0").project("order_id, customer_id, quantity")
    print(f".fetchall()        -> list de {len(pequena.fetchall())} tuplas")
    print(f".to_arrow_table()  -> {type(pequena.to_arrow_table()).__name__}")
    print(f".df()              -> {type(pequena.df()).__name__}")
    print(f".to_arrow_reader() -> {type(pequena.to_arrow_reader(1024)).__name__} (streaming, exemplo 24)")
    print(".to_parquet(path)  -> escreve direto em disco, sem passar pelo Python")

    section("A pegadinha final: a relation pertence à conexão que a criou")
    outra = duckdb.connect()
    # `orders` é uma variável do escopo: a outra conexão ATÉ a encontra pelo
    # replacement scan — e recusa explicitamente, porque ela não é dela.
    try:
        outra.sql("SELECT count(*) FROM orders").fetchone()
    except duckdb.Error as erro:
        print(f"{type(erro).__name__}:")
        for linha in str(erro).splitlines():
            print(f"  {linha}")
    print("\nUma relation carrega a conexão dentro dela. Para atravessar conexões,")
    print("o que viaja é o DADO (Arrow) ou o TEXTO da query — nunca o objeto.")
    outra.close()

    section("A lição")
    print("1. con.read_parquet devolve uma QUERY (DuckDBPyRelation), não um resultado;")
    print("   nada é lido até o primeiro .fetchall()/.to_arrow_table();")
    print("2. cada método (.filter/.project/.aggregate/.join) devolve uma nova relation —")
    print("   é isso que permite montar a consulta com if e for, sem colar string SQL;")
    print("3. para colunas normais, relation e SQL geram o mesmo pushdown até o parquet;")
    print("   .sql_query() mostra o SELECT equivalente e .query() volta ao SQL quando ele")
    print("   é mais claro (window functions, QUALIFY, CTE);")
    print("4. MAS o cast implícito numa coluna de partição mata o pruning na relation:")
    print("   compare com literal do tipo nativo da partição ('01', não 1);")
    print("5. não há cache — consumir N vezes lê N vezes; materialize com .to_table()")
    print("   se a mesma consulta vai ser reusada;")
    print("6. a relation é presa à sua conexão — entre conexões viaja dado ou texto.")
