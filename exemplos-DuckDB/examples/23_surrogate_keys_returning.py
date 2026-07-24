"""Exemplo 23 — Chaves primárias sequenciais (surrogate keys) e como resgatá-las com RETURNING.

Um problema recorrente de ETL: a dimensão de um *star schema* quer uma chave
primária **sequencial e estreita** (um `BIGINT` 1, 2, 3...) — a *surrogate key*
— em vez de usar a chave do sistema de origem (a *natural key*, que pode ser
larga, composta, ou mudar com o tempo). Duas perguntas práticas, que este
exemplo responde medindo:

1. **Como declarar uma chave que o banco preenche sozinho, em sequência?**
2. **Depois de importar um LOTE, como recuperar as chaves que o banco gerou** —
   para, por exemplo, ligar a tabela-fato à dimensão pela surrogate key?

Ponto de partida importante: **o DuckDB não tem `AUTO_INCREMENT` nem
`GENERATED ALWAYS AS IDENTITY`** (essa constraint dá "Not implemented"). O
idioma equivalente tem duas peças:

`CREATE SEQUENCE seq START 1`
    Um gerador de números independente da tabela: cada `nextval('seq')`
    devolve o próximo inteiro, sem repetir, de forma atômica (seguro sob
    concorrência — ver exemplo 21). A sequência é global e **não reinicia**
    entre cargas, o que garante chaves únicas ao longo de vários lotes.

`coluna BIGINT DEFAULT nextval('seq') PRIMARY KEY`
    Amarra a sequência à coluna: quando o `INSERT` **omite** a surrogate key, o
    `DEFAULT` chama `nextval` e preenche o próximo valor. É o análogo do
    `SERIAL`/`IDENTITY` de outros bancos, montado com as duas peças acima.

`INSERT ... SELECT ... RETURNING sk, natural_key`
    A cláusula `RETURNING` (também no `UPDATE`/`DELETE`) faz o `INSERT`
    **devolver linhas** — as que acabaram de entrar, já com as colunas
    preenchidas pelo banco (a surrogate key gerada, `DEFAULT`s, colunas
    geradas). É assim que se "resgata" a chave de um lote inteiro numa só ida
    ao banco, sem um `SELECT` extra depois.

A lição central (e a pegadinha), demonstrada na prática abaixo: **a ordem das
linhas do `RETURNING` NÃO é garantida** — o `INSERT ... SELECT` é paralelo, e a
surrogate key 1 pode não cair na primeira linha do `SELECT`. Por isso o
`RETURNING` traz **a natural key junto com a surrogate**: o mapa
natural→surrogate se monta por esse par (um join), nunca pela posição da linha.

Rode com: `uv run examples/23_surrogate_keys_returning.py`
"""

import duckdb

from _common import CUSTOMERS_GLOB, ORDERS_GLOB, section

if __name__ == "__main__":
    con = duckdb.connect()
    con.execute(
        f"CREATE VIEW customers AS SELECT * FROM read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true)"
    )
    con.execute(
        f"CREATE VIEW orders AS SELECT * FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true)"
    )

    section("O DuckDB NÃO tem IDENTITY/AUTO_INCREMENT — o idioma é SEQUENCE + DEFAULT nextval")
    try:
        con.execute("CREATE TABLE tem_identity (id INTEGER GENERATED ALWAYS AS IDENTITY)")
    except duckdb.NotImplementedException as exc:
        print(f"GENERATED ALWAYS AS IDENTITY -> {str(exc).splitlines()[0]}")
    # o equivalente: uma SEQUENCE alimenta o DEFAULT da coluna de surrogate key
    con.execute("CREATE SEQUENCE seq_cliente START 1")
    con.execute(
        """
        CREATE TABLE dim_cliente (
            sk_cliente  BIGINT DEFAULT nextval('seq_cliente') PRIMARY KEY,  -- surrogate: o banco preenche
            customer_id BIGINT UNIQUE,     -- natural key (vem do sistema de origem)
            nome        VARCHAR,
            regiao      VARCHAR
        )
        """
    )
    print("dim_cliente criada: sk_cliente é preenchida por nextval('seq_cliente') quando omitida no INSERT.")

    section("Carga em lote da dimensão: RETURNING resgata as surrogate keys geradas")
    # o INSERT omite sk_cliente (o DEFAULT a preenche) e pede de volta o par (surrogate, natural)
    mapa = con.execute(
        """
        INSERT INTO dim_cliente (customer_id, nome, regiao)
        SELECT customer_id, customer_name, region
        FROM customers
        WHERE customer_id <= 6
        RETURNING sk_cliente, customer_id
        """
    ).fetchall()
    print("mapa (sk_cliente, customer_id) NA ORDEM EM QUE O RETURNING DEVOLVEU:")
    for sk, cid in mapa:
        print(f"    sk={sk}  <-  customer_id={cid}")
    fora_de_ordem = [cid for _sk, cid in mapa] != sorted(cid for _sk, cid in mapa)
    print(f"-> as surrogate keys saíram fora da ordem da natural key? {fora_de_ordem}")
    print("   (o INSERT...SELECT é paralelo). Por isso o RETURNING traz a natural key JUNTO:")
    print("   o mapa se liga pelo par (sk, customer_id), nunca pela posição da linha.")

    section("Usando o mapa: carregar a tabela-fato referenciando a SURROGATE key")
    con.execute(
        "CREATE TABLE fato_pedido (order_id BIGINT, sk_cliente BIGINT, quantidade INTEGER)"
    )
    # traduz a natural key das orders para a surrogate key, via join com a dimensão
    # (o mapa capturado acima está materializado na própria dim_cliente)
    inseridas = con.execute(
        """
        INSERT INTO fato_pedido (order_id, sk_cliente, quantidade)
        SELECT o.order_id, d.sk_cliente, o.quantity
        FROM orders o
        JOIN dim_cliente d USING (customer_id)   -- natural key -> surrogate key
        WHERE o.order_month = 1
        RETURNING order_id
        """
    ).fetchall()
    print(f"{len(inseridas):,} linhas de fato inseridas, já apontando para sk_cliente (não para customer_id).")
    con.sql(
        """
        SELECT f.order_id, f.sk_cliente, d.customer_id, d.nome
        FROM fato_pedido f JOIN dim_cliente d USING (sk_cliente)
        ORDER BY f.order_id LIMIT 4
        """
    ).show()

    section("Carga incremental: anti-join insere só os ausentes; a sequência continua global")
    # um novo lote traz clientes já existentes (3, 5) e um novo (999); só o novo deve ganhar sk
    con.execute(
        "CREATE TABLE novos_clientes AS SELECT * FROM (VALUES (3, 'ja_existe', 'sul'), (999, 'cliente_novo', 'norte')) t(customer_id, nome, regiao)"
    )
    delta = con.execute(
        """
        INSERT INTO dim_cliente (customer_id, nome, regiao)
        SELECT customer_id, nome, regiao
        FROM novos_clientes src
        WHERE NOT EXISTS (SELECT 1 FROM dim_cliente d WHERE d.customer_id = src.customer_id)
        RETURNING sk_cliente, customer_id
        """
    ).fetchall()
    print(f"clientes realmente novos inseridos (RETURNING): {delta}")
    print("-> o cliente 3 já existia (o anti-join o descartou); só o 999 ganhou surrogate key.")
    print("   A sequência não reinicia: a nova sk continua de onde parou, sem colidir com as antigas.")
    con.sql("SELECT currval('seq_cliente') AS ultima_sk_usada").show()

    section("Resumo")
    print("- sem IDENTITY: use CREATE SEQUENCE + coluna BIGINT DEFAULT nextval('seq') PRIMARY KEY;")
    print("- RETURNING resgata as chaves geradas por um lote inteiro numa só ida ao banco;")
    print("- SEMPRE devolva a natural key junto (RETURNING sk, natural_key): a ordem não é garantida;")
    print("- monte a fato traduzindo natural->surrogate por join com a dimensão;")
    print("- carga incremental: anti-join (NOT EXISTS) insere só os ausentes; a sequência é global.")
