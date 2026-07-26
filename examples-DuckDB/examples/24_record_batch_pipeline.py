"""Exemplo 24 — `to_arrow_reader`: o padrão "lote entra, lote sai" do Rust.

O `rust-extension` tem duas funções que seguem sempre a **mesma forma**
(`add_line_total` e `compute_customer_running_spend`): recebem um
`RecordBatch`, calculam UMA ou DUAS colunas novas, e devolvem um `RecordBatch`
com o schema estendido — **reaproveitando as colunas de entrada por
referência**, sem copiar buffer nenhum. Este exemplo faz exatamente isso em
Python, com os lotes vindo do DuckDB via `to_arrow_reader`.

## Como isto difere do exemplo 15

O exemplo 15 também consome lotes, mas converte cada um para listas Python
(`to_pylist()`) e devolve listas — ele exercita o **laço linha a linha**. Aqui
o dado **nunca sai do Arrow**: o lote entra como `RecordBatch` e sai como
`RecordBatch`, e o resultado volta a ser consumível pelo DuckDB sem conversão.
São os dois lados da mesma fronteira: o 15 mostra o custo de descer para o
Python, o 24 mostra como evitar descer.

## As duas portas para o mesmo reader

```python
con.execute(sql).to_arrow_reader(n)   # a partir do result set (estilo DBAPI)
con.sql(sql).to_arrow_reader(n)       # a partir da relation (lazy)
```

As duas devolvem um `pyarrow.RecordBatchReader` — a mesma classe, a mesma
semântica; muda só por onde se chega nela. Se você encontrar
**`fetch_record_batch(n)`** em código ou tutorial mais antigo, é este mesmo
método com o nome da família `fetch*`: continua funcionando, mas desde o
DuckDB 1.5 emite `DeprecationWarning` apontando para `to_arrow_reader()`.

O reader é **lazy e de passada única**: cada `next()` puxa um lote do motor,
que fica com no máximo um lote materializado por vez. Depois de esgotado, ele
não rebobina.

## As etapas, lado a lado com o Rust

| Etapa | Rust (`src/lib.rs`) | Python (aqui) |
| --- | --- | --- |
| desembrulhar | `batch.into_inner()` | o `RecordBatch` já chega pronto |
| tipar colunas | `as_primitive::<Int32Type>()` | `batch.column("quantity")` |
| calcular | laço sobre iteradores | `pyarrow.compute` (ou laço, se sequencial) |
| schema de saída | `fields.push(Field::new(...))` | `pa.schema(list(b.schema) + [...])` |
| colunas de saída | `columns().to_vec()` (clona `Arc`s) | `list(b.columns) + [...]` |
| montar | `RecordBatch::try_new` | `pa.RecordBatch.from_arrays` |

A linha "colunas de saída" é o coração: em Rust, `to_vec()` clona apenas os
`Arc<dyn Array>` (contagem de referência); em Python, `list(b.columns)` guarda
os mesmos objetos `pa.Array`. Nos dois casos **os buffers são os mesmos** — só
a coluna nova é alocada. O exemplo comprova isso comparando o endereço do
buffer antes e depois.

## O fecho: DuckDB -> Python -> DuckDB, tudo em streaming

Com `pa.RecordBatchReader.from_batches(schema, gerador)`, o lado Python vira
mais um reader — que o DuckDB consome como se fosse uma tabela. O pipeline
inteiro fica sem materialização: o motor produz um lote, o Python enriquece
aquele lote, o motor consome, e só então o próximo é lido.

**Cuidado — use uma conexão separada para consumir.** Se o `SELECT` que lê o
reader enriquecido rodar na MESMA conexão que produziu o reader de origem, o
DuckDB reentra em si mesmo: o resultado observado foi ora um silencioso
`count = 0` (o stream de origem é invalidado pela nova query), ora um
travamento. Duas conexões resolvem — a de origem só produz, a de destino só
consome.

Rode com: `uv run examples/24_record_batch_pipeline.py`
"""

from __future__ import annotations

import time

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from _common import ORDERS_GLOB, PRODUCTS_GLOB, section

BATCH = 100_000
# os thresholds do wrapper Rust (500 / 2000) são calibrados para o recorte
# pequeno do exemplo 15; aqui cada cliente acumula milhões ao longo do mês, e
# com aqueles valores todo mundo viraria "ouro" na primeira dezena de linhas
THRESHOLD_PRATA = 500_000.0
THRESHOLD_OURO = 2_000_000.0

# a fonte do fluxo vetorizado: orders x products, um mês inteiro
SQL_LINHAS = f"""
SELECT o.order_id, o.customer_id, o.quantity, p.unit_price
FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true) o
JOIN read_parquet('{PRODUCTS_GLOB}') p USING (product_id)
WHERE o.order_month = 1 AND o.customer_id <= 500
"""

# a fonte do fluxo sequencial: ORDENADA por cliente/data (o acumulado precisa
# ser cronológico) e limitada a menos clientes, porque o laço é caro em Python
SQL_ORDENADO = f"""
SELECT o.order_id, o.customer_id, o.quantity * p.unit_price AS amount
FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true) o
JOIN read_parquet('{PRODUCTS_GLOB}') p USING (product_id)
WHERE o.order_month = 1 AND o.customer_id <= 100
ORDER BY o.customer_id, o.order_date, o.order_id
"""


def add_line_total(batch: pa.RecordBatch) -> pa.RecordBatch:
    """O gêmeo Python de `add_line_total` do Rust: anexa `quantity * unit_price`.

    Vetorizado do começo ao fim — nenhuma linha vira objeto Python. As colunas
    de entrada seguem para a saída **por referência**; só `line_total` é nova.
    """
    # Etapa 1 — localizar as colunas de entrada (equivale ao downcast tipado do
    # Rust; aqui o tipo vem no próprio array).
    quantity = batch.column("quantity")  # int32
    unit_price = batch.column("unit_price")  # double

    # Etapa 2 — calcular a coluna nova. `pc.multiply` promove int32*double para
    # double sozinho, e propaga nulo se qualquer lado for nulo — a mesma
    # semântica do `match (q, p)` na versão Rust.
    line_total = pc.multiply(quantity, unit_price)

    # Etapa 3 — schema de saída = campos originais + o campo novo.
    schema = pa.schema(list(batch.schema) + [pa.field("line_total", pa.float64())])

    # Etapa 4 — colunas de saída = os MESMOS arrays + o array novo.
    # `list(batch.columns)` é o análogo de `columns().to_vec()`: guarda as
    # referências, não os dados.
    return pa.RecordBatch.from_arrays(list(batch.columns) + [line_total], schema=schema)


def add_running_spend(
    batch: pa.RecordBatch,
    running: dict[int, float],
    prata: float = THRESHOLD_PRATA,
    ouro: float = THRESHOLD_OURO,
) -> pa.RecordBatch:
    """O gêmeo Python de `compute_customer_running_spend`: acumulado + tier.

    Aqui o cálculo **não vetoriza**: cada linha depende do total do cliente
    até a linha anterior. O dicionário `running` vem de fora justamente para
    sobreviver à fronteira entre lotes — as linhas de um cliente podem cair em
    lotes diferentes (é o `HashMap` que, no Rust, vive dentro da função porque
    lá o batch chega inteiro).
    """
    cumulative: list[float | None] = []
    tier: list[str | None] = []

    # única descida ao Python: as duas colunas que o laço precisa
    for cid, amt in zip(
        batch.column("customer_id").to_pylist(), batch.column("amount").to_pylist()
    ):
        if cid is None or amt is None:
            # nulo em qualquer entrada -> nulo nas duas saídas, sem avançar o
            # acumulado (mesma regra da implementação em Rust)
            cumulative.append(None)
            tier.append(None)
            continue
        total = running.get(cid, 0.0) + amt
        running[cid] = total
        cumulative.append(total)
        tier.append("bronze" if total < prata else "prata" if total < ouro else "ouro")

    schema = pa.schema(
        list(batch.schema)
        + [pa.field("cumulative_spend", pa.float64()), pa.field("customer_tier", pa.string())]
    )
    return pa.RecordBatch.from_arrays(
        list(batch.columns) + [pa.array(cumulative, pa.float64()), pa.array(tier, pa.string())],
        schema=schema,
    )


def stream_enriquecido(reader: pa.RecordBatchReader, fn) -> pa.RecordBatchReader:
    """Embrulha `fn` aplicada lote a lote como um novo `RecordBatchReader`.

    Um `RecordBatchReader` precisa anunciar o schema **antes** de entregar
    qualquer lote, e só `fn` sabe qual é o schema de saída — então o PRIMEIRO
    lote é processado já aqui, para descobri-lo (e guardado para ser o
    primeiro a sair). Do segundo em diante tudo é sob demanda: o gerador só
    roda quando alguém puxa. É isso que mantém o pipeline em streaming, com no
    máximo um lote vivo por vez.
    """
    primeiro = next(reader, None)
    if primeiro is None:  # resultado vazio: devolve um reader vazio com schema
        return pa.RecordBatchReader.from_batches(reader.schema, iter(()))
    saida = fn(primeiro)

    def gerador():
        yield saida
        for batch in reader:
            yield fn(batch)

    return pa.RecordBatchReader.from_batches(saida.schema, gerador())


if __name__ == "__main__":
    origem = duckdb.connect()  # produz os lotes
    destino = duckdb.connect()  # consome o resultado (conexão SEPARADA, ver docstring)

    section("As duas portas para o mesmo RecordBatchReader")
    r1 = origem.execute("SELECT 1 AS a").to_arrow_reader(10)  # via result set
    r2 = origem.sql("SELECT 1 AS a").to_arrow_reader(10)  # via relation
    print(f"execute(...).to_arrow_reader -> {type(r1).__module__}.{type(r1).__name__}")
    print(f"sql(...).to_arrow_reader     -> {type(r2).__module__}.{type(r2).__name__}")
    print(f"mesmo tipo: {type(r1) is type(r2)}")
    print("(o nome antigo `fetch_record_batch` é o mesmo método, deprecado na 1.5)")
    r1.close()
    r2.close()

    section("Etapa a etapa: um lote entra, um lote (mais largo) sai")
    reader = origem.execute(SQL_LINHAS).to_arrow_reader(BATCH)
    primeiro = next(reader)
    enriquecido = add_line_total(primeiro)
    print(f"entrada: {primeiro.num_rows:,} linhas, colunas {primeiro.schema.names}")
    print(f"saída:   {enriquecido.num_rows:,} linhas, colunas {enriquecido.schema.names}")

    # a prova do reaproveitamento: o buffer de dados de `quantity` na saída é
    # literalmente o mesmo endereço de memória da entrada (nenhuma cópia)
    addr_in = primeiro.column("quantity").buffers()[1].address
    addr_out = enriquecido.column("quantity").buffers()[1].address
    print(f"buffer de 'quantity' — entrada {addr_in:#x}, saída {addr_out:#x}")
    print(f"mesmo buffer (zero-copy, como o clone de Arc no Rust): {addr_in == addr_out}")
    print(f"colunas realmente alocadas neste lote: 1 de {enriquecido.num_columns}")
    reader.close()

    section("Pipeline vetorizado em streaming: DuckDB -> Python -> DuckDB")
    inicio = time.perf_counter()
    reader = origem.execute(SQL_LINHAS).to_arrow_reader(BATCH)
    linhas = stream_enriquecido(reader, add_line_total)
    # o DuckDB varre o reader do Python como se fosse uma tabela (replacement
    # scan pelo NOME da variável local `linhas`): os lotes são puxados sob
    # demanda, nada é materializado inteiro
    total = destino.sql(
        "SELECT count(*) AS linhas, round(sum(line_total), 2) AS receita FROM linhas"
    ).fetchone()
    tempo = time.perf_counter() - inicio
    print(f"{total[0]:,} linhas enriquecidas em {tempo:.2f}s (lotes de até {BATCH:,})")
    print(f"receita total (soma de line_total): {total[1]:,.2f}")

    section("Verificação contra o mesmo cálculo feito só em SQL")
    gabarito = origem.sql(
        f"SELECT round(sum(quantity * unit_price), 2) FROM ({SQL_LINHAS})"
    ).fetchone()[0]
    print(f"gabarito em SQL puro: {gabarito:,.2f}")
    print(f"bate com o pipeline:  {abs(gabarito - total[1]) < 0.01}")

    section("O mesmo padrão, mas com estado entre lotes (o caso do Rust)")
    inicio = time.perf_counter()
    reader = origem.execute(SQL_ORDENADO).to_arrow_reader(BATCH)
    running: dict[int, float] = {}  # ESTADO que atravessa a fronteira dos lotes
    acumulado = stream_enriquecido(reader, lambda b: add_running_spend(b, running))
    tiers = destino.sql(
        """
        SELECT customer_tier, count(*) AS qtd
        FROM acumulado
        GROUP BY customer_tier
        ORDER BY qtd DESC
        """
    ).fetchall()
    tempo_seq = time.perf_counter() - inicio
    n_linhas = sum(qtd for _, qtd in tiers)
    print(f"{n_linhas:,} linhas em {tempo_seq:.2f}s ({tempo_seq / n_linhas * 1e6:.1f} µs/linha)")
    for tier_nome, qtd in tiers:
        print(f"  {tier_nome:7s}: {qtd:,} linhas")
    print(f"clientes com estado acumulado ao fim do stream: {len(running):,}")

    section("Verificação: o acumulado final bate com o SUM agrupado")
    esperado = dict(
        origem.sql(
            f"SELECT customer_id, sum(amount) FROM ({SQL_ORDENADO}) GROUP BY customer_id"
        ).fetchall()
    )
    bate = all(abs(running[c] - esperado[c]) < 1e-6 for c in running)
    print(f"o último total de cada cliente == SUM(amount) por cliente: {bate}")

    section("A lição")
    print("O caminho vetorizado nunca desce ao Python: as colunas de entrada")
    print("viajam por referência e só a coluna nova é alocada — é o mesmo custo")
    print("do Rust, porque o trabalho está no pyarrow.compute (C++), não no laço.")
    print("Já o acumulado com estado paga microssegundos POR LINHA em Python.")
    print("É exatamente essa conta que a extensão Rust elimina: mesmo formato de")
    print("entrada e saída (RecordBatch), mesma fronteira, laço em código nativo.")
    print("Veja `../examples-rust-extension/run_etl.py` para a versão em Rust.")
