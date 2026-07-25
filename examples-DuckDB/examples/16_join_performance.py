"""Exemplo 16 — Performance de JOIN (sem agregação) e o papel (ou não) dos índices.

A pergunta natural de quem vem de bases transacionais: *"para um JOIN entre
duas tabelas ser rápido, não preciso de um índice na chave estrangeira, como
no Postgres?"*. A resposta, **medida** neste exemplo, surpreende:

**No DuckDB, um JOIN é sempre um HASH JOIN, e os índices ART NÃO o aceleram.**
Criar um índice na coluna do JOIN não muda o plano nem o tempo. Isso não é uma
limitação — é o desenho certo para carga analítica: o hash join varre as duas
tabelas uma vez, em paralelo e vetorizado, sem os saltos aleatórios de um
nested-loop indexado (que só vence quando se busca POUCAS linhas num row-store
transacional).

Então o que torna um JOIN performático no DuckDB? Para o join que varre tudo
(o caso comum de ETL), nada: o hash join já é rápido. Para o join SELETIVO
(você quer só as linhas que casam com uma pequena fatia da dimensão), há DUAS
camadas de otimização — e este exemplo mede as duas separadamente, porque é
fácil confundi-las:

1. **Pushdown de filtro (nível de linha)**: um predicado sobre uma coluna do
   FATO é avaliado já no scan, cortando as linhas que entram no hash join. A
   pegadinha: o DuckDB **não** transforma sozinho o filtro da dimensão em
   filtro do fato — `WHERE dim.id BETWEEN ...` deixa o scan do fato produzir
   TODAS as linhas. Para o pushdown acontecer, o predicado precisa referenciar
   o fato (replique-o na chave do JOIN). Isso vale independente do layout.

2. **Pruning de I/O (zonemaps)**: se o fato está CLUSTERIZADO (`ORDER BY` na
   escrita) na coluna filtrada, o scan **pula fisicamente** os row groups fora
   da faixa — menos bytes lidos, menos tempo. É o mecanismo do exemplo 12,
   agora no contexto de JOIN. A camada 1 reduz as linhas que fluem; a camada 2
   reduz o I/O para produzi-las.

Resumo prático para o seu ETL sobre parquet:
- não crie índices esperando acelerar JOINs — não funciona;
- confie no hash join para os joins que varrem tudo;
- para joins seletivos: (a) faça o predicado alcançar o fato (pushdown) e
  (b) ordene o fato na escrita pela coluna de filtro/junção (zonemaps).

Rode com: `uv run examples/16_join_performance.py`
"""

import re
import shutil
import time

import duckdb

from _common import RICH_DIR, section

OUT_DIR = RICH_DIR / "duckdb_join_perf_demo"
ROW_GROUP = 122_880
NUM_FATO = 4_000_000


def cronometrar(con, sql, rodadas=5) -> float:
    melhor = float("inf")
    for _ in range(rodadas):
        inicio = time.perf_counter()
        con.sql(sql).fetchall()
        melhor = min(melhor, time.perf_counter() - inicio)
    return melhor * 1000


def operador_de_join(con, sql) -> str:
    """Extrai o operador de JOIN do plano (HASH_JOIN, INDEX_JOIN, ...)."""
    plano = con.sql("EXPLAIN " + sql).fetchall()[0][1]
    achados = re.findall(r"[A-Z_]*JOIN", plano)
    return achados[0] if achados else "(nenhum)"


def linhas_do_fato_no_scan(con, sql) -> int:
    """Do EXPLAIN ANALYZE, quantas linhas o scan do fato ENTREGA (pós-filtro)."""
    plano = con.sql("EXPLAIN ANALYZE " + sql).fetchall()[0][1]
    m = re.search(r"READ_PARQUET.*?([\d,]+) rows", plano, re.S)
    return int(m.group(1).replace(",", "")) if m else -1


if __name__ == "__main__":
    con = duckdb.connect()

    # dados sintéticos (via range) para CONTROLE TOTAL do layout — o exemplo é
    # sobre o comportamento do MOTOR, não sobre um dataset específico
    section(f"Preparando: fato de {NUM_FATO // 1_000_000}M linhas + dimensão de 100k")
    con.execute("CREATE TABLE dim AS SELECT i AS id, 'cliente_' || i AS nome FROM range(100000) t(i)")
    con.execute(
        "CREATE TABLE fato AS "
        "SELECT i AS pedido_id, (hash(i) % 100000) AS cliente_id, i * 1.5 AS valor "
        f"FROM range({NUM_FATO}) t(i)"
    )

    section("1) JOIN completo (SEM agregação): o plano é HASH_JOIN")
    q_full = "SELECT f.pedido_id, d.nome FROM fato f JOIN dim d ON f.cliente_id = d.id"
    print(f"operador de join: {operador_de_join(con, q_full)}")
    print(f"tempo: {cronometrar(con, q_full):.0f}ms (hash join, paralelo e vetorizado)")

    section("2) CREATE INDEX na chave do JOIN — o plano e o tempo NÃO mudam")
    t_sem = cronometrar(con, q_full)
    con.execute("CREATE INDEX ix_fato ON fato(cliente_id)")
    con.execute("CREATE INDEX ix_dim ON dim(id)")
    t_com = cronometrar(con, q_full)
    print(f"operador de join ainda: {operador_de_join(con, q_full)}")
    print(f"sem índice: {t_sem:.0f}ms | com índice: {t_com:.0f}ms")
    print("-> o hash join IGNORA os índices ART; eles servem a point-lookup e constraints,")
    print("   não a JOINs. Criar índice 'para o join ficar rápido' não tem efeito.")

    # grava o fato em parquet em DOIS layouts: ordenado (clusterizado) e embaralhado
    shutil.rmtree(OUT_DIR, ignore_errors=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fato_ord = OUT_DIR / "fato_ordenado.parquet"
    fato_rand = OUT_DIR / "fato_embaralhado.parquet"
    con.execute(f"COPY (SELECT * FROM fato ORDER BY cliente_id) TO '{fato_ord}' (FORMAT parquet, ROW_GROUP_SIZE {ROW_GROUP})")
    con.execute(f"COPY (SELECT * FROM fato) TO '{fato_rand}' (FORMAT parquet, ROW_GROUP_SIZE {ROW_GROUP})")

    section("3a) Camada 1 — pushdown: o predicado precisa ALCANÇAR o fato")
    q_dim = f"""
        SELECT f.pedido_id, d.nome
        FROM read_parquet('{fato_ord}') f JOIN dim d ON f.cliente_id = d.id
        WHERE d.id BETWEEN 1000 AND 1099
    """
    q_ambos = f"""
        SELECT f.pedido_id, d.nome
        FROM read_parquet('{fato_ord}') f JOIN dim d ON f.cliente_id = d.id
        WHERE d.id BETWEEN 1000 AND 1099 AND f.cliente_id BETWEEN 1000 AND 1099
    """
    print(f"filtro SÓ na dimensão    -> scan do fato entrega {linhas_do_fato_no_scan(con, q_dim):>9,} linhas ao join")
    print(f"predicado também no fato -> scan do fato entrega {linhas_do_fato_no_scan(con, q_ambos):>9,} linhas ao join")
    print("-> o filtro da dimensão NÃO vira filtro do fato sozinho; replicado na chave")
    print("   do JOIN, o scan corta as linhas antes de entrarem no hash join.")

    section("3b) Camada 2 — zonemaps: o fato ORDENADO pula I/O (mesmo predicado)")
    # o MESMO predicado-no-fato, mas comparando os dois layouts do parquet
    q_ord = q_ambos  # sobre o parquet ordenado
    q_rand = q_ambos.replace(str(fato_ord), str(fato_rand))
    print(f"fato ORDENADO por cliente_id: {cronometrar(con, q_ord):5.1f}ms")
    print(f"fato EMBARALHADO            : {cronometrar(con, q_rand):5.1f}ms")
    print("-> as duas entregam as MESMAS linhas (o pushdown é igual); a diferença de")
    print("   TEMPO é o I/O: no ordenado, as zonemaps pulam os row groups fora da faixa")
    print("   (exemplo 12); no embaralhado, todo row group cobre a faixa toda e é lido.")

    section("Resumo: para JOIN performático no DuckDB")
    print("- índice ART: NÃO ajuda joins (só point-lookup por WHERE direto e constraints);")
    print("- join que varre tudo: o hash join já é rápido — não precisa tunar;")
    print("- join seletivo: (1) faça o predicado alcançar o fato (pushdown) e")
    print("  (2) ordene o fato na escrita pela coluna de junção/filtro (zonemaps).")
