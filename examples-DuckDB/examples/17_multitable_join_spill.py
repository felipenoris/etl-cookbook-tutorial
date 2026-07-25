"""Exemplo 17 — JOIN de múltiplas tabelas volumosas com RAM limitada a 100MB (spill).

Junta duas perguntas que aparecem cedo em ETL analítico e costumam assustar
quem vem de banco transacional:

1. *"Consigo cruzar VÁRIAS tabelas grandes — inclusive um relacionamento N:N
   ponderado — sem índice de chave estrangeira?"* Sim: o DuckDB resolve tudo
   com **hash joins** paralelos e vetorizados (ver o exemplo 16). O que muda
   aqui é a escala: cinco tabelas, quatro joins encadeados, duas delas fatos
   volumosos e uma ponte N:N que multiplica linhas.
2. *"E se o processamento intermediário não couber na memória que eu dou ao
   motor?"* O DuckDB faz **spill**: derrama para disco os blocos das hash
   tables e da agregação que estouram o `memory_limit`, e ainda assim conclui
   (o mecanismo do exemplo 04, agora sob um join pesado). Este exemplo
   **limita a RAM a 100MB** — bem menos que os ~168MB só dos parquet de
   origem — e **mede** o pico de bytes derramados para provar que o spill
   aconteceu.

Modelo de dados (estrela com uma ponte N:N), cada tabela vinda de um parquet:

    area(id_area, nome_area)                        dimensão pequena (50 linhas)
    operacao(id_oper, valor_operacao, id_area)      fato volumoso
    contrato(id_contrato, saldo_em_aberto)          fato volumoso
    fluxo(id_contrato, data_fluxo, valor_fluxo)     1 contrato -> N fluxos
    rel_operacao_contrato(id_oper, id_contrato, fator)   ponte N:N ponderada

A pergunta de negócio: **somar `valor_fluxo` dos fluxos com `data_fluxo`
posterior a 01/01/2026, apenas de contratos com `saldo_em_aberto > 0`,
agrupado por área**. Como uma operação se liga a N contratos (e vice-versa)
pela ponte, o valor de cada fluxo é **rateado** para as áreas na proporção do
`fator` da ponte — ou seja, o somatório é `SUM(valor_fluxo * fator)`. Dinheiro
é `DECIMAL` de ponta a ponta (o `fator` é `DECIMAL(5,4)`), então o produto e a
soma permanecem exatos, nunca `float`.

Por que `SET threads=2`? Sob um teto apertado, cada thread mantém suas próprias
partições de hash em memória; menos threads = menos memória concorrente, o que
permite ao spill dar conta dos 100MB de forma reprodutível em qualquer máquina
(o próprio erro de OOM do DuckDB sugere reduzir threads). Não é sobre velocidade
— é sobre caber no orçamento de memória de forma determinística.

Rode com: `uv run examples/17_multitable_join_spill.py`
"""

import re
import shutil
import threading
import time

import duckdb

from _common import RICH_DIR, section

DEMO_DIR = RICH_DIR / "duckdb_multijoin_demo"
SPILL_DIR = DEMO_DIR / "_spill"
ROW_GROUP = 122_880

# Volumes: fatos na casa dos milhões para que o join intermediário estoure os
# 100MB. A ponte N:N (rel) é a maior — ela multiplica as linhas do join.
NUM_AREA = 50
NUM_OPERACAO = 1_500_000
NUM_CONTRATO = 1_500_000
NUM_FLUXO = 6_000_000
NUM_REL = 4_000_000

MEMORY_LIMIT = "100MB"


def gerar_bases(con: duckdb.DuckDBPyConnection) -> None:
    """Gera os 5 parquet de origem via `range()` + `COPY` (dados sintéticos).

    Sintéticos de propósito: o exemplo é sobre o comportamento do MOTOR (join +
    spill), não sobre um dataset específico. `hash()` devolve UINT64, daí os
    casts explícitos para chaves e datas.
    """
    con.execute(
        f"COPY (SELECT i AS id_area, 'area_' || i AS nome_area FROM range({NUM_AREA}) t(i)) "
        f"TO '{DEMO_DIR / 'area.parquet'}' (FORMAT parquet)"
    )
    con.execute(
        "COPY (SELECT i AS id_oper, "
        "((hash(i * 7) % 500000) + 1)::DECIMAL(12,2) AS valor_operacao, "
        f"(hash(i) % {NUM_AREA}) AS id_area FROM range({NUM_OPERACAO}) t(i)) "
        f"TO '{DEMO_DIR / 'operacao.parquet'}' (FORMAT parquet, ROW_GROUP_SIZE {ROW_GROUP})"
    )
    # saldo_em_aberto vai de negativo a positivo -> o filtro saldo > 0 corta
    # ~parte dos contratos (BIGINT antes da subtração: hash() é UINT64).
    con.execute(
        "COPY (SELECT i AS id_contrato, "
        "((hash(i * 3) % 100000)::BIGINT - 20000)::DECIMAL(12,2) AS saldo_em_aberto "
        f"FROM range({NUM_CONTRATO}) t(i)) "
        f"TO '{DEMO_DIR / 'contrato.parquet'}' (FORMAT parquet, ROW_GROUP_SIZE {ROW_GROUP})"
    )
    # data_fluxo espalhada em ~1 ano a partir de 2025-07-01 -> parte cai depois
    # de 2026-01-01 (o filtro da pergunta).
    con.execute(
        "COPY (SELECT i AS id_fluxo, "
        f"(hash(i) % {NUM_CONTRATO}) AS id_contrato, "
        "DATE '2025-07-01' + (hash(i * 11) % 365)::INTEGER AS data_fluxo, "
        "((hash(i * 5) % 50000) + 1)::DECIMAL(12,2) AS valor_fluxo "
        f"FROM range({NUM_FLUXO}) t(i)) "
        f"TO '{DEMO_DIR / 'fluxo.parquet'}' (FORMAT parquet, ROW_GROUP_SIZE {ROW_GROUP})"
    )
    # ponte N:N: fator em [0.1000, 0.9999] como DECIMAL(5,4) -> o rateio
    # permanece exato (dinheiro nunca vira float).
    con.execute(
        f"COPY (SELECT (hash(i) % {NUM_OPERACAO}) AS id_oper, "
        f"(hash(i * 13) % {NUM_CONTRATO}) AS id_contrato, "
        "(((hash(i * 17) % 9000) + 1000) / 10000.0)::DECIMAL(5,4) AS fator "
        f"FROM range({NUM_REL}) t(i)) "
        f"TO '{DEMO_DIR / 'rel_operacao_contrato.parquet'}' (FORMAT parquet, ROW_GROUP_SIZE {ROW_GROUP})"
    )


def _tamanho_spill_bytes() -> int:
    return sum(f.stat().st_size for f in SPILL_DIR.rglob("*") if f.is_file())


def executar_medindo_spill(con: duckdb.DuckDBPyConnection, sql: str):
    """Roda `sql` enquanto uma thread amostra o pico de bytes em temp_directory.

    O DuckDB apaga os arquivos de spill ao terminar a query, então medir DEPOIS
    não prova nada; amostrar DURANTE captura o pico real derramado para disco.
    """
    pico = {"bytes": 0}
    parar = threading.Event()

    def vigia() -> None:
        while not parar.is_set():
            pico["bytes"] = max(pico["bytes"], _tamanho_spill_bytes())
            time.sleep(0.02)

    t = threading.Thread(target=vigia)
    t.start()
    inicio = time.perf_counter()
    linhas = con.sql(sql).fetchall()
    elapsed_ms = (time.perf_counter() - inicio) * 1000
    parar.set()
    t.join()
    return linhas, elapsed_ms, pico["bytes"]


QUERY = f"""
    SELECT a.nome_area,
           SUM(fl.valor_fluxo * r.fator) AS soma_ponderada
    FROM read_parquet('{DEMO_DIR / 'fluxo.parquet'}') fl
    JOIN read_parquet('{DEMO_DIR / 'contrato.parquet'}') c
         ON fl.id_contrato = c.id_contrato
    JOIN read_parquet('{DEMO_DIR / 'rel_operacao_contrato.parquet'}') r
         ON r.id_contrato = c.id_contrato
    JOIN read_parquet('{DEMO_DIR / 'operacao.parquet'}') o
         ON o.id_oper = r.id_oper
    JOIN read_parquet('{DEMO_DIR / 'area.parquet'}') a
         ON a.id_area = o.id_area
    WHERE fl.data_fluxo > DATE '2026-01-01'
      AND c.saldo_em_aberto > 0
    GROUP BY a.nome_area
    ORDER BY soma_ponderada DESC
"""


if __name__ == "__main__":
    shutil.rmtree(DEMO_DIR, ignore_errors=True)
    SPILL_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    section(f"Gerando as 5 bases parquet (fatos de milhões de linhas) em {DEMO_DIR.name}/")
    gerar_bases(con)
    total_mb = sum(f.stat().st_size for f in DEMO_DIR.glob("*.parquet")) / (1024 * 1024)
    for f in sorted(DEMO_DIR.glob("*.parquet")):
        print(f"  {f.name:<32} {f.stat().st_size / (1024 * 1024):6.1f}MB")
    print(f"  {'TOTAL':<32} {total_mb:6.1f}MB  (o limite de RAM abaixo é {MEMORY_LIMIT})")

    section(f"Configurando RAM = {MEMORY_LIMIT} (menor que os dados) + spill para disco")
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")  # menos threads = menos memória concorrente -> cabe nos 100MB
    print(con.sql(
        "SELECT current_setting('memory_limit') AS memory_limit, "
        "current_setting('threads') AS threads"
    ).fetchone())

    section("JOIN de 5 tabelas: SUM(valor_fluxo * fator) por área (fluxo>2026-01-01, saldo>0)")
    linhas, ms, pico = executar_medindo_spill(con, QUERY)
    print("plano (operadores de join):")
    plano = con.sql("EXPLAIN " + QUERY).fetchall()[0][1]
    print(f"  joins no plano: {re.findall(r'[A-Z_]*JOIN', plano)}")
    print(f"\nquery concluída em {ms:.0f}ms sob {MEMORY_LIMIT} de RAM; {len(linhas)} áreas no resultado")
    print(f"pico derramado para disco (spill): {pico / (1024 * 1024):.1f}MB")
    print("-> processou dados e joins bem maiores que a RAM sem estourar memória.\n")
    print("top 5 áreas por soma ponderada (DECIMAL, exato):")
    for nome_area, soma in linhas[:5]:
        print(f"  {nome_area:<12} {soma}")

    section("Contraprova: com RAM folgada, a MESMA query não toca o disco")
    con.execute("SET memory_limit='2GB'")
    _, ms_alto, pico_alto = executar_medindo_spill(con, QUERY)
    print(f"memory_limit=2GB -> {ms_alto:.0f}ms, pico de spill: {pico_alto / (1024 * 1024):.1f}MB")
    print("-> o spill é o que torna o resultado possível quando a RAM é o gargalo;")
    print("   com memória sobrando, o motor mantém tudo em RAM.")

    con.close()
    shutil.rmtree(SPILL_DIR, ignore_errors=True)
