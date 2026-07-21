"""Exemplo 4 — ORM vs processamento em lote, em Python puro.

O [`../rust-extension/run_nested_params.py`](../../rust-extension/run_nested_params.py)
mede o custo de materializar dados 1:N **em Rust**, onde os objetos são baratos
e a penalidade fica em ~4x. Este exemplo faz a pergunta equivalente **em Python
puro**, onde os objetos são caros — e a diferença muda de ordem de grandeza.

O cálculo é o mesmo nas quatro estratégias: para cada conta, o **maior saldo
acumulado** — a soma corrente dos lançamentos em ordem cronológica, guardando o
pico. É deliberadamente uma agregação *sequencial por grupo* (não um `SUM`
simples), para que o caminho ORM tenha uma razão legítima de percorrer os
lançamentos um a um, e para que exista um equivalente vetorizado honesto
(window function).

## As quatro estratégias (um gradiente, não um duelo)

**1. ORM com lazy loading** — `select(Conta)` e depois `conta.lancamentos`.
É a forma mais natural de escrever com ORM, e cai na armadilha clássica do
**N+1**: uma query para as contas, mais UMA QUERY POR CONTA para carregar seus
lançamentos. O custo não é o loop Python — é o número de idas ao banco.

**2. ORM com eager loading** (`selectinload`) — a forma *correta* de usar o
ORM: o SQLAlchemy carrega os lançamentos de todas as contas em poucas queries.
Elimina o N+1, mas ainda instancia **um objeto Python por linha**, com todo o
custo de `PyObject` + GC + atributos instrumentados.

**3. Linhas brutas (Core) + agrupamento em Python** — sem objetos ORM: uma
query só, tuplas cruas, e o agrupamento/acumulação num laço Python. Isola
quanto do custo era do ORM e quanto é do laço interpretado.

**4. Lote colunar (DuckDB)** — a agregação inteira vira uma window function
em SQL: `SUM(...) OVER (PARTITION BY id_conta ORDER BY data)` e um `MAX` por
conta. Nenhum objeto Python por linha; o motor vetorizado faz o trabalho.

## O que a medição mostra

Com 2.000 contas × 100 lançamentos (200 mil linhas), o gradiente é monotônico
e cada degrau isola um custo diferente:

| Estratégia | Tempo | Ganho acumulado | Custos eliminados |
|---|---|---|---|
| 1. ORM lazy loading (N+1) | ~9,7s | 1x | — (paga todos os cinco) |
| 2. ORM eager (`selectinload`) | ~2,4s | **~4x** | **3** — as N idas ao banco |
| 3. Linhas brutas + laço Python | ~0,3s | **~33x** | **1 e 2** — objetos e escrituração |
| 4. Lote colunar (DuckDB) | ~0,04s | **~258x** | **4 e 5** — laço e alocação |

Os números da coluna "custos" remetem à decomposição usada no
`../rust-extension/run_nested_params.py` e no README deste projeto: (1)
metadados por linha, (2) escrituração do ORM, (3) travessia de fronteira por
linha/entidade, (4) execução interpretada, (5) alocação de heap por linha.

Repare que **nenhum degrau é o "vilão" sozinho**: eliminar o N+1 dá 4x, largar
os objetos ORM dá mais 8x, e vetorizar dá mais 7x. Os degraus 2 e 3 são os
custos que o *ORM* adiciona; o degrau 4 é o custo do *Python* — e por isso só
sai indo para um motor vetorizado (ou para o Rust).

**Ressalva importante — volume importa.** Com poucos dados (ex.: 300 contas ×
50 lançamentos = 15 mil linhas), a estratégia 4 fica *mais lenta* que a 3: o
custo fixo do DuckDB (conexão, registro, planejamento da query) não se paga.
A vantagem colunar precisa de volume para compensar o overhead fixo — não vale
a pena trocar um laço Python por um motor SQL para processar mil linhas.
Experimente: `uv run examples/04_orm_vs_batch.py 300 50`.

## Sobre a comparação ser justa

As estratégias 1–3 leem do SQLite (o padrão relacional tradicional); a 4 lê os
mesmos dados em Arrow (o padrão colunar). Não é um viés acidental — é
exatamente a diferença que está sendo medida: *onde os dados moram e como são
percorridos*. A carga inicial do SQLite fica **fora** de todos os cronômetros;
todas as quatro produzem o mesmo resultado, o que o exemplo verifica.

O contraste com o exemplo em Rust é a lição central: lá, materializar por
linha custa ~4x; aqui, cada linha vira objeto Python — e a conta chega a
duas ordens de grandeza.

Rode com: `uv run examples/04_orm_vs_batch.py [n_contas] [lanc_por_conta]`
(default: 2000 contas x 100 lançamentos = 200k linhas)
"""

from __future__ import annotations

import sys
import time
from collections import defaultdict
from decimal import Decimal

import duckdb
import pyarrow as pa
from sqlalchemy import create_engine, insert, select
from sqlalchemy.orm import Session, selectinload

from _common import gerar_lancamentos, section
from models import Base, Conta, Lancamento

NUM_CONTAS = 2_000
LANC_POR_CONTA = 100


def preparar_dados(n_contas: int, por_conta: int) -> tuple[pa.Table, pa.Table]:
    """Gera contas e lançamentos como Tables Arrow (a fonte da verdade do teste)."""
    contas = pa.table(
        {
            "id_conta": pa.array(range(1, n_contas + 1), type=pa.int64()),
            "nome": pa.array([f"conta_{i:05d}" for i in range(1, n_contas + 1)]),
            "numero": pa.array([f"{i}" for i in range(1, n_contas + 1)]),
            "permite_lancamentos": pa.array([True] * n_contas),
        }
    )
    lancamentos = gerar_lancamentos(n_contas * por_conta, contas_folha=list(range(1, n_contas + 1)))
    return contas, lancamentos


def carregar_sqlite(contas: pa.Table, lancamentos: pa.Table):
    """Popula o SQLite em memória (custo de setup, FORA dos cronômetros)."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(insert(Conta), contas.to_pylist())
        conn.execute(insert(Lancamento), lancamentos.to_pylist())
    return engine


def maior_saldo(valores_ordenados) -> Decimal:
    """O núcleo do cálculo: pico da soma corrente (usado pelas 3 vias Python)."""
    saldo = Decimal(0)
    pico = Decimal(0)
    for valor in valores_ordenados:
        saldo += valor
        if saldo > pico:
            pico = saldo
    return pico


# --- 1. ORM com lazy loading (a armadilha N+1) -----------------------------
def via_orm_lazy(engine) -> dict[int, Decimal]:
    resultado = {}
    with Session(engine) as session:
        for conta in session.scalars(select(Conta)):
            # Cada acesso a `conta.lancamentos` dispara UMA query nova — é aqui
            # que o N+1 acontece.
            # O `continue` deixa de fora as contas sem lançamentos, para que as
            # quatro estratégias concordem sobre o domínio do resultado (as vias
            # 3 e 4 partem dos lançamentos, então só enxergam contas que os têm).
            if not conta.lancamentos:
                continue
            lancs = sorted(conta.lancamentos, key=lambda x: (x.data, x.id_lancamento))
            resultado[conta.id_conta] = maior_saldo(x.valor for x in lancs)
    return resultado


# --- 2. ORM com eager loading (o jeito correto de usar ORM) ----------------
def via_orm_eager(engine) -> dict[int, Decimal]:
    resultado = {}
    with Session(engine) as session:
        # selectinload carrega os lançamentos de todas as contas em poucas
        # queries — some o N+1, mas cada linha ainda vira um objeto Python
        stmt = select(Conta).options(selectinload(Conta.lancamentos))
        for conta in session.scalars(stmt):
            if not conta.lancamentos:  # mesmo alinhamento de domínio da via 1
                continue
            lancs = sorted(conta.lancamentos, key=lambda x: (x.data, x.id_lancamento))
            resultado[conta.id_conta] = maior_saldo(x.valor for x in lancs)
    return resultado


# --- 3. Linhas brutas + agrupamento em Python ------------------------------
def via_linhas(engine) -> dict[int, Decimal]:
    with engine.connect() as conn:
        # uma query só, já ordenada pelo banco; tuplas cruas, sem objetos ORM
        linhas = conn.execute(
            select(Lancamento.id_conta, Lancamento.valor).order_by(
                Lancamento.id_conta, Lancamento.data, Lancamento.id_lancamento
            )
        ).all()
    por_conta: dict[int, list[Decimal]] = defaultdict(list)
    for id_conta, valor in linhas:
        por_conta[id_conta].append(valor)
    return {cid: maior_saldo(vals) for cid, vals in por_conta.items()}


# --- 4. Lote colunar: a agregação inteira em SQL vetorizado ----------------
def via_lote(lancamentos: pa.Table) -> dict[int, Decimal]:
    con = duckdb.connect()
    con.register("lancamentos", lancamentos)
    # window function faz a soma corrente por conta; o MAX externo pega o pico.
    # Nenhum objeto Python por linha — o DuckDB percorre os buffers Arrow.
    resultado = con.sql(
        """
        SELECT id_conta, MAX(saldo) AS maior_saldo
        FROM (
            SELECT id_conta,
                   SUM(valor) OVER (
                       PARTITION BY id_conta ORDER BY data, id_lancamento
                       ROWS UNBOUNDED PRECEDING
                   ) AS saldo
            FROM lancamentos
        )
        GROUP BY id_conta
        """
    ).fetchall()
    con.close()
    # o pico nunca é negativo no núcleo Python (começa em 0); alinhamos aqui
    return {cid: max(saldo, Decimal(0)) for cid, saldo in resultado}


if __name__ == "__main__":
    n_contas = int(sys.argv[1]) if len(sys.argv) > 1 else NUM_CONTAS
    por_conta = int(sys.argv[2]) if len(sys.argv) > 2 else LANC_POR_CONTA

    section(f"Preparando {n_contas:,} contas x {por_conta} lançamentos")
    contas, lancamentos = preparar_dados(n_contas, por_conta)
    print(f"{lancamentos.num_rows:,} lançamentos no total")
    engine = carregar_sqlite(contas, lancamentos)
    print("SQLite em memória populado (custo de setup, fora dos cronômetros)")

    estrategias = [
        ("1. ORM lazy loading (N+1)", lambda: via_orm_lazy(engine), "1 + N queries, objetos"),
        ("2. ORM eager (selectinload)", lambda: via_orm_eager(engine), "poucas queries, objetos"),
        ("3. Linhas brutas + loop Python", lambda: via_linhas(engine), "1 query, sem objetos"),
        ("4. Lote colunar (DuckDB)", lambda: via_lote(lancamentos), "vetorizado, sem laço"),
    ]

    section("Medição (maior saldo acumulado por conta)")
    resultados = []
    for nome, fn, nota in estrategias:
        inicio = time.perf_counter()
        saida = fn()
        tempo = time.perf_counter() - inicio
        resultados.append((nome, tempo, saida, nota))
        print(f"{nome:34s} {tempo:7.2f}s   ({nota})")

    section("Conferência")
    base = resultados[0][2]
    iguais = all(r[2] == base for r in resultados)
    print(f"as quatro estratégias produzem o mesmo resultado: {iguais}")
    print(f"contas processadas: {len(base):,}")

    section("Placar")
    t_lento, t_lote = resultados[0][1], resultados[3][1]
    for nome, tempo, _, _ in resultados:
        print(f"{nome:34s} {tempo:7.2f}s   ({t_lento / tempo:5.1f}x vs. o ORM lazy)")
    print(f"\nDo ORM ingênuo ao lote colunar: {t_lento / t_lote:.0f}x")
    print("Compare com o mesmo estudo em Rust (rust-extension/run_nested_params.py),")
    print("onde materializar por linha custa ~4x: em Python, cada linha vira um")
    print("objeto com refcount, GC e atributos instrumentados — a conta é outra.")
