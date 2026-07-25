"""Exemplo 18 — EXPLAIN ANALYZE e profiling: lendo o "raio-X" de uma query.

Os exemplos 02, 12 e 16 usam `EXPLAIN`/`EXPLAIN ANALYZE` de passagem, para
provar um ponto pontual (pruning, zonemaps, operador de join). Este exemplo
faz do profiling **o tema**: como abrir o profile de uma query, ler o que ele
diz e usar isso para achar o gargalo — a habilidade que amarra todos os
exemplos de performance.

Três ferramentas, do mais grosso ao mais fino:

`EXPLAIN <query>`
    Só o **plano estimado** (sem executar): a árvore de operadores e a
    cardinalidade que o otimizador *chuta*. Útil para ver a forma do plano
    (qual join, em que ordem, se há filtro no scan).

`EXPLAIN ANALYZE <query>`
    Executa e anota **tempos e linhas reais** por operador. Sem configurar
    nada, devolve uma árvore em texto. É o `\\timing` do psql turbinado.

`PRAGMA enable_profiling='json'`
    Muda a saída do `EXPLAIN ANALYZE` para **JSON estruturado** — o mesmo
    profile, mas navegável em código. Cada nó traz `operator_type`,
    `operator_timing` (segundos reais gastos NAQUELE operador),
    `operator_cardinality` (linhas que ele de fato emitiu) e um `extra_info`
    com detalhes do scan. É o que este exemplo percorre para responder três
    perguntas práticas.

As três perguntas que um profile responde:

1. **Qual operador domina o tempo?** Some `operator_timing` e ache o maior —
   é onde vale otimizar. Otimizar qualquer outro é desperdício.
2. **O otimizador estimou certo?** Compare `operator_cardinality` (real) com a
   `Estimated Cardinality` do `extra_info`. Um erro grande de estimativa é a
   causa raiz clássica de um plano ruim (ordem de join errada, hash table
   subdimensionada) — e o sintoma que você reporta ao investigar.
3. **O scan leu só o necessário?** O `extra_info` do scan de parquet mostra
   `Scanning Files` (partition pruning, exemplo 02), `File Filters` (o
   predicado que desceu até o arquivo) e `Dynamic Filters` (o filtro que o
   hash join **empurra** para o scan do fato em tempo de execução — o pushdown
   do exemplo 16, aqui visível no profile).

Rode com: `uv run examples/18_explain_analyze_profiling.py`
"""

import json

import duckdb

from _common import CUSTOMERS_GLOB, ORDERS_GLOB, section

# uma query com bastante o que medir: pruning na partição, hash join fato x
# dimensão, agregação e ordenação — cada etapa vira um operador no profile
QUERY = f"""
    SELECT c.region,
           COUNT(*)          AS pedidos,
           SUM(o.quantity)   AS itens
    FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true) o
    JOIN read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true) c
      ON o.customer_id = c.customer_id
    WHERE o.order_month = 1
    GROUP BY c.region
    ORDER BY pedidos DESC
"""


def achatar(node, out=None):
    """Percorre a árvore JSON do profile e devolve uma lista plana de operadores
    (ignorando o nó sintético EXPLAIN_ANALYZE da raiz)."""
    if out is None:
        out = []
    tipo = node.get("operator_type", "")
    if tipo and tipo != "EXPLAIN_ANALYZE":
        info = node.get("extra_info", {})
        est = info.get("Estimated Cardinality")
        out.append(
            {
                "tipo": tipo,
                "timing": node.get("operator_timing", 0.0),
                "real": node.get("operator_cardinality"),
                "estimada": int(est) if est is not None else None,
                "info": info,
            }
        )
    for filho in node.get("children", []):
        achatar(filho, out)
    return out


if __name__ == "__main__":
    con = duckdb.connect()

    section("EXPLAIN (sem executar): a forma do plano e as cardinalidades ESTIMADAS")
    # row[0][1] é a árvore em texto; mostramos as primeiras linhas para ver os operadores
    plano = con.sql("EXPLAIN " + QUERY).fetchall()[0][1]
    for linha in plano.splitlines()[:18]:
        print(linha)
    print("... (o EXPLAIN puro só estima; nada foi executado)")

    section("EXPLAIN ANALYZE em texto: a MESMA árvore, agora com tempos reais")
    # sem PRAGMA, a saída de EXPLAIN ANALYZE é uma árvore textual pronta para ler
    texto = con.sql("EXPLAIN ANALYZE " + QUERY).fetchall()[0][1]
    print("\n".join(texto.splitlines()[:16]))
    print("... (repare no tempo total e nas contagens por operador)")

    # a partir daqui: profiling em JSON, para navegar o profile em código
    con.execute("PRAGMA enable_profiling='json'")
    raiz = json.loads(con.sql("EXPLAIN ANALYZE " + QUERY).fetchall()[0][1])
    operadores = achatar(raiz)

    section("Pergunta 1 — qual operador DOMINA o tempo?")
    total = sum(op["timing"] for op in operadores) or 1e-9
    for op in sorted(operadores, key=lambda o: o["timing"], reverse=True):
        barra = "#" * round(40 * op["timing"] / total)
        print(f"  {op['tipo']:<16} {op['timing'] * 1000:7.1f}ms {100 * op['timing'] / total:4.0f}%  {barra}")
    dominante = max(operadores, key=lambda o: o["timing"])
    print(f"-> gargalo: {dominante['tipo']} ({dominante['timing'] * 1000:.1f}ms). É AQUI que otimizar paga;")
    print(f"   latência total (relógio de parede): {raiz.get('latency', 0) * 1000:.1f}ms")
    print("   (os tempos por operador são CPU e se sobrepõem entre threads: somados dão")
    print("    MAIS que a latência de parede — use os % para ranquear, não para somar.)")

    section("Pergunta 2 — o otimizador ESTIMOU certo? (real vs estimada por operador)")
    print(f"  {'operador':<16} {'real':>12} {'estimada':>12}   erro")
    for op in operadores:
        if op["real"] is None or op["estimada"] is None:
            continue
        real, est = op["real"], op["estimada"]
        fator = max(real, est) / max(1, min(real, est))
        flag = "  <== estimativa muito fora" if fator >= 100 else ""
        print(f"  {op['tipo']:<16} {real:>12,} {est:>12,}   {fator:5.0f}x{flag}")
    print("-> um erro grande de estimativa é a causa-raiz clássica de plano ruim")
    print("   (ordem de join, tamanho de hash table). Aqui o GROUP BY reduz de milhões")
    print("   para poucas regiões — o otimizador superestima a saída da agregação.")

    section("Pergunta 3 — o SCAN do fato leu só o necessário?")
    for op in operadores:
        info = op["info"]
        if info.get("Function") == "READ_PARQUET" and "order" in str(info.get("Filename(s)", "")):
            print(f"  arquivos lidos (pruning) : {info.get('Scanning Files', '?')}   <- exemplo 02")
            print(f"  filtro que desceu ao arquivo: {info.get('File Filters', '(nenhum)')}")
            print(f"  filtro DINÂMICO do join   : {info.get('Dynamic Filters', '(nenhum)')}   <- exemplo 16")
            print(f"  linhas entregues ao join  : {op['real']:,}")
            break
    print("-> 1/6 partições abertas (pruning), e o hash join EMPURRA um filtro de")
    print("   customer_id para o scan do fato em tempo de execução (Dynamic Filters).")

    section("Resumo: como usar o profile")
    print("- EXPLAIN            -> a forma do plano e as cardinalidades ESTIMADAS (não executa);")
    print("- EXPLAIN ANALYZE    -> tempos e linhas REAIS por operador (árvore em texto);")
    print("- enable_profiling='json' -> o mesmo profile navegável em código;")
    print("- na investigação: ache o operador dominante (1), cheque se a estimativa bate (2)")
    print("  e confirme que o scan podou partições/empurrou filtros (3).")
