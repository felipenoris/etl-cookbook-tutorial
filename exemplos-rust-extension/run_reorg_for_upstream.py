r"""Etapa final do ETL: reorganizar o resultado do processamento paralelo antes
de subir para o upstream (Redshift).

O processamento paralelo de ``RecordBatch`` (``BoundedRevenueProjector``, o
desenho do `run_contracts_parallel`) é *embaraçosamente paralelo* de propósito:
N workers consomem lotes de uma fila e uma thread escritora grava cada
resultado assim que fica pronto. O preço disso é que **a ordem da saída não é
garantida** em relação à chave pela qual o destino é consultado — os lotes
terminam fora de ordem, e mesmo dentro de um lote a coluna calculada
(``receita_projetada``) não tem relação com a ordem de leitura. O arquivo que sai
do estágio paralelo está, portanto, **embaralhado pela sort key do upstream**.

Isso importa porque a performance de consulta no destino (e o custo de mantê-lo)
depende de o dado entrar **agrupado (clustered) pela sort key**: tanto o *zone
map* por bloco do Redshift quanto a estatística ``min``/``max`` por *row group*
do parquet só conseguem **pular** blocos quando valores próximos estão
fisicamente próximos (é o que o exemplo 12 do pyarrow mede). Dado embaralhado →
toda faixa ``[min, max]`` é larga → nada é pulável → varredura cheia.

A **ordenação global**, porém, briga com o paralelismo: para emitir a 1ª linha
ordenada é preciso ter visto todas. Ela é sempre uma **barreira** — um ponto de
materialização, disco→disco, *fora* do pool de streaming. O truque é não deixá-la
enxergar o dataset inteiro de uma vez:

- **Particionar pela sort key** transforma o "sort global" em "sort local por
  partição" (a ordem global vira a concatenação das partições, já ordenadas
  entre si pelo valor da partição) — paralelo de novo, e a partição vira a
  unidade de recarga idempotente no upstream.
- O DuckDB faz o sort **externo, memory-bounded**: derrama *runs* parciais para
  ``temp_directory`` quando estoura o ``memory_limit`` (o mecanismo dos exemplos
  04 e 17 do DuckDB), então a barreira roda com memória limitada mesmo sobre
  bases maiores que a RAM. Por baixo é um *k-way merge* de runs ordenados.

Esta etapa faz, num único ``COPY``:

1. **ordena** pela sort key (``receita_projetada``), com a chave natural
   (``id_contrato``) como **desempate** — sem ele, a saída de um dado
   embaralhado não seria reproduzível entre execuções;
2. **particiona** por faixa de receita (a unidade de recarga), gerando o layout
   Hive ``faixa=K/`` que o upstream carrega por ``COPY`` de cada partição;
3. controla o tamanho do *row group* para que a estatística ``min``/``max`` seja
   granular o suficiente para o *pruning*.

O script mede os dois lados: a desordem que sai do paralelo (fração de descidas
na sort key e *row groups* puláveis ≈ 0) e o ganho após a reorganização
(ordenação global, *row groups* puláveis em alta), provando que nenhuma linha se
perde e que a saída é exatamente a ordem total determinística esperada.

Rode com: ``uv run run_reorg_for_upstream.py`` (a partir de ``rust-extension``).
"""

from __future__ import annotations

import shutil
import tempfile
import threading
import time
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from etl_rust_ext import BoundedRevenueProjector

NUM_CONTRATOS = 600_000
BATCH_SIZE = 100_000
RNG_SEED = 7
NUM_FAIXAS = 8            # nº de partições por faixa de receita no upstream
ROW_GROUP = 50_000        # granularidade do min/max para o predicate pushdown
MEMORY_LIMIT = "60MB"     # teto apertado: a reorganização deve caber mesmo assim


def section(title: str) -> None:
    print(f"\n{'=' * 10} {title} {'=' * 10}")


def gerar_contratos_parquet(destino: Path) -> None:
    """Fonte fictícia: ``id_contrato`` (int64), ``principal``/``taxa_mensal``
    (float64) e ``prazo_meses`` (int32) — o mesmo formato do run_contracts_parallel."""
    rng = np.random.default_rng(RNG_SEED)
    tabela = pa.table(
        {
            "id_contrato": np.arange(1, NUM_CONTRATOS + 1, dtype=np.int64),
            "principal": np.round(rng.uniform(10_000, 500_000, NUM_CONTRATOS), 2),
            "taxa_mensal": np.round(rng.uniform(0.008, 0.025, NUM_CONTRATOS), 5),
            "prazo_meses": rng.integers(60, 361, NUM_CONTRATOS, dtype=np.int32),
        }
    )
    pq.write_table(tabela, destino, row_group_size=BATCH_SIZE)


def fracao_descidas(coluna: pa.ChunkedArray) -> float:
    """Fração de pares adjacentes em que o valor CAI — 0 = ordenada, ~0.5 = embaralhada."""
    v = coluna.to_numpy()
    return float(np.mean(v[1:] < v[:-1]))


def row_groups_pulaveis(caminhos: list[Path], coluna: str, lo: float, hi: float) -> tuple[int, int]:
    """(puláveis, total) para o predicado ``coluna BETWEEN lo AND hi`` sobre um
    conjunto de arquivos: um row group é pulável se ``max < lo`` ou ``min > hi``
    (nenhuma linha dele pode satisfazer o filtro), lido só dos metadados."""
    pulaveis = total = 0
    for caminho in caminhos:
        md = pq.ParquetFile(caminho).metadata
        idx = md.schema.names.index(coluna)
        for i in range(md.num_row_groups):
            st = md.row_group(i).column(idx).statistics
            total += 1
            if st.max < lo or st.min > hi:
                pulaveis += 1
    return pulaveis, total


def tamanho_dir_bytes(diretorio: Path) -> int:
    """Soma o tamanho dos arquivos sob `diretorio` (usado para medir o pico de spill)."""
    return sum(f.stat().st_size for f in diretorio.rglob("*") if f.is_file())


def executar_medindo_spill(con: duckdb.DuckDBPyConnection, sql: str, spill_dir: Path) -> int:
    """Roda `sql` enquanto uma thread amostra o pico de bytes em `spill_dir`.
    O DuckDB apaga os arquivos de spill ao terminar, então medir DEPOIS não veria
    nada — a amostragem concorrente flagra o pico (mesma técnica do exemplo 17)."""
    pico = {"bytes": 0}
    parar = threading.Event()

    def amostrar() -> None:
        while not parar.is_set():
            if spill_dir.exists():
                pico["bytes"] = max(pico["bytes"], tamanho_dir_bytes(spill_dir))
            time.sleep(0.005)

    t = threading.Thread(target=amostrar)
    t.start()
    try:
        con.execute(sql)
    finally:
        parar.set()
        t.join()
    return pico["bytes"]


def reorganizar_para_upstream(
    paralelo: Path,
    saida: Path,
    *,
    num_faixas: int,
    row_group: int,
    memory_limit: str,
    spill_dir: Path,
) -> tuple[float, float, int]:
    """A etapa de reorganização em si (testável isoladamente).

    Lê o parquet DESORDENADO do estágio paralelo e o grava em ``saida``,
    particionado por faixa de receita e ordenado pela sort key
    (``receita_projetada``, com ``id_contrato`` como desempate). O ``ORDER BY``
    é um **sort externo**: derrama para ``spill_dir`` se estourar ``memory_limit``.

    A faixa é o índice do balde de receita (0..num_faixas-1), monotônico na
    receita — concatenar as partições em ordem de faixa reconstrói a ordem global.

    Devolve ``(min_receita, largura_da_faixa, pico_de_spill_em_bytes)``.
    """
    receita = pq.read_table(paralelo, columns=["receita_projetada"])["receita_projetada"].to_numpy()
    mn, mx = float(receita.min()), float(receita.max())
    largura = (mx - mn) / num_faixas  # faixas de receita de largura igual
    if saida.exists():
        shutil.rmtree(saida)

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET temp_directory='{spill_dir}'")
    con.execute("SET preserve_insertion_order=true")  # respeita o ORDER BY ao gravar
    reorg_sql = f"""
        COPY (
            SELECT
                id_contrato,
                receita_projetada,
                LEAST(CAST(floor((receita_projetada - {mn}) / {largura}) AS INTEGER),
                      {num_faixas - 1}) AS faixa
            FROM read_parquet('{paralelo.as_posix()}')
            ORDER BY receita_projetada, id_contrato
        ) TO '{saida.as_posix()}'
        (FORMAT parquet, PARTITION_BY (faixa), ROW_GROUP_SIZE {row_group}, OVERWRITE_OR_IGNORE)
    """
    pico_spill = executar_medindo_spill(con, reorg_sql, spill_dir)
    con.close()
    return mn, largura, pico_spill


def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="reorg_upstream_"))
    fonte = workdir / "contratos.parquet"
    paralelo = workdir / "receitas_paralelo.parquet"     # saída do estágio paralelo (desordenada)
    controle = workdir / "controle_desordenado.parquet"  # mesmo dado, RG fixo, p/ medir pruning
    saida = workdir / "upstream"                          # dir particionado, pronto p/ o COPY do Redshift
    spill_dir = workdir / "_spill"

    # ========================================================================
    section(f"1) Estágio paralelo: {NUM_CONTRATOS:,} contratos -> parquet DESORDENADO")
    gerar_contratos_parquet(fonte)
    projetor = BoundedRevenueProjector(str(paralelo), queue_depth=3)
    print(f"{projetor.num_workers} workers | fila de {projetor.queue_depth} lotes "
          "(processamento embaraçosamente paralelo, ordem de saída NÃO garantida)")
    inicio = time.perf_counter()
    for batch in ds.dataset(fonte).to_batches(batch_size=BATCH_SIZE):
        projetor.submit_batch(batch)
    _, linhas = projetor.finish()
    print(f"{linhas:,} linhas projetadas em {time.perf_counter() - inicio:.2f}s -> {paralelo.name}")

    tab_paralelo = pq.read_table(paralelo)
    desc = fracao_descidas(tab_paralelo["receita_projetada"])
    print(f"desordem na sort key (receita_projetada): {desc:.0%} dos pares adjacentes "
          "caem (~50% = embaralhado; 0% seria ordenado)")

    # probe: uma faixa estreita no miolo da distribuição (o filtro típico do upstream)
    receita_np = tab_paralelo["receita_projetada"].to_numpy()
    lo, hi = np.percentile(receita_np, [48, 52])  # ~4% das linhas passam
    print(f"filtro de exemplo no upstream: receita_projetada BETWEEN {lo:,.2f} AND {hi:,.2f} "
          "(~4% das linhas)")

    # controle: o MESMO dado, mesmo row group, SEM ordenar — isola o efeito da ordenação
    pq.write_table(tab_paralelo, controle, row_group_size=ROW_GROUP)
    pul_antes, tot_antes = row_groups_pulaveis([controle], "receita_projetada", lo, hi)
    print(f"row groups puláveis ANTES (desordenado): {pul_antes}/{tot_antes} "
          f"({pul_antes / tot_antes:.0%} do I/O evitável)")

    # ========================================================================
    section("2) Reorganização (barreira, memory-bounded): sort + partição por faixa")
    print(f"memory_limit={MEMORY_LIMIT} · temp_directory={spill_dir} · "
          "sort externo (COPY ... ORDER BY) derrama p/ disco se estourar o teto")
    _, _, pico_spill = reorganizar_para_upstream(
        paralelo, saida,
        num_faixas=NUM_FAIXAS, row_group=ROW_GROUP,
        memory_limit=MEMORY_LIMIT, spill_dir=spill_dir,
    )

    particoes = sorted(saida.glob("faixa=*"))
    arquivos = sorted(saida.rglob("*.parquet"))
    print(f"gravadas {len(particoes)} partições (faixa=0..{NUM_FAIXAS - 1}) em {len(arquivos)} arquivo(s)")
    if pico_spill > 0:
        print(f"pico derramado para disco (spill sob {MEMORY_LIMIT}): {pico_spill / 1e6:.1f}MB "
              "-> o sort externo não estourou a RAM")
    else:
        print(f"o sort coube nos {MEMORY_LIMIT} sem derramar neste volume didático; em bases "
              "massivas ele derrama para o temp_directory (ver DuckDB exemplos 04 e 17)")

    # ========================================================================
    section("3) Verificação: nada se perde, ordem global correta, pruning restaurado")
    # lê o dataset particionado; ordenar por faixa (int) recompõe a ordem de receita
    dset = ds.dataset(saida, format="parquet", partitioning="hive")
    tab_saida = dset.to_table().sort_by([("faixa", "ascending"),
                                         ("receita_projetada", "ascending"),
                                         ("id_contrato", "ascending")])

    # (a) conservação: mesmas linhas e mesma soma (a reorganização não altera dados)
    linhas_ok = tab_saida.num_rows == tab_paralelo.num_rows
    soma_ok = abs(pc.sum(tab_saida["receita_projetada"]).as_py()
                  - pc.sum(tab_paralelo["receita_projetada"]).as_py()) < 1e-3
    print(f"linhas conservadas: {linhas_ok} ({tab_saida.num_rows:,}); soma conservada: {soma_ok}")

    # (b) ordem total determinística: a saída é EXATAMENTE o sort esperado (id desempata).
    # Comparamos os VALORES (via numpy), não Table.equals: o parquet escrito pelo
    # DuckDB marca as colunas como nullable e o da extensão Rust como non-null, então
    # os schemas diferem no metadado — mas os dados têm de ser idênticos.
    esperado = tab_paralelo.sort_by([("receita_projetada", "ascending"),
                                     ("id_contrato", "ascending")])
    ordem_ok = bool(
        np.array_equal(tab_saida["id_contrato"].to_numpy(), esperado["id_contrato"].to_numpy())
        and np.array_equal(tab_saida["receita_projetada"].to_numpy(),
                           esperado["receita_projetada"].to_numpy())
    )
    desc_depois = fracao_descidas(tab_saida["receita_projetada"])
    print(f"ordem == sort determinístico (sort key + desempate): {ordem_ok}; "
          f"descidas na sort key: {desc_depois:.0%}")

    # (c) pruning restaurado: os mesmos ~4% de linhas, agora concentrados em poucos row groups
    pul_depois, tot_depois = row_groups_pulaveis(arquivos, "receita_projetada", lo, hi)
    print(f"row groups puláveis DEPOIS (ordenado+particionado): {pul_depois}/{tot_depois} "
          f"({pul_depois / tot_depois:.0%} do I/O evitável) — era {pul_antes}/{tot_antes} antes")

    section("Pronto para o upstream")
    print("Cada partição faixa=K/ é a unidade de recarga idempotente: o Redshift a")
    print("carrega com COPY do parquet (já ordenado pela sort key), então as linhas")
    print("entram agrupadas — região não-ordenada mínima, VACUUM quase dispensável.")

    shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
