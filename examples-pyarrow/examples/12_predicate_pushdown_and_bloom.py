"""Exemplo 12 — Predicate pushdown por estatística e bloom filters.

Duas perguntas frequentes sobre "ler menos" de um parquet, respondidas com
medições sobre os dados reais (não só afirmadas):

1. **O predicate pushdown depende de a coluna estar ordenada fisicamente?**
   Não é requisito, mas é o que o torna *eficaz*. Todo *row group* guarda o
   `min`/`max` de cada coluna (estatísticas embutidas, ligadas por padrão), e o
   leitor pula um row group inteiro quando prova, só pelo `max`/`min`, que
   nenhuma linha dele satisfaz o filtro. Com os dados **ordenados** pela coluna
   do filtro, cada row group cobre uma faixa estreita e sem sobreposição → o
   filtro descarta quase todos. **Embaralhados**, quase todo row group tem `min`
   baixo e `max` alto → faixas largas, nada é descartado. A Parte A mede isso:
   conta os row groups *puláveis* nos dois arranjos.

2. **Bloom filter precisa ser configurado ao ESCREVER?** Sim. É um índice extra
   gravado dentro do arquivo, decidido na escrita (`bloom_filter_options`); o
   leitor só o usa se ele existir. Serve para IGUALDADE em coluna de alta
   cardinalidade — justo onde `min`/`max` não ajuda (numa coluna embaralhada,
   toda faixa `[min,max]` contém o valor procurado). A Parte B escreve com e sem
   o filtro, prova que ele foi gravado (via metadados) e mede o custo em disco.

Tudo é escrito num diretório temporário (nada fica no repo). Fonte: um
subconjunto real de `orders` (mês 1), com `amount = quantity * unit_price`.

Rode com: `uv run examples/12_predicate_pushdown_and_bloom.py`
"""

import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from _common import orders_dataset, products_dataset, section

N_LINHAS = 200_000  # subconjunto para o exemplo rodar rápido
ROW_GROUP = 20_000  # 200k / 20k = 10 row groups por arquivo (granularidade do pulo)


def carrega_amostra() -> pa.Table:
    """orders do mês 1 enriquecidas com amount = quantity * preço, N_LINHAS linhas."""
    products = products_dataset().to_table()
    preco = dict(
        zip(products.column("product_id").to_pylist(), products.column("unit_price").to_pylist())
    )
    orders = orders_dataset().to_table(
        columns=["order_id", "customer_id", "product_id", "quantity"],
        filter=(pc.field("order_month") == 1),
    )
    orders = orders.slice(0, N_LINHAS)
    precos = pa.array([preco[p] for p in orders.column("product_id").to_pylist()])
    amount = pc.multiply(pc.cast(orders.column("quantity"), pa.float64()), precos)
    return orders.append_column("amount", amount)


def stats_por_row_group(caminho: Path, coluna: str) -> list[tuple]:
    """(min, max) de `coluna` em cada row group, lidos dos metadados do arquivo."""
    md = pq.ParquetFile(caminho).metadata
    idx = md.schema.names.index(coluna)
    out = []
    for i in range(md.num_row_groups):
        st = md.row_group(i).column(idx).statistics
        out.append((st.min, st.max))
    return out


if __name__ == "__main__":
    tabela = carrega_amostra()
    tmp = Path(tempfile.mkdtemp(prefix="pushdown_"))
    print(f"{tabela.num_rows:,} linhas · {tabela.num_columns} colunas · {ROW_GROUP:,}/row group")

    # ========================================================================
    section("Parte A — predicate pushdown: ordenado vs embaralhado")
    # limiar do filtro: percentil 95 de amount → só ~5% das linhas passam
    limiar = float(np.percentile(tabela.column("amount").to_numpy(), 95))
    print(f'filtro de exemplo: amount > {limiar:,.2f}  (percentil 95 → ~5% das linhas)\n')

    ordenada = tabela.sort_by([("amount", "ascending")])
    rng = np.random.default_rng(42)
    perm = pa.array(rng.permutation(tabela.num_rows))
    embaralhada = tabela.take(perm)

    f_ord = tmp / "amount_ordenado.parquet"
    f_emb = tmp / "amount_embaralhado.parquet"
    pq.write_table(ordenada, f_ord, row_group_size=ROW_GROUP)
    pq.write_table(embaralhada, f_emb, row_group_size=ROW_GROUP)

    for rotulo, caminho in [("ORDENADO", f_ord), ("EMBARALHADO", f_emb)]:
        faixas = stats_por_row_group(caminho, "amount")
        # um row group é pulável para (amount > limiar) se seu max <= limiar
        pulaveis = sum(1 for _mn, mx in faixas if mx <= limiar)
        print(f"[{rotulo}]  {len(faixas)} row groups; faixas [min, max] de amount:")
        for i, (mn, mx) in enumerate(faixas):
            marca = "  ← pulável (max <= limiar)" if mx <= limiar else ""
            print(f"    rg{i:>2}: [{mn:>10,.2f}, {mx:>10,.2f}]{marca}")
        print(
            f"  => {pulaveis}/{len(faixas)} row groups puláveis "
            f"({pulaveis / len(faixas):.0%} do I/O evitado sem ler dados)\n"
        )

    print("Conclusão: mesma coluna, mesmo filtro. Ordenar não é requisito do")
    print("pushdown, mas concentra os valores altos em poucos row groups — os")
    print("demais são descartados só pela estatística. Embaralhado, todo row")
    print("group tem algum valor alto (max > limiar), e nada pode ser pulado.")

    # De onde vêm essas faixas [min, max]: das estatísticas por row group, que o
    # pyarrow grava por PADRÃO (write_statistics=True). Ou seja, o pushdown por
    # min/max funciona "de graça" na maioria dos parquets — só se perde se alguém
    # escrever com write_statistics=False (aí o bloco some inteiro, inclusive o
    # null_count). Detalhe medido: numa coluna TODA nula, has_min_max fica False
    # (min/max = None), mas o null_count continua presente — os três não são
    # independentes na API, mas o null_count é o mais robusto.
    print()
    print("(min/max/null_count vêm de write_statistics=True, o default — o")
    print(" pushdown é 'de graça'; só some com write_statistics=False.)")

    # ========================================================================
    section("Parte B — bloom filter: precisa ser ligado na ESCRITA")
    # coluna de alta cardinalidade (order_id é único) e um filtro de IGUALDADE
    alvo = int(tabela.column("order_id")[N_LINHAS // 2].as_py())
    print(f"filtro de exemplo: order_id == {alvo}  (igualdade, alta cardinalidade)\n")

    # min/max NÃO resolve igualdade em coluna embaralhada: toda faixa contém o alvo
    faixas_id = stats_por_row_group(f_emb, "order_id")
    candidatos = sum(1 for mn, mx in faixas_id if mn <= alvo <= mx)
    print(f"por min/max: {candidatos}/{len(faixas_id)} row groups NÃO podem ser")
    print("descartados (o alvo cai dentro da faixa [min,max] de todos) — é aqui")
    print("que o bloom filter entra.\n")

    f_sem = tmp / "sem_bloom.parquet"
    f_com = tmp / "com_bloom.parquet"
    pq.write_table(embaralhada, f_sem, row_group_size=ROW_GROUP)
    pq.write_table(
        embaralhada,
        f_com,
        row_group_size=ROW_GROUP,
        # dict {coluna -> {ndv, fpp}} — ndv ~ nº de distintos, fpp = prob. de
        # falso-positivo; True usaria os defaults (ndv=1048576, fpp=0.05)
        bloom_filter_options={"order_id": {"ndv": N_LINHAS, "fpp": 0.01}},
    )

    def tem_bloom(caminho: Path, coluna: str) -> bool:
        md = pq.ParquetFile(caminho).metadata
        idx = md.schema.names.index(coluna)
        off = md.row_group(0).column(idx).bloom_filter_offset
        return off is not None and off > 0

    tam_sem = f_sem.stat().st_size
    tam_com = f_com.stat().st_size
    print(f"order_id tem bloom filter (sem opção): {tem_bloom(f_sem, 'order_id')}")
    print(f"order_id tem bloom filter (com opção): {tem_bloom(f_com, 'order_id')}")
    md_com = pq.ParquetFile(f_com).metadata
    idx = md_com.schema.names.index("order_id")
    comp = md_com.row_group(0).column(idx).bloom_filter_length
    print(f"tamanho do bloom no row group 0: {comp:,} bytes")
    print(f"arquivo sem bloom: {tam_sem:,} bytes")
    print(f"arquivo com bloom: {tam_com:,} bytes  (+{tam_com - tam_sem:,} = índice extra)\n")

    print("Conclusão: o bloom filter é decidido na ESCRITA (bloom_filter_options)")
    print("e gravado dentro do arquivo — custa disco. Um leitor que o suporte")
    print("(ex.: DuckDB) o consulta para descartar row groups numa igualdade,")
    print("mesmo com os dados embaralhados, onde min/max não ajuda.")

    section("Contraste: quem grava bloom — pyarrow (opt-in) vs DuckDB (automático)")
    # Os dois writers da stack fazem escolhas OPOSTAS por padrão (medido):
    #
    #   pyarrow  -> NADA de bloom por padrão. Você habilita por coluna via
    #               `bloom_filter_options` (foi o que fizemos com 'order_id' acima).
    #               Ideal para nomear justamente as colunas de ALTA cardinalidade
    #               que você consulta por igualdade.
    #
    #   DuckDB    -> grava bloom AUTOMATICAMENTE (COPY ... FORMAT parquet), sem
    #   (COPY)       parâmetro. Mas por uma heurística de cardinalidade POR row
    #                group: só escreve o filtro quando os valores distintos na row
    #                group são <= 20% das linhas dela; acima disso, OMITE.
    #                (Corte medido em 20% exatos, escalando com ROW_GROUP_SIZE; em
    #                DuckDB 1.5.4 não há setting nem opção de COPY para mudar isso.)
    #
    # Consequência que inverte a intuição "bloom = alta cardinalidade": o DuckDB
    # PULA o bloom nas colunas quase-únicas. Os MESMOS dados deste exemplo,
    # escritos via DuckDB, NÃO teriam bloom em 'order_id' (100% de distintos),
    # embora aqui, no pyarrow, nós o tenhamos pedido de propósito. Motivo: num
    # column chunk quase-único o filtro cresce até deixar de compensar (o mesmo
    # trade-off ndv x fpp x espaço que `bloom_filter_options` expõe manualmente).
    print("pyarrow: opt-in por coluna (bloom_filter_options) — bom para nomear")
    print("         colunas de alta cardinalidade consultadas por igualdade.")
    print("DuckDB : automático no COPY, mas só quando distintos por row group")
    print("         <= 20% das linhas — logo, PULA colunas quase-únicas como")
    print("         order_id. Os mesmos dados, via DuckDB, não teriam bloom aqui.")
