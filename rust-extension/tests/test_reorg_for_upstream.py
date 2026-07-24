"""Testes de contrato da etapa de reorganização (run_reorg_for_upstream).

Valida o que a etapa promete sobre uma entrada pequena e propositalmente
embaralhada: nada se perde, a saída fica particionada por faixa e globalmente
ordenada pela sort key (com id como desempate → reproduzível), e o predicate
pushdown por min/max, que era inútil no dado embaralhado, passa a valer.
"""

import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from run_reorg_for_upstream import (  # noqa: E402
    fracao_descidas,
    reorganizar_para_upstream,
    row_groups_pulaveis,
)

N = 40_000
NUM_FAIXAS = 8
ROW_GROUP = 5_000


@pytest.fixture
def paralelo(tmp_path):
    """Simula a saída do estágio paralelo: id sequencial, receita EMBARALHADA
    em relação ao id (é o que a projeção paralela produz na sort key)."""
    rng = np.random.default_rng(7)
    receita = rng.uniform(100.0, 10_000.0, N)
    caminho = tmp_path / "paralelo.parquet"
    pq.write_table(
        pa.table(
            {
                "id_contrato": np.arange(1, N + 1, dtype=np.int64),
                "receita_projetada": receita,
            }
        ),
        caminho,
        row_group_size=ROW_GROUP,
    )
    return caminho


def _reorganiza(paralelo, tmp_path):
    saida = tmp_path / "upstream"
    reorganizar_para_upstream(
        paralelo, saida,
        num_faixas=NUM_FAIXAS, row_group=ROW_GROUP,
        memory_limit="200MB", spill_dir=tmp_path / "_spill",
    )
    return saida


def test_entrada_esta_desordenada_na_sort_key(paralelo):
    # pré-condição do exemplo: a saída do paralelo é embaralhada (~50% de descidas)
    col = pq.read_table(paralelo)["receita_projetada"]
    assert fracao_descidas(col) > 0.4


def test_particiona_por_faixa_e_conserva_todas_as_linhas(paralelo, tmp_path):
    saida = _reorganiza(paralelo, tmp_path)
    particoes = sorted(saida.glob("faixa=*"))
    assert 1 < len(particoes) <= NUM_FAIXAS  # gravou o layout Hive faixa=K/
    tabela = ds.dataset(saida, format="parquet", partitioning="hive").to_table()
    assert tabela.num_rows == N  # nenhuma linha perdida ou duplicada


def test_saida_e_ordem_total_deterministica_com_desempate(paralelo, tmp_path):
    saida = _reorganiza(paralelo, tmp_path)
    dset = ds.dataset(saida, format="parquet", partitioning="hive")
    saida_tab = dset.to_table().sort_by(
        [("faixa", "ascending"), ("receita_projetada", "ascending"), ("id_contrato", "ascending")]
    )
    entrada = pq.read_table(paralelo)
    esperado = entrada.sort_by([("receita_projetada", "ascending"), ("id_contrato", "ascending")])
    # é EXATAMENTE o sort determinístico (comparação por valores: o schema do parquet
    # do DuckDB marca colunas como nullable, então Table.equals divergiria só no metadado)
    assert np.array_equal(
        saida_tab["id_contrato"].to_numpy(), esperado["id_contrato"].to_numpy()
    )
    assert np.array_equal(
        saida_tab["receita_projetada"].to_numpy(), esperado["receita_projetada"].to_numpy()
    )
    # e globalmente não-decrescente na sort key
    assert fracao_descidas(saida_tab["receita_projetada"]) == 0.0


def test_reorganizar_restaura_o_predicate_pushdown(paralelo, tmp_path):
    receita = pq.read_table(paralelo)["receita_projetada"].to_numpy()
    lo, hi = np.percentile(receita, [48, 52])  # faixa estreita: só ~4% passa

    # antes: dado embaralhado -> quase nenhum row group é pulável
    pul_antes, tot_antes = row_groups_pulaveis([paralelo], "receita_projetada", lo, hi)

    saida = _reorganiza(paralelo, tmp_path)
    arquivos = sorted(saida.rglob("*.parquet"))
    pul_depois, tot_depois = row_groups_pulaveis(arquivos, "receita_projetada", lo, hi)

    assert pul_antes / tot_antes < 0.2          # embaralhado: quase nada pulável
    assert pul_depois / tot_depois > 0.5        # ordenado+particionado: maioria pulável
    assert pul_depois / tot_depois > pul_antes / tot_antes


def test_faixa_e_monotonica_na_receita(paralelo, tmp_path):
    # cada partição cobre uma faixa contígua de receita: o max da faixa K é <= min da K+1
    saida = _reorganiza(paralelo, tmp_path)
    dset = ds.dataset(saida, format="parquet", partitioning="hive")
    por_faixa = {}
    tabela = dset.to_table()
    faixas = tabela["faixa"].to_numpy()
    receita = tabela["receita_projetada"].to_numpy()
    for f in np.unique(faixas):
        vals = receita[faixas == f]
        por_faixa[int(f)] = (vals.min(), vals.max())
    chaves = sorted(por_faixa)
    for anterior, atual in zip(chaves, chaves[1:]):
        assert por_faixa[anterior][1] <= por_faixa[atual][0]  # faixas sem sobreposição, em ordem
