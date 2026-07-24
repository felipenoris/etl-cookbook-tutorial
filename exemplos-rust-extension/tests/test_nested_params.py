"""Testes das três estratégias de materialização de dados 1:N no Rust.

`project_nested_materialized` (Vec por linha), `project_nested_reused`
(buffers reaproveitados) e `project_nested_borrowed` (fatias emprestadas)
devem ser **indistinguíveis pelo resultado** — só o custo muda.
"""

import pyarrow as pa
import pytest

from etl_rust_ext import (
    project_nested_borrowed,
    project_nested_materialized,
    project_nested_reused,
)

VARIANTES = [project_nested_materialized, project_nested_reused, project_nested_borrowed]
IDS = ["materialized", "reused", "borrowed"]


def make_batch(ids, principais, taxas, prazos) -> pa.RecordBatch:
    """Monta o batch 1:N a partir de listas Python (o pyarrow calcula os offsets)."""
    return pa.record_batch(
        {
            "id_contrato": pa.array(ids, type=pa.int64()),
            "principal": pa.array(principais, type=pa.float64()),
            "parametros_taxa": pa.array(taxas, type=pa.list_(pa.float64())),
            "parametros_prazo": pa.array(prazos, type=pa.list_(pa.int32())),
        }
    )


def receita_esperada(principal: float, taxas: list[float], prazos: list[int]) -> float:
    """Reimplementa o núcleo em Python puro, como referência independente."""
    saldo, receita = principal, 0.0
    for taxa, prazo in zip(taxas, prazos):
        receita += saldo * ((1.0 + taxa) ** prazo - 1.0)
        saldo *= 0.7
    return receita


@pytest.mark.parametrize("fn", VARIANTES, ids=IDS)
def test_matches_python_reference(fn):
    taxas = [[0.01, 0.02], [0.015], [0.01, 0.005, 0.02]]
    prazos = [[12, 24], [36], [6, 12, 18]]
    principais = [100_000.0, 50_000.0, 250_000.0]
    out = fn(make_batch([1, 2, 3], principais, taxas, prazos))

    esperado = [receita_esperada(p, t, z) for p, t, z in zip(principais, taxas, prazos)]
    assert out.column("id_contrato").to_pylist() == [1, 2, 3]
    for calculado, ref in zip(out.column("receita_projetada").to_pylist(), esperado):
        assert calculado == pytest.approx(ref, rel=1e-12)


def test_all_three_variants_agree():
    # sublistas de tamanhos variados, incluindo 1 e vazia
    batch = make_batch(
        [1, 2, 3, 4],
        [10_000.0, 20_000.0, 30_000.0, 40_000.0],
        [[0.01], [0.01, 0.02, 0.03], [], [0.005, 0.01]],
        [[12], [6, 12, 24], [], [36, 48]],
    )
    resultados = [pa.Table.from_batches([fn(batch)]) for fn in VARIANTES]
    assert resultados[0].equals(resultados[1])
    assert resultados[1].equals(resultados[2])


@pytest.mark.parametrize("fn", VARIANTES, ids=IDS)
def test_empty_sublist_yields_zero(fn):
    # contrato sem parâmetros: o laço não executa -> receita 0
    out = fn(make_batch([1], [100_000.0], [[]], [[]]))
    assert out.column("receita_projetada").to_pylist() == [0.0]


@pytest.mark.parametrize("fn", VARIANTES, ids=IDS)
def test_output_schema(fn):
    out = fn(make_batch([1], [1000.0], [[0.01]], [[12]]))
    assert out.schema.names == ["id_contrato", "receita_projetada"]
    assert out.schema.field("id_contrato").type == pa.int64()
    assert out.schema.field("receita_projetada").type == pa.float64()


@pytest.mark.parametrize("fn", VARIANTES, ids=IDS)
def test_missing_column_raises(fn):
    incompleto = pa.record_batch(
        {
            "id_contrato": pa.array([1], type=pa.int64()),
            "principal": pa.array([1000.0], type=pa.float64()),
        }
    )
    with pytest.raises(ValueError, match="parametros_taxa"):
        fn(incompleto)


@pytest.mark.parametrize("fn", VARIANTES, ids=IDS)
def test_wrong_nested_type_raises(fn):
    # parametros_taxa como coluna escalar (não list) deve ser rejeitada
    errado = pa.record_batch(
        {
            "id_contrato": pa.array([1], type=pa.int64()),
            "principal": pa.array([1000.0], type=pa.float64()),
            "parametros_taxa": pa.array([0.01], type=pa.float64()),
            "parametros_prazo": pa.array([[12]], type=pa.list_(pa.int32())),
        }
    )
    with pytest.raises(ValueError, match="list<float64>"):
        fn(errado)
