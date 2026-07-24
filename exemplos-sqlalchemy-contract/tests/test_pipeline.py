"""Testes dos caminhos de dados: ORM vs colunar produzem o MESMO resultado,
e a hierarquia recursiva devolve as subárvores esperadas."""

import importlib
from decimal import Decimal

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from _common import gerar_lancamentos
from models import Base, Lancamento

exemplo02 = importlib.import_module("02_orm_vs_columnar")
exemplo03 = importlib.import_module("03_account_hierarchy")


def test_orm_and_columnar_paths_agree_on_totals(tmp_path):
    tabela = gerar_lancamentos(2_000, contas_folha=[1, 2, 3])
    total_arrow = sum(tabela["valor"].to_pylist(), Decimal(0))

    # caminho ORM: mesmo dado, via objetos + session
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(Lancamento(**linha) for linha in tabela.to_pylist())
        session.commit()
        n_orm = session.scalar(select(func.count(Lancamento.id_lancamento)))
    assert n_orm == tabela.num_rows

    # caminho colunar: parquet + DuckDB
    destino = tmp_path / "l.parquet"
    pq.write_table(tabela, destino)
    con = duckdb.connect()
    n_col, total_col = con.sql(
        f"SELECT COUNT(*), SUM(valor) FROM read_parquet('{destino}')"
    ).fetchone()
    assert n_col == tabela.num_rows
    assert total_col == total_arrow  # decimal exato: igualdade estrita, sem approx


def test_generated_valores_have_two_decimal_places():
    tabela = gerar_lancamentos(500, contas_folha=[1])
    assert tabela.schema.field("valor").type == pa.decimal128(12, 2)
    exemplo = tabela["valor"][0].as_py()
    assert exemplo == exemplo.quantize(Decimal("0.01"))


@pytest.fixture
def con_hierarquia():
    con = duckdb.connect()
    contas = pa.table(
        {
            "id_conta": pa.array([c[0] for c in exemplo03.CONTAS], pa.int64()),
            "nome": pa.array([c[1] for c in exemplo03.CONTAS], pa.string()),
        }
    )
    arestas = pa.table(
        {
            "id_hierarquia": pa.array([a[0] for a in exemplo03.ARESTAS], pa.int64()),
            "id_parent": pa.array([a[1] for a in exemplo03.ARESTAS], pa.int64()),
            "id_child": pa.array([a[2] for a in exemplo03.ARESTAS], pa.int64()),
        }
    )
    con.register("contas", contas)
    con.register("arestas", arestas)
    yield con
    con.close()


def test_recursive_flatten_produces_full_paths(con_hierarquia):
    arvore = dict(
        con_hierarquia.execute(
            f"SELECT id_conta, caminho FROM ({exemplo03.FLATTEN_SQL})", {"hierarquia": 1}
        ).fetchall()
    )
    assert arvore[1] == "Resultado"
    assert arvore[7] == "Resultado > Despesas > Pessoal > Salarios"
    assert len(arvore) == 11  # todas as contas alcançáveis na hierarquia 1


def test_subtree_filter_selects_only_descendants(con_hierarquia):
    descendentes = con_hierarquia.execute(
        f"""
        WITH arvore AS ({exemplo03.FLATTEN_SQL})
        SELECT id_conta FROM arvore
        WHERE caminho LIKE (SELECT caminho FROM arvore WHERE id_conta = 6) || '%'
        ORDER BY id_conta
        """,
        {"hierarquia": 1},
    ).fetchall()
    assert [d[0] for d in descendentes] == [6, 7, 8]  # Pessoal, Salarios, Encargos


def test_alternative_hierarchy_is_independent(con_hierarquia):
    arvore2 = dict(
        con_hierarquia.execute(
            f"SELECT id_conta, caminho FROM ({exemplo03.FLATTEN_SQL})", {"hierarquia": 2}
        ).fetchall()
    )
    # hierarquia 2 só enxerga raiz + 3 contas de custo fixo
    assert set(arvore2) == {1, 7, 10, 11}
    assert arvore2[10] == "Resultado > Aluguel"


def test_orphan_lancamentos_detected_by_anti_join(con_hierarquia, tmp_path):
    tabela = gerar_lancamentos(100, contas_folha=[3, 999])  # 999 não existe no cadastro
    destino = tmp_path / "l.parquet"
    pq.write_table(tabela, destino)
    orfaos = con_hierarquia.execute(
        f"""
        SELECT COUNT(*) FROM read_parquet('{destino}') l
        ANTI JOIN contas c ON l.id_conta = c.id_conta
        """
    ).fetchone()[0]
    assert orfaos > 0  # a "FK como query de qualidade" pega o problema
