"""Testes do exemplo 05: o SQLAlchemy como gerador de SQL para o DuckDB.

Os contratos exercitados: o DDL compilado do modelo é aceito por uma base
DuckDB vazia (com os ``comment=`` chegando ao catálogo), a carga set-based
via ``INSERT ... FROM SELECT`` preserva contagem e soma decimal exata, as FKs
geradas são de fato aplicadas pelo DuckDB, e a agregação por RecordBatches
(map-reduce) devolve exatamente o mesmo resultado que o cálculo direto sobre
a massa — igualdade estrita de ``Decimal``, nunca float.

Nota: a ORDEM das linhas dentro de cada RecordBatch varia entre execuções
(o hash join do DuckDB é paralelo), então os parciais por batch não são
determinísticos — mas a consolidação sobre TODOS os batches é. É ela que os
testes comparam.
"""

import importlib
from decimal import Decimal

import duckdb
import pandas as pd
import pyarrow as pa
import pytest

from _common import gerar_lancamentos
from models import Conta, Lancamento, Veiculo

exemplo05 = importlib.import_module("05_sql_generation_duckdb")

N_LANCAMENTOS = 10_000  # o bastante para o reader do teste produzir vários batches


@pytest.fixture()
def base_carregada():
    """Base DuckDB criada pelo DDL do contrato e carregada em batch."""
    con = duckdb.connect()
    exemplo05.ddl_do_contrato(con)
    df_veiculos, df_contas = exemplo05.montar_dimensoes()
    tabela = gerar_lancamentos(N_LANCAMENTOS, contas_folha=exemplo05.CONTAS_FOLHA)
    exemplo05.carga_batch(con, Veiculo, "df_veiculos", df_veiculos)
    exemplo05.carga_batch(con, Conta, "df_contas", df_contas)
    exemplo05.carga_batch(
        con, Lancamento, "df_lancamentos", tabela.to_pandas(types_mapper=pd.ArrowDtype)
    )
    yield con, tabela
    con.close()


def test_generated_ddl_creates_tables_and_comments_in_duckdb_catalog():
    con = duckdb.connect()
    exemplo05.ddl_do_contrato(con)
    tabelas = dict(con.sql("SELECT table_name, comment FROM duckdb_tables()").fetchall())
    assert set(tabelas) == {
        "dom_veiculos",
        "dom_hierarquias_contas",
        "cad_contas",
        "rel_contas_hierarquias",
        "cad_lancamentos",
    }
    # os comment= do modelo chegaram ao catálogo, tabela e coluna
    assert tabelas["cad_lancamentos"] == Lancamento.__table__.comment
    (comentario_valor,) = con.sql(
        "SELECT comment FROM duckdb_columns() "
        "WHERE table_name = 'cad_lancamentos' AND column_name = 'valor'"
    ).fetchone()
    assert comentario_valor == Lancamento.__table__.columns["valor"].comment
    con.close()


def test_batch_load_preserves_count_and_exact_decimal_sum(base_carregada):
    con, tabela = base_carregada
    n, soma = con.sql("SELECT COUNT(*), SUM(valor) FROM cad_lancamentos").fetchone()
    assert n == tabela.num_rows
    assert isinstance(soma, Decimal)  # DECIMAL entrou e saiu DECIMAL, nunca float
    assert soma == sum(tabela["valor"].to_pylist(), Decimal(0))


def test_generated_ddl_enforces_foreign_keys(base_carregada):
    con, tabela = base_carregada
    # um lançamento órfão (veículo 999 não existe) deve ser rejeitado pela FK
    orfao = tabela.slice(0, 1).to_pandas(types_mapper=pd.ArrowDtype)
    orfao["id_lancamento"] = orfao["id_lancamento"] + tabela.num_rows
    orfao["id_veiculo"] = pd.array([999], dtype="int64[pyarrow]")
    with pytest.raises(duckdb.ConstraintException):
        exemplo05.carga_batch(con, Lancamento, "df_orfao", orfao)


def test_batch_load_rejects_dataframe_that_diverges_from_contract(base_carregada):
    con, tabela = base_carregada
    capenga = tabela.drop_columns(["meta"]).to_pandas(types_mapper=pd.ArrowDtype)
    with pytest.raises(AssertionError, match="não bate com o contrato"):
        exemplo05.carga_batch(con, Lancamento, "df_capenga", capenga)


def test_streamed_join_aggregation_equals_direct_computation(base_carregada):
    con, tabela = base_carregada
    sql = exemplo05.sql_de(exemplo05.consulta_join(), literal=True)
    reader = con.execute(sql).to_arrow_reader(2_048)

    totais: dict[str, Decimal] = {}
    n_batches = 0
    for batch in reader:
        parcial = exemplo05.agrega_batch(batch)
        # o groupby preservou o tipo decimal do contrato (nenhum float no caminho)
        assert parcial.dtype == pd.ArrowDtype(pa.decimal128(12, 2))
        for veiculo, soma in parcial.items():
            totais[veiculo] = totais.get(veiculo, Decimal(0)) + soma
        n_batches += 1
    assert n_batches > 1  # o caminho de streaming foi exercitado de verdade

    # o mesmo cálculo, direto sobre a massa gerada, em Decimal puro
    esperado: dict[str, Decimal] = {}
    for id_veiculo, data, valor in zip(
        tabela["id_veiculo"].to_pylist(),
        tabela["data"].to_pylist(),
        tabela["valor"].to_pylist(),
    ):
        if data < exemplo05.DATA_CORTE:
            nome = exemplo05.VEICULOS[id_veiculo - 1]
            esperado[nome] = esperado.get(nome, Decimal(0)) + valor
    assert totais == esperado  # igualdade estrita, Decimal a Decimal
