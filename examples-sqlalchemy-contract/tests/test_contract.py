"""Testes do contrato: as três projeções dos modelos SQLAlchemy."""

import pyarrow as pa
from sqlalchemy import create_engine, inspect

from models import Base, Conta, Lancamento, arrow_schema_for, redshift_ddl_for


def test_create_all_creates_physical_tables():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    tabelas = set(inspect(engine).get_table_names())
    assert tabelas == {
        "dom_veiculos",
        "dom_hierarquias_contas",
        "cad_contas",
        "rel_contas_hierarquias",
        "cad_lancamentos",
    }


def test_arrow_schema_maps_types_from_contract():
    schema = arrow_schema_for(Lancamento)
    assert schema.field("id_lancamento").type == pa.int64()
    assert schema.field("data").type == pa.date32()
    assert schema.field("valor").type == pa.decimal128(12, 2)  # 2 casas: padrão do projeto
    assert schema.field("timestamp").type == pa.timestamp("us")
    assert schema.field("meta").type == pa.string()
    assert schema.field("meta").nullable
    assert not schema.field("valor").nullable


def test_arrow_schema_carries_comments_as_field_metadata():
    schema = arrow_schema_for(Lancamento)
    descricao = schema.field("valor").metadata[b"description"].decode()
    assert "2 casas decimais" in descricao


def test_redshift_ddl_projects_types_and_comments():
    ddl = redshift_ddl_for(Lancamento)
    assert "CREATE TABLE IF NOT EXISTS analytics.cad_lancamentos" in ddl
    assert "valor DECIMAL(12,2) NOT NULL" in ddl
    assert "meta VARCHAR(256)" in ddl
    assert "COMMENT ON COLUMN analytics.cad_lancamentos.valor IS" in ddl
    assert "COMMENT ON TABLE analytics.cad_lancamentos IS" in ddl


def test_redshift_ddl_uses_declared_varchar_lengths():
    ddl = redshift_ddl_for(Conta)
    assert "nome VARCHAR(128)" in ddl
    assert "numero VARCHAR(32)" in ddl
