"""Exemplo 1 — Modelos SQLAlchemy como contrato: um schema, três projeções.

O papel que o SQLAlchemy cumpre BEM na stack Arrow/parquet/DuckDB não é mover
dados — é ser a **definição executável do schema**, com os metadados
semânticos (``comment=``) morando junto do código e projetados para cada
destino:

1. **banco local** — ``Base.metadata.create_all(engine)``: as tabelas físicas
   para quem precisar de um banco relacional de trabalho (o papel do antigo
   Postgres efêmero, aqui demonstrado com SQLite em memória);
2. **Arrow/parquet** — :func:`models.arrow_schema_for`: o schema com as
   descrições como field metadata, para gravar o parquet final decorado;
3. **Redshift** — :func:`models.redshift_ddl_for`: ``CREATE TABLE`` +
   ``COMMENT ON`` idempotentes, o único caminho pelo qual as descrições
   chegam ao catálogo do Redshift (o COPY de parquet as ignora).

A tese: metadado semântico muda junto com o código do ETL — no mesmo PR, com
o mesmo review. O modelo declarativo é o contrato; os destinos são derivados.

Rode com: `uv run examples/01_models_as_contract.py`
"""

from sqlalchemy import create_engine, inspect

from _common import section
from models import Base, Conta, Lancamento, arrow_schema_for, redshift_ddl_for

if __name__ == "__main__":
    section("Projeção 1: create_all num banco local (o 'Postgres efêmero' do padrão antigo)")
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    inspector = inspect(engine)
    print("tabelas criadas fisicamente:", inspector.get_table_names())

    section("Projeção 2: schema Arrow do fato, com descrições como field metadata")
    schema = arrow_schema_for(Lancamento)
    print(schema)
    print("\nmetadata do campo 'valor':", schema.field("valor").metadata)

    section("Projeção 3: DDL do Redshift (CREATE TABLE + COMMENT ON)")
    print(redshift_ddl_for(Lancamento))

    section("O mesmo para uma dimensão (cad_contas)")
    print(redshift_ddl_for(Conta))

    section("O ponto da migração")
    print("O contrato continua sendo as mesmas classes declarativas de sempre.")
    print("O que muda: o fato NUNCA vira lista de instâncias ORM — ele viaja")
    print("como Arrow/parquet (exemplo 02 mede o porquê), e o banco efêmero")
    print("vira DuckDB in-process (exemplo 03 resolve a árvore de contas lá).")
