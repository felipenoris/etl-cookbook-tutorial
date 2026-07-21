"""Modelos SQLAlchemy como CONTRATO de schema — não como veículo de dados.

Este módulo porta o modelo de lançamentos em planos de conta (o padrão
tradicional: classes declarativas + banco relacional efêmero) para o papel que o
SQLAlchemy cumpre bem na stack nova: **definição executável do schema**, a
fonte única da verdade dos metadados. As mudanças em relação ao original:

- ``comment=`` em todas as colunas e tabelas — o metadado SEMÂNTICO mora
  aqui, e é projetado para as três pontas (DDL do Redshift via
  ``COMMENT ON``, field metadata do Arrow no parquet, e o próprio banco
  local via ``create_all``);
- ``valor`` deixou de ser ``Double`` e virou ``Numeric(12, 2)`` — lançamento
  financeiro é decimal de 2 casas (padrão do projeto), nunca float
  (``0.10 + 0.20 != 0.30`` em binário; ver `pyarrow/examples/10`);
- ``String`` ganhou comprimentos explícitos — o ``VARCHAR(n)`` do Redshift
  exige, e o parquet não tem onde guardar essa informação.

As funções :func:`arrow_schema_for` e :func:`redshift_ddl_for` são as
projeções do contrato: um modelo, três destinos. O que este módulo NÃO deve
fazer é transportar dados — o fato viaja como Arrow/parquet, nunca como
lista de instâncias ORM (ver o exemplo 02, que mede a diferença).
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
from sqlalchemy import ForeignKey, Numeric, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import sqltypes


class Base(DeclarativeBase):
    pass


class Veiculo(Base):
    __tablename__ = "dom_veiculos"
    __table_args__ = {"comment": "Veículos de comunicação/canal do lançamento"}

    id_veiculo: Mapped[int] = mapped_column(primary_key=True, comment="PK do veículo")
    nome: Mapped[str] = mapped_column(String(64), unique=True, comment="Nome único do veículo")


class HierarquiaContas(Base):
    __tablename__ = "dom_hierarquias_contas"
    __table_args__ = {"comment": "Cabeçalho de cada hierarquia (visão) do plano de contas"}

    id_hierarquia: Mapped[int] = mapped_column(primary_key=True, comment="PK da hierarquia")
    nome: Mapped[str] = mapped_column(String(64), unique=True, comment="Nome único da hierarquia")
    descricao: Mapped[str | None] = mapped_column(
        String(256), comment="Descrição livre da finalidade da hierarquia"
    )


class Conta(Base):
    __tablename__ = "cad_contas"
    __table_args__ = {"comment": "Cadastro de contas do plano de contas"}

    id_conta: Mapped[int] = mapped_column(primary_key=True, comment="PK da conta")
    nome: Mapped[str] = mapped_column(String(128), comment="Nome de exibição da conta")
    numero: Mapped[str] = mapped_column(
        String(32), unique=True, comment="Número contábil único (ex.: 1.2.03)"
    )
    permite_lancamentos: Mapped[bool] = mapped_column(
        default=True, comment="False para contas sintéticas (só agregam filhas)"
    )


class RelacionamentoContaHierarquia(Base):
    __tablename__ = "rel_contas_hierarquias"
    __table_args__ = (
        UniqueConstraint("id_hierarquia", "id_child"),
        {"comment": "Arestas parent->child da árvore de contas, por hierarquia"},
    )

    id_rel_conta_hierarquia: Mapped[int] = mapped_column(primary_key=True, comment="PK da aresta")
    id_hierarquia: Mapped[int] = mapped_column(
        ForeignKey("dom_hierarquias_contas.id_hierarquia"),
        index=True,
        comment="Hierarquia à qual a aresta pertence",
    )
    id_parent: Mapped[int] = mapped_column(
        ForeignKey("cad_contas.id_conta"), index=True, comment="Conta pai (sintética)"
    )
    id_child: Mapped[int] = mapped_column(
        ForeignKey("cad_contas.id_conta"), index=True, comment="Conta filha"
    )


class Lancamento(Base):
    __tablename__ = "cad_lancamentos"
    __table_args__ = {"comment": "Fato: lançamentos contábeis (a tabela de volume)"}

    id_lancamento: Mapped[int] = mapped_column(primary_key=True, comment="PK do lançamento")
    id_veiculo: Mapped[int] = mapped_column(
        ForeignKey("dom_veiculos.id_veiculo"), comment="FK do veículo"
    )
    id_conta: Mapped[int] = mapped_column(
        ForeignKey("cad_contas.id_conta"), comment="FK da conta (folha) lançada"
    )
    data: Mapped[date] = mapped_column(index=True, comment="Data contábil do lançamento")
    valor: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), comment="Valor em BRL, 2 casas decimais (era Double no modelo antigo)"
    )
    meta: Mapped[str | None] = mapped_column(String(256), comment="Anotação livre opcional")
    timestamp: Mapped[datetime] = mapped_column(comment="Instante de ingestão do registro")


# ---------------------------------------------------------------------------
# Projeções do contrato: um modelo, três destinos
# ---------------------------------------------------------------------------

def _arrow_type(column) -> pa.DataType:
    """Mapeia o tipo SQLAlchemy da coluna para o tipo Arrow equivalente."""
    t = column.type
    if isinstance(t, sqltypes.Numeric) and not isinstance(t, sqltypes.Float):
        return pa.decimal128(t.precision or 38, t.scale or 0)
    if isinstance(t, sqltypes.Float):
        return pa.float64()
    if isinstance(t, sqltypes.Boolean):
        return pa.bool_()
    if isinstance(t, sqltypes.Date):
        return pa.date32()
    if isinstance(t, sqltypes.DateTime):
        return pa.timestamp("us")
    if isinstance(t, sqltypes.Integer):
        return pa.int64()
    if isinstance(t, sqltypes.String):
        return pa.string()
    raise TypeError(f"tipo sem mapeamento Arrow: {t!r} (coluna {column.name})")


def arrow_schema_for(model: type[Base]) -> pa.Schema:
    """Projeta o contrato como schema Arrow, com os comments virando field metadata.

    O parquet gravado com este schema carrega as descrições no footer
    (`ARROW:schema`) — a cópia derivada de conveniência para quem lê com
    pyarrow/pandas (a fonte da verdade continua sendo este módulo).
    """
    fields = []
    for column in model.__table__.columns:
        metadata = {"description": column.comment} if column.comment else None
        fields.append(
            pa.field(column.name, _arrow_type(column), nullable=column.nullable, metadata=metadata)
        )
    return pa.schema(fields)


def _redshift_type(column) -> str:
    t = column.type
    if isinstance(t, sqltypes.Numeric) and not isinstance(t, sqltypes.Float):
        return f"DECIMAL({t.precision or 38},{t.scale or 0})"
    if isinstance(t, sqltypes.Float):
        return "DOUBLE PRECISION"
    if isinstance(t, sqltypes.Boolean):
        return "BOOLEAN"
    if isinstance(t, sqltypes.Date):
        return "DATE"
    if isinstance(t, sqltypes.DateTime):
        return "TIMESTAMP"
    if isinstance(t, sqltypes.Integer):
        return "BIGINT"
    if isinstance(t, sqltypes.String):
        return f"VARCHAR({t.length or 256})"
    raise TypeError(f"tipo sem mapeamento Redshift: {t!r} (coluna {column.name})")


def redshift_ddl_for(model: type[Base], schema: str = "analytics") -> str:
    """Projeta o contrato como DDL do Redshift: CREATE TABLE + COMMENT ON.

    Os COMMENT ON são idempotentes — o job de load pode reaplicá-los a cada
    execução, mantendo o catálogo do Redshift espelhando o contrato (é o
    caminho pelo qual as descrições chegam lá: o COPY de parquet as ignora).
    """
    table = model.__table__
    cols = ",\n".join(
        f"    {c.name} {_redshift_type(c)}{'' if c.nullable else ' NOT NULL'}"
        for c in table.columns
    )
    linhas = [f"CREATE TABLE IF NOT EXISTS {schema}.{table.name} (\n{cols}\n);"]
    if table.comment:
        linhas.append(f"COMMENT ON TABLE {schema}.{table.name} IS '{table.comment}';")
    for c in table.columns:
        if c.comment:
            linhas.append(f"COMMENT ON COLUMN {schema}.{table.name}.{c.name} IS '{c.comment}';")
    return "\n".join(linhas)
