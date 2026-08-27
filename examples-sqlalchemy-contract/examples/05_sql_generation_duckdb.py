"""Exemplo 5 — SQLAlchemy como gerador de SQL para uma base DuckDB.

Os exemplos anteriores estabeleceram dois papéis do SQLAlchemy na stack
colunar: contrato de schema (01) e fora do caminho de dados (02/04). Este
exemplo exercita o papel que fica no meio: **gerador de statements SQL**.
Nenhuma instância de ``Veiculo``/``Conta``/``Lancamento`` é criada — o modelo
declarativo trabalha como metadado executável, e todo SQL que toca a base sai
compilado dele (o script imprime cada statement gerado). O dado, como sempre,
viaja colunar: DataFrames pandas com backend pyarrow na ida, RecordBatches
Arrow na volta.

Os três passos, partindo de uma base DuckDB VAZIA:

1. **DDL do contrato** — ``CreateTable``/``CreateIndex``/``SetTableComment``/
   ``SetColumnComment`` compilados de ``Base.metadata`` (na ordem das FKs, via
   ``sorted_tables``). O DuckDB entende o dialeto genérico do SQLAlchemy
   neste subconjunto — ``VARCHAR(n)``, ``NUMERIC(12,2)``, ``DATE``,
   ``DATETIME`` (apelido de ``TIMESTAMP``), FKs e ``COMMENT ON`` — e os
   ``comment=`` do contrato chegam ao catálogo
   (``duckdb_tables()``/``duckdb_columns()``). O dialeto genérico é
   deliberado: o do Postgres, apesar de próximo do DuckDB, renderizaria
   ``SERIAL`` para as PKs inteiras, que o DuckDB rejeita. Uma divergência a
   conhecer: ``Mapped[int]`` compila como ``INTEGER`` (32 bits) no dialeto
   genérico, enquanto as projeções Arrow e Redshift do mesmo contrato dizem
   ``int64``/``BIGINT`` (ver ``models.py``) — para os ids 1..n deste exemplo
   é indiferente, mas num modelo de produção declare ``BigInteger`` para
   alinhar as três projeções.
2. **Carga batch, set-based** — cada DataFrame (backend pyarrow) é registrado
   como view na conexão e entra com um único ``INSERT ... FROM SELECT``
   (``insert().from_select()``). Nada de INSERT linha a linha, nada de lista
   de instâncias: o SQLAlchemy gera o comando; quem move os dados é o DuckDB,
   lendo o DataFrame registrado direto (integração pandas/Arrow do motor).
3. **SELECT com joins + streaming** — a consulta junta ``Lancamento`` com
   ``Veiculo`` e ``Conta`` sem escrever os ``ON``: o SQLAlchemy os infere das
   ``ForeignKey`` declaradas no modelo. O resultado volta como
   ``RecordBatchReader`` (``to_arrow_reader``, leitor preguiçoso e de passada
   única), processado um RecordBatch por vez — cada batch vira um DataFrame
   pandas, é agregado (somatório dos lançamentos por veículo) e descartado.
   O pico de memória é UM batch, não o resultado do join; os agregados
   parciais se consolidam ao final porque SUM é decomponível (o padrão
   map-reduce). A leitura para no décimo RecordBatch — o restante do
   resultado nunca chega a ser materializado.

Um detalhe de fronteira: o SQLAlchemy compila parâmetros como ``:data_1``
(paramstyle ``named``), que o cliente Python do DuckDB não aceita (ele fala
``?`` e ``$nome``). O exemplo imprime as duas formas do SELECT e executa a
compilada com ``literal_binds=True`` — adequada para SQL estático como aqui;
para binds de verdade, compile com um paramstyle compatível ou use o dialeto
`duckdb_engine <https://github.com/Mause/duckdb_engine>`_, que dá
engine/Session completos sobre o DuckDB. Ele é dispensado de propósito neste
exemplo: um result set do SQLAlchemy voltaria linha a linha — exatamente o
custo que os exemplos 02 e 04 mediram. Gerar o SQL com o SQLAlchemy e
executá-lo na conexão nativa preserva o caminho colunar de ponta a ponta.

Rode com: `uv run examples/05_sql_generation_duckdb.py [n_lancamentos]`
(default 200000 — o suficiente para o corte de 10 RecordBatches atuar)
"""

from __future__ import annotations

import sys
from datetime import date
from decimal import Decimal

import duckdb
import pandas as pd
import pyarrow as pa
from sqlalchemy import column, func, insert, select, table
from sqlalchemy.schema import CreateIndex, CreateTable, SetColumnComment, SetTableComment

from _common import gerar_lancamentos, section
from models import Base, Conta, Lancamento, Veiculo

# 5 nomes porque gerar_lancamentos (em _common.py) sorteia id_veiculo em 1..5
VEICULOS = ["TV Aberta", "Rádio FM", "Jornal Impresso", "Portal Web", "Mídia Exterior"]
CONTAS_FOLHA = list(range(1, 51))
DATA_CORTE = date(2025, 7, 1)  # só o 1º semestre: um predicado com bind de verdade
LINHAS_POR_BATCH = 8_192
MAX_BATCHES = 10

# Cada statement compilado passa por executa() e fica registrado aqui — é a
# auditoria de que TODO o SQL da sessão saiu do modelo, nenhum escrito à mão.
SQL_GERADO: list[str] = []


def sql_de(stmt, *, literal: bool = False) -> str:
    """Compila um construto SQLAlchemy (DDL ou DML) para a string SQL.

    Sem engine associada, ``compile()`` usa o dialeto genérico — que é o que
    queremos: SQL neutro de dialeto, aceito pelo DuckDB neste subconjunto
    (tipos, FKs, índices, ``COMMENT ON``). ``literal=True``
    embute os binds como literais (``literal_binds``), necessário porque o
    paramstyle ``named`` (``:data_1``) não é aceito pelo cliente do DuckDB.
    """
    if literal:
        return str(stmt.compile(compile_kwargs={"literal_binds": True})).strip()
    return str(stmt.compile()).strip()


def executa(con: duckdb.DuckDBPyConnection, sql: str) -> duckdb.DuckDBPyConnection:
    """Imprime o statement gerado, registra na auditoria e o executa no DuckDB."""
    SQL_GERADO.append(sql)
    print(f"{sql};")
    return con.execute(sql)


def ddl_do_contrato(con: duckdb.DuckDBPyConnection) -> None:
    """Emite todo o DDL do contrato na base: tabelas, índices e comentários.

    ``sorted_tables`` devolve as tabelas em ordem de dependência, então cada
    ``FOREIGN KEY`` referencia uma tabela que já existe. Os ``comment=`` do
    modelo viram ``COMMENT ON`` (o DuckDB os aceita desde a versão 0.10) —
    é o mesmo papel do ``redshift_ddl_for`` do exemplo 01, só que compilado
    pelo próprio SQLAlchemy em vez de montado à mão.
    """
    for tabela_fisica in Base.metadata.sorted_tables:
        print()
        executa(con, sql_de(CreateTable(tabela_fisica)))
        # .indexes é um set — ordena por nome para uma saída determinística
        for indice in sorted(tabela_fisica.indexes, key=lambda i: i.name):
            executa(con, sql_de(CreateIndex(indice)))
        if tabela_fisica.comment:
            executa(con, sql_de(SetTableComment(tabela_fisica)))
        for col in tabela_fisica.columns:
            if col.comment:
                executa(con, sql_de(SetColumnComment(col)))


def montar_dimensoes() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Gera as dimensões como DataFrames pandas com backend pyarrow."""
    df_veiculos = pd.DataFrame(
        {"id_veiculo": range(1, len(VEICULOS) + 1), "nome": VEICULOS}
    ).convert_dtypes(dtype_backend="pyarrow")
    numeros = [f"1.2.{i:02d}" for i in CONTAS_FOLHA]
    df_contas = pd.DataFrame(
        {
            "id_conta": CONTAS_FOLHA,
            "nome": [f"Conta analítica {n}" for n in numeros],
            "numero": numeros,
            "permite_lancamentos": [True] * len(CONTAS_FOLHA),
        }
    ).convert_dtypes(dtype_backend="pyarrow")
    return df_veiculos, df_contas


def carga_batch(
    con: duckdb.DuckDBPyConnection, model: type[Base], nome_view: str, df: pd.DataFrame
) -> None:
    """Carrega o DataFrame inteiro na tabela do modelo com UM statement.

    O DataFrame é registrado como view na conexão (``register`` é um
    ``CREATE TEMP VIEW`` sobre o objeto Python); do lado do SQLAlchemy, a
    view aparece como um ``table()`` *lite* — só nome e colunas, sem tipo,
    o suficiente para compilar o ``INSERT ... FROM SELECT``. É a carga
    set-based: uma travessia Python->banco por TABELA, não por linha.
    """
    colunas = [c.name for c in model.__table__.columns]
    # validação de contrato: o batch produzido bate coluna a coluna com o modelo
    assert list(df.columns) == colunas, f"batch não bate com o contrato de {model.__tablename__}"
    con.register(nome_view, df)
    origem = table(nome_view, *[column(c) for c in colunas])
    stmt = insert(model).from_select(colunas, select(*origem.columns))
    executa(con, sql_de(stmt))


def consulta_join():
    """Monta o SELECT do fato com as dimensões — sem escrever nenhum ON.

    ``join_from(Lancamento, Veiculo)`` e ``join(Conta)`` não dizem POR QUAL
    coluna juntar: o SQLAlchemy resolve os ``ON`` a partir das ``ForeignKey``
    declaradas no contrato. Se a FK mudar no modelo, o join gerado acompanha.
    """
    return (
        select(
            Veiculo.nome.label("veiculo"),
            Conta.numero.label("conta"),
            Lancamento.data,
            Lancamento.valor,
        )
        .join_from(Lancamento, Veiculo)
        .join(Conta)
        .where(Lancamento.data < DATA_CORTE)
    )


def agrega_batch(batch: pa.RecordBatch) -> pd.Series:
    """Converte UM RecordBatch em DataFrame pandas e soma os valores por veículo.

    O ``types_mapper=pd.ArrowDtype`` mantém o backend pyarrow na conversão:
    ``valor`` segue ``decimal128(12, 2)`` — e o ``sum`` do groupby preserva o
    decimal (nenhum float no caminho, padrão do projeto).
    """
    df = batch.to_pandas(types_mapper=pd.ArrowDtype)
    return df.groupby("veiculo")["valor"].sum()


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 200_000
    con = duckdb.connect()  # base em memória, vazia

    section("Passo 1 — DDL: do contrato para a base vazia")
    ddl_do_contrato(con)

    section("Prova: os comment= do contrato chegaram ao catálogo do DuckDB")
    catalogo = con.sql(
        "SELECT table_name, comment FROM duckdb_tables() ORDER BY table_name"
    ).fetchall()
    for nome_tabela, comentario in catalogo:
        print(f"  {nome_tabela:<24} -- {comentario}")
    (comentario_valor,) = con.sql(
        "SELECT comment FROM duckdb_columns() "
        "WHERE table_name = 'cad_lancamentos' AND column_name = 'valor'"
    ).fetchone()
    print(f"  cad_lancamentos.valor    -- {comentario_valor}")

    section("Passo 2 — carga batch: DataFrames (backend pyarrow) -> INSERT ... FROM SELECT")
    df_veiculos, df_contas = montar_dimensoes()
    df_lancamentos = gerar_lancamentos(n, CONTAS_FOLHA).to_pandas(types_mapper=pd.ArrowDtype)
    print(f"dtypes do fato ({n:,} linhas):")
    print(df_lancamentos.dtypes.to_string())
    print()
    # dimensões antes do fato: as FKs de cad_lancamentos são verificadas no INSERT
    carga_batch(con, Veiculo, "df_veiculos", df_veiculos)
    carga_batch(con, Conta, "df_contas", df_contas)
    carga_batch(con, Lancamento, "df_lancamentos", df_lancamentos)
    print("\n(dom_hierarquias_contas e rel_contas_hierarquias ficam criadas porém")
    print("vazias — a árvore de contas é o assunto do exemplo 03)")

    section("Passo 3 — SELECT com joins inferidos das FKs do modelo")
    stmt = consulta_join()
    print("como o SQLAlchemy compila (paramstyle named, que o DuckDB não fala):\n")
    print(f"{sql_de(stmt)};")
    print("\nforma executável (literal_binds) — o total, para conferência no final:\n")
    stmt_total = select(func.count()).select_from(stmt.subquery())
    total_linhas = executa(con, sql_de(stmt_total, literal=True)).fetchone()[0]
    print(f"\n-> {total_linhas:,} linhas no resultado do join")

    section(f"Streaming: RecordBatches de até {LINHAS_POR_BATCH:,} linhas, máximo de {MAX_BATCHES}")
    print("a query que o reader vai varrer:\n")
    reader = executa(con, sql_de(stmt, literal=True)).to_arrow_reader(LINHAS_POR_BATCH)
    totais: dict[str, Decimal] = {}
    linhas_processadas = 0
    numero_batch = 0
    for numero_batch, batch in enumerate(reader, start=1):
        parcial = agrega_batch(batch)
        linhas_processadas += batch.num_rows
        print(f"\nRecordBatch {numero_batch:2d} — {batch.num_rows:,} linhas; somatório por veículo:")
        for veiculo, soma in parcial.items():
            totais[veiculo] = totais.get(veiculo, Decimal("0")) + soma
            print(f"  {veiculo:<16} R$ {soma:>14,.2f}")
        if numero_batch == MAX_BATCHES:
            break  # o reader é preguiçoso: os batches restantes nunca são produzidos

    section("Consolidação dos parciais (SUM é decomponível: map-reduce)")
    if linhas_processadas < total_linhas:
        print(
            f"leitura interrompida no RecordBatch {numero_batch}: {linhas_processadas:,} de "
            f"{total_linhas:,} linhas processadas — o restante nunca foi materializado\n"
        )
    for veiculo in sorted(totais):
        print(f"  {veiculo:<16} R$ {totais[veiculo]:>15,.2f}")

    section("O ponto")
    print(f"O SQLAlchemy gerou e executou {len(SQL_GERADO)} statements — o DDL comentado,")
    print("as cargas set-based e o join — sem UMA instância ORM sequer. O modelo foi")
    print("gerador de SQL; o dado viajou colunar (DataFrame na ida, RecordBatch na")
    print("volta), com pico de memória de leitura limitado a um único batch.")
