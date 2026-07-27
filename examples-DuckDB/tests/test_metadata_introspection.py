"""Testes da introspecção de metadados do exemplo 25 (DuckDB).

Valida as duas funções que o exemplo constrói (`schema_por_arquivo` e
`detectar_drift`) e os contratos do DuckDB em que elas se apoiam: o nó raiz do
schema, a ordem das colunas, e a contagem de linhas vinda do footer.

Os testes escrevem parquet minúsculos em `tmp_path` — não tocam em `data/raw`,
para a suíte continuar rápida.
"""

import importlib

import duckdb
import pytest

exemplo = importlib.import_module("25_metadata_introspection")


@pytest.fixture
def con():
    return duckdb.connect()


def escrever(con, caminho, sql: str) -> str:
    """Materializa `sql` num parquet e devolve o caminho como string."""
    con.execute(f"COPY ({sql}) TO '{caminho}' (FORMAT parquet)")
    return str(caminho)


# --- schema_por_arquivo ----------------------------------------------------


def test_schema_por_arquivo_uma_entrada_por_arquivo(con, tmp_path):
    escrever(con, tmp_path / "a.parquet", "SELECT 1 AS x, 'a' AS y")
    escrever(con, tmp_path / "b.parquet", "SELECT 2 AS x, 'b' AS y")
    schemas = exemplo.schema_por_arquivo(con, str(tmp_path / "*.parquet"))
    assert len(schemas) == 2
    assert all(s == [("x", "INTEGER"), ("y", "VARCHAR")] for s in schemas.values())


def test_schema_por_arquivo_descarta_o_no_raiz(con, tmp_path):
    """`parquet_schema` devolve a raiz da árvore junto com as colunas."""
    arquivo = escrever(con, tmp_path / "a.parquet", "SELECT 1 AS x")
    bruto = con.execute(f"SELECT count(*) FROM parquet_schema('{arquivo}')").fetchone()[0]
    assert bruto == 2  # o nó raiz + a coluna x
    assert exemplo.schema_por_arquivo(con, arquivo)[arquivo] == [("x", "INTEGER")]


def test_schema_por_arquivo_preserva_a_ordem_das_colunas(con, tmp_path):
    """A ordem vem de `column_id`, não da ordem alfabética."""
    arquivo = escrever(con, tmp_path / "a.parquet", "SELECT 1 AS zebra, 2 AS alfa, 3 AS meio")
    assert [c for c, _ in exemplo.schema_por_arquivo(con, arquivo)[arquivo]] == [
        "zebra",
        "alfa",
        "meio",
    ]


# --- detectar_drift --------------------------------------------------------


def test_drift_vazio_quando_todos_batem():
    schemas = {
        "a.parquet": [("x", "INTEGER")],
        "b.parquet": [("x", "INTEGER")],
        "c.parquet": [("x", "INTEGER")],
    }
    assert exemplo.detectar_drift(schemas) == []


def test_drift_sem_arquivo_nenhum():
    assert exemplo.detectar_drift({}) == []


def test_drift_detecta_coluna_a_mais():
    problemas = exemplo.detectar_drift(
        {"01.parquet": [("x", "INTEGER")], "02.parquet": [("x", "INTEGER"), ("novo", "VARCHAR")]}
    )
    assert problemas == ["02.parquet: coluna a mais: novo"]


def test_drift_detecta_coluna_faltando():
    problemas = exemplo.detectar_drift(
        {"01.parquet": [("x", "INTEGER"), ("y", "VARCHAR")], "02.parquet": [("x", "INTEGER")]}
    )
    assert problemas == ["02.parquet: coluna faltando: y"]


def test_drift_detecta_tipo_diferente():
    problemas = exemplo.detectar_drift(
        {"01.parquet": [("x", "INTEGER")], "02.parquet": [("x", "BIGINT")]}
    )
    assert problemas == ["02.parquet: tipo diferente em x: BIGINT (referência: INTEGER)"]


def test_drift_referencia_e_o_primeiro_em_ordem_alfabetica():
    """O 01 é a referência: quem diverge é o 02, mesmo sendo maioria de um só."""
    problemas = exemplo.detectar_drift(
        {"02.parquet": [("x", "BIGINT")], "01.parquet": [("x", "INTEGER")]}
    )
    assert problemas == ["02.parquet: tipo diferente em x: BIGINT (referência: INTEGER)"]


def test_drift_ponta_a_ponta_sobre_arquivos_reais(con, tmp_path):
    escrever(con, tmp_path / "01_ok.parquet", "SELECT 1 AS x, 'a' AS y")
    escrever(con, tmp_path / "02_extra.parquet", "SELECT 1 AS x, 'a' AS y, 2.5 AS z")
    escrever(con, tmp_path / "03_tipo.parquet", "SELECT CAST(1 AS BIGINT) AS x, 'a' AS y")
    problemas = exemplo.detectar_drift(
        exemplo.schema_por_arquivo(con, str(tmp_path / "*.parquet"))
    )
    assert len(problemas) == 2
    assert any("coluna a mais: z" in p for p in problemas)
    assert any("tipo diferente em x" in p for p in problemas)


# --- os contratos do DuckDB em que o exemplo se apoia -----------------------


def test_footer_conta_as_linhas_sem_varrer(con, tmp_path):
    arquivo = escrever(con, tmp_path / "a.parquet", "SELECT * FROM range(1234) t(i)")
    do_footer = con.execute(
        f"SELECT sum(num_rows) FROM parquet_file_metadata('{arquivo}')"
    ).fetchone()[0]
    varrendo = con.execute(f"SELECT count(*) FROM read_parquet('{arquivo}')").fetchone()[0]
    assert do_footer == varrendo == 1234


def test_repetition_type_codifica_a_nullability(con, tmp_path):
    arquivo = escrever(con, tmp_path / "a.parquet", "SELECT 1 AS x")
    repeticao = con.execute(
        f"SELECT repetition_type FROM parquet_schema('{arquivo}') WHERE name = 'x'"
    ).fetchone()[0]
    assert repeticao == "OPTIONAL"  # o DuckDB grava toda coluna como nullable


def test_tipo_fisico_e_tipo_logico_sao_distintos(con, tmp_path):
    """DATE não existe no parquet: é um INT32 com anotação lógica."""
    arquivo = escrever(con, tmp_path / "a.parquet", "SELECT DATE '2025-01-01' AS d")
    fisico, duckdb_type = con.execute(
        f"SELECT type, duckdb_type FROM parquet_schema('{arquivo}') WHERE name = 'd'"
    ).fetchone()
    assert fisico == "INT32"
    assert duckdb_type == "DATE"


def test_union_by_name_reconcilia_o_drift(con, tmp_path):
    escrever(con, tmp_path / "a.parquet", "SELECT 1 AS x, 'a' AS y")
    escrever(con, tmp_path / "b.parquet", "SELECT CAST(1 AS BIGINT) AS x, 2.5 AS z")
    colunas = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{tmp_path / '*.parquet'}', union_by_name=true)"
    ).fetchall()
    tipos = {nome: tipo for nome, tipo, *_ in colunas}
    assert set(tipos) == {"x", "y", "z"}
    assert tipos["x"] == "BIGINT"  # INTEGER + BIGINT promove para o mais largo
