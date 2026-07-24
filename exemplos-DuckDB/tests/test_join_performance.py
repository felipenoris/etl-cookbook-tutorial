"""Testes das afirmações do exemplo 16 sobre performance de JOIN.

Valida os FATOS determinísticos (plano e linhas lidas), não os tempos:
DuckDB usa hash join, o índice não muda o plano, e só o filtro que alcança o
fato (com o fato ordenado) poda o scan via zonemaps.
"""

import importlib

import duckdb
import pytest

exemplo = importlib.import_module("16_join_performance")


@pytest.fixture
def con():
    connection = duckdb.connect()
    connection.execute("CREATE TABLE dim AS SELECT i AS id, 'n' || i AS nome FROM range(50000) t(i)")
    connection.execute(
        "CREATE TABLE fato AS SELECT i AS pid, (hash(i) % 50000) AS cid FROM range(1000000) t(i)"
    )
    yield connection
    connection.close()


QUERY_JOIN = "SELECT f.pid, d.nome FROM fato f JOIN dim d ON f.cid = d.id"


def test_join_usa_hash_join(con):
    assert exemplo.operador_de_join(con, QUERY_JOIN) == "HASH_JOIN"


def test_indice_nao_muda_o_operador_de_join(con):
    antes = exemplo.operador_de_join(con, QUERY_JOIN)
    con.execute("CREATE INDEX ix ON fato(cid)")
    con.execute("CREATE INDEX ixd ON dim(id)")
    depois = exemplo.operador_de_join(con, QUERY_JOIN)
    assert antes == depois == "HASH_JOIN"  # nunca vira INDEX_JOIN


def test_filtro_so_na_dimensao_nao_reduz_o_scan_do_fato(con, tmp_path):
    # camada 1 (pushdown): filtro só na dimensão NÃO vira filtro do fato
    fato_pq = tmp_path / "fato.parquet"
    con.execute(f"COPY (SELECT * FROM fato ORDER BY cid) TO '{fato_pq}' (FORMAT parquet, ROW_GROUP_SIZE 122880)")
    q = f"""
        SELECT f.pid, d.nome FROM read_parquet('{fato_pq}') f JOIN dim d ON f.cid = d.id
        WHERE d.id BETWEEN 1000 AND 1099
    """
    assert exemplo.linhas_do_fato_no_scan(con, q) == 1_000_000  # scan entrega tudo


def test_predicado_no_fato_ativa_o_pushdown(con, tmp_path):
    # camada 1: replicar o predicado na chave do fato corta as linhas no scan
    fato_pq = tmp_path / "fato.parquet"
    con.execute(f"COPY (SELECT * FROM fato ORDER BY cid) TO '{fato_pq}' (FORMAT parquet, ROW_GROUP_SIZE 122880)")
    q = f"""
        SELECT f.pid, d.nome FROM read_parquet('{fato_pq}') f JOIN dim d ON f.cid = d.id
        WHERE d.id BETWEEN 1000 AND 1099 AND f.cid BETWEEN 1000 AND 1099
    """
    entregues = exemplo.linhas_do_fato_no_scan(con, q)
    assert 0 < entregues < 1_000_000  # o pushdown reduziu bem


def test_pushdown_independe_do_layout(con, tmp_path):
    # o pushdown (linhas entregues) é IGUAL com ou sem clustering — o layout
    # afeta o I/O (tempo), não a contagem de linhas que sai do scan
    q_tpl = """
        SELECT f.pid, d.nome FROM read_parquet('{p}') f JOIN dim d ON f.cid = d.id
        WHERE d.id BETWEEN 1000 AND 1099 AND f.cid BETWEEN 1000 AND 1099
    """
    ord_pq = tmp_path / "ord.parquet"
    rand_pq = tmp_path / "rand.parquet"
    con.execute(f"COPY (SELECT * FROM fato ORDER BY cid) TO '{ord_pq}' (FORMAT parquet, ROW_GROUP_SIZE 122880)")
    con.execute(f"COPY (SELECT * FROM fato) TO '{rand_pq}' (FORMAT parquet, ROW_GROUP_SIZE 122880)")
    assert exemplo.linhas_do_fato_no_scan(con, q_tpl.format(p=ord_pq)) == exemplo.linhas_do_fato_no_scan(
        con, q_tpl.format(p=rand_pq)
    )
