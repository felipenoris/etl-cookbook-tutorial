"""Testes da API relacional do exemplo 26 (`con.read_parquet` e `DuckDBPyRelation`).

Valida os helpers que o exemplo define (`montar_consulta`, `resumo_do_plano`) e
os contratos do DuckDB em que ele se apoia: a relation é lazy, é equivalente à
string SQL, e o cast implícito numa coluna de partição custa o pruning.

Quase todos os testes escrevem parquet minúsculos em `tmp_path` — só os que
exercitam `montar_consulta` (que fixa `ORDERS_GLOB`) tocam em `data/raw`, e
mesmo assim sem executar a consulta.
"""

import importlib

import duckdb
import pytest

exemplo = importlib.import_module("26_relational_api_read_parquet")


@pytest.fixture
def con():
    return duckdb.connect()


@pytest.fixture
def particionado(con, tmp_path):
    """Escreve 3 partições hive `mes=01|02|03` com 10 linhas cada, e devolve o glob."""
    for mes in ("01", "02", "03"):
        destino = tmp_path / f"mes={mes}"
        destino.mkdir()
        con.execute(
            f"COPY (SELECT i AS id, i % 4 AS q FROM range(10) t(i)) "
            f"TO '{destino / 'parte.parquet'}' (FORMAT parquet)"
        )
    return str(tmp_path / "**" / "*.parquet")


# --- a relation é uma query, não um resultado -------------------------------


def test_read_parquet_devolve_uma_relation(con, particionado):
    rel = con.read_parquet(particionado, hive_partitioning=True)
    assert isinstance(rel, duckdb.DuckDBPyRelation)


def test_o_schema_ja_e_conhecido_sem_executar(con, particionado):
    """Colunas e tipos vêm do footer; a coluna de partição vira VARCHAR."""
    rel = con.read_parquet(particionado, hive_partitioning=True)
    tipos = {nome: str(tipo) for nome, tipo in zip(rel.columns, rel.types)}
    assert tipos == {"id": "BIGINT", "q": "BIGINT", "mes": "VARCHAR"}


def test_sql_query_devolve_o_select_equivalente(con, particionado):
    sql = con.read_parquet(particionado, hive_partitioning=True).limit(3).sql_query()
    assert sql.upper().startswith("SELECT")
    assert "LIMIT 3" in sql.upper()


def test_encadear_nao_muta_a_relation_original(con, particionado):
    rel = con.read_parquet(particionado, hive_partitioning=True)
    filtrada = rel.filter("mes = '01'")
    assert rel.aggregate("count(*)").fetchone() == (30,)
    assert filtrada.aggregate("count(*)").fetchone() == (10,)


def test_relation_nao_tem_cache(con, particionado, tmp_path):
    """Consumir duas vezes reexecuta: o arquivo novo entre as duas leituras aparece."""
    rel = con.read_parquet(particionado, hive_partitioning=True).aggregate("count(*)")
    assert rel.fetchall() == [(30,)]
    nova = tmp_path / "mes=04"
    nova.mkdir()
    con.execute(f"COPY (SELECT 99 AS id, 0 AS q) TO '{nova / 'parte.parquet'}' (FORMAT parquet)")
    assert rel.fetchall() == [(31,)]


# --- equivalência com a string SQL -----------------------------------------


def test_relation_e_sql_dao_o_mesmo_resultado(con, particionado):
    via_relation = (
        con.read_parquet(particionado, hive_partitioning=True)
        .filter("q > 1")
        .aggregate("mes, count(*) AS n", "mes")
        .order("mes")
        .fetchall()
    )
    via_sql = con.sql(
        f"""
        SELECT mes, count(*) AS n
        FROM read_parquet('{particionado}', hive_partitioning=true)
        WHERE q > 1 GROUP BY mes ORDER BY mes
        """
    ).fetchall()
    assert via_relation == via_sql


def test_o_filtro_de_coluna_normal_desce_ate_o_scan(con, particionado):
    """Nos dois caminhos o predicado vira `Filters:` dentro do leitor de parquet."""
    plano_rel = exemplo.resumo_do_plano(
        con.read_parquet(particionado, hive_partitioning=True).filter("q > 1").explain()
    )
    plano_sql = exemplo.resumo_do_plano(
        con.sql(
            f"SELECT * FROM read_parquet('{particionado}', hive_partitioning=true) WHERE q > 1"
        ).explain()
    )
    for plano in (plano_rel, plano_sql):
        assert any(linha.startswith("Filters:") for linha in plano)
        assert "FILTER" not in plano


def test_query_reentra_no_sql_pelo_nome(con, particionado):
    rel = con.read_parquet(particionado, hive_partitioning=True)
    assert rel.query("o", "SELECT count(DISTINCT mes) FROM o").fetchone() == (3,)


# --- a pegadinha do cast na coluna de partição ------------------------------


def test_literal_no_tipo_nativo_preserva_o_pruning(con, particionado):
    plano = con.read_parquet(particionado, hive_partitioning=True).filter("mes = '01'").explain()
    assert "Scanning Files: 1/3" in plano
    assert "FILTER" not in exemplo.resumo_do_plano(plano)


def test_cast_implicito_custa_o_pruning(con, particionado):
    """`mes = 1` vira CAST(mes AS INTEGER) = 1 — o descarte por arquivo não avalia isso."""
    plano = con.read_parquet(particionado, hive_partitioning=True).filter("mes = 1").explain()
    assert "Scanning Files" not in plano
    assert "FILTER" in exemplo.resumo_do_plano(plano)


def test_as_duas_formas_dao_o_mesmo_resultado(con, particionado):
    """A pegadinha é de custo, não de correção: o número final é idêntico."""
    rel = con.read_parquet(particionado, hive_partitioning=True)
    assert rel.filter("mes = 1").aggregate("count(*)").fetchone() == (10,)
    assert rel.filter("mes = '01'").aggregate("count(*)").fetchone() == (10,)


# --- a relation pertence à conexão que a criou ------------------------------


def test_relation_nao_atravessa_conexoes(con, particionado):
    rel = con.read_parquet(particionado)  # noqa: F841 — o replacement scan a encontra
    outra = duckdb.connect()
    with pytest.raises(duckdb.Error, match="another Connection"):
        outra.sql("SELECT count(*) FROM rel").fetchone()
    outra.close()


# --- os helpers do exemplo --------------------------------------------------


def test_resumo_do_plano_descarta_as_bordas():
    plano = "┌──────────┐\n│  FILTER  │\n│  ──────  │\n└──────────┘"
    assert exemplo.resumo_do_plano(plano) == ["FILTER"]


def test_montar_consulta_e_lazy_e_devolve_relation(con):
    rel = exemplo.montar_consulta(con, mes=1, status="entregue")
    assert isinstance(rel, duckdb.DuckDBPyRelation)
    assert rel.columns == ["status", "pedidos", "itens"]


def test_montar_consulta_formata_o_mes_como_partição(con):
    """O `:02d` é o que mantém o pruning vivo — sem ele voltaria o cast."""
    assert "order_month = '01'" in exemplo.montar_consulta(con, mes=1).sql_query()


def test_montar_consulta_sem_filtros_nao_gera_where(con):
    assert "WHERE" not in exemplo.montar_consulta(con).sql_query().upper()


def test_montar_consulta_escapa_aspas_no_status(con):
    sql = exemplo.montar_consulta(con, status="o'brien").sql_query()
    assert "'o''brien'" in sql
