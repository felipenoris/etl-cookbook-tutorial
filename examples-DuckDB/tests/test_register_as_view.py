"""Testes do `con.register` do exemplo 27: a tese "registrar é criar uma VIEW".

Cada teste ataca um aspecto da equivalência (catálogo, plano, releitura da
fonte) ou uma das três diferenças reais (temporária, sem texto SQL, escopo de
conexão). Depois, os contratos de registrar objetos Python: DataFrame/Table
como tabela, `unregister`, substituição do nome e o snapshot lógico que o
copy-on-write do pandas 3 produz.

Todos os parquet usados são minúsculos e escritos em `tmp_path` — a suíte não
toca em `data/raw`.
"""

import importlib

import duckdb
import pandas as pd
import pyarrow as pa
import pytest

exemplo = importlib.import_module("27_register_relations_and_dataframes")


@pytest.fixture
def con():
    return duckdb.connect()


@pytest.fixture
def particionado(con, tmp_path):
    """Duas partições hive `mes=01|02`, 10 linhas cada; devolve o glob."""
    for mes in ("01", "02"):
        destino = tmp_path / f"mes={mes}"
        destino.mkdir()
        con.execute(
            f"COPY (SELECT i AS id FROM range(10) t(i)) "
            f"TO '{destino / 'parte.parquet'}' (FORMAT parquet)"
        )
    return str(tmp_path / "**" / "*.parquet")


@pytest.fixture
def dois_caminhos(con, particionado):
    """Cria `reg` (via register) e `vw` (via DDL) sobre o MESMO parquet."""
    con.register("reg", con.read_parquet(particionado, hive_partitioning=True))
    con.execute(
        f"CREATE VIEW vw AS SELECT * FROM read_parquet('{particionado}', hive_partitioning=true)"
    )
    return con


# --- a equivalência: registrar é criar uma view -----------------------------


def test_o_nome_registrado_aparece_em_duckdb_views(dois_caminhos):
    nomes = {linha[0] for linha in dois_caminhos.sql("SELECT view_name FROM duckdb_views").fetchall()}
    assert {"reg", "vw"} <= nomes


def test_information_schema_classifica_os_dois_como_view(dois_caminhos):
    tipos = dict(
        dois_caminhos.sql(
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_name IN ('reg', 'vw')"
        ).fetchall()
    )
    assert tipos == {"reg": "VIEW", "vw": "VIEW"}


def test_o_nome_registrado_nao_e_tabela(dois_caminhos):
    tabelas = {
        linha[0] for linha in dois_caminhos.sql("SELECT table_name FROM duckdb_tables").fetchall()
    }
    assert "reg" not in tabelas


def test_os_dois_dao_o_mesmo_resultado(dois_caminhos):
    reg = dois_caminhos.sql("SELECT mes, count(*) FROM reg GROUP BY mes ORDER BY mes").fetchall()
    vw = dois_caminhos.sql("SELECT mes, count(*) FROM vw GROUP BY mes ORDER BY mes").fetchall()
    assert reg == vw == [("01", 10), ("02", 10)]


@pytest.mark.parametrize("fonte", ["reg", "vw"])
def test_os_dois_prunam_igual_com_literal_do_tipo_nativo(dois_caminhos, fonte):
    arquivos, tem_filter = exemplo.diagnostico_do_plano(
        dois_caminhos, f"SELECT id FROM {fonte} WHERE mes = '01'"
    )
    assert arquivos == "Scanning Files: 1/2"
    assert not tem_filter


@pytest.mark.parametrize("fonte", ["reg", "vw"])
def test_os_dois_perdem_o_pruning_igual_com_cast(dois_caminhos, fonte):
    """A patologia do exemplo 26 atinge register e CREATE VIEW do mesmo jeito."""
    arquivos, tem_filter = exemplo.diagnostico_do_plano(
        dois_caminhos, f"SELECT id FROM {fonte} WHERE mes = 1"
    )
    assert arquivos == "todos os arquivos"
    assert tem_filter


def test_nenhum_dos_dois_materializa(dois_caminhos, tmp_path):
    """Arquivo novo no diretório aparece nas duas — ao contrário do CTAS."""
    dois_caminhos.execute("CREATE TABLE tab AS SELECT * FROM reg")
    nova = tmp_path / "mes=03"
    nova.mkdir()
    dois_caminhos.execute(f"COPY (SELECT 99 AS id) TO '{nova / 'p.parquet'}' (FORMAT parquet)")

    def contar(nome: str) -> int:
        return dois_caminhos.sql(f"SELECT count(*) FROM {nome}").fetchone()[0]

    assert contar("reg") == contar("vw") == 21
    assert contar("tab") == 20


# --- as três diferenças reais ----------------------------------------------


def test_registrado_e_temporario_a_view_nao(dois_caminhos):
    temporario = dict(
        dois_caminhos.sql(
            "SELECT view_name, temporary FROM duckdb_views WHERE view_name IN ('reg', 'vw')"
        ).fetchall()
    )
    assert temporario == {"reg": True, "vw": False}


def test_registrado_nao_guarda_texto_sql(dois_caminhos):
    sqls = dict(
        dois_caminhos.sql(
            "SELECT view_name, sql FROM duckdb_views WHERE view_name IN ('reg', 'vw')"
        ).fetchall()
    )
    assert sqls["reg"] == ""
    assert sqls["vw"].upper().startswith("CREATE VIEW")


def test_export_database_ignora_o_registrado(dois_caminhos, tmp_path):
    destino = tmp_path / "dump"
    dois_caminhos.execute(f"EXPORT DATABASE '{destino}' (FORMAT parquet)")
    schema = destino.joinpath("schema.sql").read_text()
    assert "CREATE VIEW vw" in schema
    assert "reg" not in schema


def test_registrado_nao_atravessa_conexoes(dois_caminhos):
    outra = duckdb.connect()
    with pytest.raises(duckdb.CatalogException):
        outra.sql("SELECT count(*) FROM reg").fetchone()
    outra.close()


# --- registrar objetos Python ----------------------------------------------


def test_dataframe_registrado_vira_tabela_tipada(con):
    con.register("metas", pd.DataFrame({"regiao": ["sul"], "meta": [10]}))
    tipos = {nome: tipo for nome, tipo, *_ in con.sql("DESCRIBE metas").fetchall()}
    assert tipos == {"regiao": "VARCHAR", "meta": "BIGINT"}


def test_dataframe_registrado_faz_join_com_parquet(con, particionado):
    con.register("rotulos", pd.DataFrame({"mes": ["01"], "nome": ["janeiro"]}))
    assert con.sql(
        f"""
        SELECT r.nome, count(*)
        FROM read_parquet('{particionado}', hive_partitioning=true) o
        JOIN rotulos r USING (mes)
        GROUP BY r.nome
        """
    ).fetchall() == [("janeiro", 10)]


def test_pyarrow_table_registrada(con):
    con.register("t", pa.table({"k": [1, 2, 3]}))
    assert con.sql("SELECT sum(k) FROM t").fetchone() == (6,)


def test_registrar_o_mesmo_nome_substitui(con):
    con.register("x", pd.DataFrame({"v": [1]}))
    con.register("x", pd.DataFrame({"v": [10, 20]}))
    assert con.sql("SELECT sum(v) FROM x").fetchone() == (30,)


def test_unregister_remove_do_catalogo(con):
    con.register("some_aqui", pd.DataFrame({"v": [1]}))
    con.unregister("some_aqui")
    with pytest.raises(duckdb.CatalogException):
        con.sql("SELECT * FROM some_aqui").fetchone()


def test_copy_on_write_torna_o_registro_um_snapshot_logico(con):
    """pandas >= 3.0 sempre copia na escrita — o buffer que o DuckDB referencia fica."""
    df = pd.DataFrame({"v": [1, 2, 3]})
    con.register("numeros", df)
    df.iloc[0, 0] = 100
    assert df["v"].tolist() == [100, 2, 3]
    assert con.sql("SELECT sum(v) FROM numeros").fetchone() == (6,)
    con.register("numeros", df)
    assert con.sql("SELECT sum(v) FROM numeros").fetchone() == (105,)


def test_register_sobrevive_ao_escopo_da_funcao(con):
    def publica(conexao):
        local = pd.DataFrame({"v": [7]})
        conexao.register("publicado", local)

    publica(con)
    assert con.sql("SELECT sum(v) FROM publicado").fetchone() == (7,)


def test_replacement_scan_nao_sobrevive_ao_escopo_da_funcao(con):
    def consulta_local(conexao):
        so_local = pd.DataFrame({"v": [7]})  # noqa: F841
        return conexao.sql("SELECT sum(v) FROM so_local").fetchone()

    assert consulta_local(con) == (7,)  # dentro da função, o frame ainda existe
    with pytest.raises(duckdb.CatalogException):
        con.sql("SELECT * FROM so_local").fetchone()


# --- o helper do exemplo ----------------------------------------------------


def test_diagnostico_do_plano_reporta_ausencia_de_pruning(con, particionado):
    con.register("r", con.read_parquet(particionado, hive_partitioning=True))
    assert exemplo.diagnostico_do_plano(con, "SELECT id FROM r") == ("todos os arquivos", False)


def test_menor_tempo_devolve_milissegundos():
    assert exemplo.menor_tempo(lambda: None, repeticoes=2) < 100
