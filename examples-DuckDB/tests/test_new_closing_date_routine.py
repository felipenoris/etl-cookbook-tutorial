"""Testes de contrato do exemplo 28 (rotina de nova data-base).

Cada teste prova um comportamento do DuckDB em que a rotina se apoia — ou do
qual ela desvia de propósito: a tipagem automática da coluna de partição, a
sequence que não se reposiciona, o `INSERT ... BY NAME` preenchendo a chave
pelo `DEFAULT`, a sequence fora da transação, a FK que não atravessa catálogos,
e os três modos de escrita do `COPY ... PARTITION_BY` (o `OVERWRITE` apaga as
partições vizinhas). As validações e a montagem do lote vêm do próprio exemplo.

Todos os parquet são minúsculos e escritos em `tmp_path` — a suíte não toca em
`data/raw` nem em `data/rich`.
"""

import datetime as dt
import importlib
from decimal import Decimal

import duckdb
import pandas as pd
import pyarrow as pa
import pytest

exemplo = importlib.import_module("28_new_closing_date_routine")

DDL_STAGING = """
    CREATE TEMP TABLE stage_cad_lancamentos_mes (
        id_lancamento   INTEGER PRIMARY KEY DEFAULT nextval('seq_id_lancamento'),
        data_lancamento DATE NOT NULL,
        id_veiculo      INTEGER NOT NULL,
        valor           DECIMAL(12, 2) NOT NULL,
        data_base_str   VARCHAR NOT NULL
    )
"""


def escreve_particao(con, raiz, data_base, ids):
    """Uma partição hive `data_base_str=<data>` com uma linha por id (veículos 1 e 2 alternados)."""
    pasta = raiz / f"data_base_str={data_base}"
    pasta.mkdir(parents=True)
    con.execute(
        f"""
        COPY (
            SELECT CAST(i AS INTEGER) AS id_lancamento, CAST('{data_base}' AS DATE) AS data_lancamento,
                   CAST(1 + i % 2 AS INTEGER) AS id_veiculo, 10.00::DECIMAL(12, 2) AS valor
            FROM unnest({list(ids)}) t(i)
        ) TO '{pasta / "data_0.parquet"}' (FORMAT parquet)
        """
    )


@pytest.fixture
def base(tmp_path):
    """A base de origem mínima: 2 partições (novembro e dezembro de 2024) com 5 lançamentos cada."""
    con = duckdb.connect()
    raiz = tmp_path / "cad_lancamentos"
    escreve_particao(con, raiz, "2024-11-30", range(1, 6))
    escreve_particao(con, raiz, "2024-12-31", range(6, 11))
    con.close()
    return raiz


@pytest.fixture
def mundo(base):
    """A conexão no estado do fim do passo 3: dimensão, view, sequence e staging com dezembro."""
    con = duckdb.connect()
    con.execute("CREATE TABLE dom_veiculos (id_veiculo INTEGER PRIMARY KEY, nome_veiculo VARCHAR NOT NULL)")
    con.execute("INSERT INTO dom_veiculos VALUES (1, 'Alfa'), (2, 'Beta')")
    con.execute(
        f"""
        CREATE VIEW cad_lancamentos AS
        SELECT * FROM read_parquet('{base}/**/*.parquet', hive_partitioning=true,
                                   hive_types={{'data_base_str': 'VARCHAR'}})
        """
    )
    con.execute("CREATE SEQUENCE seq_id_lancamento START WITH 11")
    con.execute(DDL_STAGING)
    con.execute(
        "INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM cad_lancamentos WHERE data_base_str = '2024-12-31'"
    )
    yield con
    con.close()


def lote(*id_veiculos, nova="2025-01-31", dia="2025-01-15"):
    """Um DataFrame pandas (backend pyarrow) sem `id_lancamento`, com as colunas fora de ordem."""
    return pa.table(
        {
            "valor": pa.array([Decimal("1.00")] * len(id_veiculos), pa.decimal128(12, 2)),
            "id_veiculo": pa.array(id_veiculos, pa.int32()),
            "data_base_str": pa.array([nova] * len(id_veiculos), pa.string()),
            "data_lancamento": pa.array([pd.Timestamp(dia).date()] * len(id_veiculos), pa.date32()),
        }
    ).to_pandas(types_mapper=pd.ArrowDtype)


# --- a coluna de partição --------------------------------------------------
def test_valor_de_particao_que_parece_data_vira_DATE_sem_hive_types(base):
    con = duckdb.connect()
    glob = f"{base}/**/*.parquet"
    (tipo_padrao,) = con.execute(
        f"SELECT typeof(data_base_str) FROM read_parquet('{glob}', hive_partitioning=true) LIMIT 1"
    ).fetchone()
    (tipo_fixado,) = con.execute(
        f"SELECT typeof(data_base_str) FROM read_parquet('{glob}', hive_partitioning=true, "
        "hive_types={'data_base_str': 'VARCHAR'}) LIMIT 1"
    ).fetchone()
    assert (tipo_padrao, tipo_fixado) == ("DATE", "VARCHAR")


def test_a_view_le_so_a_particao_filtrada(mundo):
    sql = "INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM cad_lancamentos WHERE data_base_str = ?"
    assert exemplo.arquivos_abertos(mundo, sql, ["2024-12-31"]) == "1/2"


def test_proxima_data_base_e_o_ultimo_dia_do_mes_seguinte(mundo):
    proxima = "SELECT strftime(last_day(CAST(? AS DATE) + INTERVAL 1 MONTH), '%Y-%m-%d')"
    assert mundo.execute(proxima, ["2024-12-31"]).fetchone() == ("2025-01-31",)
    assert mundo.execute(proxima, ["2024-01-31"]).fetchone() == ("2024-02-29",)


# --- a sequence -------------------------------------------------------------
def test_sequence_nao_se_reposiciona_no_mesmo_catalogo():
    con = duckdb.connect()
    con.execute("CREATE SEQUENCE s")
    con.execute("CREATE TABLE t (id INTEGER DEFAULT nextval('s'))")
    with pytest.raises(duckdb.DependencyException):
        con.execute("CREATE OR REPLACE SEQUENCE s START WITH 100")
    with pytest.raises(duckdb.DependencyException):
        con.execute("DROP SEQUENCE s")
    with pytest.raises(duckdb.NotImplementedException):
        con.execute("ALTER SEQUENCE s RESTART WITH 100")
    with pytest.raises(duckdb.CatalogException):
        con.execute("SELECT setval('s', 100)")


def test_dependencia_nao_e_rastreada_entre_catalogos():
    """Tabela TEMP + sequence em main: o replace passa (e o DEFAULT resolve pelo nome), o drop também."""
    con = duckdb.connect()
    con.execute("CREATE SEQUENCE s START WITH 1")
    con.execute("CREATE TEMP TABLE t (id INTEGER DEFAULT nextval('s'), x INTEGER)")
    con.execute("CREATE OR REPLACE SEQUENCE s START WITH 100")
    assert con.execute("INSERT INTO t (x) VALUES (0) RETURNING id").fetchone() == (100,)
    con.execute("DROP SEQUENCE s")
    with pytest.raises(duckdb.CatalogException):
        con.execute("INSERT INTO t (x) VALUES (0)")


def test_insert_by_name_preenche_o_id_pela_sequence(mundo):
    df = lote(1, 2, 1)  # noqa: F841 — o replacement scan a encontra pelo nome
    ids = mundo.execute(
        "INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM df RETURNING id_lancamento"
    ).fetchall()
    assert sorted(id_ for (id_,) in ids) == [11, 12, 13]  # continua do START WITH (max da base + 1)
    gravado = mundo.execute(
        "SELECT id_veiculo, valor, data_base_str FROM stage_cad_lancamentos_mes WHERE id_lancamento = 11"
    ).fetchone()
    assert gravado == (1, Decimal("1.00"), "2025-01-31")  # as colunas casaram pelo nome, não pela posição


def test_select_star_posicional_falha_sem_a_coluna_do_id(mundo):
    df = lote(1)  # noqa: F841
    with pytest.raises(duckdb.BinderException, match="5 columns but 4 values"):
        mundo.execute("INSERT INTO stage_cad_lancamentos_mes SELECT * FROM df")


def test_rollback_desfaz_o_lote_mas_nao_a_sequence(mundo):
    df = lote(1, 2)  # noqa: F841
    (antes,) = mundo.execute("SELECT count(*) FROM stage_cad_lancamentos_mes").fetchone()
    mundo.execute("BEGIN")
    mundo.execute("INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM df")
    mundo.execute("ROLLBACK")
    (depois,) = mundo.execute("SELECT count(*) FROM stage_cad_lancamentos_mes").fetchone()
    assert depois == antes
    assert mundo.execute("SELECT currval('seq_id_lancamento')").fetchone() == (12,)  # 11 e 12 foram consumidos
    ids = mundo.execute(
        "INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM df RETURNING id_lancamento"
    ).fetchall()
    assert sorted(id_ for (id_,) in ids) == [13, 14]  # o buraco fica


def test_valor_explicito_nao_avanca_a_sequence(mundo):
    # o próximo valor da sequence é 11; um INSERT explícito com 11 não a avança
    mundo.execute(
        "INSERT INTO stage_cad_lancamentos_mes VALUES (11, DATE '2025-01-10', 1, 1.00, '2025-01-31')"
    )
    df = lote(1)  # noqa: F841
    with pytest.raises(duckdb.ConstraintException, match="Duplicate key"):
        mundo.execute("INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM df")


def test_nextval_em_select_puro_nao_e_persistido_no_arquivo(tmp_path):
    """Só uma transação de escrita leva o contador ao arquivo; nem CHECKPOINT salva um SELECT puro."""
    db = str(tmp_path / "seq.duckdb")
    con = duckdb.connect(db)
    con.execute("CREATE SEQUENCE s START WITH 1")
    con.execute("CREATE TABLE t (id INTEGER DEFAULT nextval('s'))")
    con.execute("INSERT INTO t VALUES (DEFAULT), (DEFAULT)")  # usa 1 e 2, persistidos com a tabela
    con.close()

    con = duckdb.connect(db)
    assert con.execute("SELECT nextval('s')").fetchone() == (3,)
    con.execute("CHECKPOINT")
    con.close()

    con = duckdb.connect(db)
    assert con.execute("SELECT nextval('s')").fetchone() == (3,)  # o 3 foi entregue de novo
    con.execute("BEGIN")
    assert con.execute("SELECT nextval('s')").fetchone() == (4,)
    con.execute("COMMIT")
    con.close()

    con = duckdb.connect(db)
    assert con.execute("SELECT nextval('s')").fetchone() == (5,)  # o COMMIT explícito persistiu
    con.close()


def test_currval_numa_sessao_nova_devolve_o_proximo_valor(tmp_path):
    db = str(tmp_path / "seq.duckdb")
    con = duckdb.connect(db)
    con.execute("CREATE SEQUENCE s START WITH 1")
    con.execute("CREATE TABLE t (id INTEGER DEFAULT nextval('s'))")
    con.execute("INSERT INTO t VALUES (DEFAULT), (DEFAULT)")
    assert con.execute("SELECT currval('s')").fetchone() == (2,)  # na sessão que usou: o último
    con.close()

    con = duckdb.connect(db)
    assert con.execute("SELECT max(id) FROM t").fetchone() == (2,)
    assert con.execute("SELECT currval('s')").fetchone() == (3,)  # sessão nova: o próximo, não o último
    assert con.execute("SELECT last_value FROM duckdb_sequences()").fetchone() == (3,)
    con.close()


def test_nextval_nao_segue_a_ordem_do_lote_numa_carga_paralela():
    """Um lote Arrow em muitos batches é lido em paralelo; só row_number() com ORDER BY é determinístico."""
    lote_grande = exemplo.lote_para_medicao(1_000_000)  # noqa: F841 — 16 batches de 65.536
    assert lote_grande.column("ordem").num_chunks > 1
    con = duckdb.connect()
    con.execute("SET threads = 8")
    con.execute("CREATE SEQUENCE s START WITH 1")
    con.execute("CREATE TEMP TABLE m (id_lancamento INTEGER DEFAULT nextval('s'), ordem INTEGER, valor DECIMAL(12, 2))")
    con.execute("INSERT INTO m BY NAME SELECT ordem, valor FROM lote_grande")
    con.execute("CREATE TEMP TABLE r (id_lancamento INTEGER, ordem INTEGER, valor DECIMAL(12, 2))")
    con.execute("INSERT INTO r SELECT row_number() OVER (ORDER BY ordem), ordem, valor FROM lote_grande")
    inversoes = "SELECT count(*) FILTER (WHERE ordem < anterior) FROM (SELECT ordem, lag(ordem) OVER (ORDER BY id_lancamento) AS anterior FROM {t})"
    (com_nextval,) = con.execute(inversoes.format(t="m")).fetchone()
    (com_row_number,) = con.execute(inversoes.format(t="r")).fetchone()
    assert com_row_number == 0
    assert com_nextval > 0  # os ids da sequence não acompanham a ordem das linhas do lote
    # e mesmo assim são únicos e contíguos — o problema é só a ordem
    assert con.execute("SELECT count(DISTINCT id_lancamento), min(id_lancamento), max(id_lancamento) FROM m").fetchone() == (
        1_000_000,
        1,
        1_000_000,
    )


# --- as validações ----------------------------------------------------------
def test_validacao_aprova_um_lote_correto(mundo):
    df = lote(1, 2)  # noqa: F841
    mundo.execute("INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM df")
    resultado = exemplo.valida_integridade(mundo, "2025-01-31")
    assert len(resultado) == 3
    assert all(problema is None for problema in resultado.values())


def test_validacao_detecta_veiculo_sem_cadastro(mundo):
    df = lote(1, 99, 99)  # noqa: F841
    mundo.execute("INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM df")
    resultado = exemplo.valida_integridade(mundo, "2025-01-31")
    assert resultado["todo id_veiculo existe em dom_veiculos"] == "sem cadastro: id 99 (2 linhas)"
    # a validação é por data-base: dezembro (já no staging) não entra na conta
    assert exemplo.valida_integridade(mundo, "2024-12-31")["todo id_veiculo existe em dom_veiculos"] is None


def test_validacao_detecta_data_fora_do_mes_e_id_repetido(mundo):
    df = lote(1, dia="2024-12-31")  # noqa: F841 — data de dezembro numa data-base de janeiro
    mundo.execute("INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM df")
    # um id que já existe em novembro, forçado à mão (o DEFAULT nunca faria isso)
    mundo.execute(
        "INSERT INTO stage_cad_lancamentos_mes VALUES (3, DATE '2025-01-10', 1, 1.00, '2025-01-31')"
    )
    resultado = exemplo.valida_integridade(mundo, "2025-01-31")
    assert resultado["data_lancamento dentro do mês da data-base"] == "1 linha(s) fora do mês"
    assert resultado["id_lancamento inédito na base histórica"] == "1 id(s) já usados"


def contagem(con):
    return con.execute("SELECT count(*) FROM stage_cad_lancamentos_mes").fetchone()[0]


def test_carrega_lote_commita_o_lote_aprovado(mundo):
    antes = contagem(mundo)
    assert exemplo.carrega_lote(mundo, lote(1, 2), "2025-01-31") is True
    assert contagem(mundo) == antes + 2
    assert mundo.execute("SELECT max(id_lancamento) FROM stage_cad_lancamentos_mes").fetchone() == (12,)


def test_carrega_lote_reverte_o_lote_reprovado(mundo):
    antes = contagem(mundo)
    assert exemplo.carrega_lote(mundo, lote(1, 99), "2025-01-31") is False
    assert contagem(mundo) == antes  # o lote inteiro saiu, inclusive a linha válida
    assert mundo.execute("SELECT currval('seq_id_lancamento')").fetchone() == (12,)  # a sequence não volta


def test_carrega_lote_reverte_e_propaga_um_erro_do_proprio_insert(mundo):
    antes = contagem(mundo)
    sem_valor = lote(1).assign(valor=pd.Series([None], dtype=pd.ArrowDtype(pa.decimal128(12, 2))))
    with pytest.raises(duckdb.ConstraintException, match="NOT NULL"):
        exemplo.carrega_lote(mundo, sem_valor, "2025-01-31")
    assert contagem(mundo) == antes
    # a transação abortada foi revertida: a conexão continua utilizável
    assert exemplo.carrega_lote(mundo, lote(2), "2025-01-31") is True


def test_fk_de_tabela_temporaria_para_main_nao_e_suportada(mundo):
    with pytest.raises(duckdb.BinderException, match="across different schemas or catalogs"):
        mundo.execute("CREATE TEMP TABLE com_fk (id_veiculo INTEGER REFERENCES dom_veiculos (id_veiculo))")


# --- o lote: derivado do mês anterior, em pandas ----------------------------
def mes_anterior_de_teste():
    """Dezembro/2024 como sairia do staging: 3 lançamentos, 2 deles no dia do fechamento."""
    return pa.table(
        {
            "id_lancamento": pa.array([6, 7, 8], pa.int32()),
            "data_lancamento": pa.array(
                [dt.date(2024, 12, 10), dt.date(2024, 12, 31), dt.date(2024, 12, 31)], pa.date32()
            ),
            "id_veiculo": pa.array([1, 1, 2], pa.int32()),
            "valor": pa.array([Decimal("10.00"), Decimal("-2.50"), Decimal("5.00")], pa.decimal128(12, 2)),
            "data_base_str": pa.array(["2024-12-31"] * 3, pa.string()),
        }
    ).to_pandas(types_mapper=pd.ArrowDtype)


def test_deriva_lote_saldos_de_abertura_e_recorrentes():
    lote = exemplo.deriva_lote(mes_anterior_de_teste(), "2024-12-31", "2025-01-31")
    assert "id_lancamento" not in lote.columns
    assert all(isinstance(dtype, pd.ArrowDtype) for dtype in lote.dtypes)
    assert lote["valor"].dtype == pd.ArrowDtype(pa.decimal128(12, 2))  # a soma do groupby não virou float
    assert (lote["data_base_str"] == "2025-01-31").all()
    registros = sorted(lote[["data_lancamento", "id_veiculo", "valor"]].itertuples(index=False, name=None))
    assert registros == [
        (dt.date(2025, 1, 1), 1, Decimal("7.50")),  # abertura do veículo 1: 10.00 - 2.50
        (dt.date(2025, 1, 1), 2, Decimal("5.00")),  # abertura do veículo 2
        (dt.date(2025, 1, 31), 1, Decimal("-2.50")),  # recorrente (estava no dia do fechamento)
        (dt.date(2025, 1, 31), 2, Decimal("5.00")),  # recorrente
    ]


def test_mes_anterior_sai_do_staging_e_o_lote_derivado_volta_por_bulk_insert(mundo):
    anterior = exemplo.carrega_mes_anterior(mundo, "2024-12-31")
    assert len(anterior) == 5
    assert anterior["valor"].dtype == pd.ArrowDtype(pa.decimal128(12, 2))
    lote = exemplo.deriva_lote(anterior, "2024-12-31", "2025-01-31")  # noqa: F841
    assert len(lote) == 2 + 5  # 2 saldos de abertura + 5 recorrentes (dezembro inteiro cai no fechamento)
    ids = mundo.execute(
        "INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM lote RETURNING id_lancamento"
    ).fetchall()
    assert sorted(id_ for (id_,) in ids) == list(range(11, 18))
    aberturas = mundo.execute(
        "SELECT id_veiculo, valor FROM stage_cad_lancamentos_mes WHERE data_lancamento = DATE '2025-01-01' ORDER BY 1"
    ).fetchall()
    assert aberturas == [(1, Decimal("30.00")), (2, Decimal("20.00"))]  # veículos 1 e 2 alternados, 10.00 cada


# --- a exportação -----------------------------------------------------------
def conta(con, raiz, data_base=None):
    filtro = f"WHERE data_base_str = '{data_base}'" if data_base else ""
    return con.execute(
        f"SELECT count(*) FROM read_parquet('{raiz}/**/*.parquet', hive_partitioning=true, "
        f"hive_types={{'data_base_str': 'VARCHAR'}}) {filtro}"
    ).fetchone()[0]


def test_copy_para_diretorio_existente_exige_um_modo(mundo, base):
    with pytest.raises(duckdb.IOException, match="not empty"):
        mundo.execute(
            f"COPY (SELECT 99 AS id_lancamento, '2025-01-31' AS data_base_str) TO '{base}' "
            "(FORMAT parquet, PARTITION_BY (data_base_str))"
        )


def test_overwrite_or_ignore_preserva_as_outras_particoes(mundo, base):
    mundo.execute(
        f"COPY (SELECT 99 AS id_lancamento, '2025-01-31' AS data_base_str) TO '{base}' "
        "(FORMAT parquet, PARTITION_BY (data_base_str), OVERWRITE_OR_IGNORE)"
    )
    assert conta(mundo, base) == 11
    assert conta(mundo, base, "2024-11-30") == 5


def test_overwrite_apaga_os_arquivos_das_outras_particoes(mundo, base):
    (base / "solto.txt").write_text("nem parquet escapa")
    mundo.execute(
        f"COPY (SELECT 99 AS id_lancamento, '2025-01-31' AS data_base_str) TO '{base}' "
        "(FORMAT parquet, PARTITION_BY (data_base_str), OVERWRITE)"
    )
    # some todo arquivo abaixo do diretório-alvo; as pastas das partições antigas ficam vazias
    assert [p.relative_to(base).as_posix() for p in base.rglob("*") if p.is_file()] == [
        "data_base_str=2025-01-31/data_0.parquet"
    ]
    assert sorted(p.name for p in base.iterdir()) == [
        "data_base_str=2024-11-30",
        "data_base_str=2024-12-31",
        "data_base_str=2025-01-31",
    ]
    assert conta(mundo, base) == 1


def test_append_duplica_a_particao_a_cada_execucao(mundo, base):
    for _ in range(2):
        mundo.execute(
            f"COPY (SELECT 99 AS id_lancamento, '2025-01-31' AS data_base_str) TO '{base}' "
            "(FORMAT parquet, PARTITION_BY (data_base_str), APPEND)"
        )
    assert len(list((base / "data_base_str=2025-01-31").glob("*.parquet"))) == 2
    assert conta(mundo, base, "2025-01-31") == 2


def test_exportacao_da_rotina_e_idempotente_e_a_view_enxerga_a_particao(mundo, base):
    import shutil

    df = lote(1, 2, 1)  # noqa: F841
    mundo.execute("INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM df")
    particao = base / "data_base_str=2025-01-31"
    for _ in range(2):
        shutil.rmtree(particao, ignore_errors=True)
        mundo.execute(
            f"COPY (SELECT * FROM stage_cad_lancamentos_mes WHERE data_base_str = ?) TO '{base}' "
            "(FORMAT parquet, PARTITION_BY (data_base_str), OVERWRITE_OR_IGNORE)",
            ["2025-01-31"],
        )
    assert conta(mundo, base) == 13
    # a view do passo 3, sem nenhum ajuste, já lista a data-base nova
    assert mundo.execute("SELECT max(data_base_str), count(*) FROM cad_lancamentos").fetchone() == (
        "2025-01-31",
        13,
    )
