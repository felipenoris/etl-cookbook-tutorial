"""Exemplo 28 — Rotina de nova data-base: do `.duckdb` vazio à partição parquet nova.

O cenário é um fechamento contábil mensal. A base de origem vive em parquet
particionado, fora de qualquer banco:

    base/
      dom_veiculos/*.parquet          id_veiculo, nome_veiculo
      cad_lancamentos/**/*.parquet    id_lancamento, data_lancamento, id_veiculo, valor
        data_base_str=2024-01-31/     <- coluna de partição: a data de fechamento
        data_base_str=2024-02-29/        contábil (último dia do mês), 'yyyy-mm-dd'
        ...

*Veículos* são as empresas da sociedade para as quais se elaboram demonstrativos
contábeis; *lançamentos* são os registros contábeis de cada veículo. Cada
fechamento mensal é uma **data-base**, e a rotina deste exemplo cria a próxima:
lê a base atual, recebe os lançamentos do mês novo num DataFrame pandas e grava
a partição nova **no mesmo lugar da base de origem**. Seis passos:

1. **Arquivo `.duckdb` vazio** — `duckdb.connect(caminho)` cria o arquivo; o
   catálogo nasce sem tabelas.
2. **Tabelas vazias de esquema** — o DDL que não depende dos dados
   (`dom_veiculos`, com PRIMARY KEY). A sequence e o staging ficam para o
   passo 3, e a razão está medida abaixo.
3. **Carga da base atual** — `dom_veiculos` entra inteira (`INSERT ... BY
   NAME`); `cad_lancamentos` vira uma **VIEW** sobre o glob (zero cópia); só a
   partição da última `data_base_str` é materializada em `CREATE TEMP TABLE
   stage_cad_lancamentos_mes` — o `EXPLAIN` mostra o pruning abrindo 1 arquivo
   de 12.
4. **Bulk insert do lote novo** — um DataFrame pandas (backend pyarrow) entra
   com `INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM lote`. O
   DataFrame **não traz** `id_lancamento`: a chave vem do `DEFAULT
   nextval('seq_id_lancamento')`, gerada no lado do "servidor".
5. **Validações de integridade** — todo `id_veiculo` existe em `dom_veiculos`?
   (`ANTI JOIN`), as datas caem no mês da data-base?, os ids não colidem com a
   base histórica? Os passos 4 e 5 rodam **na mesma transação**: `COMMIT` se
   passa, `ROLLBACK` se não — o lote inteiro entra ou nada entra.
6. **Exportar a nova data-base** — `COPY ... (PARTITION_BY (data_base_str))`
   para a base de origem, substituindo a partição nova (recarga idempotente).
   A view do passo 3 enxerga a partição nova sem nenhum ajuste.

O que a rotina ensina de DuckDB (tudo verificado na execução):

`CREATE SEQUENCE ... START WITH n` só na criação
    Não existe `setval()` nem `ALTER SEQUENCE ... RESTART`, e `CREATE OR
    REPLACE SEQUENCE` é barrado pela dependência da tabela que a usa no
    `DEFAULT` (`DependencyException`). Logo a sequence precisa nascer com o
    `START WITH` certo — `max(id_lancamento) + 1` — e esse número só existe
    depois de enxergar a base. É por isso que a sequence e o staging (a única
    tabela que a usa) são criados no passo 3, e não no passo 2.

`INSERT INTO tabela BY NAME SELECT ...`
    Casa as colunas **pelo nome**, não pela posição; as colunas ausentes no
    SELECT recebem o `DEFAULT`. O `SELECT *` posicional falha ("table has 5
    columns but 4 values were supplied") — o DataFrame tem 4 colunas, a tabela
    5, e a ordem das colunas do DataFrame nem precisa bater.

`hive_types={'data_base_str': 'VARCHAR'}`
    O valor de partição `2024-12-31` **parece data**, e o DuckDB tipa a coluna
    de partição como `DATE` por padrão (autodetecção; inteiros que fazem o
    round-trip viram `BIGINT`, `'01'` fica `VARCHAR`). O contrato diz `VARCHAR`
    — `hive_types` fixa o tipo, e o texto da view persiste a escolha.

Sequence não é transacional
    O `ROLLBACK` devolve o staging ao estado anterior, mas os ids consumidos
    pelo lote revertido não voltam: fica um buraco na numeração (inofensivo, e
    igual ao Postgres).

`COPY ... TO dir (PARTITION_BY ...)` tem três modos de escrita
    `OVERWRITE_OR_IGNORE` sobrescreve arquivos de mesmo nome só na partição
    gravada; **`OVERWRITE` apaga todos os arquivos do diretório-alvo** — todas
    as partições, sobram as pastas vazias — antes de gravar; `APPEND` cria um
    arquivo de nome aleatório a cada execução (rodar 2x duplica o mês). A
    receita idempotente da rotina: remover a pasta da partição e gravar com
    `OVERWRITE_OR_IGNORE` (necessário só porque a pasta-raiz já existe).

Rode com: `uv run examples/28_new_closing_date_routine.py`
"""

from __future__ import annotations

import datetime as dt
import random
import re
import shutil
from decimal import Decimal

import duckdb
import pandas as pd
import pyarrow as pa

from _common import RICH_DIR, section

WORK_DIR = RICH_DIR / "duckdb_nova_data_base"
BASE_DIR = WORK_DIR / "base"  # a "base de origem": parquet particionado
DB_PATH = WORK_DIR / "fechamento.duckdb"
VEICULOS_GLOB = str(BASE_DIR / "dom_veiculos" / "*.parquet")
LANCAMENTOS_DIR = BASE_DIR / "cad_lancamentos"
LANCAMENTOS_GLOB = str(LANCAMENTOS_DIR / "**" / "*.parquet")

VEICULOS = [
    "Alfa Participações",
    "Beta Holding",
    "Gama Fundo de Investimento",
    "Delta Securitizadora",
    "Épsilon Empreendimentos",
    "Zeta Seguros",
]
MESES_NA_BASE = 12  # 2024-01-31 .. 2024-12-31
LANCAMENTOS_POR_MES = 4_000
LANCAMENTOS_NOVOS = 3_000


def prepara_base_ficticia() -> None:
    """Escreve a base de origem: 6 veículos e 12 datas-base de lançamentos.

    `hash()` no lugar de `random()` torna a base idêntica em toda execução.
    O `COPY ... PARTITION_BY` cria uma pasta `data_base_str=.../` por
    fechamento; a coluna de partição fica só no caminho, não dentro do arquivo.
    """
    shutil.rmtree(WORK_DIR, ignore_errors=True)
    (BASE_DIR / "dom_veiculos").mkdir(parents=True)
    con = duckdb.connect()
    con.execute("CREATE TABLE veiculos (id_veiculo INTEGER, nome_veiculo VARCHAR)")
    con.executemany("INSERT INTO veiculos VALUES (?, ?)", list(enumerate(VEICULOS, start=1)))
    con.execute(f"COPY veiculos TO '{BASE_DIR / 'dom_veiculos' / 'data_0.parquet'}' (FORMAT parquet)")
    con.execute(
        f"""
        COPY (
            WITH meses AS (
                SELECT m, last_day(DATE '2024-01-01' + INTERVAL (m) MONTH) AS fechamento
                FROM range({MESES_NA_BASE}) t(m)
            ),
            sorteio AS (
                SELECT m, fechamento, i, hash(m * 100000 + i) AS h
                FROM meses, range({LANCAMENTOS_POR_MES}) t(i)
            )
            SELECT
                CAST(m * {LANCAMENTOS_POR_MES} + i + 1 AS INTEGER)   AS id_lancamento,
                fechamento - CAST(h % day(fechamento) AS INTEGER)   AS data_lancamento,
                CAST(1 + h % {len(VEICULOS)} AS INTEGER)            AS id_veiculo,
                CAST(CAST(CAST((h // 6) % 2000001 AS BIGINT) - 500000 AS DECIMAL(14, 0))
                     * 0.01::DECIMAL(3, 2) AS DECIMAL(12, 2))       AS valor,  -- -5000.00 .. 15000.00
                strftime(fechamento, '%Y-%m-%d')                    AS data_base_str
            FROM sorteio
        ) TO '{LANCAMENTOS_DIR}' (FORMAT parquet, PARTITION_BY (data_base_str))
        """
    )
    con.close()


def arquivos_abertos(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> str:
    """Devolve o `Scanning Files: k/n` do EXPLAIN — quantos arquivos o scan abre de fato."""
    plano = con.execute(f"EXPLAIN {sql}", params).fetchone()[1]
    achado = re.search(r"Scanning Files: (\S+)", plano)
    return achado.group(1) if achado else "?"


def monta_lote(con: duckdb.DuckDBPyConnection, ultima: str, nova: str) -> pd.DataFrame:
    """Os lançamentos da próxima data-base, como DataFrame pandas com backend pyarrow.

    Duas origens, concatenadas: (a) as provisões recorrentes — os lançamentos
    feitos no dia do fechamento anterior se repetem no fechamento novo (saem
    do staging via Arrow, sem passar por objetos Python); (b) os lançamentos
    novos do mês, aqui sorteados, na prática vindos do sistema de origem.
    O DataFrame NÃO traz `id_lancamento`: a chave é do banco (SEQUENCE).
    """
    recorrentes = con.execute(
        """
        SELECT id_veiculo, valor FROM stage_cad_lancamentos_mes
        WHERE data_lancamento = CAST(? AS DATE) ORDER BY id_lancamento
        """,
        [ultima],
    ).to_arrow_table().to_pandas(types_mapper=pd.ArrowDtype)
    fechamento = dt.date.fromisoformat(nova)
    recorrentes["data_lancamento"] = pd.Series([fechamento] * len(recorrentes), dtype=pd.ArrowDtype(pa.date32()))
    recorrentes["data_base_str"] = pd.Series([nova] * len(recorrentes), dtype=pd.ArrowDtype(pa.string()))

    sorteio = random.Random(nova)  # seed = a data-base: o lote é reproduzível
    n = LANCAMENTOS_NOVOS
    novos = pa.table(
        {
            "data_lancamento": pa.array(
                [fechamento.replace(day=sorteio.randint(1, fechamento.day)) for _ in range(n)], pa.date32()
            ),
            "id_veiculo": pa.array([sorteio.randint(1, len(VEICULOS)) for _ in range(n)], pa.int32()),
            "valor": pa.array(
                [Decimal(sorteio.randint(-500_000, 1_500_000)) / 100 for _ in range(n)], pa.decimal128(12, 2)
            ),
            "data_base_str": pa.array([nova] * n, pa.string()),
        }
    ).to_pandas(types_mapper=pd.ArrowDtype)
    return pd.concat([recorrentes, novos], ignore_index=True)


def valida_integridade(con: duckdb.DuckDBPyConnection, nova: str) -> dict[str, str | None]:
    """Checagens em lote, por SQL, sobre a data-base nova no staging.

    Devolve `{checagem: problema}`, com `None` onde passou. Tudo é consulta
    vetorizada sobre o mês inteiro — nenhuma constraint linha a linha.
    """
    orfaos = con.execute(
        """
        SELECT s.id_veiculo, count(*) AS linhas
        FROM stage_cad_lancamentos_mes s
        ANTI JOIN dom_veiculos d USING (id_veiculo)
        WHERE s.data_base_str = ?
        GROUP BY 1 ORDER BY 1
        """,
        [nova],
    ).fetchall()
    (fora_do_mes,) = con.execute(
        """
        SELECT count(*) FROM stage_cad_lancamentos_mes
        WHERE data_base_str = ?
          AND data_lancamento NOT BETWEEN date_trunc('month', CAST(? AS DATE)) AND CAST(? AS DATE)
        """,
        [nova, nova, nova],
    ).fetchone()
    (colisoes,) = con.execute(
        """
        SELECT count(*) FROM stage_cad_lancamentos_mes s
        WHERE s.data_base_str = ?
          AND EXISTS (SELECT 1 FROM cad_lancamentos h
                      WHERE h.id_lancamento = s.id_lancamento AND h.data_base_str <> s.data_base_str)
        """,
        [nova],
    ).fetchone()
    return {
        "todo id_veiculo existe em dom_veiculos": (
            f"sem cadastro: {', '.join(f'id {i} ({n} linhas)' for i, n in orfaos)}" if orfaos else None
        ),
        "data_lancamento dentro do mês da data-base": f"{fora_do_mes} linha(s) fora do mês" if fora_do_mes else None,
        "id_lancamento inédito na base histórica": f"{colisoes} id(s) já usados" if colisoes else None,
    }


def imprime_validacao(resultado: dict[str, str | None]) -> bool:
    for checagem, problema in resultado.items():
        print(f"  [{'FALHOU' if problema else 'OK    '}] {checagem}{f' — {problema}' if problema else ''}")
    return all(problema is None for problema in resultado.values())


if __name__ == "__main__":
    section("Preparação: a base de origem fictícia (6 veículos, 12 datas-base em parquet)")
    prepara_base_ficticia()
    particoes = sorted(pasta.name for pasta in LANCAMENTOS_DIR.glob("data_base_str=*"))
    print(f"{BASE_DIR.relative_to(RICH_DIR)}/")
    print("  dom_veiculos/data_0.parquet")
    for nome in (*particoes[:2], "...", particoes[-1]):
        print(f"  cad_lancamentos/{nome}/")
    print(f"({len(particoes)} partições, {LANCAMENTOS_POR_MES:,} lançamentos cada)")

    section("1) Arquivo .duckdb vazio")
    print(f"antes do connect: {DB_PATH.name} existe? {DB_PATH.exists()}")
    con = duckdb.connect(str(DB_PATH))
    (n_tabelas,) = con.execute("SELECT count(*) FROM duckdb_tables()").fetchone()
    print(f"depois:           existe? {DB_PATH.exists()} — {DB_PATH.stat().st_size / 1024:.0f}KB, {n_tabelas} tabelas")

    section("2) Tabelas vazias de esquema: o DDL que não depende dos dados")
    con.execute(
        """
        CREATE TABLE dom_veiculos (
            id_veiculo   INTEGER PRIMARY KEY,
            nome_veiculo VARCHAR NOT NULL
        )
        """
    )
    con.sql("SELECT table_name, has_primary_key, estimated_size AS linhas FROM duckdb_tables()").show()
    print("A sequence e o staging ficam para o passo 3, de propósito: o DEFAULT nextval()")
    print("amarra a tabela à sequence, e a sequence tem que nascer com o START WITH certo")
    print("(max(id) + 1, que só se conhece lendo a base). Não há como reposicioná-la depois:")
    con.execute("CREATE SEQUENCE seq_prova")
    con.execute("CREATE TABLE prova (id INTEGER DEFAULT nextval('seq_prova'))")
    for sql in (
        "CREATE OR REPLACE SEQUENCE seq_prova START WITH 100",
        "ALTER SEQUENCE seq_prova RESTART WITH 100",
        "SELECT setval('seq_prova', 100)",
    ):
        try:
            con.execute(sql)
        except duckdb.Error as erro:
            print(f"  {sql:52s} -> {type(erro).__name__}")
    con.execute("DROP TABLE prova")
    con.execute("DROP SEQUENCE seq_prova")
    print("(A dependência é rastreada por catálogo: com o staging TEMP e a sequence em main,")
    print(" o CREATE OR REPLACE passaria — e um DROP SEQUENCE também, deixando o DEFAULT")
    print(" apontando para o nada. É uma lacuna do rastreamento, não uma API.)")

    section("3) Carga da base atual: dimensão inteira, fatos como view, último mês em staging")
    con.execute(f"INSERT INTO dom_veiculos BY NAME SELECT * FROM read_parquet('{VEICULOS_GLOB}')")
    con.execute(
        f"""
        CREATE VIEW cad_lancamentos AS
        SELECT * FROM read_parquet('{LANCAMENTOS_GLOB}', hive_partitioning=true,
                                   hive_types={{'data_base_str': 'VARCHAR'}})
        """
    )
    ultima, max_id, n_datas = con.execute(
        "SELECT max(data_base_str), max(id_lancamento), count(DISTINCT data_base_str) FROM cad_lancamentos"
    ).fetchone()
    (n_veiculos,) = con.execute("SELECT count(*) FROM dom_veiculos").fetchone()
    print(f"dom_veiculos: {n_veiculos} veículos carregados (INSERT ... BY NAME a partir do parquet)")
    print(f"cad_lancamentos (view): {n_datas} datas-base, maior id_lancamento = {max_id:,}")
    print(f"última data-base = {ultima!r} — um {type(ultima).__name__}, graças ao hive_types;")
    print("sem ele o DuckDB tiparia a coluna de partição como DATE, porque o valor parece data.")

    con.execute(f"CREATE SEQUENCE seq_id_lancamento START WITH {max_id + 1}")
    con.execute(
        """
        CREATE TEMP TABLE stage_cad_lancamentos_mes (
            id_lancamento   INTEGER PRIMARY KEY DEFAULT nextval('seq_id_lancamento'),
            data_lancamento DATE NOT NULL,
            id_veiculo      INTEGER NOT NULL,
            valor           DECIMAL(12, 2) NOT NULL,
            data_base_str   VARCHAR NOT NULL
        )
        """
    )
    carga_staging = (
        "INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM cad_lancamentos WHERE data_base_str = ?"
    )
    print(f"\nstaging da última partição — arquivos abertos pelo scan: {arquivos_abertos(con, carga_staging, [ultima])}")
    con.execute(carga_staging, [ultima])
    con.sql(
        """
        SELECT count(*) AS linhas, min(id_lancamento) AS menor_id, max(id_lancamento) AS maior_id,
               sum(valor) AS total
        FROM stage_cad_lancamentos_mes
        """
    ).show()
    con.sql(
        """
        SELECT 'tabela' AS tipo, table_name AS nome, temporary FROM duckdb_tables()
        UNION ALL
        SELECT 'view', view_name, temporary FROM duckdb_views() WHERE NOT internal
        ORDER BY 1, 2
        """
    ).show()

    section("4) Bulk insert: DataFrame pandas (backend pyarrow) -> staging, id gerado pela SEQUENCE")
    (nova,) = con.execute(
        "SELECT strftime(last_day(CAST(? AS DATE) + INTERVAL 1 MONTH), '%Y-%m-%d')", [ultima]
    ).fetchone()
    print(f"próxima data-base: {ultima} -> {nova}")
    lote = monta_lote(con, ultima, nova)
    print(f"lote: {len(lote):,} linhas, sem id_lancamento; dtypes:")
    print("  " + lote.dtypes.to_string().replace("\n", "\n  "))
    print("\nSELECT * posicional não serve — o DataFrame tem 4 colunas, a tabela 5:")
    try:
        con.execute("INSERT INTO stage_cad_lancamentos_mes SELECT * FROM lote")
    except duckdb.BinderException as erro:
        print(f"  {type(erro).__name__}: {str(erro).splitlines()[0]}")
    con.execute("BEGIN")  # 4 e 5 na mesma transação: o lote só fica se passar na validação
    ids = [
        id_
        for (id_,) in con.execute(
            "INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM lote RETURNING id_lancamento"
        ).fetchall()
    ]
    print("INSERT ... BY NAME casa as colunas pelo nome e o DEFAULT preenche a que falta:")
    print(f"  {len(ids):,} linhas inseridas, id_lancamento {min(ids):,}..{max(ids):,} (a base parou em {max_id:,})")

    section("5) Validações de integridade (ainda dentro da transação)")
    aprovado = imprime_validacao(valida_integridade(con, nova))
    con.execute("COMMIT" if aprovado else "ROLLBACK")
    print(f"-> {'COMMIT' if aprovado else 'ROLLBACK'}")

    section("O que a validação pega: lote com veículo inexistente é revertido inteiro")
    lote_ruim = pa.table(
        {
            "data_lancamento": pa.array([dt.date.fromisoformat(nova)] * 3, pa.date32()),
            "id_veiculo": pa.array([1, 99, 2], pa.int32()),  # 99 não existe em dom_veiculos
            "valor": pa.array([Decimal("10.00")] * 3, pa.decimal128(12, 2)),
            "data_base_str": pa.array([nova] * 3, pa.string()),
        }
    ).to_pandas(types_mapper=pd.ArrowDtype)
    (antes,) = con.execute("SELECT count(*) FROM stage_cad_lancamentos_mes").fetchone()
    con.execute("BEGIN")
    con.execute("INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM lote_ruim")
    aprovado = imprime_validacao(valida_integridade(con, nova))
    con.execute("COMMIT" if aprovado else "ROLLBACK")
    (depois,) = con.execute("SELECT count(*) FROM stage_cad_lancamentos_mes").fetchone()
    (ultimo_id_usado,) = con.execute("SELECT currval('seq_id_lancamento')").fetchone()
    (maior_id_gravado,) = con.execute("SELECT max(id_lancamento) FROM stage_cad_lancamentos_mes").fetchone()
    print(f"-> ROLLBACK: {antes:,} linhas antes, {depois:,} depois — nada do lote ficou.")
    print(f"   A sequence não é transacional: currval = {ultimo_id_usado:,}, maior id gravado = {maior_id_gravado:,};")
    print(f"   os {ultimo_id_usado - maior_id_gravado} ids do lote revertido viram um buraco na numeração (inofensivo).")
    print("\nE uma FOREIGN KEY no staging não substituiria a checagem? Não dá — tabela temporária")
    print("(catálogo temp) não referencia tabela do catálogo main:")
    try:
        con.execute("CREATE TEMP TABLE com_fk (id_veiculo INTEGER REFERENCES dom_veiculos (id_veiculo))")
    except duckdb.BinderException as erro:
        print(f"  {type(erro).__name__}: {str(erro).splitlines()[0]}")

    section("6) Exportar a nova data-base para a base de origem (partição nova)")
    particao = LANCAMENTOS_DIR / f"data_base_str={nova}"
    exporta = f"""
        COPY (SELECT * FROM stage_cad_lancamentos_mes WHERE data_base_str = ?)
        TO '{LANCAMENTOS_DIR}' (FORMAT parquet, PARTITION_BY (data_base_str), OVERWRITE_OR_IGNORE)
    """
    shutil.rmtree(particao, ignore_errors=True)  # recarga idempotente: a partição é substituída inteira
    con.execute(exporta, [nova])
    for caminho in sorted(particao.glob("*.parquet")):
        print(f"gravado: {caminho.relative_to(BASE_DIR)} ({caminho.stat().st_size / 1024:.0f}KB)")
    print("OVERWRITE_OR_IGNORE só é necessário porque cad_lancamentos/ já existe (sem ele:")
    print("'Directory is not empty'). Cuidado com os irmãos: OVERWRITE apaga TODOS os arquivos")
    print("do diretório-alvo — todas as partições — antes de gravar; APPEND cria um arquivo")
    print("de nome aleatório a cada execução (rodar 2x duplica o mês).")

    print("\nA view do passo 3 não foi tocada e já enxerga a partição nova:")
    con.sql(
        """
        SELECT data_base_str, count(*) AS lancamentos, sum(valor) AS total
        FROM cad_lancamentos GROUP BY 1 ORDER BY 1 DESC LIMIT 3
        """
    ).show()
    (linhas_antes,) = con.execute("SELECT count(*) FROM cad_lancamentos").fetchone()
    shutil.rmtree(particao)
    con.execute(exporta, [nova])
    (linhas_depois,) = con.execute("SELECT count(*) FROM cad_lancamentos").fetchone()
    print(f"passo 6 executado 2x: {linhas_antes:,} -> {linhas_depois:,} linhas na base (idempotente: {linhas_antes == linhas_depois})")

    section("O que ficou no .duckdb: o catálogo (dimensão + view), não os fatos")
    con.close()
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.sql(
        """
        SELECT 'tabela' AS tipo, table_name AS nome, estimated_size AS linhas FROM duckdb_tables()
        UNION ALL
        SELECT 'view', view_name, NULL FROM duckdb_views() WHERE NOT internal
        """
    ).show()
    (n_datas,) = con.execute("SELECT count(DISTINCT data_base_str) FROM cad_lancamentos").fetchone()
    print(f"{DB_PATH.name} reaberto: o staging (TEMP) morreu com a conexão; os lançamentos")
    print("continuam nos parquet, e a view (com o caminho absoluto no seu texto) lista")
    print(f"{n_datas} datas-base. A próxima execução da rotina partiria de {nova}.")
    con.close()
