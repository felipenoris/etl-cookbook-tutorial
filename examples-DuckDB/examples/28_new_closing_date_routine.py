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
   stage_cad_lancamentos_mes` — é dela que o mês novo é derivado, e o
   `EXPLAIN` mostra o pruning abrindo 1 arquivo de 12.
4. **O lote novo, derivado do staging** — o mês fechado sai do staging como
   DataFrame pandas (backend pyarrow, via Arrow, sem passar por objetos
   Python) e a regra de derivação roda em Python. A regra aqui é fictícia e
   simples (saldos de abertura + recorrentes); na vida real é a lógica de
   negócio da rotina — o caminho do dado é o mesmo. O DataFrame **não traz**
   `id_lancamento`: a chave vem do `DEFAULT nextval('seq_id_lancamento')`,
   gerada no lado do "servidor".
5. **`carrega_lote`: bulk insert + validações numa única transação** —
   `INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM lote` e, em
   seguida, as checagens: todo `id_veiculo` existe em `dom_veiculos`?
   (`ANTI JOIN`), as datas caem no mês da data-base?, os ids não colidem com
   a base histórica? `COMMIT` se passam, `ROLLBACK` se não — o lote inteiro
   entra ou nada entra, e não existe caminho para inserir sem validar.
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
    tabela que a usa) são criados no passo 3, e não no passo 2. Outras duas
    restrições pesam na rotina: numa carga paralela os valores de `nextval`
    **não seguem a ordem do lote**, e um `nextval` consumido num `SELECT` puro
    **nem é persistido** no `.duckdb` — a sequence não serve de autoridade
    para ids gravados em parquet; o dado é. (A tabela completa de restrições
    está no README, em "Chaves sequenciais".)

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

O epílogo mede o que cada peça custa num lote de 1M de linhas (staging com e
sem PRIMARY KEY, `nextval` vs `row_number()`, staging vs `COPY` direto do
Arrow) e conta quantos ids saem fora da ordem do lote em cada variante.

Rode com: `uv run examples/28_new_closing_date_routine.py`
"""

from __future__ import annotations

import datetime as dt
import re
import shutil
import time
from pathlib import Path

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


def carrega_mes_anterior(con: duckdb.DuckDBPyConnection, ultima: str) -> pd.DataFrame:
    """O mês fechado sai do staging como DataFrame pandas com backend pyarrow (via Arrow, sem cópia por linha)."""
    return (
        con.execute(
            "SELECT * FROM stage_cad_lancamentos_mes WHERE data_base_str = ? ORDER BY id_lancamento", [ultima]
        )
        .to_arrow_table()
        .to_pandas(types_mapper=pd.ArrowDtype)
    )


def deriva_lote(anterior: pd.DataFrame, ultima: str, nova: str) -> pd.DataFrame:
    """Deriva os lançamentos da data-base nova a partir dos da anterior — em pandas.

    É aqui que mora a lógica de negócio da rotina. A regra abaixo é um
    substituto fictício e simples; o que este exemplo exercita é o caminho do
    dado, que é o real: o mês anterior chega do staging como DataFrame
    (backend pyarrow), a derivação roda em Python e o resultado volta ao
    staging por bulk insert.

    1. **Saldo de abertura** — o total do mês anterior por veículo entra como
       um lançamento no 1º dia do mês novo (`groupby` + `sum`; a soma preserva
       o `decimal128(12, 2)`, sem passar por float).
    2. **Recorrentes** — os lançamentos feitos no dia do fechamento anterior
       (as provisões) se repetem no dia do fechamento novo.

    O resultado NÃO traz `id_lancamento`: a chave é do banco (SEQUENCE).
    """
    fechamento_anterior = dt.date.fromisoformat(ultima)
    fechamento = dt.date.fromisoformat(nova)
    tipos = {"data_lancamento": pd.ArrowDtype(pa.date32()), "data_base_str": pd.ArrowDtype(pa.string())}

    abertura = (
        anterior.groupby("id_veiculo", as_index=False)["valor"]
        .sum()
        .assign(data_lancamento=fechamento.replace(day=1), data_base_str=nova)
        .astype(tipos)
    )
    recorrentes = (
        anterior.loc[anterior["data_lancamento"] == fechamento_anterior, ["id_veiculo", "valor"]]
        .assign(data_lancamento=fechamento, data_base_str=nova)
        .astype(tipos)
    )
    return pd.concat([abertura, recorrentes], ignore_index=True)


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


LINHAS_MEDICAO = 1_000_000


def lote_para_medicao(n: int) -> pa.Table:
    """Um lote sintético de `n` linhas, com a coluna `ordem` (1..n) para conferir a ordem dos ids.

    O lote sai em batches de 65.536 linhas — a forma de um DataFrame montado por
    `concat` ou lido de arquivos. Importa para a medição: o DuckDB lê os batches
    em paralelo, e um lote de um único batch sairia ordenado só por não ser
    paralelizado.
    """
    tabela = duckdb.connect().execute(
        f"""
        SELECT CAST(i + 1 AS INTEGER) AS ordem,
               DATE '2025-01-01' + CAST(hash(i) % 31 AS INTEGER) AS data_lancamento,
               CAST(1 + hash(i) % {len(VEICULOS)} AS INTEGER) AS id_veiculo,
               CAST(CAST(CAST((hash(i) // 6) % 2000001 AS BIGINT) - 500000 AS DECIMAL(14, 0))
                    * 0.01::DECIMAL(3, 2) AS DECIMAL(12, 2)) AS valor,
               '2025-01-31' AS data_base_str
        FROM range({n}) t(i)
        """
    ).to_arrow_table()
    return pa.Table.from_batches(tabela.to_batches(max_chunksize=65_536))


def mede_variantes(lote_grande: pa.Table, destino: Path) -> None:
    """Quatro mecânicas para levar o mesmo lote até a partição parquet (melhor de 3 execuções).

    Cada variante parte de uma conexão nova em memória e termina com o mesmo
    `COPY ... PARTITION_BY`. Além do tempo, conta quantas vezes o id atribuído
    "volta" quando se percorre o lote na ordem original — zero significa que
    os ids seguem a ordem das linhas do lote.
    """
    ddl = (
        "CREATE TEMP TABLE medida (id_lancamento INTEGER {pk} {default}, ordem INTEGER, data_lancamento DATE, "
        "id_veiculo INTEGER, valor DECIMAL(12, 2), data_base_str VARCHAR)"
    )
    copia = (
        "COPY (SELECT * EXCLUDE (ordem) FROM {fonte}) TO '{destino}' "
        "(FORMAT parquet, PARTITION_BY (data_base_str), OVERWRITE_OR_IGNORE)"
    )
    variantes = {
        "staging com PRIMARY KEY + DEFAULT nextval, .duckdb em arquivo (a rotina)": (
            "PRIMARY KEY", "DEFAULT nextval('seq_medida')", "*",
        ),
        "staging com PRIMARY KEY + DEFAULT nextval, em memória": ("PRIMARY KEY", "DEFAULT nextval('seq_medida')", "*"),
        "staging sem PK + DEFAULT nextval": ("", "DEFAULT nextval('seq_medida')", "*"),
        "staging sem PK + row_number() OVER ()": ("", "", "row_number() OVER () AS id_lancamento, *"),
        "sem staging: row_number() OVER () dentro do COPY": "(SELECT row_number() OVER () AS id_lancamento, * FROM lote_grande)",
        "sem staging: id já vem no lote, COPY direto do Arrow": "(SELECT ordem AS id_lancamento, * FROM lote_grande)",
    }
    arquivo = destino.parent / "medicao.duckdb"
    for rotulo, receita in variantes.items():
        tempos, fora_de_ordem = [], 0
        for _ in range(3):
            arquivo.unlink(missing_ok=True)
            con = duckdb.connect(str(arquivo)) if "arquivo" in rotulo else duckdb.connect()
            con.execute("CREATE SEQUENCE seq_medida START WITH 1")
            inicio = time.perf_counter()
            if isinstance(receita, str):  # sem staging: o COPY lê o Arrow diretamente
                con.execute(copia.format(fonte=receita, destino=destino))
            else:
                pk, default, projecao = receita
                con.execute(ddl.format(pk=pk, default=default))
                con.execute(f"INSERT INTO medida BY NAME SELECT {projecao} FROM lote_grande")
                con.execute(copia.format(fonte="medida", destino=destino))
            tempos.append(time.perf_counter() - inicio)
            if not isinstance(receita, str):
                (fora_de_ordem,) = con.execute(
                    "SELECT count(*) FILTER (WHERE ordem < anterior) FROM "
                    "(SELECT ordem, lag(ordem) OVER (ORDER BY id_lancamento) AS anterior FROM medida)"
                ).fetchone()
            con.close()
        ordem = f"ids fora da ordem do lote: {fora_de_ordem:,}" if not isinstance(receita, str) else ""
        print(f"{rotulo:72s} {min(tempos) * 1000:5.0f}ms   {ordem}")
    arquivo.unlink(missing_ok=True)


def imprime_validacao(resultado: dict[str, str | None]) -> bool:
    for checagem, problema in resultado.items():
        print(f"  [{'FALHOU' if problema else 'OK    '}] {checagem}{f' — {problema}' if problema else ''}")
    return all(problema is None for problema in resultado.values())


def carrega_lote(con: duckdb.DuckDBPyConnection, lote: pd.DataFrame, nova: str) -> bool:
    """Bulk insert + validações numa única transação: o lote entra inteiro ou não entra.

    É o único caminho de entrada do staging para a data-base nova — não há como
    inserir sem validar. É a garantia que uma FOREIGN KEY daria, sem o custo
    linha a linha dela (e a FK nem é possível aqui: tabela TEMP não referencia
    o catálogo main). Um erro no próprio INSERT (NOT NULL, PK) aborta a
    transação: a função reverte e propaga.

    O `SELECT * FROM lote` funciona dentro da função porque o replacement scan
    procura o nome no frame de quem chama `execute` — aqui, o parâmetro `lote`
    (exemplo 27). Devolve True se o lote foi commitado.
    """
    con.execute("BEGIN")
    try:
        ids = [
            id_
            for (id_,) in con.execute(
                "INSERT INTO stage_cad_lancamentos_mes BY NAME SELECT * FROM lote RETURNING id_lancamento"
            ).fetchall()
        ]
        faixa = f"id_lancamento {min(ids):,}..{max(ids):,} (gerados pela sequence)" if ids else "nenhum id"
        print(f"INSERT ... BY NAME: {len(ids):,} linhas, {faixa}")
        aprovado = imprime_validacao(valida_integridade(con, nova))
    except duckdb.Error:
        con.execute("ROLLBACK")
        raise
    con.execute("COMMIT" if aprovado else "ROLLBACK")
    print(f"-> {'COMMIT' if aprovado else 'ROLLBACK'}")
    return aprovado


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

    section("4) O lote novo: derivado do staging em pandas (backend pyarrow)")
    (nova,) = con.execute(
        "SELECT strftime(last_day(CAST(? AS DATE) + INTERVAL 1 MONTH), '%Y-%m-%d')", [ultima]
    ).fetchone()
    print(f"próxima data-base: {ultima} -> {nova}")
    anterior = carrega_mes_anterior(con, ultima)
    print(f"mês anterior, lido do staging: {len(anterior):,} linhas num {type(anterior).__name__} (backend pyarrow)")
    lote = deriva_lote(anterior, ultima, nova)
    n_abertura = anterior["id_veiculo"].nunique()
    print(f"lote derivado: {len(lote):,} linhas = {n_abertura} saldos de abertura + {len(lote) - n_abertura} recorrentes;")
    print("sem id_lancamento, e com os dtypes que saíram do staging:")
    print("  " + lote.dtypes.to_string().replace("\n", "\n  "))
    print("\nSELECT * posicional não serve — o DataFrame tem 4 colunas, a tabela 5:")
    try:
        con.execute("INSERT INTO stage_cad_lancamentos_mes SELECT * FROM lote")
    except duckdb.BinderException as erro:
        print(f"  {type(erro).__name__}: {str(erro).splitlines()[0]}")
    print("INSERT ... BY NAME casa as colunas pelo nome e o DEFAULT preenche a que falta —")
    print(f"é o que carrega_lote faz (a base parou em {max_id:,}; a sequence continua de lá).")

    section("5) carrega_lote: bulk insert + validações numa única transação")
    if not carrega_lote(con, lote, nova):
        raise SystemExit("lote rejeitado: nada foi gravado no staging")

    section("O que a validação pega: um defeito na derivação é revertido inteiro")
    # simula um bug na regra de derivação: o veículo 99 não existe em dom_veiculos
    lote_ruim = lote.head(3).assign(id_veiculo=pd.Series([1, 99, 2], dtype=pd.ArrowDtype(pa.int32())))
    (antes,) = con.execute("SELECT count(*) FROM stage_cad_lancamentos_mes").fetchone()
    carrega_lote(con, lote_ruim, nova)
    (depois,) = con.execute("SELECT count(*) FROM stage_cad_lancamentos_mes").fetchone()
    (ultimo_id_usado,) = con.execute("SELECT currval('seq_id_lancamento')").fetchone()
    (maior_id_gravado,) = con.execute("SELECT max(id_lancamento) FROM stage_cad_lancamentos_mes").fetchone()
    print(f"   {antes:,} linhas antes, {depois:,} depois — nada do lote ficou.")
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

    section(f"Epílogo: o que custa cada peça da rotina — lote de {LINHAS_MEDICAO:,} linhas")
    lote_grande = lote_para_medicao(LINHAS_MEDICAO)
    mede_variantes(lote_grande, WORK_DIR / "medicao")
    shutil.rmtree(WORK_DIR / "medicao", ignore_errors=True)
    print("\nO custo dominante da rotina é a PRIMARY KEY do staging (o índice ART é construído")
    print("linha a linha) — não a sequence nem o arquivo .duckdb (o staging é TEMP e não toca")
    print("o arquivo). nextval custa pouco mais que row_number(), mas numa carga paralela os")
    print("ids saem fora da ordem do lote; para ids determinísticos, max_id + row_number()")
    print("OVER (ORDER BY ...). O caminho mais curto é não ter staging: com o id já no lote, o")
    print("COPY lê os batches do Arrow em paralelo e grava a partição direto — mas uma window")
    print("function dentro do COPY desfaz o ganho (sem PARTITION BY, ela roda numa thread só).")
