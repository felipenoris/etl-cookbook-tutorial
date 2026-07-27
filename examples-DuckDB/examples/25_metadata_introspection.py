"""Exemplo 25 — Introspecção: descobrir o que tem no arquivo sem ler o arquivo.

A primeira pergunta de todo ETL que recebe um diretório de parquet de terceiros
é sempre a mesma: *o que tem aqui dentro?* Quais colunas, com que tipos, quantas
linhas, escrito por qual ferramenta, e — a que mais dói descobrir tarde —
**todos os arquivos têm o mesmo schema?**

Nada disso exige ler os dados. Todo arquivo parquet carrega no fim (o *footer*)
um bloco de metadados que descreve o arquivo inteiro, e o DuckDB expõe esse
bloco como quatro table functions. Ler o footer custa **microssegundos**;
varrer os dados custa centenas de milissegundos.

## A hierarquia dos metadados, e a função de cada nível

Um arquivo parquet é uma árvore: o arquivo tem N *row groups*, cada row group
tem uma fatia de cada coluna (o *column chunk*), e o schema descreve as colunas
uma única vez para o arquivo todo.

| Nível | Função | Uma linha por | Responde |
| --- | --- | --- | --- |
| schema | `parquet_schema(f)` | nó do schema | quais colunas e de que tipo |
| arquivo | `parquet_file_metadata(f)` | arquivo | quantas linhas, quem escreveu, tamanho |
| row group × coluna | `parquet_metadata(f)` | column chunk | compressão, encoding, min/max, nulos |
| chave/valor | `parquet_kv_metadata(f)` | par key/value | metadados livres da ferramenta que escreveu |

Todas as quatro aceitam **glob** e devolvem `file_name` — é isso que permite
comparar arquivos entre si numa query só, que é o que o detector de schema
drift no fim deste exemplo faz.

## Dois tipos para a mesma coluna: físico e lógico

O parquet só sabe guardar meia dúzia de tipos físicos (`INT32`, `INT64`,
`BYTE_ARRAY`, ...). Tudo o mais é um tipo físico **mais uma anotação lógica**:
uma data é `INT32` + `DateType()`, um texto é `BYTE_ARRAY` + `StringType()`.
`parquet_schema` mostra as duas colunas lado a lado — e ainda uma terceira,
`duckdb_type`, com o tipo já traduzido para o sistema de tipos do DuckDB.

## Metadados do arquivo ≠ catálogo do banco

As funções `parquet_*` leem **arquivos em disco**. Para tabelas que vivem
dentro do DuckDB o caminho é outro: `DESCRIBE`, `duckdb_columns()`,
`duckdb_tables()`, `information_schema.columns`. O `DESCRIBE` é a ponte entre
os dois mundos — ele aceita tanto um nome de tabela quanto uma query
qualquer, inclusive `SELECT * FROM read_parquet(...)`.

Rode com: `uv run examples/25_metadata_introspection.py`
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

import duckdb

from _common import ORDERS_GLOB, RAW_DIR, RICH_DIR, section

# diretório onde o exemplo escreve os arquivos propositalmente divergentes
DRIFT_DIR = RICH_DIR / "duckdb_introspection_demo"
DRIFT_GLOB = str(DRIFT_DIR / "*.parquet")

# um arquivo concreto para as inspeções que não precisam do glob inteiro:
# a primeira partição de orders, seja qual for o ano/mês gerado
UM_ARQUIVO = str(sorted((RAW_DIR / "orders").rglob("*.parquet"))[0])


def schema_por_arquivo(
    con: duckdb.DuckDBPyConnection, glob: str
) -> dict[str, list[tuple[str, str]]]:
    """Mapeia cada arquivo do glob para sua lista `[(coluna, tipo)]` na ordem do arquivo.

    Uma consulta só, para o diretório inteiro: `parquet_schema` aceita glob e
    traz `file_name`. O filtro `type IS NOT NULL` descarta o **nó raiz** do
    schema (o parquet representa o registro como um nó pai que agrupa as
    colunas; ele não tem tipo físico). `column_id` preserva a ordem original
    das colunas dentro de cada arquivo.
    """
    linhas = con.execute(
        """
        SELECT file_name, name, duckdb_type
        FROM parquet_schema(?)
        WHERE type IS NOT NULL
        ORDER BY file_name, column_id
        """,
        [glob],
    ).fetchall()

    schemas: dict[str, list[tuple[str, str]]] = {}
    for arquivo, coluna, tipo in linhas:
        schemas.setdefault(arquivo, []).append((coluna, tipo))
    return schemas


def detectar_drift(schemas: dict[str, list[tuple[str, str]]]) -> list[str]:
    """Compara todos os schemas contra o do primeiro arquivo e descreve as diferenças.

    Devolve uma lista de mensagens (vazia = todos os arquivos batem). A
    referência é o primeiro arquivo em ordem alfabética — numa carga real seria
    o schema contratado, guardado fora dos dados; aqui o primeiro arquivo faz
    esse papel para o exemplo ficar autocontido.
    """
    if not schemas:
        return []

    arquivos = sorted(schemas)
    referencia = dict(schemas[arquivos[0]])
    problemas: list[str] = []

    for arquivo in arquivos[1:]:
        atual = dict(schemas[arquivo])
        nome = Path(arquivo).name
        for coluna in sorted(atual.keys() - referencia.keys()):
            problemas.append(f"{nome}: coluna a mais: {coluna}")
        for coluna in sorted(referencia.keys() - atual.keys()):
            problemas.append(f"{nome}: coluna faltando: {coluna}")
        for coluna in sorted(atual.keys() & referencia.keys()):
            if atual[coluna] != referencia[coluna]:
                problemas.append(
                    f"{nome}: tipo diferente em {coluna}: "
                    f"{atual[coluna]} (referência: {referencia[coluna]})"
                )
    return problemas


def preparar_diretorio_divergente(con: duckdb.DuckDBPyConnection) -> None:
    """Escreve 4 arquivos pequenos: dois iguais, um com coluna a mais, um com tipo trocado."""
    shutil.rmtree(DRIFT_DIR, ignore_errors=True)
    DRIFT_DIR.mkdir(parents=True, exist_ok=True)

    base = f"SELECT order_id, customer_id, quantity, status FROM read_parquet('{UM_ARQUIVO}')"
    arquivos = {
        # os dois primeiros são o "contrato": mesmas colunas, mesmos tipos
        "01_ok.parquet": f"{base} LIMIT 1000",
        "02_ok.parquet": f"{base} LIMIT 1000 OFFSET 1000",
        # uma coluna nova apareceu na origem
        "03_coluna_extra.parquet": (
            "SELECT order_id, customer_id, quantity, status, "
            f"CAST(0.10 AS DECIMAL(4,2)) AS desconto FROM read_parquet('{UM_ARQUIVO}') LIMIT 1000"
        ),
        # o tipo de quantity mudou de INTEGER para BIGINT
        "04_tipo_alterado.parquet": (
            "SELECT order_id, customer_id, CAST(quantity AS BIGINT) AS quantity, status "
            f"FROM read_parquet('{UM_ARQUIVO}') LIMIT 1000"
        ),
    }
    for nome, sql in arquivos.items():
        con.execute(f"COPY ({sql}) TO '{DRIFT_DIR / nome}' (FORMAT parquet)")


if __name__ == "__main__":
    con = duckdb.connect()

    section("parquet_schema: as colunas e seus dois tipos (físico e lógico)")
    con.sql(
        f"""
        SELECT name AS coluna, type AS tipo_fisico, logical_type AS anotacao_logica,
               duckdb_type AS tipo_duckdb, repetition_type AS repeticao
        FROM parquet_schema('{UM_ARQUIVO}')
        WHERE type IS NOT NULL          -- descarta o nó raiz do schema
        ORDER BY column_id
        """
    ).show()
    print("INT32 + DateType() = DATE; BYTE_ARRAY + StringType() = VARCHAR.")
    print("repetition_type OPTIONAL é como o parquet diz 'esta coluna aceita nulo'.")

    section("parquet_file_metadata: o footer de cada partição, num glob só")
    con.sql(
        f"""
        SELECT regexp_extract(file_name, 'order_month=(\\d+)', 1) AS mes,
               num_rows, num_row_groups, created_by,
               round(file_size_bytes / 1024.0 / 1024, 1) AS mb, footer_size AS footer_bytes
        FROM parquet_file_metadata('{ORDERS_GLOB}')
        ORDER BY mes
        """
    ).show()
    print("O footer inteiro que descreve 5,6M de linhas cabe em ~5KB — por isso")
    print("responder 'quantas linhas tem?' pelos metadados é praticamente de graça.")

    section("parquet_metadata: descendo ao row group")
    con.sql(
        f"""
        SELECT row_group_id, row_group_num_rows AS linhas,
               round(row_group_bytes / 1024.0 / 1024, 1) AS mb
        FROM parquet_metadata('{UM_ARQUIVO}')
        WHERE column_id = 0             -- uma linha por row group, não por coluna
        ORDER BY row_group_id
        """
    ).show()
    print("Row groups de ~1M de linhas: são a unidade de paralelismo da leitura")
    print("e o grão das zonemaps (min/max) que o exemplo 12 usa para pular dados.")

    section("parquet_metadata: compressão e encoding, coluna a coluna")
    con.sql(
        f"""
        SELECT path_in_schema AS coluna,
               any_value(compression) AS compressao,
               any_value(encodings) AS encodings,
               round(sum(total_uncompressed_size) / 1024.0 / 1024, 1) AS mb_bruto,
               round(sum(total_compressed_size) / 1024.0 / 1024, 1) AS mb_final,
               round(sum(total_uncompressed_size)
                     / sum(total_compressed_size), 2) AS ganho_do_snappy,
               sum(stats_null_count) AS nulos
        FROM parquet_metadata('{UM_ARQUIVO}')
        GROUP BY coluna
        ORDER BY mb_bruto DESC
        """
    ).show()
    print("Leia a coluna 'ganho_do_snappy' junto com 'encodings': o RLE_DICTIONARY")
    print("já comprimiu as colunas de baixa cardinalidade ANTES do snappy — nelas o")
    print("snappy não tem mais o que fazer (ganho 1.00). Só order_id, que é único e")
    print("crescente (dicionário inútil), chega ao snappy ainda gordo e encolhe.")
    print("'nulos' vem de stats_null_count: dá para auditar completude sem ler nada.")

    section("parquet_kv_metadata: o espaço livre para a ferramenta que escreveu")
    con.sql(
        f"""
        SELECT CAST(key AS VARCHAR) AS chave, octet_length(value) AS tamanho_do_valor
        FROM parquet_kv_metadata('{UM_ARQUIVO}')
        """
    ).show()
    print("key e value são BLOB (podem guardar qualquer coisa) — daí o CAST para ler.")
    print("Aqui há um único par: ARROW:schema, o schema Arrow serializado que o")
    print("pyarrow embute para preservar tipos que o parquet sozinho não distingue.")
    print("É neste espaço que um ETL grava sua própria proveniência (versão do job,")
    print("hash do fonte, timestamp da carga) — e depois audita sem abrir os dados.")

    section("Descobrir o schema sem ler os dados: DESCRIBE contra varredura")
    inicio = time.perf_counter()
    colunas = con.sql(f"DESCRIBE SELECT * FROM read_parquet('{ORDERS_GLOB}')").fetchall()
    t_describe = time.perf_counter() - inicio

    inicio = time.perf_counter()
    (linhas_footer,) = con.sql(
        f"SELECT sum(num_rows) FROM parquet_file_metadata('{ORDERS_GLOB}')"
    ).fetchone()
    t_footer = time.perf_counter() - inicio

    inicio = time.perf_counter()
    con.sql(f"SELECT sum(quantity) FROM read_parquet('{ORDERS_GLOB}')").fetchone()
    t_scan = time.perf_counter() - inicio

    print(f"DESCRIBE do glob inteiro:      {t_describe * 1000:7.1f}ms ({len(colunas)} colunas)")
    print(f"sum(num_rows) pelo footer:     {t_footer * 1000:7.1f}ms ({linhas_footer:,} linhas)")
    print(f"sum(quantity), varredura real: {t_scan * 1000:7.1f}ms")
    print(f"-> descobrir o schema é {t_scan / t_describe:.0f}x mais barato que ler uma coluna")
    print("(o DESCRIBE ainda inclui order_year/order_month, que não estão dentro de")
    print(" arquivo nenhum: vêm do nome dos diretórios, via hive partitioning)")

    section("O outro lado: o catálogo do próprio DuckDB")
    con.execute(
        f"""
        CREATE TABLE orders_amostra AS
        SELECT * FROM read_parquet('{UM_ARQUIVO}') LIMIT 100000
        """
    )

    print("DESCRIBE <tabela> — o caminho mais curto:")
    con.sql("DESCRIBE orders_amostra").show()

    print("duckdb_columns() — a mesma informação como TABELA, então filtrável e joinável:")
    con.sql(
        """
        SELECT column_name, data_type, is_nullable, column_index
        FROM duckdb_columns()
        WHERE table_name = 'orders_amostra'
        ORDER BY column_index
        """
    ).show()

    print("information_schema.columns — o equivalente padrão SQL (portável entre bancos):")
    con.sql(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'orders_amostra'
        ORDER BY ordinal_position
        LIMIT 3
        """
    ).show()

    print("duckdb_tables() — o nível acima: o que existe no catálogo:")
    con.sql(
        """
        SELECT table_name, column_count, estimated_size AS linhas_estimadas,
               has_primary_key, temporary
        FROM duckdb_tables()
        """
    ).show()

    print("E no lado Python, o schema do result set vem pela DBAPI, sem SQL nenhum:")
    for nome, tipo, *_ in con.execute("SELECT order_id, status FROM orders_amostra").description:
        print(f"  {nome:10s} -> {tipo}")

    section("Aplicação: um detector de schema drift entre partições")
    schemas_reais = schema_por_arquivo(con, ORDERS_GLOB)
    problemas = detectar_drift(schemas_reais)
    print(f"orders: {len(schemas_reais)} arquivos inspecionados (uma query de metadados)")
    veredito = "todas as partições batem" if not problemas else "há divergências"
    print(f"divergências encontradas: {len(problemas)} — {veredito}")

    print("\nAgora o mesmo detector sobre um diretório propositalmente quebrado:")
    preparar_diretorio_divergente(con)
    schemas_drift = schema_por_arquivo(con, DRIFT_GLOB)
    for arquivo in sorted(schemas_drift):
        colunas_fmt = ", ".join(f"{c}:{t}" for c, t in schemas_drift[arquivo])
        print(f"  {Path(arquivo).name:24s} {colunas_fmt}")

    print("\nrelatório do detector (referência = 01_ok.parquet):")
    for problema in detectar_drift(schemas_drift):
        print(f"  ! {problema}")

    section("Quando dá para conviver com a divergência: union_by_name")
    con.sql(f"DESCRIBE SELECT * FROM read_parquet('{DRIFT_GLOB}', union_by_name=true)").show()
    print("union_by_name casa as colunas pelo NOME (não pela posição), preenche com")
    print("nulo o que falta e promove tipos compatíveis (INTEGER + BIGINT -> BIGINT).")
    print("Sem ele, o read_parquet exige que todos os arquivos tenham o mesmo schema")
    print("posicional — e falha, ou pior, alinha colunas erradas.")

    section("A lição")
    print("1. o footer descreve o arquivo inteiro e custa microssegundos para ler;")
    print("   toda pergunta sobre ESTRUTURA (colunas, tipos, contagem, nulos, tamanho)")
    print("   deve ser respondida por ele, nunca por uma varredura;")
    print("2. as quatro funções são a mesma árvore em níveis diferentes: schema ->")
    print("   arquivo -> row group x coluna -> chave/valor; escolha o nível da pergunta;")
    print("3. todas aceitam glob e trazem file_name — comparar mil arquivos entre si")
    print("   é UMA query, e é assim que se detecta schema drift antes de ele virar bug;")
    print("4. arquivo em disco é parquet_*(); tabela no banco é DESCRIBE/duckdb_columns()")
    print("   /information_schema; o DESCRIBE atravessa os dois lados;")
    print("5. detectado o drift, union_by_name resolve o caso benigno (coluna nova,")
    print("   tipo promovível); o resto é problema de contrato, e aí o ETL deve parar.")

    shutil.rmtree(DRIFT_DIR, ignore_errors=True)
