"""Exemplo 19 — JSON no DuckDB: ingestão, extração por caminho e o contraste com tipos nativos.

Todo o resto do tutorial trata dados aninhados como colunas **tipadas** do
parquet: `STRUCT` (`address.city`), `LIST` (`tags[1]`) e `MAP`
(`preferences['canal']`) — ver exemplo 14. JSON é a outra forma de guardar
aninhamento: um **documento de texto**, sem schema declarado. O DuckDB traz o
extension `json` embutido (sem instalar nada) e trata JSON em dois regimes bem
diferentes — este exemplo mostra os dois e quando escolher cada um.

Regime 1 — **JSON opaco como texto** (a Parte A): a coluna guarda o documento
inteiro como string e você extrai campos por *caminho* em tempo de consulta.
É o que brilha quando o payload é **heterogêneo ou variável** (cada linha tem
uma forma), o caso em que um schema fixo não caberia. Operadores/funções:

- `doc->'$.a.b'` devolve o valor como **JSON** (mantém aspas, aninhamento);
- `doc->>'$.a.b'` devolve como **texto** (`VARCHAR`) já desembrulhado;
- `json_extract` / `json_extract_string` são as formas por função;
- caminhos: `$.chave`, `$.lista[0]`, `$.lista[*].campo` (curinga);
- introspecção: `json_keys`, `json_array_length`, `json_type`.

Regime 2 — **JSON sniffado para colunas tipadas** (a Parte B): `read_json_auto`
lê um arquivo, **infere o schema** (objetos viram `STRUCT`, arrays viram
`LIST`) e devolve colunas normais — a partir daí é dot-notation, como se fosse
parquet. É o caminho de **ingestão**: JSON entra na borda, vira colunar.

A recomendação (a Parte C) fecha o raciocínio: se a forma é **estável**,
materialize o JSON em colunas tipadas uma vez (parquet `STRUCT`/`MAP`/`LIST`) —
é validado, colunar, comprime melhor e permite pushdown; guarde JSON de texto
só para o que é **genuinamente sem schema**.

Rode com: `uv run examples/19_json_ingestion_and_extraction.py`
"""

import shutil

import duckdb

from _common import CUSTOMERS_GLOB, RICH_DIR, section

OUT_DIR = RICH_DIR / "duckdb_json_demo"

if __name__ == "__main__":
    con = duckdb.connect()

    # =====================================================================
    # PARTE A — JSON opaco (texto): payload heterogêneo, extração por caminho
    # =====================================================================
    section("A) Payload heterogêneo guardado como JSON de texto (cada linha tem outra forma)")
    con.execute(
        """
        CREATE TABLE eventos (id INTEGER, payload JSON);
        INSERT INTO eventos VALUES
            (1, '{"tipo":"login","user":{"id":7,"plano":"pro"},"tags":["web","novo"]}'),
            (2, '{"tipo":"compra","valor":19.90,"itens":[{"sku":"X1"},{"sku":"X2"}]}'),
            (3, '{"tipo":"login","user":{"id":9}}');
        """
    )
    con.sql("SELECT id, payload FROM eventos").show(max_width=100)

    section("Extração por CAMINHO: ->> (texto) e -> (JSON), tolerando campos ausentes")
    con.sql(
        """
        SELECT id,
               payload->>'$.tipo'                     AS tipo,       -- ->> devolve VARCHAR
               payload->>'$.user.plano'               AS plano,      -- caminho aninhado
               payload->>'$.valor'                    AS valor,      -- NULL onde não existe
               json_array_length(payload->'$.itens')  AS n_itens,    -- -> mantém JSON p/ a função
               json_keys(payload)                     AS chaves_raiz
        FROM eventos
        """
    ).show(max_width=120)
    print("-> cada linha tem uma forma diferente e nada quebra: campo ausente vira NULL.")
    print("   É exatamente o cenário em que um STRUCT de schema fixo NÃO caberia.")

    section("Curinga [*] e UNNEST: explodindo um array DENTRO do documento em linhas")
    con.sql(
        """
        SELECT e.id, item.sku
        FROM eventos e,
             UNNEST(json_extract_string(e.payload, '$.itens[*].sku')) AS item(sku)
        WHERE json_array_length(e.payload->'$.itens') > 0
        ORDER BY item.sku
        """
    ).show()

    section("Introspecção: json_type descreve a forma sem conhecê-la de antemão")
    con.sql("SELECT id, json_type(payload->'$.user') AS tipo_do_campo_user FROM eventos ORDER BY id").show()

    # =====================================================================
    # PARTE B — read_json_auto: ingestão que SNIFFA o schema (JSON -> colunar)
    # =====================================================================
    section("B) Ingestão: gerando um JSON de linhas a partir dos dados nativos")
    shutil.rmtree(OUT_DIR, ignore_errors=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    json_file = OUT_DIR / "clientes.json"
    # COPY ... (FORMAT json) grava JSON de linhas (NDJSON); struct->objeto, list->array, map->objeto
    con.execute(
        f"""
        COPY (
            SELECT customer_id,
                   customer_name AS nome,
                   address       AS endereco,     -- STRUCT vira objeto JSON
                   tags,                          -- LIST vira array JSON
                   preferences   AS prefs         -- MAP vira objeto JSON
            FROM read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true)
            WHERE customer_id <= 1000
        ) TO '{json_file}' (FORMAT json)
        """
    )
    print("primeira linha do arquivo (NDJSON — um objeto por linha):")
    print("  " + json_file.read_text().splitlines()[0][:140] + " ...")

    section("read_json_auto INFERE o schema: objeto->STRUCT, array->LIST")
    con.sql(f"DESCRIBE SELECT * FROM read_json_auto('{json_file}')").show(max_width=90)
    print("-> uma vez inferido, é dot-notation como em parquet (o JSON de texto sumiu):")
    con.sql(
        f"""
        SELECT endereco.city AS cidade, COUNT(*) AS clientes
        FROM read_json_auto('{json_file}')
        GROUP BY cidade ORDER BY clientes DESC LIMIT 5
        """
    ).show()

    # =====================================================================
    # PARTE C — o contraste: os MESMOS dados como colunas nativas do parquet
    # =====================================================================
    section("C) Os mesmos dados já são STRUCT/LIST/MAP tipados no parquet (exemplo 14)")
    con.sql(
        f"""
        SELECT address.city          AS cidade,          -- STRUCT: dot-notation nativa
               tags[1]               AS primeira_tag,     -- LIST: índice 1-based
               preferences['canal']  AS canal            -- MAP: acesso por chave
        FROM read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true)
        WHERE customer_id <= 3
        ORDER BY customer_id
        """
    ).show()

    section("Quando usar cada um")
    print("- JSON de texto (Parte A): payload heterogêneo/variável, schema desconhecido,")
    print("  campos que aparecem e somem — extração por caminho tolera tudo isso;")
    print("- read_json_auto (Parte B): ingestão na borda — sniffa e entrega colunas tipadas;")
    print("- colunas nativas STRUCT/LIST/MAP (Parte C): a forma é ESTÁVEL — materialize uma")
    print("  vez em parquet. É validado, colunar, comprime melhor e permite pushdown/pruning.")
    print("  Regra: JSON entra na borda; o miolo do lakehouse é tipado.")
