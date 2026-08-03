"""JSON opaco atravessando a stack: Parquet -> DuckDB -> Arrow -> Rust -> Parquet.

Os 11 tipos do ``run_data_types.py`` cobrem o aninhamento **tipado**
(``struct``/``list``/``map``): a forma é conhecida, está no schema e é validada
em toda camada. JSON é o outro regime — um documento de **texto** cuja forma
varia por linha, útil justamente onde um schema fixo não caberia.

No Arrow, JSON não é um ``DataType`` novo: é o tipo de extensão canônico
``arrow.json``, que são **duas coisas** — storage ``utf8`` mais um marcador
(``ARROW:extension:name``) nos metadados do campo. Todo o cuidado deste exemplo
é com o marcador, porque ele é a única coisa que separa um documento de uma
string qualquer, e porque ele **se perde em silêncio**: os dados chegam
íntegros, só a semântica some, e o erro só aparece muitos hops depois.

O que cada parte demonstra:

- **[1/4] O elo fraco da corrente.** O marcador sobrevive a Parquet, pandas,
  ``write_dataset`` particionado e à fronteira com o Rust. Ele NÃO sobrevive ao
  ``DuckDB -> Arrow`` por padrão: a coluna ``JSON`` volta como ``utf8`` simples,
  a menos que a conexão declare ``SET arrow_lossless_conversion = true``.
- **[2/4] Parquet.** Uma coluna ``arrow.json`` vira o *logical type* ``JSON`` do
  Parquet e volta como ``arrow.json`` — o marcador viaja no arquivo, não só na
  memória.
- **[3/4] Round-trip pelo Rust** (``normalize_json_column``): o Rust devolve o
  marcador que recebeu, mas o texto **não volta byte-a-byte** — o whitespace
  some e as chaves saem ordenadas. O contrato de um round-trip JSON é
  igualdade **semântica**, nunca de bytes.
- **[4/4] Shredding** (``shred_json_column``): a recomendação em código —
  documento opaco vira colunas tipadas. Duas armadilhas ficam visíveis: o
  **decimal**, que degrada para float na leitura ingênua, e os **três estados**
  (presente / nulo / ausente) que o SQL colapsa em um só.

A regra que sai daqui: **nada que precisa ser exato viaja DENTRO do JSON.**
JSON não tem tipo decimal nem tipo data — dinheiro e datas são colunas
shredded, sempre. JSON opaco carrega só o que é genuinamente sem schema.

Rode com: ``uv run run_json_types.py`` (a partir de ``rust-extension``).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from etl_rust_ext import (
    as_json_column,
    is_json_column,
    normalize_json_column,
    shred_json_column,
    sum_decimal_column,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw"
OUT_DIR = REPO_ROOT / "data" / "rich" / "rust_json_demo"
CUSTOMERS_GLOB = str(RAW_DIR / "customers" / "**" / "*.parquet")


def section(title: str) -> None:
    """Imprime um título de seção, separando as etapas na saída do script."""
    print(f"\n{'=' * 10} {title} {'=' * 10}")


def build_json_from_raw(con: duckdb.DuckDBPyConnection) -> None:
    """Cria uma tabela com um documento JSON por cliente, a partir de data/raw.

    Os dados de origem já são tipados (``struct``/``list``/``map``); aqui eles
    são **serializados** para texto de propósito, simulando o payload opaco que
    chega de um sistema sem schema declarado. O ``valor`` entra como decimal de
    2 casas — é ele que a parte [4/4] persegue pelas camadas.
    """
    con.execute(
        f"""
        CREATE TABLE eventos AS
        SELECT customer_id,
               to_json({{
                   'canal':  preferences['canal'],
                   'valor':  ROUND(customer_id % 3 * 0.10 + 0.10, 2)::DECIMAL(12,2),
                   'tags':   tags,
                   'cliente': {{'id': customer_id, 'cidade': address.city}}
               }}) AS payload
        FROM read_parquet('{CUSTOMERS_GLOB}', hive_partitioning=true)
        WHERE customer_id <= 6
        ORDER BY customer_id
        """
    )


def main() -> None:
    """Percorre em 4 etapas o caminho do JSON opaco: perda do marcador, reparo, normalização e shredding."""
    con = duckdb.connect()
    build_json_from_raw(con)

    # =====================================================================
    section("[1/4] O elo fraco: DuckDB -> Arrow descarta o marcador")
    # =====================================================================
    print("no SQL do DuckDB a coluna é JSON de verdade:")
    print("  tipos da relação:", con.sql("SELECT customer_id, payload FROM eventos").types)

    padrao = con.sql("SELECT customer_id, payload FROM eventos").to_arrow_table()
    print(f"\n  -> Arrow (default)          : {padrao.schema.field('payload').type}")

    con.execute("SET arrow_lossless_conversion = true")
    lossless = con.sql("SELECT customer_id, payload FROM eventos").to_arrow_table()
    print(f"  -> Arrow (lossless=true)    : {lossless.schema.field('payload').type}")
    print(
        "\n  Os DADOS são idênticos nos dois casos — o que muda é só a semântica.\n"
        "  Uma coluna que perde o marcador não quebra nada na hora: ela vira\n"
        "  texto e segue viajando, até alguém lá na frente tratá-la como string."
    )

    batch = lossless.combine_chunks().to_batches()[0]
    perdido = padrao.combine_chunks().to_batches()[0]

    print("\n  o Rust RECUSA a coluna sem marcador, em vez de adivinhar:")
    try:
        shred_json_column(perdido, "payload")
    except ValueError as exc:
        print(f"    ValueError: {' '.join(str(exc).split())[:96]}...")

    print("\n  reparo explícito, quando a origem não está sob seu controle:")
    remarcado = as_json_column(perdido, "payload")
    print(f"    as_json_column -> {remarcado.schema.field('payload').type} "
          f"(is_json_column: {is_json_column(remarcado, 'payload')})")
    print("    ...mas preferir a flag é melhor: preserva a semântica na ORIGEM.")

    # =====================================================================
    section("[2/4] Parquet: o marcador viaja no arquivo (logical type JSON)")
    # =====================================================================
    shutil.rmtree(OUT_DIR, ignore_errors=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    arquivo = OUT_DIR / "eventos.parquet"
    pq.write_table(pa.Table.from_batches([batch]), arquivo)

    logico = pq.read_metadata(arquivo).schema.column(1).logical_type
    print(f"  logical type gravado no Parquet : {logico}")
    print(f"  tipo ao reler com pyarrow       : {pq.read_schema(arquivo).field('payload').type}")
    print(f"  tipo ao reler com DuckDB        : "
          f"{con.sql(f"SELECT payload FROM read_parquet('{arquivo}')").types[0]}")
    print("\n  -> a ida e a volta do disco preservam a semântica; o Parquet tem\n"
          "     um logical type JSON justamente para isso.")

    # =====================================================================
    section("[3/4] Round-trip pelo Rust: a semântica volta, os bytes não")
    # =====================================================================
    saida = normalize_json_column(batch, "payload")
    print(f"  marcador na saída do Rust: {saida.schema.field('payload').type}\n")

    antes = batch.column("payload").to_pylist()[0]
    depois = saida.column("payload").to_pylist()[0]
    print(f"  antes : {antes}")
    print(f"  depois: {depois}")
    print(f"\n  iguais byte-a-byte? {antes == depois}")
    print("  -> repare, de passagem, no `valor`: ele era DECIMAL(12,2) no DuckDB")
    print("     e virou `0.2` no texto — a serialização já comeu a casa decimal,")
    print("     antes mesmo do Rust. É a parte [4/4] em miniatura.")
    print("  -> reparsear NORMALIZA: whitespace some e as chaves saem ordenadas.")
    print("     Logo o contrato de um round-trip JSON é igualdade SEMÂNTICA.")
    print("     Se o texto exato importar (assinatura, hash, auditoria), guarde")
    print("     os bytes originais numa coluna à parte e não os reparseie.")

    # =====================================================================
    section("[4/4] Shredding: o documento opaco vira colunas tipadas")
    # =====================================================================
    variantes = pa.record_batch(
        {
            "caso": pa.array(["completo", "canal nulo", "valor NULO", "valor AUSENTE"]),
            "payload": pa.ExtensionArray.from_storage(
                pa.json_(),
                pa.array(
                    [
                        '{"canal":"web","valor":0.10,"tags":["a","b"],"cliente":{"id":7}}',
                        '{"canal":null,"valor":0.20,"tags":[]}',
                        '{"canal":"loja","valor":null}',
                        '{"canal":"loja"}',
                    ],
                    type=pa.string(),
                ),
            ),
        }
    )
    shredded = shred_json_column(variantes, "payload")
    print(pa.Table.from_batches([shredded]).to_pandas().to_string(index=False))

    print("\n  (a) os TRÊS estados do JSON que o SQL colapsa em um só:")
    print("      'nulo' (chave presente valendo null) != 'ausente' (chave não existe).")
    print("      No DuckDB, `payload->>'$.valor'` devolve NULL para os DOIS.")

    print("\n  (b) o decimal: JSON não tem tipo decimal, e o caminho ingênuo")
    print("      (json.loads / serde_json::Value) entrega f64. Some as duas colunas:")
    exato = sum_decimal_column(shredded, "valor_exato")
    aprox = pc.sum(shredded.column("valor_float")).as_py()
    print(f"        valor_exato (decimal128, do token cru) : {exato}")
    print(f"        valor_float (f64, caminho degradado)   : {aprox}")
    print(f"        iguais? {str(exato) == str(aprox)}")
    print("\n      -> a REGRA: nada que precisa ser exato viaja DENTRO do JSON.")
    print("         Dinheiro e datas são colunas shredded, sempre. O documento")
    print("         opaco carrega só o que é genuinamente sem schema.")

    con.close()
    print(f"\nParquet de demonstração em: {arquivo.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
