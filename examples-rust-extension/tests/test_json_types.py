"""Testes do JSON opaco: o tipo de extensão `arrow.json` atravessando a stack.

Enquanto `test_data_types.py` cobre o aninhamento TIPADO (struct/list/map), aqui
o assunto é o documento de texto e o marcador que o distingue de uma string
qualquer. Os contratos exercitados:

- o marcador `arrow.json` sobrevive a Parquet, ao Rust e à volta;
- o `DuckDB -> Arrow` o descarta por padrão (regressão de ambiente, não do
  nosso código) e `arrow_lossless_conversion` o preserva;
- o Rust RECUSA uma coluna sem marcador em vez de adivinhar;
- um round-trip JSON garante igualdade SEMÂNTICA, nunca de bytes;
- o shred entrega decimal exato (do token cru) onde o caminho f64 degrada;
- os três estados do JSON (presente/nulo/ausente) não colapsam.
"""

import json

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from etl_rust_ext import (
    as_json_column,
    is_json_column,
    normalize_json_column,
    shred_json_column,
    sum_decimal_column,
)

DOCS = [
    '{"canal":"web","valor":0.10,"tags":["a","b"],"cliente":{"id":7}}',
    '{"canal":null,"valor":0.20,"tags":[]}',
    '{"canal":"loja","valor":null}',
    '{"canal":"loja"}',
]


def make_json_batch(docs=None) -> pa.RecordBatch:
    """Batch com uma coluna `id` (int64) e uma coluna `arrow.json`."""
    docs = DOCS if docs is None else docs
    storage = pa.array(docs, type=pa.string())
    return pa.record_batch(
        {
            "id": pa.array(range(1, len(docs) + 1), type=pa.int64()),
            "payload": pa.ExtensionArray.from_storage(pa.json_(), storage),
        }
    )


def make_plain_batch(docs=None) -> pa.RecordBatch:
    """O MESMO conteúdo, mas como utf8 puro — o marcador perdido."""
    docs = DOCS if docs is None else docs
    return pa.record_batch(
        {
            "id": pa.array(range(1, len(docs) + 1), type=pa.int64()),
            "payload": pa.array(docs, type=pa.string()),
        }
    )


class TestMarcadorArrowJson:
    def test_extension_type_is_utf8_plus_marker(self):
        # `arrow.json` não é um DataType novo: é storage utf8 + marcador
        assert pa.json_().storage_type == pa.string()
        assert pa.json_().extension_name == "arrow.json"

    def test_marker_survives_the_rust_boundary(self):
        # o pyo3-arrow leva o marcador nas DUAS direções (Arrow C Data Interface)
        out = normalize_json_column(make_json_batch(), "payload")
        assert out.schema.field("payload").type == pa.json_()
        assert is_json_column(out, "payload")

    def test_marker_survives_parquet(self, tmp_path):
        arquivo = tmp_path / "eventos.parquet"
        pq.write_table(pa.Table.from_batches([make_json_batch()]), arquivo)
        # no arquivo ele vira o logical type JSON do Parquet, não só metadado Arrow
        assert str(pq.read_metadata(arquivo).schema.column(1).logical_type) == "JSON"
        assert pq.read_schema(arquivo).field("payload").type == pa.json_()

    def test_rust_refuses_a_column_without_the_marker(self):
        # falhar alto > adivinhar: uma utf8 sem marcador pode ser qualquer texto
        with pytest.raises(ValueError, match="arrow.json"):
            normalize_json_column(make_plain_batch(), "payload")
        with pytest.raises(ValueError, match="arrow.json"):
            shred_json_column(make_plain_batch(), "payload")

    def test_as_json_column_repairs_and_is_idempotent(self):
        remarcado = as_json_column(make_plain_batch(), "payload")
        assert is_json_column(remarcado, "payload")
        assert remarcado.column("payload").to_pylist() == DOCS
        assert as_json_column(remarcado, "payload") is remarcado  # já marcada

    def test_as_json_column_rejects_non_utf8_storage(self):
        batch = pa.record_batch({"payload": pa.array([[1, 2]], type=pa.list_(pa.int64()))})
        with pytest.raises(TypeError, match="utf8"):
            as_json_column(batch, "payload")

    def test_as_json_column_unknown_column(self):
        with pytest.raises(ValueError, match="não encontrada"):
            as_json_column(make_plain_batch(), "inexistente")

    def test_is_json_column_is_false_for_plain_utf8_and_missing(self):
        assert not is_json_column(make_plain_batch(), "payload")
        assert not is_json_column(make_json_batch(), "inexistente")


class TestDuckDBDescartaOMarcador:
    """O elo fraco da corrente — e a flag que o conserta.

    Não é um bug do nosso código: é o comportamento default do DuckDB, e o
    teste existe para travá-lo (se um dia mudar, queremos saber).
    """

    def _relation(self, con):
        con.execute("CREATE TABLE eventos (id INTEGER, payload JSON)")
        con.execute("INSERT INTO eventos VALUES (1, '{\"a\":1}')")
        return con.sql("SELECT payload FROM eventos")

    def test_default_conversion_drops_the_marker(self):
        con = duckdb.connect()
        assert self._relation(con).types[0] == "JSON"  # no SQL, é JSON
        tabela = con.sql("SELECT payload FROM eventos").to_arrow_table()
        assert tabela.schema.field("payload").type == pa.string()  # no Arrow, não

    def test_lossless_conversion_preserves_the_marker(self):
        con = duckdb.connect()
        self._relation(con)
        con.execute("SET arrow_lossless_conversion = true")
        tabela = con.sql("SELECT payload FROM eventos").to_arrow_table()
        assert tabela.schema.field("payload").type == pa.json_()

    def test_arrow_json_goes_back_into_duckdb_as_json(self):
        # a volta funciona sem flag nenhuma: o replacement scan reconhece o tipo
        con = duckdb.connect()
        marcado = pa.Table.from_batches([make_json_batch()])  # noqa: F841
        assert con.sql("SELECT payload FROM marcado").types[0] == "JSON"


class TestNormalizeJsonColumn:
    def test_semantic_equality_but_not_byte_equality(self):
        entrada = make_json_batch(['{"b": 1,  "a": {"z": [1, 2]}}'])
        saida = normalize_json_column(entrada, "payload")
        antes = entrada.column("payload").to_pylist()[0]
        depois = saida.column("payload").to_pylist()[0]

        assert antes != depois  # whitespace some, chaves são ordenadas
        assert json.loads(antes) == json.loads(depois)  # o contrato que VALE

    def test_other_columns_are_preserved(self):
        saida = normalize_json_column(make_json_batch(), "payload")
        assert saida.column("id").to_pylist() == [1, 2, 3, 4]
        assert saida.schema.names == ["id", "payload"]

    def test_nulls_stay_null(self):
        storage = pa.array(['{"a":1}', None], type=pa.string())
        batch = pa.record_batch(
            {"payload": pa.ExtensionArray.from_storage(pa.json_(), storage)}
        )
        assert normalize_json_column(batch, "payload").column("payload").to_pylist()[1] is None

    def test_malformed_document_raises(self):
        with pytest.raises(ValueError, match="não é JSON válido"):
            normalize_json_column(make_json_batch(['{"a":']), "payload")

    def test_unknown_column_raises(self):
        with pytest.raises(ValueError, match="não encontrada"):
            normalize_json_column(make_json_batch(), "inexistente")


class TestShredJsonColumn:
    def test_output_schema(self):
        out = shred_json_column(make_json_batch(), "payload")
        # a coluna JSON dá lugar às seis tipadas, na mesma posição
        assert out.schema.names == [
            "id", "canal", "valor_exato", "valor_float", "valor_estado",
            "num_tags", "cliente_id",
        ]
        assert out.schema.field("valor_exato").type == pa.decimal128(12, 2)
        assert out.schema.field("valor_float").type == pa.float64()
        assert out.schema.field("num_tags").type == pa.int32()
        assert out.schema.field("cliente_id").type == pa.int64()

    def test_extracted_values(self):
        out = shred_json_column(make_json_batch(), "payload")
        assert out.column("canal").to_pylist() == ["web", None, "loja", "loja"]
        assert out.column("num_tags").to_pylist() == [2, 0, None, None]
        assert out.column("cliente_id").to_pylist() == [7, None, None, None]
        assert out.column("id").to_pylist() == [1, 2, 3, 4]  # colunas vizinhas intactas

    def test_the_three_json_states_do_not_collapse(self):
        # presente / nulo / ausente — o SQL colapsa os dois últimos em NULL
        out = shred_json_column(make_json_batch(), "payload")
        assert out.column("valor_estado").to_pylist() == [
            "presente", "presente", "nulo", "ausente",
        ]

    def test_decimal_comes_from_the_raw_token_not_from_f64(self):
        from decimal import Decimal

        out = shred_json_column(make_json_batch(), "payload")
        assert out.column("valor_exato").to_pylist()[:2] == [Decimal("0.10"), Decimal("0.20")]
        assert all(isinstance(v, Decimal) for v in out.column("valor_exato").to_pylist()[:2])

    def test_summing_exposes_the_f64_degradation(self):
        from decimal import Decimal

        # o motivo de as duas colunas existirem lado a lado: 0.10+0.20+0.30
        batch = make_json_batch(
            ['{"valor":0.10}', '{"valor":0.20}', '{"valor":0.30}']
        )
        out = shred_json_column(batch, "payload")
        assert sum_decimal_column(out, "valor_exato") == Decimal("0.60")  # exato
        assert pc.sum(out.column("valor_float")).as_py() != 0.60  # degradado

    def test_preserves_trailing_zero_scale(self):
        from decimal import Decimal

        # `0.5` no texto vira 0.50 na coluna decimal128(12,2): a ESCALA é do
        # schema, não do documento — JSON não tem como carregá-la
        out = shred_json_column(make_json_batch(['{"valor":0.5}']), "payload")
        assert out.column("valor_exato").to_pylist() == [Decimal("0.50")]

    def test_non_numeric_valor_raises(self):
        with pytest.raises(ValueError, match="valor"):
            shred_json_column(make_json_batch(['{"valor":"muito"}']), "payload")

    def test_non_object_document_raises(self):
        with pytest.raises(ValueError, match="não é um objeto JSON"):
            shred_json_column(make_json_batch(["[1, 2, 3]"]), "payload")

    def test_null_document_yields_nulls(self):
        storage = pa.array([None], type=pa.string())
        batch = pa.record_batch(
            {"payload": pa.ExtensionArray.from_storage(pa.json_(), storage)}
        )
        out = shred_json_column(batch, "payload")
        assert out.column("canal").to_pylist() == [None]
        assert out.column("valor_exato").to_pylist() == [None]
        assert out.column("valor_estado").to_pylist() == ["ausente"]


class TestStackCompleta:
    def test_parquet_duckdb_rust_parquet_roundtrip(self, tmp_path):
        """A corrente inteira, com o marcador verificado em cada elo."""
        origem = tmp_path / "origem.parquet"
        pq.write_table(pa.Table.from_batches([make_json_batch()]), origem)

        # Parquet -> DuckDB (JSON nativo) -> Arrow (com a flag) -> Rust
        con = duckdb.connect()
        con.execute("SET arrow_lossless_conversion = true")
        rel = con.sql(f"SELECT id, payload FROM read_parquet('{origem}') ORDER BY id")
        assert rel.types[1] == "JSON"

        tabela = rel.to_arrow_table()
        assert tabela.schema.field("payload").type == pa.json_()

        batch = tabela.combine_chunks().to_batches()[0]
        enriquecido = normalize_json_column(batch, "payload")
        assert enriquecido.schema.field("payload").type == pa.json_()

        # Rust -> Parquet, e o marcador continua lá
        destino = tmp_path / "destino.parquet"
        pq.write_table(pa.Table.from_batches([enriquecido]), destino)
        assert pq.read_schema(destino).field("payload").type == pa.json_()
        assert str(pq.read_metadata(destino).schema.column(1).logical_type) == "JSON"

        # e o conteúdo continua semanticamente igual ao que entrou
        final = pq.read_table(destino).column("payload").to_pylist()
        assert [json.loads(d) for d in final] == [json.loads(d) for d in DOCS]
