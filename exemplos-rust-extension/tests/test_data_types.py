"""Testes das funções Rust que manipulam os tipos Arrow complexos.

`roundtrip_all_types`: leitura E escrita dos 11 tipos da stack no Rust.
`flatten_customer_profile`: struct, list, map, timestamp, bool e a fronteira
`datetime.date` -> `chrono::NaiveDate` (feature `chrono` do pyo3).
`compute_product_margin`: decimal128(12,2) — sempre 2 casas — e binary.
"""

from datetime import date, datetime, timezone
from decimal import Decimal

import pyarrow as pa
import pytest

from etl_rust_ext import (
    compute_product_margin,
    flatten_customer_profile,
    roundtrip_all_types,
    sum_decimal_column,
)

REF_DATE = date(2026, 1, 1)
REF_TS_US = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000)
UM_DIA_US = 86_400_000_000


def make_customers_batch() -> pa.RecordBatch:
    return pa.record_batch(
        {
            "customer_id": pa.array([1, 2], type=pa.int64()),
            "is_active": pa.array([True, False]),
            "signup_ts": pa.array(
                [REF_TS_US - 10 * UM_DIA_US, REF_TS_US - 100 * UM_DIA_US], type=pa.timestamp("us")
            ),
            "address": pa.array(
                [
                    {"street": "Rua A", "city": "Recife", "zip": "50000-000"},
                    {"street": "Rua B", "city": "Manaus", "zip": "69000-000"},
                ],
                type=pa.struct([("street", pa.string()), ("city", pa.string()), ("zip", pa.string())]),
            ),
            "tags": pa.array([["vip", "online"], []], type=pa.list_(pa.string())),
            "preferences": pa.array(
                [[("canal", "email"), ("idioma", "pt")], [("idioma", "es")]],
                type=pa.map_(pa.string(), pa.string()),
            ),
        }
    )


class TestFlattenCustomerProfile:
    def test_extracts_all_type_families(self):
        out = flatten_customer_profile(make_customers_batch(), REF_DATE)
        assert out.column("city").to_pylist() == ["Recife", "Manaus"]  # struct
        assert out.column("num_tags").to_pylist() == [2, 0]  # list (inclusive vazia)
        assert out.column("canal").to_pylist() == ["email", None]  # map: ausente vira nulo
        assert out.column("dias_desde_cadastro").to_pylist() == [10, 100]  # chrono
        assert out.column("is_active").to_pylist() == [True, False]  # bool

    def test_reference_date_is_a_python_date_and_output_has_date32(self):
        # datetime.date -> chrono::NaiveDate na ida; date32 -> datetime.date na volta
        out = flatten_customer_profile(make_customers_batch(), REF_DATE)
        assert out.schema.field("signup_date").type == pa.date32()
        datas = out.column("signup_date").to_pylist()
        assert datas == [date(2025, 12, 22), date(2025, 9, 23)]
        assert all(isinstance(d, date) for d in datas)

    def test_output_types(self):
        out = flatten_customer_profile(make_customers_batch(), REF_DATE)
        assert out.schema.field("canal").type == pa.string()
        assert out.schema.field("num_tags").type == pa.int32()
        assert out.schema.field("is_active").type == pa.bool_()

    def test_wrong_timestamp_unit_raises(self):
        batch = make_customers_batch()
        idx = batch.schema.get_field_index("signup_ts")
        errado = batch.set_column(
            idx, "signup_ts", batch.column("signup_ts").cast(pa.timestamp("s"))
        )
        with pytest.raises(ValueError, match="timestamp\\[us\\]"):
            flatten_customer_profile(errado, REF_DATE)

    def test_missing_struct_field_raises(self):
        batch = make_customers_batch()
        idx = batch.schema.get_field_index("address")
        sem_city = batch.set_column(
            idx,
            "address",
            pa.array([{"street": "x"}, {"street": "y"}], type=pa.struct([("street", pa.string())])),
        )
        with pytest.raises(ValueError, match="city"):
            flatten_customer_profile(sem_city, REF_DATE)


def make_products_batch() -> pa.RecordBatch:
    return pa.record_batch(
        {
            "product_id": pa.array([1, 2], type=pa.int64()),
            "unit_price": pa.array([10.00, 99.90], type=pa.float64()),
            "unit_cost": pa.array([Decimal("3.50"), Decimal("59.94")], type=pa.decimal128(12, 2)),
            "sku": pa.array([b"\x01\x02", b"\xde\xad\xbe\xef"], type=pa.binary()),
        }
    )


class TestComputeProductMargin:
    def test_margin_is_exact_decimal_with_2_places(self):
        out = compute_product_margin(make_products_batch())
        assert out.schema.field("margin").type == pa.decimal128(12, 2)
        assert out.column("margin").to_pylist() == [Decimal("6.50"), Decimal("39.96")]

    def test_margin_pct_is_float(self):
        out = compute_product_margin(make_products_batch())
        assert out.column("margin_pct").to_pylist()[0] == pytest.approx(0.65)

    def test_sku_hex(self):
        out = compute_product_margin(make_products_batch())
        assert out.column("sku_hex").to_pylist() == ["0102", "deadbeef"]

    def test_wrong_decimal_scale_raises(self):
        batch = make_products_batch()
        idx = batch.schema.get_field_index("unit_cost")
        escala_errada = batch.set_column(
            idx, "unit_cost", batch.column("unit_cost").cast(pa.decimal128(12, 3))
        )
        with pytest.raises(ValueError, match="escala 2"):
            compute_product_margin(escala_errada)

    def test_desconto_as_decimal_crosses_the_boundary(self):
        # decimal.Decimal (Python) -> rust_decimal::Decimal (Rust), exato:
        # preço 10.00 com 10% de desconto -> líquido 9.00; margem 9.00 - 3.50
        out = compute_product_margin(make_products_batch(), desconto=Decimal("0.10"))
        assert out.column("margin").to_pylist()[0] == Decimal("5.50")

    def test_desconto_float_is_rejected(self):
        # a feature rust_decimal do pyo3 NÃO aceita float — exatidão obrigatória
        with pytest.raises(TypeError):
            compute_product_margin(make_products_batch(), desconto=0.10)

    def test_desconto_out_of_range_raises(self):
        with pytest.raises(ValueError, match="desconto"):
            compute_product_margin(make_products_batch(), desconto=Decimal("1.00"))


def make_all_types_batch() -> pa.RecordBatch:
    return pa.record_batch(
        {
            "texto": pa.array(["ana"], type=pa.string()),
            "inteiro": pa.array([41], type=pa.int64()),
            "flutuante": pa.array([1.5], type=pa.float64()),
            "logico": pa.array([True]),
            "data": pa.array([date(2026, 1, 31)], type=pa.date32()),
            "instante": pa.array([datetime(2026, 1, 1, 23, 30)], type=pa.timestamp("us")),
            "valor": pa.array([Decimal("19.90")], type=pa.decimal128(12, 2)),
            "lista": pa.array([["a", "b"]], type=pa.list_(pa.string())),
            "estrutura": pa.array(
                [{"nome": "caixa", "quantidade": 3}],
                type=pa.struct([("nome", pa.string()), ("quantidade", pa.int32())]),
            ),
            "mapa": pa.array([[("k", "v")]], type=pa.map_(pa.string(), pa.string())),
            "binario": pa.array([b"\x01\x02\x03"], type=pa.binary()),
        }
    )


class TestRoundtripAllTypes:
    def test_every_type_read_and_written(self):
        out = roundtrip_all_types(make_all_types_batch())
        linha = {c: out.column(c).to_pylist()[0] for c in out.schema.names}
        assert linha["texto"] == "ANA"
        assert linha["inteiro"] == 42
        assert linha["flutuante"] == 3.0
        assert linha["logico"] is False
        # +30 dias de CALENDÁRIO (chrono): 31/jan -> 02/mar (fev tem 28 em 2026)
        assert linha["data"] == date(2026, 3, 2)
        assert linha["instante"] == datetime(2026, 1, 2, 0, 30)  # +1h cruzando o dia
        assert linha["valor"] == Decimal("21.89")  # 19.90 * 1.10, 2 casas exatas
        assert linha["lista"] == ["A", "B"]
        assert linha["estrutura"] == {"nome": "CAIXA", "quantidade": 6}
        assert linha["mapa"] == [("k", "V")]
        assert linha["binario"] == b"\x03\x02\x01"

    def test_schema_types_are_preserved(self):
        entrada = make_all_types_batch()
        saida = roundtrip_all_types(entrada)
        for coluna in entrada.schema.names:
            assert saida.schema.field(coluna).type == entrada.schema.field(coluna).type, coluna

    def test_python_scalars_come_back_as_native_types(self):
        # a volta chega ao Python com os tipos nativos: datetime.date e Decimal
        out = roundtrip_all_types(make_all_types_batch())
        assert isinstance(out.column("data").to_pylist()[0], date)
        assert isinstance(out.column("valor").to_pylist()[0], Decimal)
        assert isinstance(out.column("binario").to_pylist()[0], bytes)


class TestSumDecimalColumn:
    def test_returns_python_decimal_exact(self):
        total = sum_decimal_column(make_products_batch(), "unit_cost")
        # rust_decimal::Decimal (Rust) -> decimal.Decimal (Python), sem float no meio
        assert isinstance(total, Decimal)
        assert total == Decimal("63.44")  # 3.50 + 59.94

    def test_non_decimal_column_raises(self):
        with pytest.raises(ValueError, match="decimal128"):
            sum_decimal_column(make_products_batch(), "unit_price")
