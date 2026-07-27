"""Extensão Rust (PyO3 + pyo3-arrow) com transformações otimizadas para o ETL de orders.

As funções abaixo são implementadas em Rust (`src/lib.rs`) e expostas aqui via
PyO3. A troca de dados tabulares entre Python e Rust usa `pyo3-arrow`, que
aceita qualquer objeto Python compatível com a Arrow C Data Interface
(`pyarrow.RecordBatch` incluso) e devolve o resultado como um
`pyarrow.RecordBatch` de verdade — sem serializar ou copiar os buffers de
coluna. Escalares atravessam a fronteira pelas conversões opcionais do pyo3:
`decimal.Decimal` (feature `rust_decimal`) e `datetime.date` (feature
`chrono`).

`compute_customer_running_spend` e `compute_product_margin` ilustram um padrão
comum em extensões nativas: a função Rust exige todos os argumentos, e um
helper fino em Python de mesmo nome fornece os defaults, a docstring e a
política de tipos (ex.: rejeitar float onde dinheiro exige `decimal.Decimal`)
— a assinatura amigável fica na camada Python, o trabalho pesado na camada
Rust.
"""

from decimal import Decimal

import pyarrow as pa

from ._etl_rust_ext import (
    BoundedRevenueProjector,
    ParallelRevenueProjector,
    add_line_total,
    flatten_customer_profile,
    normalize_json_column,
    project_nested_borrowed,
    project_nested_materialized,
    project_nested_reused,
    project_revenue_batch,
    roundtrip_all_types,
    shred_json_column,
    sum_decimal_column,
)
from ._etl_rust_ext import compute_customer_running_spend as _compute_customer_running_spend
from ._etl_rust_ext import compute_product_margin as _compute_product_margin

DEFAULT_THRESHOLD_PRATA = 500.0
DEFAULT_THRESHOLD_OURO = 2000.0
SEM_DESCONTO = Decimal("0.00")


def compute_customer_running_spend(
    batch: pa.RecordBatch,
    threshold_prata: float = DEFAULT_THRESHOLD_PRATA,
    threshold_ouro: float = DEFAULT_THRESHOLD_OURO,
) -> pa.RecordBatch:
    """Calcula gasto acumulado por cliente e classifica um tier (bronze/prata/ouro).

    Helper fino sobre a função Rust homônima (`src/lib.rs`): apenas fornece os
    defaults dos thresholds e repassa a chamada. O cálculo — uma passada
    sequencial com estado (``HashMap<customer_id, total>``) sobre as colunas
    ``customer_id`` (int64) e ``amount`` (float64) — acontece inteiro em Rust,
    com passagem de dados zero-copy via pyo3-arrow.

    Args:
        batch: RecordBatch com as colunas ``customer_id`` e ``amount``, já
            ordenado por cliente/data (o ``run_etl.py`` garante via ``ORDER BY``).
        threshold_prata: gasto acumulado a partir do qual o cliente deixa de
            ser "bronze" e vira "prata" (default: 500.0).
        threshold_ouro: gasto acumulado a partir do qual o cliente vira "ouro"
            (default: 2000.0). Deve ser >= ``threshold_prata``.

    Returns:
        Novo ``pyarrow.RecordBatch`` com as colunas de entrada mais
        ``cumulative_spend`` (float64) e ``customer_tier`` (string).

    Raises:
        ValueError: se ``threshold_prata > threshold_ouro`` ou se alguma das
            colunas esperadas não existir no batch.
    """
    return _compute_customer_running_spend(batch, threshold_prata, threshold_ouro)


def compute_product_margin(
    batch: pa.RecordBatch,
    desconto: Decimal = SEM_DESCONTO,
) -> pa.RecordBatch:
    """Calcula a margem dos produtos com aritmética decimal exata (2 casas).

    Helper fino sobre a função Rust homônima: fornece o default de
    ``desconto`` e repassa a chamada. No Rust, toda a aritmética roda em
    ``rust_decimal::Decimal`` — o ``desconto`` atravessa a fronteira como
    ``decimal.Decimal`` -> ``rust_decimal::Decimal`` (feature ``rust_decimal``
    do pyo3), e a coluna ``margin`` volta como ``decimal128(12,2)``.

    Args:
        batch: RecordBatch com ``product_id`` (int64), ``unit_price``
            (float64), ``unit_cost`` (decimal128 de escala 2) e ``sku``
            (binary).
        desconto: fração de desconto sobre o preço, como ``decimal.Decimal``
            em [0, 1) — ex.: ``Decimal("0.10")`` = 10%. Um float é REJEITADO
            com ``TypeError``: exatidão obrigatória para valores monetários
            (o pyo3 até converteria, mas este wrapper impõe a política).
            Default: sem desconto.

    Returns:
        Novo ``pyarrow.RecordBatch`` com ``product_id``, ``margin``
        (decimal128(12,2)), ``margin_pct`` (float64) e ``sku_hex`` (string).

    Raises:
        TypeError: se ``desconto`` não for ``decimal.Decimal``.
        ValueError: se ``desconto`` estiver fora de [0, 1), se faltarem
            colunas ou se ``unit_cost`` não tiver escala 2.
    """
    if not isinstance(desconto, Decimal):
        raise TypeError(
            f"desconto deve ser decimal.Decimal (recebi {type(desconto).__name__}); "
            "para valores monetários, floats são proibidos — use Decimal('0.10')"
        )
    return _compute_product_margin(batch, desconto)


def is_json_column(batch: pa.RecordBatch, column: str) -> bool:
    """Diz se a coluna carrega o tipo de extensão canônico ``arrow.json``.

    O marcador é o que separa um *documento* de uma string qualquer. Como ele
    se perde em silêncio (ver :func:`as_json_column`), vale checá-lo nas
    fronteiras do pipeline em vez de assumir que sobreviveu.

    Args:
        batch: RecordBatch a inspecionar.
        column: nome da coluna.

    Returns:
        ``True`` se o campo for ``extension<arrow.json>``; ``False`` se a
        coluna não existir ou for de outro tipo (inclusive ``utf8`` puro).
    """
    idx = batch.schema.get_field_index(column)
    return idx >= 0 and batch.schema.field(idx).type == pa.json_()


def as_json_column(batch: pa.RecordBatch, column: str) -> pa.RecordBatch:
    """Marca uma coluna ``utf8`` como ``arrow.json``, sem copiar os buffers.

    O reparo explícito para quando o marcador foi descartado por algum hop. O
    caso comum é o DuckDB: ``con.sql(...).arrow()`` devolve uma coluna ``JSON``
    como ``utf8`` simples, a menos que a conexão tenha
    ``SET arrow_lossless_conversion = true``. Preferir a flag é melhor — ela
    preserva a semântica na origem; esta função é a saída para quando a origem
    não está sob seu controle.

    Note que **marcar é uma declaração, não uma validação**: o conteúdo não é
    parseado aqui. Quem valida é o Rust, ao consumir a coluna
    (:func:`normalize_json_column` e :func:`shred_json_column` falham com
    ``ValueError`` em documento malformado).

    Args:
        batch: RecordBatch de origem.
        column: nome de uma coluna ``string``/``utf8`` (ou já ``arrow.json``).

    Returns:
        Novo ``pyarrow.RecordBatch`` com a coluna remarcada. Se ela já for
        ``arrow.json``, devolve o batch inalterado.

    Raises:
        ValueError: se a coluna não existir.
        TypeError: se o storage não for ``utf8`` — o ``arrow.json`` só aceita
            texto, e converter um tipo aninhado para JSON é *serializar*,
            não remarcar.
    """
    idx = batch.schema.get_field_index(column)
    if idx < 0:
        raise ValueError(f"coluna '{column}' não encontrada no batch")
    coluna = batch.column(idx)
    if coluna.type == pa.json_():
        return batch
    if coluna.type != pa.string():
        raise TypeError(
            f"coluna '{column}' é {coluna.type}, e arrow.json exige storage utf8; "
            "para um tipo aninhado, serialize o conteúdo antes de marcar"
        )
    marcada = pa.ExtensionArray.from_storage(pa.json_(), coluna)
    return batch.set_column(idx, pa.field(column, pa.json_()), marcada)


__all__ = [
    "BoundedRevenueProjector",
    "ParallelRevenueProjector",
    "add_line_total",
    "as_json_column",
    "compute_customer_running_spend",
    "compute_product_margin",
    "flatten_customer_profile",
    "is_json_column",
    "normalize_json_column",
    "project_nested_borrowed",
    "project_nested_materialized",
    "project_nested_reused",
    "project_revenue_batch",
    "roundtrip_all_types",
    "shred_json_column",
    "sum_decimal_column",
]
