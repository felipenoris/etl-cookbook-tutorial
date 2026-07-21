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
    project_revenue_batch,
    roundtrip_all_types,
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


__all__ = [
    "BoundedRevenueProjector",
    "ParallelRevenueProjector",
    "add_line_total",
    "compute_customer_running_spend",
    "compute_product_margin",
    "flatten_customer_profile",
    "project_revenue_batch",
    "roundtrip_all_types",
    "sum_decimal_column",
]
