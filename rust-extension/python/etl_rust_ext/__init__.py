"""Extensão Rust (PyO3 + pyo3-arrow) com transformações otimizadas para o ETL de orders.

As funções abaixo são implementadas em Rust (`src/lib.rs`) e expostas aqui via
PyO3. A troca de dados entre Python e Rust usa `pyo3-arrow`, que aceita qualquer
objeto Python compatível com a Arrow C Data Interface (`pyarrow.RecordBatch`
incluso) e devolve o resultado como um `pyarrow.RecordBatch` de verdade — sem
serializar ou copiar os buffers de coluna.

`compute_customer_running_spend` ilustra um padrão comum em extensões nativas:
a função Rust exige todos os argumentos, e um helper fino em Python de mesmo
nome fornece os defaults e a docstring — a assinatura amigável fica na camada
Python, o trabalho pesado na camada Rust.
"""

import pyarrow as pa

from ._etl_rust_ext import add_line_total
from ._etl_rust_ext import compute_customer_running_spend as _compute_customer_running_spend

DEFAULT_THRESHOLD_PRATA = 500.0
DEFAULT_THRESHOLD_OURO = 2000.0


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


__all__ = ["add_line_total", "compute_customer_running_spend"]
