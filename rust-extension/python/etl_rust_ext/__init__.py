"""Extensão Rust (PyO3 + pyo3-arrow) com transformações otimizadas para o ETL de orders.

As duas funções abaixo são implementadas em Rust (`src/lib.rs`) e expostas aqui via
PyO3. A troca de dados entre Python e Rust usa `pyo3-arrow`, que aceita qualquer
objeto Python compatível com a Arrow C Data Interface (`pyarrow.RecordBatch`
incluso) e devolve o resultado como um `pyarrow.RecordBatch` de verdade — sem
serializar ou copiar os buffers de coluna.
"""

from ._etl_rust_ext import add_line_total, compute_customer_running_spend

__all__ = ["add_line_total", "compute_customer_running_spend"]
