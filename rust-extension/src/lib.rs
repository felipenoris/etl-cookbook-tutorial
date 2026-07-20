//! Extensão Rust (PyO3 + pyo3-arrow) usada pelo ETL em `run_etl.py`.
//!
//! As duas funções expostas recebem e devolvem `pyarrow.RecordBatch` diretamente,
//! sem copiar os buffers de dados: a entrada chega via a Arrow C Data Interface
//! (protocolo `__arrow_c_array__`, que o `pyo3-arrow` reconhece em qualquer
//! objeto Python compatível — pyarrow, arro3, polars, etc.), e a saída é
//! reconstruída em cima dos mesmos `Arc<dyn Array>` de entrada mais as colunas
//! novas calculadas em Rust.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type};
use arrow_array::{Float64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatch;

fn arrow_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyValueError::new_err(err.to_string())
}

fn get_column<'a>(batch: &'a RecordBatch, name: &str) -> PyResult<&'a arrow_array::ArrayRef> {
    batch
        .column_by_name(name)
        .ok_or_else(|| PyValueError::new_err(format!("coluna '{name}' não encontrada")))
}

/// Anexa `line_total = quantity * unit_price` a um RecordBatch, em Rust.
///
/// Exemplo simples de acesso zero-copy: as colunas de entrada (`quantity`,
/// `unit_price`) são lidas por downcast direto do array Arrow recebido, sem
/// nenhuma cópia de buffer; só a coluna nova é de fato alocada.
#[pyfunction]
fn add_line_total(py: Python<'_>, batch: PyRecordBatch) -> PyResult<Py<PyAny>> {
    let record_batch = batch.into_inner();

    let quantity = get_column(&record_batch, "quantity")?.as_primitive::<Int32Type>();
    let unit_price = get_column(&record_batch, "unit_price")?.as_primitive::<Float64Type>();

    let line_total: Float64Array = quantity
        .iter()
        .zip(unit_price.iter())
        .map(|(q, p)| match (q, p) {
            (Some(q), Some(p)) => Some(q as f64 * p),
            _ => None,
        })
        .collect();

    let mut fields: Vec<Field> = record_batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new("line_total", DataType::Float64, true));
    let schema = Arc::new(Schema::new(fields));

    let mut columns = record_batch.columns().to_vec();
    columns.push(Arc::new(line_total));

    let out = RecordBatch::try_new(schema, columns).map_err(arrow_err)?;
    let obj = PyRecordBatch::new(out).into_pyarrow(py)?;
    Ok(obj.unbind())
}

/// Calcula gasto acumulado por cliente e classifica um tier (bronze/prata/ouro).
///
/// Este é o exemplo que justifica sair do domínio vetorizado: o cálculo é
/// sequencial e mantém estado (`HashMap<customer_id, total>`) — algo lento em
/// Python puro (loop com estado por linha) e trivial em Rust com uma única
/// passada sobre as colunas de entrada.
///
/// Espera as colunas `customer_id` (int64) e `amount` (float64); assume que o
/// batch já está ordenado por `customer_id`/data (o `run_etl.py` garante isso
/// via `ORDER BY` no DuckDB antes de chamar esta função).
#[pyfunction]
fn compute_customer_running_spend(py: Python<'_>, batch: PyRecordBatch) -> PyResult<Py<PyAny>> {
    let record_batch = batch.into_inner();

    let customer_id = get_column(&record_batch, "customer_id")?.as_primitive::<Int64Type>();
    let amount = get_column(&record_batch, "amount")?.as_primitive::<Float64Type>();

    let mut running_totals: HashMap<i64, f64> = HashMap::new();
    let mut cumulative: Vec<Option<f64>> = Vec::with_capacity(record_batch.num_rows());
    let mut tier: Vec<Option<&'static str>> = Vec::with_capacity(record_batch.num_rows());

    for (cid, amt) in customer_id.iter().zip(amount.iter()) {
        let (cid, amt) = match (cid, amt) {
            (Some(c), Some(a)) => (c, a),
            _ => {
                cumulative.push(None);
                tier.push(None);
                continue;
            }
        };
        let total = running_totals.entry(cid).or_insert(0.0);
        *total += amt;
        let tier_label = if *total < 500.0 {
            "bronze"
        } else if *total < 2000.0 {
            "prata"
        } else {
            "ouro"
        };
        cumulative.push(Some(*total));
        tier.push(Some(tier_label));
    }

    let cumulative_array = Float64Array::from(cumulative);
    let tier_array = StringArray::from(tier);

    let mut fields: Vec<Field> = record_batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new("cumulative_spend", DataType::Float64, true));
    fields.push(Field::new("customer_tier", DataType::Utf8, true));
    let schema = Arc::new(Schema::new(fields));

    let mut columns = record_batch.columns().to_vec();
    columns.push(Arc::new(cumulative_array));
    columns.push(Arc::new(tier_array));

    let out = RecordBatch::try_new(schema, columns).map_err(arrow_err)?;
    let obj = PyRecordBatch::new(out).into_pyarrow(py)?;
    Ok(obj.unbind())
}

#[pymodule]
fn _etl_rust_ext(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(add_line_total, m)?)?;
    m.add_function(wrap_pyfunction!(compute_customer_running_spend, m)?)?;
    Ok(())
}
