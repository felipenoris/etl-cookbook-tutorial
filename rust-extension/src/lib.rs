//! Extensão Rust (PyO3 + pyo3-arrow) usada pelo ETL em `run_etl.py`.
//!
//! As duas funções expostas recebem e devolvem `pyarrow.RecordBatch` diretamente,
//! sem copiar os buffers de dados: a entrada chega via a Arrow C Data Interface
//! (protocolo `__arrow_c_array__`, que o `pyo3-arrow` reconhece em qualquer
//! objeto Python compatível — pyarrow, arro3, polars, etc.), e a saída é
//! reconstruída em cima dos mesmos `Arc<dyn Array>` de entrada mais as colunas
//! novas calculadas em Rust.
//!
//! # Sobre o tipo de retorno das funções (`Py<PyAny>` vs `PyRecordBatch`)
//!
//! Há duas formas de devolver um RecordBatch ao Python com o pyo3-arrow:
//!
//! 1. `-> PyResult<PyRecordBatch>`: o pyo3-arrow converte o retorno para a
//!    classe `RecordBatch` **do arro3** (implementação minimalista do Arrow,
//!    compilada dentro da própria extensão). O objeto suporta o protocolo
//!    `__arrow_c_array__`, mas *não é* um `pyarrow.RecordBatch` — o chamador
//!    precisaria converter com `pa.record_batch(saida)` no lado Python.
//! 2. `-> PyResult<Py<PyAny>>` chamando `.into_pyarrow(py)` (o padrão adotado
//!    aqui): a conversão para pyarrow acontece já dentro do Rust, e o chamador
//!    recebe um `pyarrow.RecordBatch` genuíno, pronto para uso.
//!
//! Os dois caminhos são zero-copy (só os ponteiros/schema atravessam a
//! fronteira). Adotamos o (2) porque este tutorial é centrado em pyarrow: o
//! tipo `Py<PyAny>` na assinatura é o preço de devolver um objeto construído
//! por uma biblioteca Python em runtime, para o qual não existe tipo Rust
//! mais específico.

use std::collections::HashMap;
use std::sync::Arc;
use std::thread::JoinHandle;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type};
use arrow_array::{Float64Array, Int64Array, PrimitiveArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatch;

/// Converte qualquer erro do arrow-rs em `ValueError` do Python.
fn arrow_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyValueError::new_err(err.to_string())
}

/// Busca uma coluna pelo nome, com `ValueError` amigável se não existir.
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
    // Etapa 1 — desembrulhar o wrapper do pyo3-arrow para o RecordBatch nativo
    // do arrow-rs. `into_inner()` não copia dados: o batch já foi importado
    // zero-copy via C Data Interface quando o PyO3 converteu o argumento.
    let record_batch = batch.into_inner();

    // Etapa 2 — localizar as colunas de entrada e fazer downcast para os tipos
    // concretos esperados (int32 / float64). `as_primitive` devolve uma view
    // tipada sobre os mesmos buffers — de novo, nenhuma cópia.
    let quantity = get_column(&record_batch, "quantity")?.as_primitive::<Int32Type>();
    let unit_price = get_column(&record_batch, "unit_price")?.as_primitive::<Float64Type>();

    // Etapa 3 — calcular a coluna nova. O zip dos iteradores produz pares
    // `(Option<i32>, Option<f64>)`; qualquer lado nulo propaga nulo no
    // resultado (semântica padrão do Arrow para valores faltantes).
    let line_total: Float64Array = quantity
        .iter()
        .zip(unit_price.iter())
        .map(|(q, p)| match (q, p) {
            (Some(q), Some(p)) => Some(q as f64 * p),
            _ => None,
        })
        .collect();

    // Etapa 4 — montar o schema de saída: os campos originais (clonados por
    // referência, são `Arc`s baratos) mais o campo novo `line_total`
    // (nullable, pois a entrada pode ter nulos).
    let mut fields: Vec<Field> = record_batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new("line_total", DataType::Float64, true));
    let schema = Arc::new(Schema::new(fields));

    // Etapa 5 — montar as colunas de saída: `columns().to_vec()` clona apenas
    // os `Arc<dyn Array>` (contagem de referência), reaproveitando os buffers
    // de entrada; só `line_total` é alocação nova.
    let mut columns = record_batch.columns().to_vec();
    columns.push(Arc::new(line_total));

    // Etapa 6 — construir o RecordBatch de saída e devolvê-lo ao Python.
    // Aqui entra a questão discutida no doc do módulo: `PyRecordBatch::new`
    // embrulha o batch para atravessar a fronteira, e `.into_pyarrow(py)`
    // faz a conversão para um `pyarrow.RecordBatch` genuíno AINDA NO RUST
    // (internamente chama `pyarrow.record_batch(...)` via C Data Interface,
    // zero-copy). Se retornássemos `PyResult<PyRecordBatch>` direto, o
    // chamador receberia um RecordBatch do arro3 e teria que converter por
    // conta própria. `.unbind()` solta o lifetime do GIL para podermos
    // retornar o objeto (`Bound<'py, PyAny>` -> `Py<PyAny>`).
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
///
/// Os limites de classificação chegam como argumentos: gasto acumulado abaixo
/// de `threshold_prata` é "bronze", abaixo de `threshold_ouro` é "prata" e a
/// partir dele, "ouro". Os defaults (500.0 / 2000.0) ficam no wrapper Python
/// (`etl_rust_ext/__init__.py`) — aqui os dois são obrigatórios.
#[pyfunction]
#[pyo3(signature = (batch, threshold_prata, threshold_ouro))]
fn compute_customer_running_spend(
    py: Python<'_>,
    batch: PyRecordBatch,
    threshold_prata: f64,
    threshold_ouro: f64,
) -> PyResult<Py<PyAny>> {
    // Etapa 1 — validar os argumentos escalares antes de tocar nos dados.
    // Falhar cedo com ValueError é mais barato e dá mensagem melhor do que
    // deixar a inconsistência aparecer no meio da classificação.
    if threshold_prata > threshold_ouro {
        return Err(PyValueError::new_err(format!(
            "threshold_prata ({threshold_prata}) deve ser <= threshold_ouro ({threshold_ouro})"
        )));
    }

    // Etapa 2 — desembrulhar o batch (zero-copy, ver `add_line_total`).
    let record_batch = batch.into_inner();

    // Etapa 3 — localizar e tipar as colunas de entrada.
    let customer_id = get_column(&record_batch, "customer_id")?.as_primitive::<Int64Type>();
    let amount = get_column(&record_batch, "amount")?.as_primitive::<Float64Type>();

    // Etapa 4 — preparar o estado do loop: o acumulador por cliente e os
    // vetores de saída (pré-alocados com a capacidade exata, uma alocação só).
    let mut running_totals: HashMap<i64, f64> = HashMap::new();
    let mut cumulative: Vec<Option<f64>> = Vec::with_capacity(record_batch.num_rows());
    let mut tier: Vec<Option<&'static str>> = Vec::with_capacity(record_batch.num_rows());

    // Etapa 5 — a passada única sobre as linhas. É exatamente o tipo de
    // computação com estado que é caro em pandas/pyarrow/SQL vetorizados e
    // natural aqui: para cada linha, atualiza o total do cliente no HashMap
    // e classifica o tier pelo acumulado *naquele momento*.
    for (cid, amt) in customer_id.iter().zip(amount.iter()) {
        // Linhas com nulo em qualquer entrada produzem nulo nas duas saídas
        // (e não avançam o acumulado do cliente).
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
        let tier_label = if *total < threshold_prata {
            "bronze"
        } else if *total < threshold_ouro {
            "prata"
        } else {
            "ouro"
        };
        cumulative.push(Some(*total));
        tier.push(Some(tier_label));
    }

    // Etapa 6 — materializar os vetores Rust como arrays Arrow (aqui há cópia,
    // mas apenas das DUAS colunas novas — as colunas de entrada continuam
    // compartilhadas por Arc).
    let cumulative_array = Float64Array::from(cumulative);
    let tier_array = StringArray::from(tier);

    // Etapa 7 — schema de saída = campos originais + os dois campos novos.
    let mut fields: Vec<Field> = record_batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new("cumulative_spend", DataType::Float64, true));
    fields.push(Field::new("customer_tier", DataType::Utf8, true));
    let schema = Arc::new(Schema::new(fields));

    // Etapa 8 — colunas de saída: Arcs das originais + as duas novas.
    let mut columns = record_batch.columns().to_vec();
    columns.push(Arc::new(cumulative_array));
    columns.push(Arc::new(tier_array));

    // Etapa 9 — devolver ao Python como `pyarrow.RecordBatch` genuíno,
    // convertendo dentro do Rust via `.into_pyarrow(py)` — mesma discussão
    // da Etapa 6 de `add_line_total` e do doc do módulo: evita devolver o
    // RecordBatch minimalista do arro3 e empurrar a conversão para o chamador.
    let out = RecordBatch::try_new(schema, columns).map_err(arrow_err)?;
    let obj = PyRecordBatch::new(out).into_pyarrow(py)?;
    Ok(obj.unbind())
}

// ---------------------------------------------------------------------------
// Multithreading: projeção de receita de contratos em paralelo
// ---------------------------------------------------------------------------

/// Busca uma coluna e faz downcast tipado com mensagem de erro amigável
/// (`as_primitive_opt` devolve `None` em vez de dar panic se o tipo divergir).
fn typed_column<'a, T: arrow_array::types::ArrowPrimitiveType>(
    batch: &'a RecordBatch,
    name: &str,
) -> PyResult<&'a PrimitiveArray<T>> {
    let col = get_column(batch, name)?;
    if col.null_count() > 0 {
        return Err(PyValueError::new_err(format!(
            "coluna '{name}' tem {} nulo(s); contratos devem estar completos",
            col.null_count()
        )));
    }
    col.as_primitive_opt::<T>().ok_or_else(|| {
        PyValueError::new_err(format!(
            "coluna '{name}' deveria ser {:?}, mas é {:?}",
            T::DATA_TYPE,
            col.data_type()
        ))
    })
}

/// Núcleo do cálculo: receita total projetada de UM contrato (juros da tabela
/// Price), simulando a evolução do saldo devedor mês a mês.
///
/// Existe forma fechada para este caso (`n * parcela - principal` — os testes
/// usam-na como referência), mas o loop mensal representa as projeções reais
/// (indexadores, curvas, prépagamento) que não têm forma fechada — e é o
/// custo de CPU por contrato que justifica paralelizar.
fn project_contract_revenue(principal: f64, monthly_rate: f64, months: i32) -> f64 {
    if monthly_rate <= 0.0 || months <= 0 {
        return 0.0;
    }
    let factor = (1.0 + monthly_rate).powi(months);
    let installment = principal * monthly_rate * factor / (factor - 1.0);
    let mut balance = principal;
    let mut revenue = 0.0;
    for _ in 0..months {
        let interest = balance * monthly_rate;
        revenue += interest;
        balance -= installment - interest;
    }
    revenue
}

/// Processa um lote inteiro de contratos (roda DENTRO da worker thread).
///
/// Recebe os arrays tipados já validados — são `Arc`s sobre os buffers Arrow
/// originais, então mover para a thread não copia dados. Devolve os vetores
/// de saída prontos para consolidação.
fn process_contracts_batch(
    ids: PrimitiveArray<Int64Type>,
    principals: PrimitiveArray<Float64Type>,
    rates: PrimitiveArray<Float64Type>,
    months: PrimitiveArray<Int32Type>,
) -> (Vec<i64>, Vec<f64>) {
    let n = ids.len();
    let mut out_ids = Vec::with_capacity(n);
    let mut out_revenues = Vec::with_capacity(n);
    for row in 0..n {
        out_ids.push(ids.value(row));
        out_revenues.push(project_contract_revenue(
            principals.value(row),
            rates.value(row),
            months.value(row),
        ));
    }
    (out_ids, out_revenues)
}

/// Projeta a receita de um lote de contratos de forma **sequencial** (síncrona).
///
/// Espera as colunas `id_contrato` (int64), `principal` (float64),
/// `taxa_mensal` (float64) e `prazo_meses` (int32), sem nulos. Devolve um
/// `pyarrow.RecordBatch` com `id_contrato` e `receita_projetada`.
///
/// Serve de linha de base para comparar com `ParallelRevenueProjector`, que
/// faz o mesmo cálculo distribuindo os lotes entre threads.
#[pyfunction]
fn project_revenue_batch(py: Python<'_>, batch: PyRecordBatch) -> PyResult<Py<PyAny>> {
    let record_batch = batch.into_inner();
    let ids = typed_column::<Int64Type>(&record_batch, "id_contrato")?.clone();
    let principals = typed_column::<Float64Type>(&record_batch, "principal")?.clone();
    let rates = typed_column::<Float64Type>(&record_batch, "taxa_mensal")?.clone();
    let months = typed_column::<Int32Type>(&record_batch, "prazo_meses")?.clone();

    // py.detach() solta o GIL durante o número-crunching: outras threads
    // Python podem rodar enquanto o Rust calcula.
    let (out_ids, out_revenues) =
        py.detach(|| process_contracts_batch(ids, principals, rates, months));

    build_revenue_batch(py, out_ids, out_revenues)
}

/// Monta o RecordBatch de saída (id_contrato, receita_projetada) e converte
/// para `pyarrow.RecordBatch` (mesma discussão de retorno do doc do módulo).
fn build_revenue_batch(py: Python<'_>, ids: Vec<i64>, revenues: Vec<f64>) -> PyResult<Py<PyAny>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id_contrato", DataType::Int64, false),
        Field::new("receita_projetada", DataType::Float64, false),
    ]));
    let columns: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(Float64Array::from(revenues)),
    ];
    let out = RecordBatch::try_new(schema, columns).map_err(arrow_err)?;
    let obj = PyRecordBatch::new(out).into_pyarrow(py)?;
    Ok(obj.unbind())
}

/// Projetor de receita com processamento paralelo por lote.
///
/// O padrão de uso (ver `run_contracts_parallel.py`):
///
/// 1. o Python lê a fonte de dados em lotes e chama `submit_batch(batch)`
///    para cada um, **serialmente**;
/// 2. cada `submit_batch` valida o lote, dispara uma thread Rust para
///    processá-lo e **retorna imediatamente** — o Python segue lendo o
///    próximo lote enquanto os anteriores são calculados em paralelo;
/// 3. ao final, `collect()` espera todas as threads terminarem e devolve um
///    único `pyarrow.RecordBatch` consolidado (`id_contrato`,
///    `receita_projetada`), na ordem de submissão.
///
/// As threads são Rust puro e não tocam em objetos Python, então rodam fora
/// do GIL — o paralelismo é real. Para produção com muitos lotes pequenos,
/// um pool de tamanho fixo (ex.: rayon) evita criar uma thread por lote;
/// aqui `thread::spawn` mantém o exemplo enxuto.
#[pyclass]
struct ParallelRevenueProjector {
    handles: Vec<JoinHandle<(Vec<i64>, Vec<f64>)>>,
    // contador separado dos handles: sobrevive ao mem::take do collect()
    submitted: usize,
    finished: bool,
}

#[pymethods]
impl ParallelRevenueProjector {
    #[new]
    fn new() -> Self {
        Self {
            handles: Vec::new(),
            submitted: 0,
            finished: false,
        }
    }

    /// Submete um lote para processamento em background; retorna o número de
    /// lotes submetidos até agora. Lança `ValueError` se `collect()` já foi
    /// chamado ou se o lote não tem as colunas esperadas.
    fn submit_batch(&mut self, batch: PyRecordBatch) -> PyResult<usize> {
        if self.finished {
            return Err(PyValueError::new_err(
                "collect() já foi chamado; crie um novo ParallelRevenueProjector",
            ));
        }
        let record_batch = batch.into_inner();

        // Validação na thread chamadora: erros de schema aparecem aqui, na
        // submissão, e não escondidos dentro de uma worker thread.
        let ids = typed_column::<Int64Type>(&record_batch, "id_contrato")?.clone();
        let principals = typed_column::<Float64Type>(&record_batch, "principal")?.clone();
        let rates = typed_column::<Float64Type>(&record_batch, "taxa_mensal")?.clone();
        let months = typed_column::<Int32Type>(&record_batch, "prazo_meses")?.clone();

        // `spawn` move os Arcs (não os dados) para a thread e retorna na hora.
        self.handles.push(std::thread::spawn(move || {
            process_contracts_batch(ids, principals, rates, months)
        }));
        self.submitted += 1;
        Ok(self.submitted)
    }

    /// Quantidade de lotes submetidos desde a criação (não zera no collect).
    fn batches_submitted(&self) -> usize {
        self.submitted
    }

    /// Espera todas as threads terminarem e devolve o resultado consolidado
    /// como um `pyarrow.RecordBatch` (`id_contrato`, `receita_projetada`).
    fn collect(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.finished = true;
        let handles = std::mem::take(&mut self.handles);

        // O join espera as workers — py.detach() solta o GIL nesse meio
        // tempo para não bloquear outras threads Python.
        let results: Vec<(Vec<i64>, Vec<f64>)> = py.detach(|| {
            handles
                .into_iter()
                .map(|h| h.join())
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|_| PyRuntimeError::new_err("uma worker thread do projetor entrou em panic"))?;

        // Consolidação: concatena os resultados na ordem de submissão.
        let total: usize = results.iter().map(|(ids, _)| ids.len()).sum();
        let mut all_ids = Vec::with_capacity(total);
        let mut all_revenues = Vec::with_capacity(total);
        for (ids, revenues) in results {
            all_ids.extend(ids);
            all_revenues.extend(revenues);
        }
        build_revenue_batch(py, all_ids, all_revenues)
    }
}

/// Registro do módulo Python `etl_rust_ext._etl_rust_ext` (nome definido no
/// `[tool.maturin]` do `pyproject.toml`); o pacote `etl_rust_ext` reexporta
/// as funções em `python/etl_rust_ext/__init__.py`.
#[pymodule]
fn _etl_rust_ext(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(add_line_total, m)?)?;
    m.add_function(wrap_pyfunction!(compute_customer_running_spend, m)?)?;
    m.add_function(wrap_pyfunction!(project_revenue_batch, m)?)?;
    m.add_class::<ParallelRevenueProjector>()?;
    Ok(())
}
