//! Extensão Rust (PyO3 + pyo3-arrow) usada pelo ETL em `run_etl.py`.
//!
//! Os dados tabulares entram e saem como `pyarrow.RecordBatch`, sem cópia dos
//! buffers na entrada: o batch chega via a Arrow C Data Interface (protocolo
//! `__arrow_c_array__`, que o `pyo3-arrow` reconhece em qualquer objeto
//! Python compatível — pyarrow, arro3, polars, etc.). Escalares atravessam a
//! fronteira pelas conversões opcionais do pyo3: `decimal.Decimal` ↔
//! `rust_decimal::Decimal` (feature `rust_decimal`) e `datetime.date` ↔
//! `chrono::NaiveDate` (feature `chrono`).
//!
//! O que é exposto ao Python:
//!
//! - `add_line_total` e `compute_customer_running_spend`: enriquecem o batch —
//!   a saída **reaproveita os mesmos `Arc<dyn Array>` de entrada** e acrescenta
//!   as colunas novas calculadas em Rust (só as colunas novas são alocadas).
//! - `project_revenue_batch` e a classe `ParallelRevenueProjector`: projetam
//!   um resultado **novo** (`id_contrato`, `receita_projetada`), sem carregar
//!   as colunas de entrada adiante — o `ParallelRevenueProjector` distribui os
//!   lotes entre threads (ver a doc da própria classe).
//! - `flatten_customer_profile` e `compute_product_margin`: manipulam os
//!   tipos Arrow complexos (struct/list/map/timestamp/bool; decimal/binary)
//!   no lado nativo, recebendo escalares `datetime.date` e `decimal.Decimal`
//!   como parâmetros.
//! - `roundtrip_all_types`: leitura E escrita dos 11 tipos da stack em uma
//!   função só — o teste integral da fronteira.
//! - `sum_decimal_column`: a exceção do retorno tabular — devolve um ESCALAR
//!   (`rust_decimal::Decimal`, que chega como `decimal.Decimal`).
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
use std::fmt::Write as _;
use std::sync::Arc;
use std::thread::JoinHandle;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{
    Array, BooleanArray, Decimal128Array, Float64Array, Int32Array, Int64Array, PrimitiveArray,
    RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Duration, NaiveDate};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatch;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal as RustDecimal;

/// Época Unix como `NaiveDate` — a referência do tipo Arrow `date32`
/// (dias desde 1970-01-01).
fn epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("data válida")
}

/// `date32` (i32 de dias) -> `chrono::NaiveDate`.
fn date32_to_naive(days: i32) -> NaiveDate {
    epoch() + Duration::days(days as i64)
}

/// `chrono::NaiveDate` -> `date32` (i32 de dias).
fn naive_to_date32(date: NaiveDate) -> i32 {
    (date - epoch()).num_days() as i32
}

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

// ---------------------------------------------------------------------------
// Tipos de dados Arrow no Rust: bool, timestamp, struct, list, map, decimal,
// binary — leitura e manipulação zero-copy do lado nativo
// ---------------------------------------------------------------------------

/// Downcast genérico com mensagem de erro amigável (para tipos NÃO primitivos,
/// que o `typed_column` não cobre: struct, list, map, binary, bool...).
fn downcast_column<'a, T: 'static>(
    batch: &'a RecordBatch,
    name: &str,
    expected: &str,
) -> PyResult<&'a T> {
    let col = get_column(batch, name)?;
    col.as_any().downcast_ref::<T>().ok_or_else(|| {
        PyValueError::new_err(format!(
            "coluna '{name}' deveria ser {expected}, mas é {:?}",
            col.data_type()
        ))
    })
}

/// Achata o perfil do cliente, exercitando os tipos aninhados/complexos no Rust.
///
/// Entrada (schema de `data/raw/customers`): `customer_id` (int64),
/// `is_active` (**bool**), `signup_ts` (**timestamp µs**), `address`
/// (**struct**<street,city,zip>), `tags` (**list**<string>) e `preferences`
/// (**map**<string,string>).
///
/// O argumento `reference_date` chega do Python como **`datetime.date`** e
/// vira **`chrono::NaiveDate`** automaticamente — a feature opcional `chrono`
/// do pyo3, análoga à `rust_decimal` usada em `compute_product_margin`.
///
/// Saída: `customer_id`, `city` (extraída do struct), `num_tags` (tamanho da
/// list), `canal` (lookup da chave "canal" no map; nulo se ausente),
/// `signup_date` (**date32** derivada do timestamp — escrita de data
/// Rust -> Python), `dias_desde_cadastro` (aritmética de calendário com
/// `chrono`: `reference_date - data_do_cadastro`) e `is_active` (bool).
///
/// Tudo é lido zero-copy: struct expõe os filhos como arrays-coluna
/// (`column_by_name`), list expõe offsets + valores achatados, e map é uma
/// list de pares (key, value) — nenhum objeto Python é criado por linha.
#[pyfunction]
fn flatten_customer_profile(
    py: Python<'_>,
    batch: PyRecordBatch,
    reference_date: NaiveDate,
) -> PyResult<Py<PyAny>> {
    let rb = batch.into_inner();
    let n = rb.num_rows();

    let ids = typed_column::<Int64Type>(&rb, "customer_id")?;
    let is_active = downcast_column::<BooleanArray>(&rb, "is_active", "bool")?;

    // timestamp[us] é um PrimitiveArray de i64 (microssegundos desde a época)
    let signup_col = get_column(&rb, "signup_ts")?;
    if !matches!(signup_col.data_type(), DataType::Timestamp(TimeUnit::Microsecond, _)) {
        return Err(PyValueError::new_err(format!(
            "coluna 'signup_ts' deveria ser timestamp[us], mas é {:?}",
            signup_col.data_type()
        )));
    }
    let signup_ts = signup_col.as_primitive::<TimestampMicrosecondType>();

    // struct: os campos são arrays-filha; extrair "city" é pegar uma referência
    let address = downcast_column::<arrow_array::StructArray>(&rb, "address", "struct")?;
    let city = address
        .column_by_name("city")
        .ok_or_else(|| PyValueError::new_err("struct 'address' não tem o campo 'city'"))?
        .as_string::<i32>();

    // list<string>: offsets delimitam a fatia de cada linha no array achatado
    let tags = downcast_column::<arrow_array::ListArray>(&rb, "tags", "list<string>")?;

    // map<string,string>: keys() e values() achatados + offsets por linha
    let prefs = downcast_column::<arrow_array::MapArray>(&rb, "preferences", "map<string,string>")?;
    let map_keys = prefs.keys().as_string::<i32>();
    let map_values = prefs.values().as_string::<i32>();
    let map_offsets = prefs.value_offsets();

    let mut cities: Vec<&str> = Vec::with_capacity(n);
    let mut num_tags: Vec<i32> = Vec::with_capacity(n);
    let mut canais: Vec<Option<String>> = Vec::with_capacity(n);
    let mut signup_dates: Vec<i32> = Vec::with_capacity(n);
    let mut dias: Vec<i64> = Vec::with_capacity(n);

    for row in 0..n {
        cities.push(city.value(row));
        num_tags.push(tags.value_length(row));
        // lookup manual no map: varre só os pares da linha `row`
        let (ini, fim) = (map_offsets[row] as usize, map_offsets[row + 1] as usize);
        let canal = (ini..fim)
            .find(|&j| map_keys.value(j) == "canal")
            .map(|j| map_values.value(j).to_string());
        canais.push(canal);
        // timestamp[us] -> NaiveDate (chrono), e daí aritmética de CALENDÁRIO
        // — em vez de dividir microssegundos na mão
        let cadastro = DateTime::from_timestamp_micros(signup_ts.value(row))
            .ok_or_else(|| PyValueError::new_err("signup_ts fora do intervalo representável"))?
            .date_naive();
        signup_dates.push(naive_to_date32(cadastro));
        dias.push((reference_date - cadastro).num_days());
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("num_tags", DataType::Int32, false),
        Field::new("canal", DataType::Utf8, true),
        Field::new("signup_date", DataType::Date32, false),
        Field::new("dias_desde_cadastro", DataType::Int64, false),
        Field::new("is_active", DataType::Boolean, false),
    ]));
    let columns: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(ids.clone()),
        Arc::new(StringArray::from(cities)),
        Arc::new(Int32Array::from(num_tags)),
        Arc::new(StringArray::from(canais)),
        Arc::new(arrow_array::Date32Array::from(signup_dates)),
        Arc::new(Int64Array::from(dias)),
        Arc::new(is_active.clone()),
    ];
    let out = RecordBatch::try_new(schema, columns).map_err(arrow_err)?;
    Ok(PyRecordBatch::new(out).into_pyarrow(py)?.unbind())
}

/// Extrai e valida uma coluna decimal128 de escala 2 (o padrão do projeto).
fn decimal2_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> PyResult<&'a PrimitiveArray<Decimal128Type>> {
    let col = get_column(batch, name)?;
    match col.data_type() {
        DataType::Decimal128(_, 2) => Ok(col.as_primitive::<Decimal128Type>()),
        outro => Err(PyValueError::new_err(format!(
            "coluna '{name}' deveria ser decimal128 com escala 2, mas é {outro:?}"
        ))),
    }
}

/// Converte uma célula decimal128(_, 2) para `rust_decimal::Decimal`.
///
/// A ponte entre os dois mundos decimais: o Arrow guarda o valor como i128 +
/// escala (metadado da coluna); o `rust_decimal` embute a escala no próprio
/// valor e dá aritmética decimal completa (+, -, *, /, `round_dp`...).
fn decimal2_to_rust(cents: i128) -> RustDecimal {
    RustDecimal::from_i128_with_scale(cents, 2)
}

/// Calcula a margem dos produtos com `rust_decimal::Decimal`, preservando 2 casas.
///
/// Entrada (schema de `data/raw/products`): `product_id` (int64),
/// `unit_price` (float64), `unit_cost` (**decimal128 com escala 2** — o
/// padrão do projeto: 2 casas decimais) e `sku` (**binary**).
///
/// O argumento `desconto` (fração, ex.: 0.10 = 10%) chega do Python como
/// `decimal.Decimal` e vira `rust_decimal::Decimal` automaticamente — é a
/// feature opcional `rust_decimal` do pyo3 fazendo a conversão na fronteira.
/// (Nota: o pyo3 também aceita int/float aqui, convertendo-os; quem impõe
/// `decimal.Decimal` estrito — TypeError para float — é o wrapper Python em
/// `etl_rust_ext/__init__.py`, política do projeto para valores monetários.)
///
/// Toda a aritmética roda em `rust_decimal::Decimal` (exata, base 10):
/// `preco_liquido = round_dp(preco * (1 - desconto), 2)` e
/// `margin = preco_liquido - custo`. A saída volta para o Arrow como
/// **decimal128(12,2)** via `mantissa()` após `rescale(2)`; `margin_pct` é
/// float64 (proporção — aí float é adequado) e `sku_hex` expõe o binary.
#[pyfunction]
fn compute_product_margin(
    py: Python<'_>,
    batch: PyRecordBatch,
    desconto: RustDecimal,
) -> PyResult<Py<PyAny>> {
    if desconto < RustDecimal::ZERO || desconto >= RustDecimal::ONE {
        return Err(PyValueError::new_err(format!(
            "desconto deve estar em [0, 1), recebi {desconto}"
        )));
    }

    let rb = batch.into_inner();
    let n = rb.num_rows();

    let ids = typed_column::<Int64Type>(&rb, "product_id")?;
    let prices = typed_column::<Float64Type>(&rb, "unit_price")?;
    let costs = decimal2_column(&rb, "unit_cost")?;
    let sku = downcast_column::<arrow_array::BinaryArray>(&rb, "sku", "binary")?;

    let fator = RustDecimal::ONE - desconto;
    let mut margins_cents: Vec<i128> = Vec::with_capacity(n);
    let mut margin_pcts: Vec<f64> = Vec::with_capacity(n);
    let mut sku_hex: Vec<String> = Vec::with_capacity(n);

    for row in 0..n {
        // float64 -> Decimal uma única vez, na fronteira; daí em diante a
        // aritmética é decimal exata (base 10), com arredondamento explícito
        let price = RustDecimal::from_f64(prices.value(row))
            .ok_or_else(|| PyValueError::new_err("unit_price não representável como Decimal"))?
            .round_dp(2);
        let preco_liquido = (price * fator).round_dp(2);
        let margin = preco_liquido - decimal2_to_rust(costs.value(row));

        // de volta ao Arrow: rescale(2) fixa a escala, mantissa() dá o i128
        let mut normalizada = margin;
        normalizada.rescale(2);
        margins_cents.push(normalizada.mantissa());
        margin_pcts.push(
            (margin / preco_liquido)
                .to_f64()
                .ok_or_else(|| PyValueError::new_err("margin_pct não representável como f64"))?,
        );

        let mut hex = String::with_capacity(sku.value(row).len() * 2);
        for byte in sku.value(row) {
            write!(hex, "{byte:02x}").expect("write em String não falha");
        }
        sku_hex.push(hex);
    }

    let margin_array = Decimal128Array::from_iter_values(margins_cents)
        .with_precision_and_scale(12, 2)
        .map_err(arrow_err)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("product_id", DataType::Int64, false),
        Field::new("margin", DataType::Decimal128(12, 2), false),
        Field::new("margin_pct", DataType::Float64, false),
        Field::new("sku_hex", DataType::Utf8, false),
    ]));
    let columns: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(ids.clone()),
        Arc::new(margin_array),
        Arc::new(Float64Array::from(margin_pcts)),
        Arc::new(StringArray::from(sku_hex)),
    ];
    let out = RecordBatch::try_new(schema, columns).map_err(arrow_err)?;
    Ok(PyRecordBatch::new(out).into_pyarrow(py)?.unbind())
}

/// Roundtrip integral: lê E escreve TODOS os tipos da stack no lado Rust.
///
/// Recebe um batch com 11 colunas — uma por tipo — e devolve um batch com o
/// MESMO schema, onde cada coluna foi derivada da entrada em Rust. Cada tipo
/// é portanto exercitado nas duas direções (leitura Python -> Rust via
/// downcast zero-copy; escrita Rust -> Python via arrays/builders do
/// arrow-rs):
///
/// | coluna      | tipo Arrow         | transformação no Rust           |
/// |-------------|--------------------|---------------------------------|
/// | `texto`     | utf8               | uppercase                       |
/// | `inteiro`   | int64              | + 1                             |
/// | `flutuante` | float64            | * 2                             |
/// | `logico`    | bool               | negado                          |
/// | `data`      | date32             | + 30 dias (`chrono::NaiveDate`) |
/// | `instante`  | timestamp[us]      | + 1 hora                        |
/// | `valor`     | decimal128(12,2)   | + 10% (`rust_decimal`, 2 casas) |
/// | `lista`     | list<utf8>         | cada elemento em uppercase      |
/// | `estrutura` | struct<nome,quantidade> | nome uppercase, quantidade * 2 |
/// | `mapa`      | map<utf8,utf8>     | valores em uppercase            |
/// | `binario`   | binary             | bytes revertidos                |
///
/// Os aninhados usam os *builders* do arrow-rs na escrita (`ListBuilder`,
/// `MapBuilder`, `StructArray`) — o caminho idiomático para construir arrays
/// com offsets. Assume colunas sem nulos.
#[pyfunction]
fn roundtrip_all_types(py: Python<'_>, batch: PyRecordBatch) -> PyResult<Py<PyAny>> {
    use arrow_array::builder::{BinaryBuilder, ListBuilder, MapBuilder, StringBuilder};
    use arrow_array::types::Date32Type;
    use arrow_array::{ArrayRef, Date32Array, TimestampMicrosecondArray};

    let rb = batch.into_inner();
    let n = rb.num_rows();

    // --- LEITURA (Python -> Rust): um downcast tipado por família de tipo ---
    let texto = downcast_column::<StringArray>(&rb, "texto", "utf8")?;
    let inteiro = typed_column::<Int64Type>(&rb, "inteiro")?;
    let flutuante = typed_column::<Float64Type>(&rb, "flutuante")?;
    let logico = downcast_column::<BooleanArray>(&rb, "logico", "bool")?;
    let data = typed_column::<Date32Type>(&rb, "data")?;
    let instante_col = get_column(&rb, "instante")?;
    if !matches!(instante_col.data_type(), DataType::Timestamp(TimeUnit::Microsecond, _)) {
        return Err(PyValueError::new_err(format!(
            "coluna 'instante' deveria ser timestamp[us], mas é {:?}",
            instante_col.data_type()
        )));
    }
    let instante = instante_col.as_primitive::<TimestampMicrosecondType>();
    let valor = decimal2_column(&rb, "valor")?;
    let lista = downcast_column::<arrow_array::ListArray>(&rb, "lista", "list<utf8>")?;
    let lista_vals = lista.values().as_string::<i32>();
    let estrutura = downcast_column::<arrow_array::StructArray>(&rb, "estrutura", "struct")?;
    let mapa = downcast_column::<arrow_array::MapArray>(&rb, "mapa", "map<utf8,utf8>")?;
    let binario = downcast_column::<arrow_array::BinaryArray>(&rb, "binario", "binary")?;

    // --- TRANSFORMAÇÃO + ESCRITA (Rust -> Python) ---

    // simples: coletar iteradores em arrays novos
    let texto_out: StringArray = (0..n).map(|i| Some(texto.value(i).to_uppercase())).collect();
    let inteiro_out = Int64Array::from((0..n).map(|i| inteiro.value(i) + 1).collect::<Vec<_>>());
    let flutuante_out =
        Float64Array::from((0..n).map(|i| flutuante.value(i) * 2.0).collect::<Vec<_>>());
    let logico_out: BooleanArray = (0..n).map(|i| Some(!logico.value(i))).collect();

    // date32 -> NaiveDate -> +30 dias de CALENDÁRIO -> date32
    let data_out = Date32Array::from(
        (0..n)
            .map(|i| naive_to_date32(date32_to_naive(data.value(i)) + Duration::days(30)))
            .collect::<Vec<_>>(),
    );

    // timestamp[us]: aritmética direta nos microssegundos
    let instante_out = TimestampMicrosecondArray::from(
        (0..n).map(|i| instante.value(i) + 3_600_000_000).collect::<Vec<_>>(),
    );

    // decimal: +10% com rust_decimal, arredondado para as 2 casas do projeto
    let fator = RustDecimal::new(110, 2); // 1.10
    let valor_out = Decimal128Array::from_iter_values((0..n).map(|i| {
        let mut novo = (decimal2_to_rust(valor.value(i)) * fator).round_dp(2);
        novo.rescale(2);
        novo.mantissa()
    }))
    .with_precision_and_scale(12, 2)
    .map_err(arrow_err)?;

    // list<utf8>: ListBuilder reconstrói offsets + valores
    let mut lista_builder = ListBuilder::new(StringBuilder::new());
    let loff = lista.value_offsets();
    for i in 0..n {
        for j in loff[i] as usize..loff[i + 1] as usize {
            lista_builder.values().append_value(lista_vals.value(j).to_uppercase());
        }
        lista_builder.append(true);
    }
    let lista_out = lista_builder.finish();

    // struct: transforma as arrays-filha e remonta com StructArray::from
    let nome = estrutura
        .column_by_name("nome")
        .ok_or_else(|| PyValueError::new_err("struct 'estrutura' não tem o campo 'nome'"))?
        .as_string::<i32>();
    let quantidade = estrutura
        .column_by_name("quantidade")
        .ok_or_else(|| PyValueError::new_err("struct 'estrutura' não tem o campo 'quantidade'"))?
        .as_primitive::<Int32Type>();
    let nome_out: StringArray = (0..n).map(|i| Some(nome.value(i).to_uppercase())).collect();
    let quantidade_out =
        Int32Array::from((0..n).map(|i| quantidade.value(i) * 2).collect::<Vec<_>>());
    let estrutura_out = arrow_array::StructArray::from(vec![
        (
            Arc::new(Field::new("nome", DataType::Utf8, true)),
            Arc::new(nome_out) as ArrayRef,
        ),
        (
            Arc::new(Field::new("quantidade", DataType::Int32, true)),
            Arc::new(quantidade_out) as ArrayRef,
        ),
    ]);

    // map<utf8,utf8>: MapBuilder com chaves preservadas e valores derivados
    let mut mapa_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    let mkeys = mapa.keys().as_string::<i32>();
    let mvals = mapa.values().as_string::<i32>();
    let moff = mapa.value_offsets();
    for i in 0..n {
        for j in moff[i] as usize..moff[i + 1] as usize {
            mapa_builder.keys().append_value(mkeys.value(j));
            mapa_builder.values().append_value(mvals.value(j).to_uppercase());
        }
        mapa_builder.append(true).map_err(arrow_err)?;
    }
    let mapa_out = mapa_builder.finish();

    // binary: bytes revertidos via BinaryBuilder
    let mut binario_builder = BinaryBuilder::new();
    for i in 0..n {
        let invertido: Vec<u8> = binario.value(i).iter().rev().copied().collect();
        binario_builder.append_value(&invertido);
    }
    let binario_out = binario_builder.finish();

    // schema de saída: os DataTypes dos aninhados vêm dos próprios arrays
    // construídos (evita divergência de nomes internos de campos)
    let schema = Arc::new(Schema::new(vec![
        Field::new("texto", DataType::Utf8, true),
        Field::new("inteiro", DataType::Int64, true),
        Field::new("flutuante", DataType::Float64, true),
        Field::new("logico", DataType::Boolean, true),
        Field::new("data", DataType::Date32, true),
        Field::new("instante", instante_out.data_type().clone(), true),
        Field::new("valor", DataType::Decimal128(12, 2), true),
        Field::new("lista", lista_out.data_type().clone(), true),
        Field::new("estrutura", estrutura_out.data_type().clone(), true),
        Field::new("mapa", mapa_out.data_type().clone(), true),
        Field::new("binario", DataType::Binary, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(texto_out),
        Arc::new(inteiro_out),
        Arc::new(flutuante_out),
        Arc::new(logico_out),
        Arc::new(data_out),
        Arc::new(instante_out),
        Arc::new(valor_out),
        Arc::new(lista_out),
        Arc::new(estrutura_out),
        Arc::new(mapa_out),
        Arc::new(binario_out),
    ];
    let out = RecordBatch::try_new(schema, columns).map_err(arrow_err)?;
    Ok(PyRecordBatch::new(out).into_pyarrow(py)?.unbind())
}

/// Soma uma coluna decimal128 e devolve o total como `rust_decimal::Decimal`.
///
/// A direção de volta da feature `rust_decimal` do pyo3: o retorno
/// `RustDecimal` chega ao Python como um `decimal.Decimal` genuíno — a soma
/// de uma coluna monetária atravessa a fronteira sem NUNCA passar por float.
/// A soma em si é feita nos i128 crus (exata por construção); o Decimal só
/// entra na borda, carregando a escala junto do valor.
#[pyfunction]
fn sum_decimal_column(batch: PyRecordBatch, column: &str) -> PyResult<RustDecimal> {
    let rb = batch.into_inner();
    let col = get_column(&rb, column)?;
    let scale = match col.data_type() {
        DataType::Decimal128(_, s) => *s,
        outro => {
            return Err(PyValueError::new_err(format!(
                "coluna '{column}' deveria ser decimal128, mas é {outro:?}"
            )))
        }
    };
    let values = col.as_primitive::<Decimal128Type>();
    let total: i128 = (0..values.len())
        .filter(|&i| values.is_valid(i))
        .map(|i| values.value(i))
        .sum();
    Ok(RustDecimal::from_i128_with_scale(total, scale as u32))
}

/// Registro do módulo Python `etl_rust_ext._etl_rust_ext` (nome definido no
/// `[tool.maturin]` do `pyproject.toml`); o pacote `etl_rust_ext` reexporta
/// as funções em `python/etl_rust_ext/__init__.py`.
#[pymodule]
fn _etl_rust_ext(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(add_line_total, m)?)?;
    m.add_function(wrap_pyfunction!(compute_customer_running_spend, m)?)?;
    m.add_function(wrap_pyfunction!(project_revenue_batch, m)?)?;
    m.add_function(wrap_pyfunction!(flatten_customer_profile, m)?)?;
    m.add_function(wrap_pyfunction!(compute_product_margin, m)?)?;
    m.add_function(wrap_pyfunction!(sum_decimal_column, m)?)?;
    m.add_function(wrap_pyfunction!(roundtrip_all_types, m)?)?;
    m.add_class::<ParallelRevenueProjector>()?;
    Ok(())
}
