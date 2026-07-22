# rust-extension — PyO3 + pyo3-arrow (zero-copy Rust ↔ Python)

Projeto Python isolado (gerenciado com `uv`) com uma extensão nativa em Rust
(`etl_rust_ext`), construída com [PyO3](https://pyo3.rs) e
[pyo3-arrow](https://github.com/kylebarron/arro3/tree/main/pyo3-arrow) para
passagem **zero-copy** de dados Arrow entre Python e Rust. Fecha o tutorial
com um ETL completo que usa DuckDB, pyarrow, pandas e Rust juntos.

## Estrutura

```
rust-extension/
  Cargo.toml              # crate Rust: pyo3 + pyo3-arrow
  pyproject.toml          # build-system = maturin (compilado automaticamente por `uv sync`)
  src/lib.rs              # funções expostas ao Python (ver "Funções expostas" abaixo)
  python/etl_rust_ext/     # pacote Python (mixed layout do maturin)
    __init__.py
  run_etl.py               # pipeline de ETL completo (DuckDB -> pyarrow -> Rust -> pandas -> parquet)
  run_contracts_parallel.py # multithreading: lotes submetidos serialmente, processados em paralelo
  run_data_types.py        # tipos Arrow (struct/list/map/decimal/binary...) manipulados no Rust
  run_nested_params.py     # 1:N no Rust: materializar vs emprestar fatias (a discussão "isso vira um ORM?")
  docs_demo.py             # demonstração dos recursos do pdoc (math, mermaid, include, markdown)
  docs_includes/            # arquivos markdown puxados via `.. include::` nas docstrings
  docs/                     # gerado por `pdoc` (etapa 7) — abrir docs/index.html no browser
```

## Setup e build

```bash
cd rust-extension
uv sync   # compila a extensão Rust via maturin automaticamente
```

Não é preciso instalar `maturin` manualmente: como ele está declarado em
`[build-system]` do `pyproject.toml`, o `uv` cuida de baixá-lo e invocá-lo
durante o build. Qualquer mudança em `src/lib.rs` exige rodar `uv sync` (ou
`uv run ...`, que também reconstrói se necessário) de novo.

## Como o zero-copy funciona

1. `run_etl.py` monta um `pyarrow.RecordBatch` (via DuckDB + pyarrow).
2. Ao chamar `etl_rust_ext.compute_customer_running_spend(batch)`, o
   `pyo3-arrow` reconhece que `batch` implementa o protocolo
   `__arrow_c_array__` (a Arrow C Data Interface) e importa os arrays
   **sem copiar buffers** — o Rust literalmente lê a mesma região de memória
   que o pyarrow já tinha alocado.
3. Em Rust, as colunas de entrada são acessadas por downcast direto
   (`as_primitive::<Int64Type>()`); só as colunas novas calculadas
   (`cumulative_spend`, `customer_tier`) são de fato alocadas.
4. O `RecordBatch` de saída é devolvido ao Python via `.into_pyarrow(py)`,
   virando um `pyarrow.RecordBatch` de verdade (não um wrapper específico do
   `pyo3-arrow`), pronto para seguir no pipeline.

## Funções expostas (`etl_rust_ext`)

- `add_line_total(batch)` — exemplo simples: `line_total = quantity * unit_price`.
- `compute_customer_running_spend(batch, threshold_prata=500.0, threshold_ouro=2000.0)`
  — exemplo que justifica sair do domínio vetorizado: acumula o gasto por
  cliente num único loop sequencial com estado (`HashMap<customer_id, total>`)
  e classifica um tier (bronze/prata/ouro) segundo os thresholds informados.
  O MESMO cálculo aparece em Python puro nos exemplos "sequential_stateful_loop"
  de [`../pandas`](../pandas/examples/10_sequential_stateful_loop.py),
  [`../pyarrow`](../pyarrow/examples/11_sequential_stateful_loop.py) e
  [`../DuckDB`](../DuckDB/examples/15_sequential_stateful_loop.py) — que
  exercitam a API de streaming de cada biblioteca e mostram por que esse caso
  (lógica sequencial que não vetoriza) é onde uma extensão nativa compensa.
- `project_revenue_batch(batch)` / classes `ParallelRevenueProjector` (paralelo
  simples) e `BoundedRevenueProjector` (memória constante: pool fixo + fila
  limitada + escrita incremental em parquet) — projeção de receita de
  contratos, serial e paralela (ver seção de multithreading).
- `flatten_customer_profile(batch, reference_date)` — tipos Arrow complexos
  lidos no Rust: struct (`address.city`), list (tamanho de `tags`), map
  (lookup de chave), timestamp e bool. O `reference_date` atravessa a
  fronteira como `datetime.date` -> [`chrono::NaiveDate`](https://docs.rs/chrono)
  (feature opcional `chrono` do pyo3), a aritmética de dias usa o calendário
  do chrono e a coluna `signup_date` volta como date32.
- `roundtrip_all_types(batch)` — o teste integral da fronteira: um batch com
  uma coluna de CADA um dos 11 tipos da stack entra no Rust e volta com o
  mesmo schema, cada coluna derivada nativamente (uppercase, +30 dias de
  calendário, +10% decimal, bytes revertidos...). Exercita leitura E escrita
  de todos os tipos, incluindo os aninhados via builders do arrow-rs
  (`ListBuilder`, `MapBuilder`, `StructArray`).
- `compute_product_margin(batch, desconto=Decimal("0.00"))` — decimal de
  ponta a ponta: a coluna `unit_cost` (decimal128 de escala 2) vira
  [`rust_decimal::Decimal`](https://docs.rs/rust_decimal) no Rust (aritmética
  decimal exata, `round_dp(2)`), e o `desconto` atravessa a fronteira como
  `decimal.Decimal` -> `rust_decimal::Decimal` via a feature opcional
  `rust_decimal` do pyo3. O wrapper Python rejeita float com `TypeError`
  (política do projeto para valores monetários). Também expõe o binary
  (`sku` -> hex).
- `sum_decimal_column(batch, coluna)` — a direção de volta: o total sai do
  Rust como `rust_decimal::Decimal` e chega ao Python como `decimal.Decimal`,
  sem passar por float em momento algum.
  Rode `uv run run_data_types.py` para ver tudo em ação sobre `data/raw`.
- `project_nested_materialized` / `project_nested_reused` /
  `project_nested_borrowed` — a mesma projeção de contratos com parâmetros
  1:N, em três estratégias de materialização (ver seção abaixo).

`compute_customer_running_spend` também ilustra um padrão comum em extensões
nativas: a função Rust (`src/lib.rs`) exige todos os argumentos, e um helper
fino em Python de mesmo nome (`python/etl_rust_ext/__init__.py`) fornece os
defaults e a docstring — a assinatura amigável fica na camada Python, o
trabalho pesado na camada Rust.

## Multithreading: `ParallelRevenueProjector`

```bash
uv run run_contracts_parallel.py
```

Exercita o padrão **submissão serial, processamento paralelo, coleta
consolidada** com um caso de projeção de receita de contratos (juros da
tabela Price, simulados mês a mês — cálculo independente por contrato, ideal
para paralelizar):

1. o Python lê a fonte (parquet de contratos) em lotes e chama
   `submit_batch(batch)` para cada um — a chamada valida o schema, dispara
   uma thread Rust e **retorna imediatamente** (~0.1ms por lote);
2. as threads são Rust puro e não tocam em objetos Python, então rodam
   **fora do GIL** — o cálculo dos lotes anteriores acontece enquanto o
   Python ainda lê os próximos;
3. `collect()` faz join de todas as threads (soltando o GIL na espera, via
   `py.detach`) e devolve um único `pyarrow.RecordBatch` consolidado
   (`id_contrato`, `receita_projetada`), na ordem de submissão.

Resultado na prática: ~5.5x de speedup sobre a versão sequencial
(`project_revenue_batch`, mesma computação), com resultados bit a bit
idênticos.

### `BoundedRevenueProjector` — memória constante para bases massivas

O `ParallelRevenueProjector` acima tem uma limitação importante: o
`submit_batch` **nunca bloqueia**, então se o Python ler mais rápido que os
workers processam, os lotes em voo se acumulam sem limite (no pior caso, a
base inteira fica residente), e o `collect()` materializa a saída inteira.
Ok para bases pequenas; arriscado para as massivas.

O `BoundedRevenueProjector` mantém o pico de memória **constante,
independente do tamanho da base**, com três mecanismos:

- **pool de tamanho fixo** (N workers reaproveitados, não uma thread por lote);
- **fila limitada** (`queue_depth`): `submit_batch` **bloqueia** — soltando o
  GIL via `py.detach` — quando a fila enche. É o *backpressure* que impede o
  Python de correr à frente dos workers (`crossbeam-channel` provê a fila
  MPMC limitada);
- **escrita incremental**: uma thread escritora grava cada resultado direto
  num parquet (via `parquet::arrow::ArrowWriter`); a saída nunca volta
  consolidada — `finish()` devolve só `(caminho, linhas_escritas)`.

Topologia: `submit_batch` → fila limitada → N workers → fila de resultados →
1 escritora → parquet. Pico de memória ≈ `queue_depth` lotes de entrada + N
em processamento. No exemplo, 1,6M de contratos são processados com uma fila
de apenas 3 lotes e gravados em parquet — a base inteira nunca fica na RAM.

## Dados 1:N no Rust: materializar ou emprestar?

```bash
uv run run_nested_params.py
```

Responde à dúvida natural de quem migra de um ETL com ORM: *"se eu construo
um `Contrato` com um `Vec<Parametro>` dentro para rodar o algoritmo, não
recriei o problema do ORM?"*. O exemplo mede as três estratégias sobre 1M de
contratos com parâmetros 1:N (colunas `list<...>`), todas chamando o **mesmo**
núcleo de cálculo — então a diferença isola só o custo de materialização:

| Estratégia | Alocações | Ganho de performance (aprox.) |
| --- | --- | --- |
| **A)** `Vec` próprio por contrato (estilo ORM) | 2 por linha — O(n) | 1x (linha de base) |
| **B)** buffers reaproveitados (`clear()` + refill) | O(1) | **~3x** |
| **C)** fatias emprestadas sobre o `ListArray` | **zero** | **~4x** |

A chave da variante C: no Arrow, uma coluna `list` é um **array plano de
valores + offsets**, então os parâmetros de cada contrato *já são* uma fatia
contígua do buffer. Uma struct `ContratoRef<'a>` com campos `&'a [f64]` dá a
ergonomia de "contrato com seu vetor de parâmetros" **sem copiar um byte** —
e o lifetime garante, em compilação, que a fatia não sobrevive ao buffer.

A lição não é que A seja inviável (as três rodam 1M de contratos em dezenas
de ms — em Python, objetos por linha custariam segundos e pressão de GC), e
sim que **"não materialize por linha" continua valendo dentro do Rust**, com
um preço bem mais benigno. O docstring do exemplo traz a tabela completa de
quais custos do ORM sobrevivem à mudança de linguagem — vale a leitura no
[pdoc gerado](docs/run_nested_params.html).

## Rodando o ETL completo

```bash
uv run run_etl.py
```

Lê `../data/raw/{orders,customers,products}`, junta as 3 tabelas via DuckDB
(com `memory_limit`/`temp_directory` configurados para exercitar spill, igual
ao exemplo `../DuckDB/examples/04_memory_limit_and_spill.py`), enriquece com
a extensão Rust, resume com pandas (backend Arrow) e grava o resultado em
`../data/rich/order_metrics/`, particionado por `customer_tier`.

## Documentação (etapa 7)

```bash
uv run pdoc --math --mermaid --docformat google --output-dir docs etl_rust_ext ./run_etl.py ./run_contracts_parallel.py ./run_data_types.py ./run_nested_params.py ./docs_demo.py
```

Gera HTML estático em `docs/`, navegável abrindo `docs/index.html` direto do
disco no browser (sem precisar de servidor).

As flags e o módulo extra:

- `--math` — renderiza fórmulas LaTeX nas docstrings (`$...$` inline,
  `$$...$$` em destaque) via MathJax;
- `--mermaid` — renderiza blocos ```` ```mermaid ```` como diagramas
  (fluxograma, sequência, pizza e gráficos de barras/linha via
  `xychart-beta`);
- `--docformat google` — formata seções `Args:`/`Returns:`/`Raises:` das
  docstrings estilo Google como listas estruturadas;
- `docs_demo.py` — módulo de demonstração que exercita esses recursos, mais a
  inclusão de arquivo markdown externo (diretiva `.. include::`, puxando
  `docs_includes/glossario.md`), docstring dinâmica (seção do `__doc__`
  construída executando uma função do próprio módulo no momento da geração)
  e os marcadores usuais (tabelas, listas, negrito/itálico, citações, blocos
  de código). Abra `docs/docs_demo.html` lado a lado com o fonte
  `docs_demo.py` para ver como cada efeito é obtido.

Observação: MathJax e mermaid são carregados de CDN — o HTML abre localmente
sem servidor, mas fórmulas e diagramas precisam de acesso à internet para
renderizar.

### Documentação do crate Rust (rustdoc)

A camada Rust tem documentação própria, gerada pelo `cargo doc` a partir dos
comentários `//!` (módulo) e `///` (itens) de `src/lib.rs`:

```bash
cargo doc --no-deps --document-private-items
```

Abra `target/doc/_etl_rust_ext/index.html` no browser (HTML estático, direto
do disco). As flags:

- `--no-deps` — documenta só o nosso crate, sem as dependências (pyo3,
  arrow-rs etc., que já têm docs no [docs.rs](https://docs.rs/));
- `--document-private-items` — necessário porque as funções `#[pyfunction]`
  são privadas no Rust (quem as expõe ao Python é o `#[pymodule]`); sem a
  flag, a página sairia só com a documentação do módulo.

Para limpar apenas os artefatos de documentação (sem descartar o build):

```bash
cargo clean --doc
```

A saída fica dentro de `target/`, que já está no `.gitignore` da raiz — nada
a versionar.

A pasta `docs/` é gerada automaticamente e **não é versionada** (está no
`.gitignore` da raiz). Para limpar a documentação gerada:

```bash
rm -rf docs
```

E para regenerá-la, basta rodar o comando `pdoc` acima novamente (a extensão
Rust precisa estar compilada — `uv sync` cuida disso).

## Testes

```bash
uv run pytest -m "not slow"   # rápido: funções Rust + etapas do ETL com dados pequenos
uv run pytest                 # inclui o pipeline completo sobre data/raw (~15s)
```

`tests/test_rust_extension.py` exercita `add_line_total` e
`compute_customer_running_spend` com RecordBatches pequenos (valores
calculados, propagação de nulos, erro para coluna ausente).
`tests/test_data_types.py` cobre as funções de tipos: o roundtrip dos 11
tipos, as fronteiras `datetime.date`/`decimal.Decimal` (incluindo o float
rejeitado) e as validações de schema/escala.
`tests/test_parallel_projection.py` cobre `project_revenue_batch` e as duas
classes de projeção: o `ParallelRevenueProjector` (resultado consolidado igual
ao serial, ordem de submissão, erros de schema) e o
`BoundedRevenueProjector` (saída em parquet igual ao serial, fila de
profundidade 1 sem deadlock, erros de `finish`/caminho inválido).
`tests/test_nested_params.py` cobre as três estratégias de materialização 1:N
(as três contra uma referência independente em Python puro, concordância
mútua, sublistas vazias e erros de tipo aninhado).
`tests/test_run_etl.py` testa cada etapa do pipeline isoladamente e, no teste
marcado `slow`, roda o ETL inteiro gravando num diretório temporário.

## Referências

- [PyO3 — guia oficial](https://pyo3.rs/) — o livro do PyO3 (`#[pyfunction]`, `#[pymodule]`, conversões, GIL); ver também a [API reference no docs.rs](https://docs.rs/pyo3).
- [pyo3-arrow — docs.rs](https://docs.rs/pyo3-arrow) — a ponte Arrow usada aqui (`PyRecordBatch`, `into_pyarrow`), com a explicação do transporte zero-copy.
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) — a especificação (protocolo `__arrow_c_array__`) que permite passar arrays entre pyarrow e Rust sem copiar buffers.
- [arrow-rs — implementação Rust do Arrow](https://arrow.apache.org/rust/) — as crates `arrow-array`/`arrow-schema` usadas em `src/lib.rs`.
- [maturin — guia oficial](https://www.maturin.rs/) — o build backend que compila a crate e a empacota como módulo Python (configurado no `pyproject.toml`).
- [arro3](https://github.com/kylebarron/arro3) — biblioteca Arrow para Python construída sobre pyo3-arrow, boa fonte de exemplos reais da API.
- [pyo3-cookbook](https://github.com/felipenoris/pyo3-cookbook) — coletânea de receitas PyO3 que inspirou a organização desta camada Rust.
- [pdoc — documentação oficial](https://pdoc.dev/) — a ferramenta que gera o HTML estático de `docs/` a partir das docstrings.
