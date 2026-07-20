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
  src/lib.rs              # funções expostas: add_line_total, compute_customer_running_spend
  python/etl_rust_ext/     # pacote Python (mixed layout do maturin)
    __init__.py
  run_etl.py               # pipeline de ETL completo (DuckDB -> pyarrow -> Rust -> pandas -> parquet)
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

`compute_customer_running_spend` também ilustra um padrão comum em extensões
nativas: a função Rust (`src/lib.rs`) exige todos os argumentos, e um helper
fino em Python de mesmo nome (`python/etl_rust_ext/__init__.py`) fornece os
defaults e a docstring — a assinatura amigável fica na camada Python, o
trabalho pesado na camada Rust.

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
uv run pdoc --math --mermaid --docformat google --output-dir docs etl_rust_ext ./run_etl.py ./docs_demo.py
```

Gera HTML estático em `docs/`, navegável abrindo `docs/index.html` direto do
disco no browser (sem precisar de servidor).

As flags e o módulo extra:

- `--math` — renderiza fórmulas LaTeX nas docstrings (`$...$` inline,
  `$$...$$` em destaque) via MathJax;
- `--mermaid` — renderiza blocos ```` ```mermaid ```` como diagramas;
- `--docformat google` — formata seções `Args:`/`Returns:`/`Raises:` das
  docstrings estilo Google como listas estruturadas;
- `docs_demo.py` — módulo de demonstração que exercita esses recursos, mais a
  inclusão de arquivo markdown externo (diretiva `.. include::`, puxando
  `docs_includes/glossario.md`) e os marcadores usuais (tabelas, listas,
  negrito/itálico, citações, blocos de código). Abra `docs/docs_demo.html`
  lado a lado com o fonte `docs_demo.py` para ver como cada efeito é obtido.

Observação: MathJax e mermaid são carregados de CDN — o HTML abre localmente
sem servidor, mas fórmulas e diagramas precisam de acesso à internet para
renderizar.

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

`tests/test_rust_extension.py` exercita as duas funções Rust com RecordBatches
pequenos (valores calculados, propagação de nulos, erro para coluna ausente).
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
