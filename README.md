# ETL Cookbook Tutorial

Projeto didático com exemplos independentes exercitando etapas específicas de
um pipeline de ETL de dados, cada um em seu próprio projeto Python isolado
(gerenciado com [`uv`](https://docs.astral.sh/uv/)), lendo a mesma base de
dados fictícia particionada em parquet.

## Estrutura

```
etl-cookbook-tutorial/
  data/
    generate_data.py   # gera as bases fictícias (script standalone, PEP 723)
    raw/               # parquet particionado de entrada (customers, products, orders)
    rich/              # parquet de saída do ETL (rust-extension/run_etl.py)
  pandas/              # API do pandas com backend Arrow
  pyarrow/             # API nativa do pyarrow
  DuckDB/              # SQL em memória sobre parquet, com spill configurável
  rust-extension/      # extensão Rust (PyO3 + pyo3-arrow) + ETL completo + docs (pdoc)
```

## Mapa de objetivos

| # | Objetivo | Onde |
| --- | --- | --- |
| O1 | Python | todos os projetos |
| O2 | `uv` para gerenciar dependências | um `pyproject.toml`/`.venv` isolado por pasta |
| O3 | Extensão Python em Rust via PyO3 | [`rust-extension/src/lib.rs`](rust-extension/src/lib.rs) |
| O4 | pyarrow | [`pyarrow/`](pyarrow), e usado também em `pandas`/`DuckDB`/`rust-extension` |
| O5 | pandas com Arrow como backend | [`pandas/`](pandas) (`dtype_backend="pyarrow"`) |
| O6 | Passagem zero-copy Python↔Rust via `pyo3-arrow` | [`rust-extension/`](rust-extension) (inspirado em [pyo3-cookbook](https://github.com/felipenoris/pyo3-cookbook)) |
| O7 | ETL a partir de parquet particionado | [`data/raw/`](data/raw) (orders, customers, products) |
| O8 | DuckDB com JOIN/SQL complexo + spill | [`DuckDB/`](DuckDB) |
| O9 | Documentação HTML estática a partir de docstrings | [`rust-extension/docs/`](rust-extension/docs) (gerado com `pdoc`, abre via `file://`) |

## Base de dados fictícia (`data/raw`)

Modelo simples de 3 tabelas para exercitar JOINs (fato + 2 dimensões):

- **customers** — particionado por `region` (Hive-style): `customer_id, customer_name, region, signup_date`.
- **products** — arquivo único pequeno: `product_id, product_name, category, unit_price`.
- **orders** — fato, particionado por `order_year=2025/order_month=01..06` (6 partições
  de ~44MB cada, ~33.7M linhas no total): `order_id, customer_id, product_id, order_date, quantity, status`.

Os arquivos parquet não são versionados no git (ver `.gitignore`). Para gerar
(ou regenerar) os dados:

```bash
uv run data/generate_data.py --generate           # gera as bases em data/raw
uv run data/generate_data.py --clean              # remove os parquet de raw/ e rich/
uv run data/generate_data.py --clean --generate   # regenera do zero
```

## Por onde começar

1. `uv run data/generate_data.py --generate` — obrigatório após clonar o
   repositório, já que os parquet não são versionados.
2. [`pandas/`](pandas) e [`pyarrow/`](pyarrow) — mesmos conceitos (seleção,
   limpeza, groupby, joins, pivot), comparando a API de alto nível do pandas
   com a API nativa do Arrow.
3. [`DuckDB/`](DuckDB) — os mesmos joins/agregações em SQL, mais o exemplo de
   `memory_limit`/spill em disco.
4. [`rust-extension/`](rust-extension) — fecha o ciclo: um ETL real que usa
   DuckDB (extract+join+spill) → pyarrow (projeção) → Rust via `pyo3-arrow`
   (transformação com estado, zero-copy) → pandas (resumo) → grava em
   `data/rich/order_metrics/`.

Cada subpasta tem seu próprio `README.md` com a lista de exemplos e os
conceitos exercitados.

## Testes

Cada projeto tem sua própria suíte pytest (smoke tests dos exemplos + testes
unitários dos contratos assumidos). Para rodar tudo, a partir da raiz:

```bash
(cd pandas && uv run pytest)
(cd pyarrow && uv run pytest)
(cd DuckDB && uv run pytest)
(cd rust-extension && uv run pytest)   # -m "not slow" pula o pipeline completo
```
