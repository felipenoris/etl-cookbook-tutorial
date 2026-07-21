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

Modelo simples de 3 tabelas para exercitar JOINs (fato + 2 dimensões). As
dimensões concentram os tipos de dados da stack (ver a tabela de
compatibilidade abaixo); a fato fica só com tipos básicos, para manter as
partições calibradas:

- **customers** — particionado por `region` (Hive-style): `customer_id (int64),
  customer_name (string), region (string), signup_date (date32), is_active
  (bool), signup_ts (timestamp[us]), address (struct<street,city,zip>), tags
  (list<string>), preferences (map<string,string>)`.
- **products** — arquivo único pequeno: `product_id (int64), product_name
  (string), category (string), unit_price (float64), unit_cost
  (decimal128(12,2) — 2 casas decimais, o padrão do projeto), sku (binary)`.
- **orders** — fato, particionado por `order_year=2025/order_month=01..06` (6 partições
  de ~44MB cada, ~33.7M linhas no total): `order_id, customer_id, product_id, order_date, quantity, status`.

Os arquivos parquet não são versionados no git (ver `.gitignore`). Para gerar
(ou regenerar) os dados:

```bash
uv run data/generate_data.py --generate           # gera as bases em data/raw
uv run data/generate_data.py --clean              # remove os parquet de raw/ e rich/
uv run data/generate_data.py --clean --generate   # regenera do zero
```

## Pré-requisitos

Para rodar o `./check_all.sh` (e o repositório em geral), a máquina precisa de:

1. **[`uv`](https://docs.astral.sh/uv/)** — o único gerenciador a instalar
   para o lado Python. Ele resolve sozinho, na primeira execução, tudo o
   mais: o interpretador Python de cada projeto, as dependências
   (pandas/pyarrow/duckdb/numpy), o `maturin` que compila a extensão e as
   ferramentas de dev (`pytest`, `pdoc`). Não é preciso ter Python instalado
   nem ativar venv manualmente.
2. **Toolchain Rust** ([rustup.rs](https://rustup.rs)) — `cargo`/`rustc`,
   usados para compilar a extensão PyO3 (`rust-extension`) e gerar o rustdoc.
   As crates (pyo3, arrow) são baixadas pelo cargo na primeira compilação.
3. **Acesso à internet na primeira execução** — para o `uv` e o `cargo`
   baixarem dependências. Depois disso, apenas 3 testes do DuckDB (leitura de
   buckets S3 públicos, exemplo 13) precisam de rede — `./check_all.sh
   --no-network` os pula.
4. **bash** — os scripts `check_all.sh`/`clean_all.sh` são shell scripts
   (macOS e Linux funcionam direto; no Windows, use WSL ou Git Bash).
5. **~2.5GB de disco livre** — dados fictícios gerados (~1.5GB em
   `data/raw` + `data/rich`), um `.venv` por projeto (~200-250MB cada) e o
   build Rust (~120MB). O `./clean_all.sh` recupera esse espaço.

Nada além disso: sem servidor de banco, sem Docker, sem credenciais — os
exemplos de S3 usam buckets públicos com acesso anônimo.

## Compatibilidade de tipos entre as tecnologias

Como cada tipo de dado viaja pela stack — do SQL do DuckDB, passando pela
representação em memória (Arrow) e em disco (Parquet), até os objetos Python
e os arrays do Rust (`arrow-rs`, usados na extensão via `pyo3-arrow`):

| Tipo | DuckDB | Arrow | Parquet | Python (`ArrowDtype`/escalar) | Rust (arrow-rs) |
| --- | --- | --- | --- | --- | --- |
| String | `VARCHAR` | `utf8` | `BYTE_ARRAY` (String) | `string[pyarrow]` / `str` | `StringArray` (`&str`) |
| Inteiro | `BIGINT`/`INTEGER` | `int64`/`int32` | `INT64`/`INT32` | `int64[pyarrow]` / `int` | `Int64Array` (`i64`) |
| Float64 | `DOUBLE` | `float64` | `DOUBLE` | `double[pyarrow]` / `float` | `Float64Array` (`f64`) |
| Booleano | `BOOLEAN` | `bool` | `BOOLEAN` | `bool[pyarrow]` / `bool` | `BooleanArray` (`bool`) |
| Date | `DATE` | `date32[day]` | `INT32` (Date) | `date32[pyarrow]` / `datetime.date` | `Date32Array` (`i32` dias) |
| Timestamp | `TIMESTAMP` | `timestamp[us]` | `INT64` (Timestamp µs) | `timestamp[us][pyarrow]` / `datetime` | `TimestampMicrosecondArray` (`i64` µs) |
| Decimal | `DECIMAL(12,2)` | `decimal128(12,2)` | `FIXED_LEN_BYTE_ARRAY` (Decimal) | `decimal128(12,2)[pyarrow]` / `decimal.Decimal` | `Decimal128Array` (`i128` escalado) |
| List | `VARCHAR[]` | `list<utf8>` | `LIST` (3 níveis) | `list<string>[pyarrow]` / `list` | `ListArray` (offsets + valores) |
| Struct | `STRUCT(...)` | `struct<...>` | grupo aninhado | `struct<...>[pyarrow]` / `dict` | `StructArray` (arrays-filha) |
| Map | `MAP(VARCHAR,VARCHAR)` | `map<utf8,utf8>` | `MAP` (key_value) | `map<...>[pyarrow]` / lista de pares | `MapArray` (keys/values + offsets) |
| Binary | `BLOB` | `binary` | `BYTE_ARRAY` | `binary[pyarrow]` / `bytes` | `BinaryArray` (`&[u8]`) |

Observações que os exemplos demonstram na prática:

- **Decimal**: o projeto padroniza **2 casas decimais** (`decimal128(12,2)`).
  Somas e multiplicações preservam o tipo exato em todas as camadas (a escala
  2 se mantém; a precisão cresce). No Python, o tipo escalar é sempre o
  `decimal.Decimal` da stdlib; no Rust, as colunas Arrow (`i128` + escala)
  são convertidas para [`rust_decimal::Decimal`](https://docs.rs/rust_decimal)
  para a aritmética, e escalares atravessam a fronteira Python↔Rust como
  `decimal.Decimal` ↔ `rust_decimal::Decimal` (feature `rust_decimal` do
  pyo3). Cuidado com o que degrada para float: `AVG` no DuckDB e a mistura
  float×decimal (promova o float para decimal antes).
- **Date/Timestamp**: no Python, o tipo escalar é o `datetime.date` /
  `datetime.datetime` da stdlib (é o que `.as_py()` e o `fetchone()` do
  DuckDB devolvem, e o que `pa.array(...)` aceita na construção). No Rust,
  escalares atravessam a fronteira como `datetime.date` ↔
  [`chrono::NaiveDate`](https://docs.rs/chrono) (feature `chrono` do pyo3), e
  as colunas date32 (i32 de dias) são convertidas para `NaiveDate` para
  aritmética de calendário.
- **Aninhados (list/struct/map)**: DuckDB acessa com `tags[1]`,
  `address.city` e `preferences['chave']`; pandas tem os acessores `.list` e
  `.struct` (map exige a escotilha pyarrow); o motor de join do pyarrow
  (Acero) **não aceita colunas aninhadas como payload** — projete/achate
  antes do join. No Rust, a escrita usa os builders do arrow-rs
  (`ListBuilder`, `MapBuilder`, `StructArray`) — ver `roundtrip_all_types`
  em `rust-extension`, que exercita leitura e escrita dos 11 tipos.
- Onde ver cada camada: [`pyarrow/examples/10`](pyarrow/examples/10_data_types.py),
  [`pandas/examples/09`](pandas/examples/09_arrow_data_types.py),
  [`DuckDB/examples/14`](DuckDB/examples/14_data_types.py) e
  [`rust-extension/run_data_types.py`](rust-extension/run_data_types.py).

## Verificação completa com um comando

Acabou de clonar? Um único comando gera os dados, roda as 4 suítes de testes
(cujos smoke tests executam **todos** os scripts de `examples/`), executa os
dois pipelines do `rust-extension` e gera as documentações (pdoc e cargo doc):

```bash
./check_all.sh                # completo (3 testes do DuckDB usam internet)
./check_all.sh --no-network   # ambiente sem acesso à internet
```

Qualquer falha interrompe o script; ao final, um "Tudo OK!" confirma que o
repositório está funcional.

O inverso — remover tudo que foi gerado (dados parquet, documentações, build
Rust, caches), voltando ao estado pós-clone:

```bash
./clean_all.sh          # limpa artefatos gerados (mantém os .venv)
./clean_all.sh --all    # também remove os .venv
```

## Por onde começar

1. `uv run data/generate_data.py --generate` — obrigatório após clonar o
   repositório, já que os parquet não são versionados (o `./check_all.sh`
   acima já faz isso automaticamente).
2. [`pandas/`](pandas) e [`pyarrow/`](pyarrow) — mesmos conceitos (seleção,
   limpeza, groupby, joins, pivot), comparando a API de alto nível do pandas
   com a API nativa do Arrow — mais o interop zero-copy entre as duas e o
   padrão híbrido (pyarrow nas bordas, pandas no miolo) para equipes
   proficientes em pandas.
3. [`DuckDB/`](DuckDB) — os mesmos joins/agregações em SQL, mais o exemplo de
   `memory_limit`/spill em disco e um bloco de funcionalidades de ETL:
   `COPY TO` particionado com recarga idempotente, staging persistente com
   UPSERT, ingestão de CSV com quarentena de rejeitadas, SQL avançado
   (recursiva, `PIVOT`, `ASOF JOIN`), macros/UDFs Python e
   `EXPORT`/`IMPORT DATABASE`.
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

## Referências

Ferramentas usadas em todo o tutorial:

- [uv — documentação oficial](https://docs.astral.sh/uv/) — gerenciador de projetos/dependências Python usado em todas as subpastas; ver também [scripts standalone com PEP 723](https://peps.python.org/pep-0723/), o formato usado por `data/generate_data.py`.
- [Formato Apache Parquet](https://parquet.apache.org/docs/) — o formato colunar de arquivo usado como origem (`data/raw`) e destino (`data/rich`).
- [Formato colunar Apache Arrow](https://arrow.apache.org/docs/format/Columnar.html) — a representação em memória que conecta pandas, pyarrow, DuckDB e a extensão Rust sem cópias.
- [pytest — documentação oficial](https://docs.pytest.org/en/stable/) — usado nas suítes de teste de todas as subpastas.

Referências específicas de cada tecnologia estão no `README.md` da subpasta
correspondente ([`pandas/`](pandas), [`pyarrow/`](pyarrow), [`DuckDB/`](DuckDB),
[`rust-extension/`](rust-extension)).

## Licença

Distribuído sob a licença MIT — ver [LICENSE](LICENSE).
