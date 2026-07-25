# ETL Cookbook Tutorial

[![CI](https://github.com/felipenoris/etl-cookbook-tutorial/actions/workflows/ci.yml/badge.svg)](https://github.com/felipenoris/etl-cookbook-tutorial/actions/workflows/ci.yml)
[![Docs](https://github.com/felipenoris/etl-cookbook-tutorial/actions/workflows/docs.yml/badge.svg)](https://felipenoris.github.io/etl-cookbook-tutorial/)

📖 **Documentação publicada:** <https://felipenoris.github.io/etl-cookbook-tutorial/>

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
    rich/              # parquet de saída do ETL (examples-rust-extension/run_etl.py)
  examples-pandas/              # API do pandas com backend Arrow
  examples-pyarrow/             # API nativa do pyarrow
  examples-DuckDB/              # SQL em memória sobre parquet, com spill configurável
  examples-rust-extension/      # extensão Rust (PyO3 + pyo3-arrow) + ETL completo + docs (pdoc)
  examples-sqlalchemy-contract/ # migração do padrão ORM: modelos como contrato, ORM vs colunar, árvore de contas
```

## Mapa de objetivos

| # | Objetivo | Onde |
| --- | --- | --- |
| O1 | Python | todos os projetos |
| O2 | `uv` para gerenciar dependências | um `pyproject.toml`/`.venv` isolado por pasta |
| O3 | Extensão Python em Rust via PyO3 | [`examples-rust-extension/src/lib.rs`](examples-rust-extension/src/lib.rs) |
| O4 | pyarrow | [`examples-pyarrow/`](examples-pyarrow), e usado também em `examples-pandas`/`examples-DuckDB`/`examples-rust-extension` |
| O5 | pandas com Arrow como backend | [`examples-pandas/`](examples-pandas) (`dtype_backend="pyarrow"`) |
| O6 | Passagem zero-copy Python↔Rust via `pyo3-arrow` | [`examples-rust-extension/`](examples-rust-extension) (inspirado em [pyo3-cookbook](https://github.com/felipenoris/pyo3-cookbook)) |
| O7 | ETL a partir de parquet particionado | [`data/raw/`](data/raw) (orders, customers, products) |
| O8 | DuckDB com JOIN/SQL complexo + spill | [`examples-DuckDB/`](examples-DuckDB) |
| O9 | Documentação HTML estática a partir de docstrings | [`examples-rust-extension/docs/`](examples-rust-extension/docs) (gerado com `pdoc`, abre via `file://`) |

## Base de dados fictícia (`data/raw`)

Modelo simples de 3 tabelas para exercitar JOINs (fato + 2 dimensões). As
dimensões concentram os tipos de dados da stack (ver a tabela de
compatibilidade de tipos na [documentação publicada](https://felipenoris.github.io/etl-cookbook-tutorial/python/));
a fato fica só com tipos básicos, para manter as partições calibradas:

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
uv run --script data/generate_data.py --generate           # gera as bases em data/raw
uv run --script data/generate_data.py --clean              # remove os parquet de raw/ e rich/
uv run --script data/generate_data.py --clean --generate   # regenera do zero
```

## Pré-requisitos

Para rodar o `./check-all.sh` (e o repositório em geral), a máquina precisa de:

1. **[`uv`](https://docs.astral.sh/uv/)** — o único gerenciador a instalar
   para o lado Python. Ele resolve sozinho, na primeira execução, tudo o
   mais: o interpretador Python de cada projeto, as dependências
   (pandas/pyarrow/duckdb/numpy), o `maturin` que compila a extensão e as
   ferramentas de dev (`pytest`, `pdoc`). Não é preciso ter Python instalado
   nem ativar venv manualmente.
2. **Toolchain Rust** ([rustup.rs](https://rustup.rs)) — `cargo`/`rustc`,
   usados para compilar a extensão PyO3 (`examples-rust-extension`) e gerar o rustdoc.
   As crates (pyo3, arrow) são baixadas pelo cargo na primeira compilação.
3. **Acesso à internet na primeira execução** — para o `uv` e o `cargo`
   baixarem dependências. Depois disso, apenas 3 testes do DuckDB (leitura de
   buckets S3 públicos, exemplo 13) precisam de rede — `./check-all.sh
   --no-network` os pula.
4. **bash** — os scripts `check-all.sh`/`clean-all.sh` são shell scripts
   (macOS e Linux funcionam direto; no Windows, use WSL ou Git Bash).
5. **~2.7GB de disco livre** — dados fictícios gerados (~1.5GB em
   `data/raw` + `data/rich`), um `.venv` por projeto (5 projetos,
   ~200-250MB cada) e o build Rust (~130MB). O `./clean-all.sh` recupera
   esse espaço.

Nada além disso: sem servidor de banco, sem Docker, sem credenciais — os
exemplos de S3 usam buckets públicos com acesso anônimo.

## Compatibilidade de tipos e performance

A tabela de **compatibilidade de tipos entre as tecnologias** (como cada tipo
viaja do DuckDB ao Arrow, ao Parquet, aos objetos Python e aos arrays do Rust)
e a comparação de **performance entre as abordagens** (todos os números
medidos, com as ressalvas de interpretação) ficam na
[documentação publicada](https://felipenoris.github.io/etl-cookbook-tutorial/python/),
ao lado da referência da extensão Rust e dos scripts do pipeline.

## Verificação completa com um comando

Acabou de clonar? Um único comando gera os dados, roda as 5 suítes de testes
(cujos smoke tests executam **todos** os scripts de `examples/`), executa os
scripts standalone do `examples-rust-extension` e gera as documentações (doctest, pdoc
e cargo doc):

```bash
./check-all.sh                # completo (3 testes do DuckDB usam internet)
./check-all.sh --no-network   # ambiente sem acesso à internet
```

Qualquer falha interrompe o script; ao final, um "Tudo OK!" confirma que o
repositório está funcional.

O inverso — remover tudo que foi gerado (dados parquet, documentações, build
Rust, caches), voltando ao estado pós-clone:

```bash
./clean-all.sh          # limpa artefatos gerados (mantém os .venv)
./clean-all.sh --all    # também remove os .venv e uv.lock (estado pós-clone)
```

### Integração contínua (GitHub Actions)

O workflow [`.github/workflows/ci.yml`](.github/workflows/ci.yml) roda esse
mesmo `./check-all.sh` a cada push na `main` e em cada pull request (instalando
`uv` e a toolchain Rust, com cache): testes das 5 suítes, todos os exemplos,
os pipelines e a geração de documentação. O HTML gerado (pdoc + rustdoc) é
publicado como artefato baixável da execução. O status aparece no badge no
topo deste README.

Um segundo workflow, [`.github/workflows/docs.yml`](.github/workflows/docs.yml),
gera as duas documentações (pdoc do lado Python + rustdoc do crate) e as
publica no **GitHub Pages** a cada push na `main`, sem precisar gerar os dados
fictícios (o pdoc só importa os módulos, que não leem `data/raw` em tempo de
import). O site fica em
<https://felipenoris.github.io/etl-cookbook-tutorial/>, com uma página inicial
ligando a documentação Python (`/python`) e a Rust (`/rust`).

## Por onde começar

1. `uv run --script data/generate_data.py --generate` — obrigatório após clonar o
   repositório, já que os parquet não são versionados (o `./check-all.sh`
   acima já faz isso automaticamente).
2. [`examples-pandas/`](examples-pandas) e [`examples-pyarrow/`](examples-pyarrow) — mesmos conceitos (seleção,
   limpeza, groupby, joins, pivot), comparando a API de alto nível do pandas
   com a API nativa do Arrow — mais o interop zero-copy entre as duas e o
   padrão híbrido (pyarrow nas bordas, pandas no miolo) para equipes
   proficientes em pandas.
3. [`examples-DuckDB/`](examples-DuckDB) — os mesmos joins/agregações em SQL, mais o exemplo de
   `memory_limit`/spill em disco e um bloco de funcionalidades de ETL:
   `COPY TO` particionado com recarga idempotente, staging persistente com
   UPSERT, ingestão de CSV com quarentena de rejeitadas, SQL avançado
   (recursiva, `PIVOT`, `ASOF JOIN`), macros/UDFs Python e
   `EXPORT`/`IMPORT DATABASE`.
4. [`examples-rust-extension/`](examples-rust-extension) — fecha o ciclo: um ETL real que usa
   DuckDB (extract+join+spill) → pyarrow (projeção) → Rust via `pyo3-arrow`
   (transformação com estado, zero-copy) → pandas (resumo) → grava em
   `data/rich/order_metrics/`. Além do pipeline, exercita **multithreading**
   (submissão serial + pool paralelo, com uma variante de memória constante
   por backpressure), **todos os tipos Arrow** manipulados no lado nativo
   (incluindo `decimal.Decimal`/`datetime.date` cruzando a fronteira) e o
   estudo de **materialização de dados 1:N** (copiar vs. emprestar fatias).
5. [`examples-sqlalchemy-contract/`](examples-sqlalchemy-contract) — para equipes vindas do
   padrão ORM + banco relacional efêmero: modelos SQLAlchemy no papel de
   contrato de schema (não de veículo de dados), a decomposição dos **cinco
   custos** que tornam o ORM lento, medidos na escrita (ORM vs. Core vs.
   colunar) e na leitura (o gradiente lazy loading → eager → linhas brutas →
   lote vetorizado), e a árvore de plano de contas resolvida com
   `WITH RECURSIVE` no DuckDB.

Cada subpasta tem seu próprio `README.md` com a lista de exemplos e os
conceitos exercitados.

## Testes

Cada projeto tem sua própria suíte pytest (smoke tests dos exemplos + testes
unitários dos contratos assumidos). Para rodar tudo, a partir da raiz:

```bash
(cd examples-pandas && uv run pytest)
(cd examples-pyarrow && uv run pytest)
(cd examples-DuckDB && uv run pytest)
(cd examples-rust-extension && uv run pytest)   # -m "not slow" pula o pipeline completo
(cd examples-sqlalchemy-contract && uv run pytest)
```

## Referências

Ferramentas usadas em todo o tutorial:

- [uv — documentação oficial](https://docs.astral.sh/uv/) — gerenciador de projetos/dependências Python usado em todas as subpastas; ver também [scripts standalone com PEP 723](https://peps.python.org/pep-0723/), o formato usado por `data/generate_data.py`.
- [Formato Apache Parquet](https://parquet.apache.org/docs/) — o formato colunar de arquivo usado como origem (`data/raw`) e destino (`data/rich`).
- [Formato colunar Apache Arrow](https://arrow.apache.org/docs/format/Columnar.html) — a representação em memória que conecta pandas, pyarrow, DuckDB e a extensão Rust sem cópias.
- [pytest — documentação oficial](https://docs.pytest.org/en/stable/) — usado nas suítes de teste de todas as subpastas.

Referências específicas de cada tecnologia estão no `README.md` da subpasta
correspondente ([`examples-pandas/`](examples-pandas), [`examples-pyarrow/`](examples-pyarrow), [`examples-DuckDB/`](examples-DuckDB),
[`examples-rust-extension/`](examples-rust-extension)).

## Licença

Distribuído sob a licença MIT — ver [LICENSE](LICENSE).
