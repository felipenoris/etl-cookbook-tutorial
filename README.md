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
    rich/              # parquet de saída do ETL (rust-extension/run_etl.py)
  pandas/              # API do pandas com backend Arrow
  pyarrow/             # API nativa do pyarrow
  DuckDB/              # SQL em memória sobre parquet, com spill configurável
  rust-extension/      # extensão Rust (PyO3 + pyo3-arrow) + ETL completo + docs (pdoc)
  sqlalchemy-contract/ # migração do padrão ORM: modelos como contrato, ORM vs colunar, árvore de contas
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
5. **~2.7GB de disco livre** — dados fictícios gerados (~1.5GB em
   `data/raw` + `data/rich`), um `.venv` por projeto (5 projetos,
   ~200-250MB cada) e o build Rust (~130MB). O `./clean_all.sh` recupera
   esse espaço.

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

## Performance: comparando as abordagens

Os exemplos deste tutorial medem, com o mesmo cálculo em cada caso, as
abordagens possíveis para implementar um ETL. O resultado consolidado está
abaixo. **Todos os números foram medidos** nas máquinas/dados deste
repositório — não são estimativas de catálogo.

### A conclusão em uma frase

O fator dominante **não é a linguagem**, e sim a **granularidade com que se
atravessa fronteiras e se materializam objetos**. Trocar Python por Rust dá
um fator; trocar processamento linha a linha por processamento em lote dá
ordens de grandeza.

### Tabela comparativa

Três siglas aparecem abaixo:

- **UDF** (*User-Defined Function*) — função escrita por você e registrada no
  motor SQL para ser chamada de dentro de uma consulta; no DuckDB, pode ser em
  Python, injetando lógica que o SQL não expressa nativamente (ver
  [`DuckDB/10`](DuckDB/examples/10_macros_and_python_udfs.py)).
- **N+1** — a armadilha do ORM em que carregar N registros relacionados dispara
  uma query inicial *mais uma query por registro* (1 + N idas ao banco), em vez
  de trazer tudo de uma vez (ver [`sqlalchemy-contract/04`](sqlalchemy-contract/examples/04_orm_vs_batch.py)).
- **GIL** (*Global Interpreter Lock*) — o mecanismo do CPython que permite só
  uma thread executar bytecode Python por vez; código Rust nativo pode
  liberá-lo e assim rodar em paralelo de verdade (ver [`rust-extension`](rust-extension/README.md)).

| Abordagem | Vazão medida | Exemplo | Vantagens | Desvantagens |
| --- | --- | --- | --- | --- |
| **ORM com lazy loading** (N+1) | **~20k linhas/s** | [`sqlalchemy-contract/04`](sqlalchemy-contract/examples/04_orm_vs_batch.py) | a mais produtiva de escrever; navegação natural | N+1 silencioso; paga os 5 custos do ORM |
| **ORM com eager loading** | **~80k linhas/s** | [`sqlalchemy-contract/04`](sqlalchemy-contract/examples/04_orm_vs_batch.py) | elimina o N+1 mantendo a ergonomia | 1 objeto Python por linha (GC, refcount) |
| **INSERT via ORM** (escrita) | **~50k linhas/s** | [`sqlalchemy-contract/02`](sqlalchemy-contract/examples/02_orm_vs_columnar.py) | unit of work cuida de tudo | inviável para carga massiva |
| **SQLAlchemy Core** (executemany) | **~320k linhas/s** | [`sqlalchemy-contract/02`](sqlalchemy-contract/examples/02_orm_vs_columnar.py) | sem objetos ORM; ainda é SQL portável | continua orientado a linha |
| **Linhas brutas + laço Python** | **~670k linhas/s** ¹ | [`sqlalchemy-contract/04`](sqlalchemy-contract/examples/04_orm_vs_batch.py) | simples; sem dependência extra | limitado pelo interpretador *e pela carga por linha* |
| **UDF Python no DuckDB** | **~10-16M linhas/s** ¹ | [`DuckDB/10`](DuckDB/examples/10_macros_and_python_udfs.py) | lógica Python arbitrária dentro do SQL | **24-39x mais lento que o SQL equivalente** (medido com controle) |
| **SQL colunar puro** (DuckDB) | **~300-650M linhas/s** | [`DuckDB/03`](DuckDB/examples/03_joins_and_aggregations.py), [`DuckDB/12`](DuckDB/examples/12_performance_without_indexes.py) | vetorizado e paralelo; sem código por linha | só o que é expressável em SQL |
| **Escrita colunar** (Arrow→parquet) | **~4.3M linhas/s** | [`sqlalchemy-contract/02`](sqlalchemy-contract/examples/02_orm_vs_columnar.py) | ~87x o INSERT do ORM | destino é arquivo, não tabela transacional |
| **Rust serial** (pyo3-arrow) | **~2.2M contratos/s** | [`rust-extension`](rust-extension/run_contracts_parallel.py) | cálculo com estado, impossível de vetorizar | exige toolchain Rust |
| **Rust multithread** | **~12M contratos/s** | [`rust-extension`](rust-extension/run_contracts_parallel.py) | ~5,5x sobre o serial (11 CPUs), fora do GIL | complexidade de concorrência |
| **Rust, fatias emprestadas** | **~55M contratos/s** | [`rust-extension/run_nested_params.py`](rust-extension/run_nested_params.py) | zero alocação/cópia sobre `ListArray` | exige pensar em lifetimes |
| **Pipeline completo de ETL** | **~4M linhas/s** | [`rust-extension/run_etl.py`](rust-extension/run_etl.py) | 33,7M linhas em ~8s: join+sort+Rust+escrita | — |

### Os cinco custos que explicam a tabela

A [decomposição detalhada](sqlalchemy-contract/README.md#por-que-o-orm-é-lento-os-cinco-custos)
está no `sqlalchemy-contract`: (1) metadados por linha, (2) escrituração do
ORM, (3) travessia de fronteira por linha, (4) execução interpretada e (5)
alocação de heap por linha. Cada degrau da tabela elimina um subconjunto
deles. O [estudo em Rust](rust-extension/run_nested_params.py) mostra que
**quatro dos cinco desaparecem só por sair do Python** — por isso a mesma
lição ("não processe linha a linha") custa ~4x lá e ~258x aqui.

¹ As duas linhas marcadas são "Python percorrendo linhas" e ainda assim diferem ~24x — ver ressalva 3.

### Quatro ressalvas importantes

**1. Volume importa — colunar nem sempre ganha.** Com ~15 mil linhas, o
caminho DuckDB fica *mais lento* que um laço Python: o custo fixo de conexão
e planejamento não se paga. Não troque um laço por um motor SQL para
processar mil registros.

**2. O custo é SAIR do motor, não o estilo da UDF.** Com um controle
isolando o scan+join, a mesma regra de desconto sobre 5,6M de linhas custa
~0,01s em SQL puro (`CASE WHEN`), ~0,36s na UDF `native` e ~0,57s na `arrow`
— ou seja, **qualquer UDF Python custa 24-39x o SQL equivalente**. Entre as
duas variantes a diferença é modesta, e contrariando a intuição a `native`
ganha (o DuckDB amortiza o overhead internamente; a `arrow` aloca arrays
intermediários por kernel). Decida primeiro *se* precisa de UDF; só depois
qual variante.

**3. "Velocidade de um laço Python" não é um número — depende da carga.**
Dois números desta tabela são ambos "Python percorrendo linhas", e diferem
~24x entre si: o laço do exemplo 04 faz ~670k linhas/s porque usa aritmética
`Decimal` e constrói listas; a UDF do exemplo 10 faz ~16M linhas/s porque
compara e multiplica `float`. O interpretador é o mesmo — muda **o trabalho
por linha**. Ao estimar o seu caso, olhe o que o laço faz (Decimal? strings?
alocação?) antes de extrapolar qualquer uma dessas vazões.

**4. As vazões não são comparáveis entre si diretamente.** Cada linha da
tabela mede um *trabalho diferente* (agregar, inserir, projetar receita com
juros compostos). Os números servem para comparar **abordagens dentro de um
mesmo exemplo** e para dar ordem de grandeza — não para extrapolar que "SQL é
50x mais rápido que Rust" (não é; são cargas distintas). Medições em máquina
de 11 CPUs, dados em cache do SO.

### Como escolher

- **SQL colunar** para tudo que for expressável em SQL (join, agregação,
  window function): é o mais rápido e o mais simples.
- **UDF Python** quando a lógica não couber em SQL mas o volume for moderado.
- **Rust** quando houver cálculo sequencial com estado por entidade (projeções
  financeiras, simulações) sobre volume alto — e aí use
  [pool com backpressure](rust-extension/README.md) para manter memória
  constante.
- **ORM** apenas fora do caminho de dados: schema/contrato e consultas
  pontuais.

## Verificação completa com um comando

Acabou de clonar? Um único comando gera os dados, roda as 5 suítes de testes
(cujos smoke tests executam **todos** os scripts de `examples/`), executa os
scripts standalone do `rust-extension` e gera as documentações (doctest, pdoc
e cargo doc):

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
./clean_all.sh --all    # também remove os .venv e uv.lock (estado pós-clone)
```

### Integração contínua (GitHub Actions)

O workflow [`.github/workflows/ci.yml`](.github/workflows/ci.yml) roda esse
mesmo `./check_all.sh` a cada push na `main` e em cada pull request (instalando
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
   `data/rich/order_metrics/`. Além do pipeline, exercita **multithreading**
   (submissão serial + pool paralelo, com uma variante de memória constante
   por backpressure), **todos os tipos Arrow** manipulados no lado nativo
   (incluindo `decimal.Decimal`/`datetime.date` cruzando a fronteira) e o
   estudo de **materialização de dados 1:N** (copiar vs. emprestar fatias).
5. [`sqlalchemy-contract/`](sqlalchemy-contract) — para equipes vindas do
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
(cd pandas && uv run pytest)
(cd pyarrow && uv run pytest)
(cd DuckDB && uv run pytest)
(cd rust-extension && uv run pytest)   # -m "not slow" pula o pipeline completo
(cd sqlalchemy-contract && uv run pytest)
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
