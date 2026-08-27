# sqlalchemy-contract — migrando o padrão ORM para a stack colunar

Projeto Python isolado (gerenciado com `uv`) que porta o padrão tradicional de
desenvolvimento de ETLs — modelos SQLAlchemy ORM + banco relacional efêmero +
INSERT massivo de instâncias — para a stack Arrow/parquet/DuckDB, respondendo
à pergunta: **onde o SQLAlchemy ainda encaixa, e de onde ele deve sair?**

A resposta em uma linha: o SQLAlchemy fica como **contrato de schema** e
**gerador de SQL** (e como cliente da base final); sai do **caminho por onde
os dados passam**.

## O modelo portado

[`examples/models.py`](examples/models.py) porta o modelo de lançamentos em
planos de conta (Veiculo, HierarquiaContas, Conta,
RelacionamentoContaHierarquia, Lancamento) com três mudanças deliberadas:

1. **`comment=` em tudo** — o metadado semântico mora nas classes e é
   projetado para os três destinos (banco local, parquet, Redshift);
2. **`valor: Numeric(12,2)`** em vez de `Double` — lançamento financeiro é
   decimal de 2 casas (padrão do projeto; `0.10 + 0.20 != 0.30` em float);
3. **`String(n)` com comprimentos explícitos** — o `VARCHAR(n)` do Redshift
   exige, e o parquet não tem onde guardar essa informação.

## Exemplos

| Script | Conceitos |
| --- | --- |
| `01_models_as_contract.py` | um schema, três projeções: `create_all` (banco local), `arrow_schema_for` (field metadata no parquet), `redshift_ddl_for` (`CREATE TABLE` + `COMMENT ON`) |
| `02_orm_vs_columnar.py` | a medição que motiva a migração: ORM vs Core vs Arrow→parquet→DuckDB CTAS, com linhas/s de cada caminho |
| `03_account_hierarchy.py` | a árvore de contas (arestas parent→child por hierarquia) via `WITH RECURSIVE` no DuckDB, filtro por subárvore, N visões sobre as mesmas contas, FKs como queries de qualidade |
| `04_orm_vs_batch.py` | o gradiente ORM → lote em Python puro: lazy loading (N+1), eager loading, linhas brutas e agregação vetorizada no DuckDB |
| `05_sql_generation_duckdb.py` | o modelo como gerador de SQL para uma base DuckDB vazia: DDL compilado do contrato (com os `comment=` chegando ao catálogo), carga batch via `INSERT ... FROM SELECT` sobre DataFrames pandas (backend pyarrow), joins com `ON` inferidos das FKs e leitura em RecordBatches com memória constante |

```bash
cd examples-sqlalchemy-contract
uv sync
uv run examples/01_models_as_contract.py
uv run examples/02_orm_vs_columnar.py          # aceita [n_linhas], default 100000
uv run examples/03_account_hierarchy.py
uv run examples/04_orm_vs_batch.py       # aceita [n_contas] [lanc_por_conta]
uv run examples/05_sql_generation_duckdb.py    # aceita [n_lancamentos], default 200000
```

## O placar do exemplo 02 (100k lançamentos, SQLite em memória)

| Caminho | Tempo | Vazão | vs ORM |
| --- | --- | --- | --- |
| ORM (objetos + session + commit) | ~2.2s | ~45k linhas/s | 1x |
| SQLAlchemy Core (executemany) | ~0.3s | ~310k linhas/s | ~7x |
| Colunar (Arrow → parquet → DuckDB CTAS) | ~0.04s | **~2.3M linhas/s** | **~50x** |

Os tempos absolutos variam com a máquina — o script imprime a razão medida na
sua ao final, e é ela que vale citar. O que é estável é a ordem: o passo de
ORM para Core vale um fator pequeno, e o de Core para colunar, uma ordem de
grandeza.

O SQLite em memória é o cenário MAIS favorável ao ORM (sem rede, sem fsync);
contra um Postgres real a diferença só cresce. A lentidão não é má
configuração — é a soma dos cinco custos descritos na próxima seção: o ORM
paga metadados de objeto (1), escrituração da session (2) e serialização por
linha (3) em cada `Lancamento`. O `insert()` do Core corta 1 e 2, mas segue
orientado a linha; só o caminho colunar elimina os cinco, porque o fato nunca
vira objeto — são buffers Arrow do início ao fim.

## Por que o ORM é lento: os cinco custos

A lentidão do ORM não vem de "materializar objetos" em abstrato, mas de cinco
custos distintos. Vale nomeá-los, porque cada estratégia do exemplo 04 elimina
um subconjunto deles — e porque é o mesmo arcabouço usado no [estudo
equivalente em Rust](../examples-rust-extension/run_nested_params.py):

| # | Custo do ORM | Onde aparece |
| --- | --- | --- |
| 1 | **Metadados por linha em runtime** — cada instância é um `PyObject` com refcount, `__dict__` e rastreamento de GC (centenas de bytes de overhead por objeto) | qualquer query que devolva objetos |
| 2 | **Escrituração do ORM** — identity map, unit of work, atributos instrumentados (todo acesso passa por *descriptors* que registram estado), lazy loading | sessions com objetos rastreados |
| 3 | **Travessia de fronteira por linha (ou por entidade)** — cada ida ao banco é um round trip; o flush serializa linha a linha pelo protocolo | INSERT massivo; o N+1 do lazy loading |
| 4 | **Execução interpretada** — cada operação é dispatch de bytecode | todo laço Python sobre linhas |
| 5 | **Alocação de heap por linha** | criar objetos/listas por registro |

### Quem elimina cada custo: a stack ou a linguagem?

Aqui mora a confusão mais comum ao ler os números deste projeto. São **duas
trocas independentes**, e elas eliminam custos diferentes:

- **Troca 1 — sair do ORM+banco para Arrow/colunar**, continuando em Python.
- **Troca 2 — descer para Rust**, quando a lógica não couber num motor
  vetorizado.

| # | Custo | Troca 1 (ORM → Arrow, em Python) | Troca 2 (Python → Rust) |
| --- | --- | --- | --- |
| 1 | Metadados por linha (`PyObject`) | **Quase todo** — saem `__dict__`, *descriptors* e identity map; sobra o `PyObject` da tupla | **O resíduo** — struct Rust é memória pura |
| 2 | Escrituração do ORM | **Todo** — é literalmente o que se abandona | já eliminado |
| 3 | Travessia de fronteira por linha | **Todo** — o dado vem de um buffer em memória, não de round trips | já eliminado |
| 4 | Execução interpretada | **Todo, se a lógica vetorizar** — o laço passa a rodar dentro do DuckDB/kernels Arrow, em C++ | **Todo**, inclusive quando *não* vetoriza |
| 5 | Alocação de heap por linha | dentro do motor, não existe | **sobrevive** (~100x mais barata; só some emprestando fatias) |

Duas leituras importantes:

**A maior parte do ganho não exige trocar de linguagem.** Os custos 2 e 3 —
os especificamente *do ORM* — saem inteiros na Troca 1, ainda em Python. É o
que o degrau 3 do exemplo 04 mede: ~32x sem uma linha de Rust.

**O Rust não é o degrau seguinte do mesmo eixo; é a saída para outro
problema.** Se a lógica vetoriza, o motor colunar já resolve o custo 4 e não
há o que o Rust acrescente. O Rust entra quando a lógica **não** vetoriza —
dependência sequencial entre linhas, estado que atravessa o laço — como no
[laço com estado do pandas](../examples-pandas/examples/10_sequential_stateful_loop.py).

Por isso os fatores dos dois estudos **não são comparáveis**: os ~4x do
[estudo em Rust](../examples-rust-extension/run_nested_params.py) isolam só a
estratégia de alocação (custo 5, dentro do Rust), enquanto o ~95x daqui
acumula ORM, N+1 e laço interpretado. O par estritamente comparável é o degrau
3 → 4 deste README (~3x): ambos medem "parar de processar linha a linha",
mantida a linguagem.

## O gradiente do exemplo 04 (200k lançamentos, agregação por conta)

O exemplo 02 mede a **escrita** (INSERT); o 04 mede a **leitura +
processamento**, que é onde o ETL passa a maior parte do tempo. Quatro
estratégias calculando a mesma coisa (maior saldo acumulado por conta), cada
degrau eliminando custos da tabela acima:

| Estratégia | Tempo | Ganho acumulado | Custos que o degrau elimina |
| --- | --- | --- | --- |
| 1. ORM lazy loading (N+1) | ~9,9s | 1x | — (paga todos os cinco) |
| 2. ORM eager (`selectinload`) | ~2,5s | **~4x** | **3** — as N idas ao banco |
| 3. Linhas brutas + laço Python | ~0,3s | **~32x** | **2 e a maior parte de 1** — escrituração e objetos ORM (a tupla ainda é um `PyObject`) |
| 4. Lote colunar (DuckDB) | ~0,1s | **~95x** | **4 e 5** — laço interpretado e alocação |

Nenhum degrau é o vilão sozinho: o N+1 custa ~4x, os objetos ORM mais ~8x, e o
laço interpretado mais ~3x. Os degraus 2 e 3 são a **Troca 1** da seção
anterior — saem os custos do ORM, sem sair do Python. O degrau 4 é a
vetorização: o laço deixa de ser interpretado porque passa a rodar dentro do
motor. Note que os três primeiros degraus somam ~32x **sem trocar de
linguagem** — o Rust não aparece em nenhum deles.

**Ressalva**: com pouco volume (~15k linhas) a estratégia 4 fica *mais lenta*
que a 3 — o custo fixo do DuckDB (conexão, planejamento) não se paga. A
vantagem colunar precisa de volume; não vale trocar um laço Python por um
motor SQL para mil linhas.

## Onde cada peça do padrão antigo foi parar

| Padrão antigo | Stack nova |
| --- | --- |
| Postgres efêmero no compute | DuckDB in-process (zero infra) |
| classes ORM como schema | **continuam** — como contrato (`comment=`, tipos, DDL) |
| INSERT massivo de instâncias | Arrow → parquet → `CREATE TABLE AS`/`COPY` |
| navegação da árvore de contas | `WITH RECURSIVE` materializando a árvore achatada 1x |
| FKs `DEFERRED` / constraints | anti-joins e contagens como queries de qualidade |
| ORM como cliente da base final | **continua** — consultas pontuais é o habitat do ORM |

## Produtividade: o que se ganha e o que se perde na troca

O argumento mais comum para adotar um ORM é **produtividade**: desenvolver sem
escrever as consultas SQL à mão e sem codificar a serialização/desserialização
(banco → objeto Python → banco). É uma preocupação legítima ao migrar para a
stack colunar — mas o balanço é mais favorável do que parece, e vale separar o
que é perda real do que é necessidade que simplesmente deixa de existir.

| O que o ORM entrega | O que acontece na stack colunar |
| --- | --- |
| Serialização banco ↔ objeto Python | **Deixa de ser necessária** (não é perda — é eliminação) |
| Schema declarativo como código | **Mantido** — os modelos seguem como contrato |
| DDL automático (`create_all`) | **Mantido**, e ganha geração de DDL para o destino final |
| Não escrever SQL | **Muda de figura** — em carga analítica, o SQL é mais produtivo |
| Navegação de relacionamentos (`contrato.parametros`) | **Perdida** — vira join explícito ou `list<...>` |
| Autocomplete/checagem de tipos nas colunas | **Perdida parcialmente** — a perda ergonômica real |
| Unit of work (mutar objetos → UPDATEs) | Perdido, mas ETL raramente precisa disso |

### A serialização não é perdida — ela é dispensada

O mapeamento objeto↔relacional existe para resolver um *descasamento de
impedância*: o banco fala linhas e SQL, o Python fala objetos. Na stack
colunar esse descasamento **não existe**: o dado nasce Arrow no parquet e
permanece Arrow do início ao fim (DuckDB → pandas → Rust → parquet). Não há
conversão para objetos em lugar nenhum.

Compare o esforço de "ler uma tabela e começar a trabalhar":

- **com ORM**: declarar a classe com todas as colunas e tipos → configurar
  engine/session → query → objetos;
- **na stack colunar**: `pd.read_parquet(caminho)` ou
  `SELECT * FROM read_parquet(...)`. **Zero linhas de modelagem** — o schema
  vem do próprio arquivo.

No caminho de dados, portanto, escreve-se *menos* código, não mais.

### Sobre "não escrever SQL": a premissa merece exame

Esse argumento se aplica bem a cargas **OLTP** (buscar por chave, navegar
relacionamentos, salvar um objeto). Para transformação **analítica** — o que
um ETL faz — a relação se inverte: expressar `GROUP BY` com window functions,
CTE recursiva, `PIVOT` ou `ASOF JOIN` *através do ORM* é mais verboso e menos
expressivo do que escrever o SQL diretamente.

O [exemplo 03](examples/03_account_hierarchy.py) ilustra: a hierarquia de
contas achatada com `WITH RECURSIVE` são ~8 linhas de SQL legível; a mesma
navegação via ORM seria um loop com estado ou uma query recursiva construída
em objetos — mais código e mais difícil de ler. Em outras palavras, a
"produtividade de não escrever SQL" tende a não se realizar justamente nas
partes analíticas.

Há ainda um ganho que o ORM não oferece: **exploração sem modelagem prévia**.
Apontar o DuckDB para um parquet desconhecido e rodar `DESCRIBE`/`SUMMARIZE`
na hora, sem definir classe nenhuma.

### As perdas genuínas (e como mitigá-las)

**1. Autocomplete e checagem de tipos nas colunas.** `df["valor"]` é uma chave
string: a IDE não sabe que a coluna existe nem que é `Decimal` — enquanto
`Lancamento.valor` era verificado. É a perda ergonômica real, e custa em erros
de digitação que só aparecem em runtime.

*Mitigação*: é exatamente o papel do contrato deste projeto. Os modelos
declarativos seguem como fonte da verdade do schema, e a validação vira
explícita (o batch produzido bate com `arrow_schema_for(Lancamento)`?),
rodando como teste. Troca-se "a IDE avisa" por "o pipeline falha cedo, com
mensagem clara". Para algo mais próximo do autocomplete, existem bibliotecas
de DataFrame tipado (pandera, patito) — mas contrato + validação já cobre o
essencial.

**2. Navegação de relacionamentos.** Perder `contrato.parametros` é real. Em
compensação, ganha-se controle explícito sobre o custo: o lazy loading é
notório por gerar N+1 queries silenciosas, enquanto o join explícito (ou a
coluna `list<...>`, ver [`../examples-rust-extension/run_nested_params.py`](../examples-rust-extension/run_nested_params.py))
deixa o custo visível no código.

### O custo que não é da ferramenta

Há uma queda de produtividade **durante a transição**, enquanto se internaliza
SQL analítico, pensamento colunar e a API do pyarrow. É um custo real de
migração, que deve entrar no planejamento — mas é transitório, não uma
característica permanente da stack. Encurtá-lo é justamente o propósito deste
tutorial.

### Resumo

Para o **caminho de dados** (o que o ETL faz o tempo todo) a stack colunar é
mais produtiva: menos código, sem modelagem prévia, sem camada de
serialização. Para **schema e metadados**, o SQLAlchemy permanece no papel em
que é excelente. Perde-se de fato o conforto do autocomplete nas colunas e a
navegação implícita de relacionamentos — o primeiro compensável com contrato +
validação.

Em uma frase: troca-se **conveniência implícita** (a ferramenta decide e
esconde o custo) por **explicitude com custo visível**. Em ETL de volume, essa
troca costuma compensar — mas é uma troca, não um almoço grátis.

## Testes

```bash
uv run pytest
```

Smoke tests dos 5 exemplos + testes das projeções do contrato (tipos Arrow,
DDL com `COMMENT ON`, comprimentos de VARCHAR), da equivalência de resultados
entre o caminho ORM e o colunar (mesmo COUNT e mesma SOMA decimal, igualdade
estrita), da CTE recursiva (caminhos completos, filtro por subárvore,
hierarquia alternativa independente), do anti-join pegando lançamentos
órfãos, das quatro estratégias do exemplo 04 (as quatro contra um cenário
determinístico calculado à mão, incluindo saldo que nunca fica positivo e
contas sem lançamentos) e do SQL gerado do exemplo 05 (DDL aceito pelo
DuckDB com os `comment=` no catálogo, carga set-based preservando a soma
decimal exata, FK gerada rejeitando lançamento órfão, batch divergente do
contrato rejeitado, e a agregação por RecordBatches batendo com o cálculo
direto — Decimal a Decimal).

## Referências

- [SQLAlchemy 2.0 — ORM declarativo](https://docs.sqlalchemy.org/en/20/orm/declarative_mapping.html) — `Mapped`/`mapped_column`, incluindo o parâmetro `comment=`.
- [SQLAlchemy — Core vs ORM](https://docs.sqlalchemy.org/en/20/tutorial/dbapi_transactions.html) — a distinção que o exemplo 02 mede.
- [DuckDB — WITH RECURSIVE](https://duckdb.org/docs/stable/sql/query_syntax/with) — a CTE recursiva do exemplo 03 (introduzida em [`../examples-DuckDB/examples/09`](../examples-DuckDB/examples/09_advanced_sql_transforms.py)).
- [Redshift — COMMENT](https://docs.aws.amazon.com/redshift/latest/dg/r_COMMENT.html) — o comando que a projeção `redshift_ddl_for` emite.
- [SQLAlchemy — INSERT ... FROM SELECT](https://docs.sqlalchemy.org/en/20/tutorial/data_insert.html#insert-from-select) — a carga set-based do exemplo 05.
- [DuckDB — COMMENT ON](https://duckdb.org/docs/stable/sql/statements/comment_on) — o statement (aceito desde a versão 0.10) que leva os `comment=` do contrato ao catálogo no exemplo 05.
- [duckdb_engine](https://github.com/Mause/duckdb_engine) — o dialeto SQLAlchemy completo para o DuckDB; dispensado de propósito no exemplo 05, que gera o SQL com o dialeto genérico e o executa na conexão nativa para preservar o caminho colunar.
