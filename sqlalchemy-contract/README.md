# sqlalchemy-contract — migrando o padrão ORM para a stack colunar

Projeto Python isolado (gerenciado com `uv`) que porta o padrão atual de
desenvolvimento de ETLs da equipe — modelos SQLAlchemy ORM + Postgres efêmero
+ INSERT massivo de instâncias — para a stack Arrow/parquet/DuckDB,
respondendo à pergunta: **onde o SQLAlchemy ainda encaixa, e de onde ele deve
sair?**

A resposta em uma linha: o SQLAlchemy fica como **contrato de schema** (e
como cliente da base final); sai do **caminho por onde os dados passam**.

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

```bash
cd sqlalchemy-contract
uv sync
uv run examples/01_models_as_contract.py
uv run examples/02_orm_vs_columnar.py          # aceita [n_linhas], default 100000
uv run examples/03_account_hierarchy.py
```

## O placar do exemplo 02 (100k lançamentos, SQLite em memória)

| Caminho | Tempo | Vazão |
| --- | --- | --- |
| ORM (objetos + session + commit) | ~2.0s | ~50k linhas/s |
| SQLAlchemy Core (executemany) | ~0.3s | ~320k linhas/s |
| Colunar (Arrow → parquet → DuckDB CTAS) | ~0.02s | **~4.3M linhas/s** |

O SQLite em memória é o cenário MAIS favorável ao ORM (sem rede, sem fsync);
contra um Postgres real a diferença só cresce. A lentidão não é má
configuração: o ORM paga um objeto Python + rastreamento de estado (unit of
work) POR LINHA. No caminho colunar o fato nunca vira objeto — só buffers
Arrow.

## Onde cada peça do padrão antigo foi parar

| Padrão antigo | Stack nova |
| --- | --- |
| Postgres efêmero no compute | DuckDB in-process (zero infra) |
| classes ORM como schema | **continuam** — como contrato (`comment=`, tipos, DDL) |
| INSERT massivo de instâncias | Arrow → parquet → `CREATE TABLE AS`/`COPY` |
| navegação da árvore de contas | `WITH RECURSIVE` materializando a árvore achatada 1x |
| FKs `DEFERRED` / constraints | anti-joins e contagens como queries de qualidade |
| ORM como cliente da base final | **continua** — consultas pontuais é o habitat do ORM |

## Testes

```bash
uv run pytest
```

Smoke tests dos 3 exemplos + testes das projeções do contrato (tipos Arrow,
DDL com `COMMENT ON`, comprimentos de VARCHAR), da equivalência de resultados
entre o caminho ORM e o colunar (mesmo COUNT e mesma SOMA decimal, igualdade
estrita), da CTE recursiva (caminhos completos, filtro por subárvore,
hierarquia alternativa independente) e do anti-join pegando lançamentos
órfãos.

## Referências

- [SQLAlchemy 2.0 — ORM declarativo](https://docs.sqlalchemy.org/en/20/orm/declarative_mapping.html) — `Mapped`/`mapped_column`, incluindo o parâmetro `comment=`.
- [SQLAlchemy — Core vs ORM](https://docs.sqlalchemy.org/en/20/tutorial/dbapi_transactions.html) — a distinção que o exemplo 02 mede.
- [DuckDB — WITH RECURSIVE](https://duckdb.org/docs/stable/sql/query_syntax/with) — a CTE recursiva do exemplo 03 (introduzida em [`../DuckDB/examples/09`](../DuckDB/examples/09_advanced_sql_transforms.py)).
- [Redshift — COMMENT](https://docs.aws.amazon.com/redshift/latest/dg/r_COMMENT.html) — o comando que a projeção `redshift_ddl_for` emite.
