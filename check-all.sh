#!/usr/bin/env bash
# Verificação completa do repositório em um comando.
#
# Executa, em sequência: geração dos dados fictícios (se necessário), as 5
# suítes pytest (cujos smoke tests executam TODOS os scripts de examples/),
# os 6 scripts standalone do rust-extension (run_etl, run_contracts_parallel,
# run_reorg_for_upstream, run_data_types, run_json_types, run_nested_params) e a geração das
# documentações (doctest + pdoc + cargo doc). Qualquer falha interrompe o script
# com erro.
#
# Uso:
#   ./check-all.sh                # verificação completa (3 testes usam internet)
#   ./check-all.sh --no-network   # pula os testes que exigem internet
#
# Pré-requisitos: uv (https://docs.astral.sh/uv/) e toolchain Rust/cargo
# (https://rustup.rs) — o resto (Python, dependências, maturin) o uv resolve.

set -euo pipefail
cd "$(dirname "$0")"

DUCKDB_FLAGS=""
if [[ "${1:-}" == "--no-network" ]]; then
    DUCKDB_FLAGS="--no-network"
elif [[ -n "${1:-}" ]]; then
    echo "argumento desconhecido: $1 (use --no-network ou nenhum)" >&2
    exit 1
fi

step() { printf '\n\033[1m==> [%s] %s\033[0m\n' "$1" "$2"; }

step 1/9 "Dados fictícios em data/raw"
if [[ -d data/raw/orders ]]; then
    echo "data/raw já existe — pulando (regenere com: uv run --script data/generate_data.py --clean --generate)"
else
    uv run --script data/generate_data.py --generate
fi

step 2/9 "pandas: suíte pytest (os smoke tests executam os 10 exemplos)"
(cd examples-pandas && uv run pytest)

step 3/9 "pyarrow: suíte pytest (13 exemplos)"
(cd examples-pyarrow && uv run pytest)

step 4/9 "DuckDB: suíte pytest (27 exemplos)"
(cd examples-DuckDB && uv run pytest $DUCKDB_FLAGS)

step 5/9 "rust-extension: recompila a extensão via maturin e roda a suíte pytest"
# O --reinstall-package não é zelo excessivo: o uv NÃO observa src/lib.rs, só os
# metadados do pacote. Nem `uv sync` nem `uv run` recompilam depois de o fonte
# Rust mudar (por edição ou por `git pull`), e a suíte passaria a exercitar um
# .so defasado — quebrando na coleta se o fonte ganhou um símbolo novo, ou, pior,
# ficando verde contra o binário antigo se apenas o corpo de uma função mudou.
# Com target/ quente a recompilação custa ~9s; a frio, ~47s.
(cd examples-rust-extension && uv sync --reinstall-package etl-rust-ext)
(cd examples-rust-extension && uv run pytest)

step 6/9 "ETL completo (DuckDB -> pyarrow -> Rust -> pandas -> parquet)"
(cd examples-rust-extension && uv run run_etl.py)

step 7/9 "Projeção paralela, reorganização pré-upstream, tipos Arrow, JSON e 1:N no Rust"
(cd examples-rust-extension && uv run run_contracts_parallel.py)
(cd examples-rust-extension && uv run run_reorg_for_upstream.py)
(cd examples-rust-extension && uv run run_data_types.py)
(cd examples-rust-extension && uv run run_json_types.py)
(cd examples-rust-extension && uv run run_nested_params.py)

step 8/9 "sqlalchemy-contract: suíte pytest (4 exemplos: contrato, ORM vs colunar/lote, hierarquia)"
(cd examples-sqlalchemy-contract && uv run pytest)

step 9/9 "Documentação: doctest do docs_demo, pdoc (docs/) e cargo doc (target/doc/)"
(cd examples-rust-extension && uv run python -m doctest docs_demo.py -v > /dev/null)
# A lista de módulos abaixo é EXPLÍCITA: um run_*.py novo só é documentado se
# for acrescentado aqui, em .github/workflows/docs.yml (o que publica no Pages)
# e na seção "Módulos documentados" de pdoc-templates/index.html.jinja2. Nada
# verifica isso — esquecer o docs.yml deixa o site sem o módulo, em silêncio.
(cd examples-rust-extension && uv run pdoc --math --mermaid --docformat google --template-dir pdoc-templates --output-dir docs \
    etl_rust_ext ./run_etl.py ./run_contracts_parallel.py ./run_reorg_for_upstream.py ./run_data_types.py ./run_json_types.py ./run_nested_params.py ./docs_demo.py)
# `cargo doc` compila o pyo3-ffi, cujo build script precisa de um interpretador
# Python >= 3.8. Rodar sob `uv run` faz o pyo3 usar o Python do venv isolado
# (via VIRTUAL_ENV) em vez do Python do sistema, que pode ser antigo demais.
(cd examples-rust-extension && uv run cargo doc --no-deps --document-private-items)

printf '\n\033[1;32mTudo OK!\033[0m Documentação em examples-rust-extension/docs/index.html '
printf 'e examples-rust-extension/target/doc/_etl_rust_ext/index.html\n'
