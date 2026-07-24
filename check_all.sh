#!/usr/bin/env bash
# Verificação completa do repositório em um comando.
#
# Executa, em sequência: geração dos dados fictícios (se necessário), as 5
# suítes pytest (cujos smoke tests executam TODOS os scripts de examples/),
# os 5 scripts standalone do rust-extension (run_etl, run_contracts_parallel,
# run_reorg_for_upstream, run_data_types, run_nested_params) e a geração das
# documentações (doctest + pdoc + cargo doc). Qualquer falha interrompe o script
# com erro.
#
# Uso:
#   ./check_all.sh                # verificação completa (3 testes usam internet)
#   ./check_all.sh --no-network   # pula os testes que exigem internet
#
# Pré-requisitos: uv (https://docs.astral.sh/uv/) e toolchain Rust/cargo
# (https://rustup.rs) — o resto (Python, dependências, maturin) o uv resolve.

set -euo pipefail
cd "$(dirname "$0")"

# Ambiente isolado: este cookbook resolve TODAS as dependências via uv (um venv
# por subprojeto). Um PYTHONPATH herdado do shell não tem uso legítimo aqui e,
# se apontar para a raiz do repo, os diretórios-irmãos `pandas/`, `pyarrow/` e
# `DuckDB/` passam a sombrear os pacotes reais do PyPI (p.ex. o pyarrow importa
# `pandas` de forma lazy e acha o diretório `pandas/` deste repo). Limpar aqui
# blinda todas as etapas de uma vez.
unset PYTHONPATH

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
(cd pandas && uv run pytest)

step 3/9 "pyarrow: suíte pytest (13 exemplos)"
(cd pyarrow && uv run pytest)

step 4/9 "DuckDB: suíte pytest (23 exemplos)"
(cd DuckDB && uv run pytest $DUCKDB_FLAGS)

step 5/9 "rust-extension: suíte pytest (compila a extensão via maturin no 1º uso)"
(cd rust-extension && uv run pytest)

step 6/9 "ETL completo (DuckDB -> pyarrow -> Rust -> pandas -> parquet)"
(cd rust-extension && uv run run_etl.py)

step 7/9 "Projeção paralela, reorganização pré-upstream, tipos Arrow e 1:N no Rust"
(cd rust-extension && uv run run_contracts_parallel.py)
(cd rust-extension && uv run run_reorg_for_upstream.py)
(cd rust-extension && uv run run_data_types.py)
(cd rust-extension && uv run run_nested_params.py)

step 8/9 "sqlalchemy-contract: suíte pytest (contrato, ORM vs colunar/lote, hierarquia)"
(cd sqlalchemy-contract && uv run pytest)

step 9/9 "Documentação: doctest do docs_demo, pdoc (docs/) e cargo doc (target/doc/)"
(cd rust-extension && uv run python -m doctest docs_demo.py -v > /dev/null)
(cd rust-extension && uv run pdoc --math --mermaid --docformat google --template-dir pdoc-templates --output-dir docs \
    etl_rust_ext ./run_etl.py ./run_contracts_parallel.py ./run_reorg_for_upstream.py ./run_data_types.py ./run_nested_params.py ./docs_demo.py)
(cd rust-extension && cargo doc --no-deps --document-private-items)

printf '\n\033[1;32mTudo OK!\033[0m Documentação em rust-extension/docs/index.html '
printf 'e rust-extension/target/doc/_etl_rust_ext/index.html\n'
