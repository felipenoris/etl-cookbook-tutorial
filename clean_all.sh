#!/usr/bin/env bash
# Limpeza dos artefatos gerados — o inverso do ./check_all.sh.
#
# Remove tudo que é produzido localmente (dados parquet, documentações, build
# Rust, caches de teste), devolvendo o repositório ao estado pós-clone.
# Nada versionado no git é tocado. Restaure tudo com ./check_all.sh.
#
# Uso:
#   ./clean_all.sh          # limpa artefatos gerados (mantém os .venv)
#   ./clean_all.sh --all    # também remove os .venv (próximo uv run re-resolve)

set -euo pipefail
cd "$(dirname "$0")"

DEEP=false
if [[ "${1:-}" == "--all" ]]; then
    DEEP=true
elif [[ -n "${1:-}" ]]; then
    echo "argumento desconhecido: $1 (use --all ou nenhum)" >&2
    exit 1
fi

step() { printf '\n\033[1m==> [%s] %s\033[0m\n' "$1" "$2"; }

step 1/5 "Dados parquet (data/raw e data/rich)"
uv run --script data/generate_data.py --clean

step 2/5 "Documentação gerada (rust-extension/docs)"
rm -rf rust-extension/docs
echo "removida"

step 3/5 "Build Rust (rust-extension/target: extensão compilada + cargo doc)"
(cd rust-extension && cargo clean 2>/dev/null) || rm -rf rust-extension/target
echo "removido"

step 4/5 "Caches de teste e bytecode (__pycache__, .pytest_cache)"
find . -path '*/.venv' -prune -o -type d -name '__pycache__' -print -exec rm -rf {} + 2>/dev/null || true
find . -path '*/.venv' -prune -o -type d -name '.pytest_cache' -print -exec rm -rf {} + 2>/dev/null || true

step 5/5 "Sobras de execuções interrompidas (_tmp_spill)"
rm -rf DuckDB/examples/_tmp_spill rust-extension/_tmp_spill
echo "ok"

if $DEEP; then
    step extra "Ambientes virtuais e lockfiles (--all): estado pós-clone completo"
    # uv.lock e Cargo.lock são gitignored (não existem num clone novo);
    # removê-los junto do .venv faz o --all reproduzir um clone recém-feito
    for proj in pandas pyarrow DuckDB rust-extension sqlalchemy-contract; do
        rm -rf "$proj/.venv" "$proj/uv.lock"
    done
    rm -f rust-extension/Cargo.lock
    echo "removidos .venv + uv.lock + Cargo.lock (o próximo build re-resolve tudo)"
fi

printf '\n\033[1;32mLimpo!\033[0m Restaure tudo com ./check_all.sh\n'
