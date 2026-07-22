#!/usr/bin/env bash
# Envia a branch main para o remoto 'upstream'.
#
# Atalho para: git push upstream main
#
# Uso:
#   ./push_upstream.sh

set -euo pipefail
cd "$(dirname "$0")"

git push upstream main
