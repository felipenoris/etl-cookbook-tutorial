"""Configuração compartilhada dos testes: torna `examples/` importável.

Também define a flag `--no-network`: testes marcados com `@pytest.mark.network`
(os que leem buckets S3/HTTP públicos, ex.: exemplo 13) são pulados quando a
suíte roda com `uv run pytest --no-network` — para ambientes sem internet.
"""

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "examples"

sys.path.insert(0, str(EXAMPLES_DIR))


def pytest_addoption(parser):
    parser.addoption(
        "--no-network",
        action="store_true",
        default=False,
        help="pula os testes que exigem acesso à internet (marcados com 'network')",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--no-network"):
        return
    skip_network = pytest.mark.skip(reason="--no-network: teste exige acesso à internet")
    for item in items:
        if "network" in item.keywords:
            item.add_marker(skip_network)
