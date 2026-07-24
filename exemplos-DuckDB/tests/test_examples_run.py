"""Smoke tests: cada script de exemplo deve rodar do início ao fim sem erro."""

import subprocess
import sys

import pytest

from conftest import EXAMPLES_DIR, PROJECT_ROOT

NETWORK_EXAMPLES = {"13_reading_public_s3.py"}

EXAMPLE_SCRIPTS = [
    pytest.param(
        path,
        id=path.name,
        marks=pytest.mark.network if path.name in NETWORK_EXAMPLES else (),
    )
    for path in sorted(EXAMPLES_DIR.glob("[0-9]*.py"))
]


@pytest.mark.parametrize("script", EXAMPLE_SCRIPTS)
def test_example_runs_without_error(script):
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, f"{script.name} falhou:\n{result.stderr}"
    assert result.stdout.strip(), f"{script.name} não produziu saída"
