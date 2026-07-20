"""Smoke tests: cada script de exemplo deve rodar do início ao fim sem erro."""

import subprocess
import sys

import pytest

from conftest import EXAMPLES_DIR, PROJECT_ROOT

EXAMPLE_SCRIPTS = sorted(EXAMPLES_DIR.glob("0*.py"))


@pytest.mark.parametrize("script", EXAMPLE_SCRIPTS, ids=lambda p: p.name)
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
