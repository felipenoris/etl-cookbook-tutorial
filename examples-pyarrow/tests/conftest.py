"""Configuração compartilhada dos testes: torna `examples/` importável."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = PROJECT_ROOT / "examples"

sys.path.insert(0, str(EXAMPLES_DIR))
