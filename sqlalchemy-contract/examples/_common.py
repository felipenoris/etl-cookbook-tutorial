"""Helpers compartilhados pelos exemplos: seção de saída e geração de lançamentos."""

from __future__ import annotations

from decimal import Decimal

import numpy as np
import pyarrow as pa

from models import Lancamento, arrow_schema_for

RNG_SEED = 42


def section(title: str) -> None:
    print(f"\n{'=' * 10} {title} {'=' * 10}")


def gerar_lancamentos(n: int, contas_folha: list[int]) -> pa.Table:
    """Gera n lançamentos sintéticos já como Table Arrow (o formato colunar).

    Os valores nascem como centavos inteiros e viram decimal128(12,2) por
    cast — nunca passam por float. O schema vem do contrato
    (:func:`models.arrow_schema_for`), incluindo as descrições de coluna.
    """
    rng = np.random.default_rng(RNG_SEED)
    dias = rng.integers(0, 365, size=n)
    centavos = rng.integers(100, 5_000_000, size=n)  # R$ 1,00 a R$ 50.000,00
    # centavos inteiros -> decimal de 2 casas via scaleb(-2): sem float no caminho
    valor = pa.array([Decimal(int(c)).scaleb(-2) for c in centavos], type=pa.decimal128(12, 2))
    tabela = pa.table(
        {
            "id_lancamento": np.arange(1, n + 1, dtype=np.int64),
            "id_veiculo": rng.integers(1, 6, size=n, dtype=np.int64),
            "id_conta": rng.choice(np.array(contas_folha, dtype=np.int64), size=n),
            "data": pa.array(
                np.datetime64("2025-01-01") + dias.astype("timedelta64[D]"), type=pa.date32()
            ),
            "valor": valor,
            "meta": pa.array([None] * n, type=pa.string()),
            "timestamp": pa.array(
                np.full(n, np.datetime64("2026-01-01T12:00:00")), type=pa.timestamp("us")
            ),
        }
    )
    return tabela.cast(arrow_schema_for(Lancamento))
