# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pyarrow>=17",
#     "numpy>=1.26",
# ]
# ///
"""Gera as bases ficticias em data/raw usadas por todos os exemplos do tutorial.

Modelo de dados (poucas colunas, propositalmente simples):

- customers: dimensao particionada por `region` (Hive-style: region=<valor>/).
- products:  dimensao pequena, arquivo unico (sem particionamento).
- orders:    fato particionado por `order_year`/`order_month`, uma partição por
             mes (6 partições, cada uma calibrada para ~50MB) para permitir
             exercitar leitura particionada, JOINs e spill em memoria limitada.

As DIMENSÕES concentram os tipos de dados exercitados pela stack
Arrow/Parquet/DuckDB (a fato `orders` fica só com os tipos básicos, para
manter as partições calibradas):

- customers: bool (`is_active`), timestamp µs (`signup_ts`), struct
  (`address{street,city,zip}`), list<string> (`tags`) e map<string,string>
  (`preferences`) — além de string, int64 e date32.
- products: decimal128(12,2) (`unit_cost`, sempre 2 casas decimais) e binary
  (`sku`, 8 bytes) — além de string, int64 e float64.

Uso (o ``--script`` força o uv a resolver o ambiente isolado declarado nos
metadados PEP 723 acima, ignorando qualquer venv ativo ou projeto ao redor):

    uv run --script data/generate_data.py --generate           # gera as bases em data/raw
    uv run --script data/generate_data.py --clean              # remove os parquet de raw/ e rich/
    uv run --script data/generate_data.py --clean --generate   # regenera do zero
"""

from __future__ import annotations

import argparse
from decimal import Decimal
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

RNG_SEED = 42
DATA_DIR = Path(__file__).resolve().parent
RAW_DIR = DATA_DIR / "raw"
RICH_DIR = DATA_DIR / "rich"

NUM_CUSTOMERS = 2_000
NUM_PRODUCTS = 200
REGIONS = ["norte", "nordeste", "centro_oeste", "sudeste", "sul"]
CATEGORIES = ["eletronicos", "alimentos", "vestuario", "livros", "casa"]
STATUSES = ["novo", "enviado", "entregue", "cancelado", "devolvido"]
STATUS_WEIGHTS = [0.15, 0.20, 0.50, 0.10, 0.05]

CITIES = {
    "norte": ["Manaus", "Belem", "Porto Velho"],
    "nordeste": ["Recife", "Salvador", "Fortaleza"],
    "centro_oeste": ["Goiania", "Cuiaba", "Campo Grande"],
    "sudeste": ["Sao Paulo", "Rio de Janeiro", "Belo Horizonte"],
    "sul": ["Curitiba", "Porto Alegre", "Florianopolis"],
}
TAGS = ["vip", "atacado", "varejo", "online", "fidelidade"]
PREF_CHOICES = {
    "canal": ["email", "sms", "whatsapp"],
    "newsletter": ["sim", "nao"],
    "idioma": ["pt", "es", "en"],
}

ORDER_YEAR = 2025
ORDER_MONTHS = list(range(1, 7))  # 6 partições
TARGET_PARTITION_BYTES = 50 * 1024 * 1024
CALIBRATION_ROWS = 200_000


def _rng(offset: int) -> np.random.Generator:
    return np.random.default_rng(RNG_SEED + offset)


def generate_customers() -> pa.Table:
    rng = _rng(1)
    n = NUM_CUSTOMERS
    customer_id = np.arange(1, n + 1, dtype=np.int64)
    region = rng.choice(REGIONS, size=n)
    customer_name = [f"cliente_{i:05d}" for i in customer_id]
    signup_offset_days = rng.integers(0, 3 * 365, size=n)
    signup_date = np.datetime64("2023-01-01") + signup_offset_days.astype("timedelta64[D]")

    # bool: ~85% de clientes ativos
    is_active = rng.random(n) < 0.85

    # timestamp[us]: o instante exato do cadastro (data + hora aleatória do dia)
    seconds = rng.integers(0, 86_400, size=n)
    signup_ts = signup_date.astype("datetime64[s]") + seconds.astype("timedelta64[s]")

    # struct<street, city, zip>: endereço aninhado, com cidade coerente à região
    address = pa.array(
        [
            {
                "street": f"Rua {chr(65 + int(rng.integers(0, 26)))}, {int(rng.integers(1, 2000))}",
                "city": str(rng.choice(CITIES[str(reg)])),
                "zip": f"{int(rng.integers(10_000, 99_999)):05d}-{int(rng.integers(0, 999)):03d}",
            }
            for reg in region
        ],
        type=pa.struct([("street", pa.string()), ("city", pa.string()), ("zip", pa.string())]),
    )

    # list<string>: 0 a 3 tags por cliente, sem repetição
    tags = pa.array(
        [sorted(rng.choice(TAGS, size=int(rng.integers(0, 4)), replace=False).tolist()) for _ in range(n)],
        type=pa.list_(pa.string()),
    )

    # map<string,string>: preferências chave->valor (nem todo cliente tem todas)
    preferences = pa.array(
        [
            [(chave, str(rng.choice(valores))) for chave, valores in PREF_CHOICES.items() if rng.random() < 0.7]
            for _ in range(n)
        ],
        type=pa.map_(pa.string(), pa.string()),
    )

    return pa.table(
        {
            "customer_id": customer_id,
            "customer_name": customer_name,
            "region": region,
            "signup_date": pa.array(signup_date, type=pa.date32()),
            "is_active": pa.array(is_active),
            "signup_ts": pa.array(signup_ts.astype("datetime64[us]"), type=pa.timestamp("us")),
            "address": address,
            "tags": tags,
            "preferences": preferences,
        }
    )


def generate_products() -> pa.Table:
    rng = _rng(2)
    product_id = np.arange(1, NUM_PRODUCTS + 1, dtype=np.int64)
    category = rng.choice(CATEGORIES, size=NUM_PRODUCTS)
    product_name = [f"produto_{i:04d}" for i in product_id]
    unit_price = np.round(rng.uniform(5.0, 500.0, size=NUM_PRODUCTS), 2)

    # decimal128(12,2): custo unitário com EXATAMENTE 2 casas decimais —
    # dinheiro deve ser decimal, não float (0.1 + 0.2 != 0.3 em float64)
    unit_cost = pa.array(
        [Decimal(f"{preco * frac:.2f}") for preco, frac in zip(unit_price, rng.uniform(0.5, 0.8, NUM_PRODUCTS))],
        type=pa.decimal128(12, 2),
    )

    # binary: um SKU opaco de 8 bytes (hash/código de barras binário)
    sku = pa.array([rng.bytes(8) for _ in range(NUM_PRODUCTS)], type=pa.binary())

    return pa.table(
        {
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "unit_price": unit_price,
            "unit_cost": unit_cost,
            "sku": sku,
        }
    )


def _make_orders_batch(
    rng: np.random.Generator, n: int, order_id_start: int, year: int, month: int
) -> pa.Table:
    order_id = np.arange(order_id_start, order_id_start + n, dtype=np.int64)
    customer_id = rng.integers(1, NUM_CUSTOMERS + 1, size=n, dtype=np.int64)
    product_id = rng.integers(1, NUM_PRODUCTS + 1, size=n, dtype=np.int64)
    day_offset = rng.integers(0, 27, size=n)  # dias 1..27, seguro p/ qualquer mes
    base_day = np.datetime64(f"{year:04d}-{month:02d}-01")
    order_date = base_day + day_offset.astype("timedelta64[D]")
    quantity = rng.integers(1, 11, size=n, dtype=np.int32)
    status = rng.choice(STATUSES, size=n, p=STATUS_WEIGHTS)
    return pa.table(
        {
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "order_date": pa.array(order_date, type=pa.date32()),
            "quantity": quantity,
            "status": status,
        }
    )


def estimate_rows_per_partition() -> int:
    """Gera um lote de calibracao em memoria para estimar linhas/MB do parquet."""
    rng = _rng(999)
    sample = _make_orders_batch(rng, CALIBRATION_ROWS, order_id_start=1, year=ORDER_YEAR, month=1)
    buf = pa.BufferOutputStream()
    pq.write_table(sample, buf)
    bytes_per_row = buf.getvalue().size / CALIBRATION_ROWS
    rows = int(TARGET_PARTITION_BYTES / bytes_per_row)
    print(f"[calibracao] ~{bytes_per_row:.2f} bytes/linha -> {rows:,} linhas/partição (~50MB)")
    return rows


def write_customers(table: pa.Table) -> None:
    # Segue a convenção Hive: a coluna de partição fica só no nome do diretório,
    # não duplicada dentro do arquivo parquet (evita conflito de schema ao ler
    # o dataset inteiro, já que a leitura reconstrói `region` a partir do path).
    out_dir = RAW_DIR / "customers"
    for region in REGIONS:
        part_dir = out_dir / f"region={region}"
        part_dir.mkdir(parents=True, exist_ok=True)
        subset = table.filter(pc.equal(table["region"], region)).drop(["region"])
        pq.write_table(subset, part_dir / "part-0.parquet")
    print(f"[customers] {table.num_rows:,} linhas em {len(REGIONS)} partições -> {out_dir}")


def write_products(table: pa.Table) -> None:
    out_dir = RAW_DIR / "products"
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out_dir / "part-0.parquet")
    print(f"[products] {table.num_rows:,} linhas (arquivo único) -> {out_dir}")


def write_orders(rows_per_partition: int) -> None:
    out_dir = RAW_DIR / "orders"
    order_id_start = 1
    total_rows = 0
    for i, month in enumerate(ORDER_MONTHS):
        rng = _rng(100 + month)
        batch = _make_orders_batch(rng, rows_per_partition, order_id_start, ORDER_YEAR, month)
        part_dir = out_dir / f"order_year={ORDER_YEAR}" / f"order_month={month:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        path = part_dir / "part-0.parquet"
        pq.write_table(batch, path)
        size_mb = path.stat().st_size / (1024 * 1024)
        print(f"[orders] {ORDER_YEAR}-{month:02d}: {batch.num_rows:,} linhas, {size_mb:.1f}MB -> {path}")
        order_id_start += rows_per_partition
        total_rows += batch.num_rows
    print(f"[orders] total: {total_rows:,} linhas em {len(ORDER_MONTHS)} partições -> {out_dir}")


def clean_parquet() -> None:
    """Remove os arquivos parquet de data/raw e data/rich.

    Apaga os `*.parquet` e depois os diretórios de partição que ficaram vazios
    (bottom-up), preservando os diretórios `raw/` e `rich/` em si e eventuais
    arquivos não-parquet (ex.: `.gitkeep`).
    """
    removed = 0
    for base in (RAW_DIR, RICH_DIR):
        if not base.exists():
            continue
        for path in base.rglob("*.parquet"):
            path.unlink()
            removed += 1
        subdirs = sorted((p for p in base.rglob("*") if p.is_dir()), reverse=True)
        for directory in subdirs:
            if not any(directory.iterdir()):
                directory.rmdir()
    print(f"[clean] {removed} arquivos parquet removidos de {RAW_DIR}/ e {RICH_DIR}/")


def generate() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    write_customers(generate_customers())
    write_products(generate_products())

    rows_per_partition = estimate_rows_per_partition()
    write_orders(rows_per_partition)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gera (ou limpa) as bases parquet fictícias do tutorial."
    )
    parser.add_argument(
        "--generate",
        action="store_true",
        help="gera as bases particionadas em data/raw",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="remove os arquivos parquet de data/raw e data/rich",
    )
    args = parser.parse_args()

    if not (args.generate or args.clean):
        parser.error("informe --generate, --clean ou ambos (--clean roda primeiro)")

    if args.clean:
        clean_parquet()
    if args.generate:
        generate()


if __name__ == "__main__":
    main()
