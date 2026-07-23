# CLAUDE.md

Guidance for AI assistants working in this repository.

## What this is

A didactic ETL cookbook: independent, self-contained examples that each
exercise a specific stage of a data ETL pipeline, all reading the **same**
fictional dataset (partitioned parquet in `data/raw`). Each technology lives in
its **own isolated Python project** (a separate `pyproject.toml` + `.venv`
managed with [`uv`](https://docs.astral.sh/uv/)). It is teaching material — the
goal is that every example runs end-to-end and is clearly explained, not that
it be a reusable library.

The published docs live at <https://felipenoris.github.io/etl-cookbook-tutorial/>.
The root `README.md` is the authoritative, in-depth overview (data model, type
compatibility table, measured performance comparison) — read it before making
non-trivial changes.

## Language convention (important)

**All prose in this repo is Brazilian Portuguese (pt-BR)**: README files,
code comments, docstrings, printed output, test messages, and shell-script
comments. When editing or adding content, **keep writing in pt-BR** to match.
Identifiers (variable/function names) are a mix of English and Portuguese —
follow the convention of the file you are editing. This `CLAUDE.md` is the one
intentional exception (English, for AI-assistant tooling).

## Repository layout

```
etl-cookbook-tutorial/
  data/
    generate_data.py     # generates the fictional dataset (standalone PEP 723 script)
    raw/                 # input: partitioned parquet (customers, products, orders) — gitignored
    rich/                # ETL output (written by rust-extension/run_etl.py) — gitignored
  pandas/                # pandas API with the Arrow backend
  pyarrow/               # native pyarrow API
  DuckDB/                # in-memory SQL over parquet, with configurable spill
  rust-extension/        # Rust extension (PyO3 + pyo3-arrow) + full ETL + docs (pdoc/rustdoc)
  sqlalchemy-contract/   # ORM-pattern migration: models as schema contract, ORM vs columnar
  check_all.sh           # one command: generate data, run all suites, build docs
  clean_all.sh           # inverse of check_all.sh: remove generated artifacts
  .github/workflows/     # ci.yml (runs check_all.sh) + docs.yml (publishes to GitHub Pages)
```

The five subprojects (`pandas`, `pyarrow`, `DuckDB`, `rust-extension`,
`sqlalchemy-contract`) each have their own `README.md`, `pyproject.toml`,
`examples/`, and `tests/`. They are **fully independent** — there is no
top-level Python package and no shared virtualenv.

## Toolchain / prerequisites

- **`uv`** — the only tool to install for the Python side. It resolves the
  Python interpreter per project, the dependencies, `maturin` (compiles the
  Rust extension), and dev tools (`pytest`, `pdoc`). No system Python or manual
  venv activation needed.
- **Rust toolchain** ([rustup.rs](https://rustup.rs)) — `cargo`/`rustc`, only
  for `rust-extension` (compiling the PyO3 extension via maturin and generating
  rustdoc).
- **bash** — for `check_all.sh` / `clean_all.sh`.
- **Internet on first run** — for `uv`/`cargo` to fetch dependencies.
  Afterward, only 3 DuckDB tests (public S3 buckets, example 13) need network.

There is **no database server, no Docker, no credentials**. S3 examples use
public buckets with anonymous access.

## Common commands

Run everything (generates data, runs the 5 pytest suites whose smoke tests
execute every example, runs the rust-extension standalone scripts, builds all
docs). Any failure aborts:

```bash
./check_all.sh                # full run (3 DuckDB tests use the internet)
./check_all.sh --no-network   # skip the internet-dependent tests
```

Generate / clean the dataset (parquet is **not** committed — regenerate after a
fresh clone; `check_all.sh` does this automatically):

```bash
uv run data/generate_data.py --generate           # generate into data/raw
uv run data/generate_data.py --clean              # remove parquet from raw/ and rich/
uv run data/generate_data.py --clean --generate   # regenerate from scratch
```

Remove generated artifacts (inverse of `check_all.sh`):

```bash
./clean_all.sh          # remove generated artifacts (keeps the .venv)
./clean_all.sh --all    # also remove .venv + lockfiles (post-clone state)
```

Per-project work — always operate **inside** the subproject directory:

```bash
(cd pandas && uv run pytest)                 # run one suite
(cd pandas && uv run examples/01_loading_and_dtypes.py)   # run one example
(cd rust-extension && uv sync)               # (re)compile the Rust extension after editing src/lib.rs
(cd rust-extension && uv run pytest -m "not slow")        # skip the full ~15s pipeline test
```

## Conventions to follow

- **Isolated projects.** Never add a dependency to the repo root — add it to the
  relevant subproject's `pyproject.toml` and let `uv` resolve. Run tools with
  `uv run ...` from inside the subproject; do not activate venvs manually.
- **Examples are numbered, standalone, and runnable.** Files are named
  `examples/NN_description.py` and must run start-to-finish with `uv run
  examples/NN_*.py`, printing output. Each example's smoke test is automatic:
  `tests/test_examples_run.py` discovers every `examples/[0-9]*.py`, runs it in
  a subprocess, and asserts exit 0 **and** non-empty stdout. So a new example is
  covered the moment it lands in `examples/` — but it must actually print
  something and exit cleanly.
- **Shared helpers live in `examples/_common.py`** per subproject: repo-root/
  data paths (`REPO_ROOT = Path(__file__).resolve().parents[2]`), dataset
  loaders, and a `section(title)` printer. Reuse these instead of re-deriving
  paths.
- **Arrow-backed everywhere.** pandas reads use `engine="pyarrow",
  dtype_backend="pyarrow"` (see `pandas/examples/_common.py`). The whole point
  is zero-copy interop across pandas/pyarrow/DuckDB/Rust via the Arrow format.
- **Money is `decimal128(12,2)` (2 decimal places), never float.** Summation/
  multiplication preserve the exact type. The Rust layer converts to
  `rust_decimal::Decimal`; scalars cross the Python↔Rust boundary as
  `decimal.Decimal`. Python wrappers reject `float` for monetary args with
  `TypeError`. Dates cross as `datetime.date` ↔ `chrono::NaiveDate`.
- **Rust extension pattern.** The `#[pyfunction]` in `src/lib.rs` takes all
  arguments explicitly; a thin same-named Python wrapper in
  `python/etl_rust_ext/__init__.py` supplies defaults and the docstring. After
  any change to `src/lib.rs`, run `uv sync` (or `uv run ...`) to recompile.
- **Network-dependent tests** are marked `@pytest.mark.network` and skipped by
  the `--no-network` flag (wired in `DuckDB/tests/conftest.py`). Slow tests
  (full pipeline over `data/raw`) are marked `slow` in `rust-extension`.
- **Generated artifacts are gitignored** and must not be committed: parquet
  (`*.parquet`), `.venv/`, `target/`, `*.so`, `rust-extension/docs/`, and the
  lockfiles `uv.lock` / `Cargo.lock` (this project deliberately does not version
  lockfiles). See `.gitignore`.

## Testing

Each subproject has its own pytest suite = smoke tests (every example runs) +
unit tests asserting the contracts each example assumes. `testpaths = ["tests"]`
is set in every `pyproject.toml`, and `tests/conftest.py` puts `examples/` on
`sys.path`. The fastest full verification is `./check_all.sh` (add
`--no-network` when offline). Before pushing changes that touch code, run the
affected subproject's suite (or `./check_all.sh`) — CI runs exactly
`./check_all.sh`.

## Documentation

- **Python side:** `pdoc` renders `rust-extension/docs/` from Google-style
  docstrings (`--math --mermaid --docformat google`, custom
  `pdoc-templates/`). Generated, not versioned.
- **Rust side:** `cargo doc --no-deps --document-private-items` renders rustdoc
  from `//!`/`///` comments in `src/lib.rs` (private items needed because the
  `#[pyfunction]`s are private).
- `check_all.sh` step 9 builds both; `docs.yml` publishes them to GitHub Pages
  on every push to `main`.

## CI

`.github/workflows/ci.yml` runs `./check_all.sh` on pushes to `main` and
`claude/**` branches, and on every PR (installs `uv` + Rust toolchain with
caching, 30-min timeout, publishes generated HTML docs as an artifact).
`.github/workflows/docs.yml` builds and publishes the docs to GitHub Pages.

## Git workflow

- Default branch is `main`. **Never commit directly to `main`.** Every commit an
  AI assistant creates goes on a branch whose name is prefixed with `claude/`
  (e.g. `claude/fix-diagram`). Create the branch first, commit there, and push
  with `git push -u origin claude/<name>`. The maintainer reviews, merges, and
  deletes the branch; the assistant then syncs local `main`
  (`git checkout main && git pull origin main && git branch -D claude/<name>`).
- Commit and/or push **only when explicitly asked**. Write clear
  pt-BR-friendly commit messages.
- Do **not** open a pull request unless explicitly asked (and note the assistant
  cannot open one from the local environment anyway — no `gh`/token there).
- Remote is `origin` (SSH). Pushing `main` is just `git push`; there is no push
  wrapper script.
