# CLAUDE.md

Guidance for AI assistants working in this repository.

## What this is

A didactic ETL cookbook: independent, self-contained examples that each
exercise a specific stage of a data ETL pipeline, all reading the **same**
fictional dataset (partitioned parquet in `data/raw`) — with two deliberate
exceptions that use their own fictional accounting base (vehicles + monthly
ledger entries): `examples-sqlalchemy-contract` and DuckDB example 28, which
generates its base under `data/rich/duckdb_nova_data_base/` on every run. Each technology lives in
its **own isolated Python project** (a separate `pyproject.toml` + `.venv`
managed with [`uv`](https://docs.astral.sh/uv/)). It is teaching material — the
goal is that every example runs end-to-end and is clearly explained, not that
it be a reusable library.

The published docs live at <https://felipenoris.github.io/etl-cookbook-tutorial/>.
The root `README.md` is the authoritative overview of the repository (goals,
data model, prerequisites, how to run everything) — read it before making
non-trivial changes. The **type compatibility table** and the **measured
performance comparison** are not in the README: it delegates both to the
published pdoc docs (`examples-rust-extension/`, rendered under `/python/`).

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
    rich/                # generated outputs: the ETL (examples-rust-extension/run_etl.py) and DuckDB demos 06/19/28 — gitignored
  examples-pandas/                # pandas API with the Arrow backend
  examples-pyarrow/               # native pyarrow API
  examples-DuckDB/                # in-memory SQL over parquet, with configurable spill
  examples-rust-extension/        # Rust extension (PyO3 + pyo3-arrow) + full ETL + docs (pdoc/rustdoc)
  examples-sqlalchemy-contract/   # ORM-pattern migration: models as schema contract, ORM vs columnar
  check-all.sh           # one command: generate data, run all suites, build docs
  clean-all.sh           # inverse of check-all.sh: remove generated artifacts
  .github/workflows/     # ci.yml (runs check-all.sh) + docs.yml (publishes to GitHub Pages)
```

The five subprojects (`examples-pandas`, `examples-pyarrow`, `examples-DuckDB`,
`examples-rust-extension`, `examples-sqlalchemy-contract`) each have their own
`README.md`, `pyproject.toml`,
`examples/`, and `tests/`. They are **fully independent** — there is no
top-level Python package and no shared virtualenv.

## Toolchain / prerequisites

- **`uv`** — the only tool to install for the Python side. It resolves the
  Python interpreter per project, the dependencies, `maturin` (compiles the
  Rust extension), and dev tools (`pytest`, `pdoc`). No system Python or manual
  venv activation needed.
- **Rust toolchain** ([rustup.rs](https://rustup.rs)) — `cargo`/`rustc`, only
  for `examples-rust-extension` (compiling the PyO3 extension via maturin and generating
  rustdoc).
- **bash** — for `check-all.sh` / `clean-all.sh`.
- **Internet on first run** — for `uv`/`cargo` to fetch dependencies.
  Afterward, only 3 DuckDB tests (public S3 buckets, example 13) need network.

There is **no database server, no Docker, no credentials**. S3 examples use
public buckets with anonymous access.

## Common commands

Run everything (generates data, runs the 5 pytest suites whose smoke tests
execute every example, runs the rust-extension standalone scripts, builds all
docs). Any failure aborts:

```bash
./check-all.sh                # full run (3 DuckDB tests use the internet)
./check-all.sh --no-network   # skip the internet-dependent tests
```

Generate / clean the dataset (parquet is **not** committed — regenerate after a
fresh clone; `check-all.sh` does this automatically):

```bash
uv run --script data/generate_data.py --generate           # generate into data/raw
uv run --script data/generate_data.py --clean              # remove parquet from raw/ and rich/
uv run --script data/generate_data.py --clean --generate   # regenerate from scratch
```

Remove generated artifacts (inverse of `check-all.sh`):

```bash
./clean-all.sh          # remove generated artifacts (keeps the .venv)
./clean-all.sh --all    # also remove .venv + lockfiles (post-clone state)
```

Per-project work — always operate **inside** the subproject directory:

```bash
(cd examples-pandas && uv run pytest)                 # run one suite
(cd examples-pandas && uv run examples/01_loading_and_dtypes.py)   # run one example
(cd examples-rust-extension && uv sync)               # (re)compile the Rust extension after editing src/lib.rs
(cd examples-rust-extension && uv run pytest -m "not slow")        # skip the full ~15s pipeline test
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
- **Adding an example touches three places, not one.** Besides
  `examples/NN_*.py` (+ a `tests/test_*.py` for its contracts), update the
  example table in the subproject's `README.md` **and** the example count
  hardcoded in the `check-all.sh` step label (e.g. `"DuckDB: suíte pytest (24
  exemplos)"`). Nothing enforces those two — they silently go stale.
- **Shared helpers live in `examples/_common.py`** per subproject: repo-root/
  data paths (`REPO_ROOT = Path(__file__).resolve().parents[2]`), dataset
  loaders, and a `section(title)` printer. Reuse these instead of re-deriving
  paths.
- **Arrow-backed everywhere.** pandas reads use `engine="pyarrow",
  dtype_backend="pyarrow"` (see `examples-pandas/examples/_common.py`). The whole point
  is zero-copy interop across pandas/pyarrow/DuckDB/Rust via the Arrow format.
- **Money is `decimal128(12,2)` (2 decimal places), never float.** Summation/
  multiplication preserve the exact type. The Rust layer converts to
  `rust_decimal::Decimal`; scalars cross the Python↔Rust boundary as
  `decimal.Decimal`. Python wrappers reject `float` for monetary args with
  `TypeError`. Dates cross as `datetime.date` ↔ `chrono::NaiveDate`.
- **Rust extension pattern.** The `#[pyfunction]` in `src/lib.rs` takes all
  arguments explicitly; a thin same-named Python wrapper in
  `python/etl_rust_ext/__init__.py` supplies defaults and the docstring. After
  any change to `src/lib.rs`, recompile with `uv sync --reinstall-package
  etl-rust-ext` — **plain `uv sync` and `uv run` do not rebuild**: `uv` watches
  the package metadata, not the Rust source, so a changed `src/lib.rs` leaves
  the previously built `python/etl_rust_ext/_etl_rust_ext.*.so` in place. The
  stale `.so` either breaks pytest collection with `ImportError` on a
  newly added symbol, or — worse — passes green against the old binary when
  only a function body changed. `check-all.sh` step 5 therefore always
  reinstalls (~9s warm). Note that `clean-all.sh` must delete that `.so`
  explicitly: it lives outside `target/`, so `cargo clean` misses it and
  maturin has no `clean` subcommand.
- **DuckDB → Arrow streaming.** Use `to_arrow_reader(n)` (from either
  `con.execute(sql)` or `con.sql(sql)` — same `pyarrow.RecordBatchReader`
  class); `fetch_record_batch(n)` is the same method under its old name and
  emits `DeprecationWarning` since DuckDB 1.5. The reader is lazy and
  single-pass: it never rewinds. **Never consume a reader with the connection
  that produced it** — feeding an enriched reader back into the same `con` via
  replacement scan makes DuckDB reenter itself, observed as either a silent
  `count = 0` or a hang. Use two connections (one produces, one consumes). See
  `examples-DuckDB/examples/24_record_batch_pipeline.py`.
- **DuckDB relational API / `register`.** `con.read_parquet(glob)` returns a
  lazy `DuckDBPyRelation` (a query, not a result) bound to its connection —
  passing it to another connection raises. `con.register(name, obj)` is
  literally `CREATE TEMP VIEW`: it shows up in `duckdb_views()`, has empty
  `sql`, and `EXPORT DATABASE` skips it. **Hive partition columns are typed by
  autodetection**: a value that round-trips becomes `BIGINT` (`1`, `10`) or
  `DATE` (`2024-12-31`); one that does not stays `VARCHAR` (`'01'`, the case of
  `data/raw`). Pin the contract with `hive_types={'col': 'VARCHAR'}` when it
  matters (example 28). Because `order_month` is `VARCHAR`,
  `rel.filter("order_month = 1")` inserts a `CAST` that
  kills file pruning (reads 6 files instead of 1; the time penalty measures
  ~2-3x on a millisecond-scale query, so cite the pruning, not the ratio) —
  compare against a literal of the
  partition's own type. The raw SQL string form rewrites the cast itself; a
  relation or a view does not. See `examples-DuckDB/examples/26_*.py` and
  `27_*.py`.
- **Sequences cannot be repositioned.** No `setval()`, no `ALTER SEQUENCE ...
  RESTART`; `CREATE OR REPLACE SEQUENCE`/`DROP SEQUENCE` raise
  `DependencyException` while a table in the same catalog uses it in a
  `DEFAULT` (across catalogs — `TEMP` table, `main` sequence — the dependency is
  simply not tracked: replace and drop both pass). So `START WITH max(id) + 1`
  must be right at creation, i.e. after reading the existing data. Also: an
  explicit id does not advance the sequence (next `nextval` collides), values
  from a parallel `INSERT ... SELECT` do not follow the source order, and a
  `nextval` in a plain auto-commit `SELECT` is **not persisted** to the file
  (reopen hands out the same value; only a write transaction — explicit
  `BEGIN/COMMIT` or any `INSERT` — saves the counter). Never treat the
  sequence as the authority for ids stored outside the database. `INSERT INTO
  t BY NAME SELECT * FROM df` lets a DataFrame without the key column rely on
  that `DEFAULT`. **`COPY ... PARTITION_BY` with `OVERWRITE` deletes every file
  under the target directory** (all partitions); the idempotent partition
  reload is `rmtree(partition dir)` + `OVERWRITE_OR_IGNORE`. See
  `examples-DuckDB/examples/28_*.py`.
- **JSON is an Arrow extension type, not a `DataType`.** `arrow.json` =
  storage `utf8` **plus** an `ARROW:extension:name` marker on the field
  (`pa.json_()` in pyarrow, `arrow_schema::extension::Json` in Rust — needs the
  `canonical_extension_types` feature). The marker survives Parquet (logical
  type `JSON`), pandas, `write_dataset`, and the pyo3-arrow boundary, but
  **`DuckDB → Arrow` drops it by default** — the column comes back as plain
  `utf8` unless the connection sets `arrow_lossless_conversion = true`. It is a
  *silent* loss: data intact, semantics gone. The Rust functions therefore
  reject an unmarked `utf8` rather than guessing; `as_json_column` is the
  explicit repair. Two consequences: JSON has **no decimal and no date type**,
  so money/dates degrade to float/string if carried inside a document (shred
  them into typed columns instead — `shred_json_column` reads the raw number
  token via `serde_json`'s `RawValue` to avoid `f64`); and a JSON round-trip
  guarantees **semantic**, never byte, equality (reparsing sorts keys and eats
  whitespace). See `examples-rust-extension/run_json_types.py`.
- **Network-dependent tests** are marked `@pytest.mark.network` and skipped by
  the `--no-network` flag (wired in `examples-DuckDB/tests/conftest.py`). Slow tests
  (full pipeline over `data/raw`) are marked `slow` in `examples-rust-extension`.
- **Generated artifacts are gitignored** and must not be committed: parquet
  (`*.parquet`), `.venv/`, `target/`, `*.so`, `examples-rust-extension/docs/`, and the
  lockfiles `uv.lock` / `Cargo.lock` (this project deliberately does not version
  lockfiles). See `.gitignore`.

## Testing

Each subproject has its own pytest suite = smoke tests (every example runs) +
unit tests asserting the contracts each example assumes. `testpaths = ["tests"]`
is set in every `pyproject.toml`, and `tests/conftest.py` puts `examples/` on
`sys.path`. The fastest full verification is `./check-all.sh` (add
`--no-network` when offline). Before pushing changes that touch code, run the
affected subproject's suite (or `./check-all.sh`) — CI runs exactly
`./check-all.sh`.

## Documentation

- **Python side:** `pdoc` renders `examples-rust-extension/docs/` from Google-style
  docstrings (`--math --mermaid --docformat google`, custom
  `pdoc-templates/`). Generated, not versioned.
- **Rust side:** `cargo doc --no-deps --document-private-items` renders rustdoc
  from `//!`/`///` comments in `src/lib.rs` (private items needed because the
  `#[pyfunction]`s are private).
- `check-all.sh` step 9 builds both; `docs.yml` publishes them to GitHub Pages
  on every push to `main`.

## CI

`.github/workflows/ci.yml` runs `./check-all.sh` on pushes to `main` and
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
- **Never delete a remote branch.** No `git push origin --delete <branch>` and no
  `git push origin :<branch>`, not even for a `claude/**` branch whose PR is
  already merged. Deleting branches on the remote is always the maintainer's job.
  If remote cleanup seems warranted, point it out and let the maintainer do it.
- **Never alter a local branch that is not prefixed `claude/`.** Do not commit to,
  amend, rebase, reset, cherry-pick onto, or otherwise change the working state of
  `main` (or any non-`claude/` branch). The **only** permitted touch of `main` is
  the fast-forward sync from `origin/main` after a merge
  (`git checkout main && git pull origin main`), which introduces no local changes.
  All actual work happens on `claude/**` branches; deleting a local `claude/**`
  branch after its merge is fine.
- Commit and/or push **only when explicitly asked**. Write clear
  pt-BR-friendly commit messages.
- Do **not** open a pull request unless explicitly asked. When asked, use the
  GitHub CLI (`gh`), which is installed and authenticated in this environment
  (SSH protocol, scope `repo`). The flow is: create a `claude/<name>` branch,
  commit and push it (`git push -u origin claude/<name>`), then open the PR
  against `main` with `gh pr create --base main --title ... --body ...`. Write
  the title and body in pt-BR-friendly wording. Never open a PR from `main`.
- Remote is `origin` (SSH). Pushing `main` is just `git push`; there is no push
  wrapper script.
