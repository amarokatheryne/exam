# AGENTS.md

## Cursor Cloud specific instructions

### Overview

This is a BigQuery data pipeline repository for UTP's BI data operations. It contains:

- `rebuild_raw_tables.sql` — BigQuery SQL that drops and recreates 12 raw tables from landing datasets, adding partitioning/clustering and audit timestamps.
- `run_bigquery.sh` — Bash wrapper that executes the SQL via the `bq` CLI.

No application runtime, package manager, or build system is used.

### Required tools

- **Google Cloud SDK** (`bq` CLI) — installed at `/opt/google-cloud-sdk/bin/`. Ensure `PATH` includes this directory.
- **shellcheck** — for linting `run_bigquery.sh`.
- **sqlfluff** — for linting `rebuild_raw_tables.sql` (use `--dialect bigquery`).

### Lint commands

- `shellcheck run_bigquery.sh` — lint the shell script (should pass clean).
- `sqlfluff lint --dialect bigquery rebuild_raw_tables.sql` — lint SQL. Current warnings are style-only (indentation, line length, `SELECT *`); no syntax errors.

### Running the pipeline

```bash
# Default project: dev-utpbi-data-operation
bash run_bigquery.sh

# Override project
PROJECT_ID=my-project bash run_bigquery.sh

# Override BigQuery location
BQ_LOCATION=US bash run_bigquery.sh
```

**Requires GCP authentication.** Run `gcloud auth login` or set `GOOGLE_APPLICATION_CREDENTIALS` to a service account key file before executing.

### Key caveats

- The script uses `set -euo pipefail` — any single SQL statement failure aborts the entire run.
- All 12 tables are dropped and recreated; this is **destructive** and cannot be partially rolled back.
- The SQL uses `SELECT *` from landing tables, so schema changes in landing tables propagate automatically.
- Timestamps use `America/Lima` timezone.
