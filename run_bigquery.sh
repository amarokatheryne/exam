#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-dev-utpbi-data-operation}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${1:-${SCRIPT_DIR}/rebuild_raw_tables.sql}"

if [[ ! -f "${SQL_FILE}" ]]; then
  echo "No existe el archivo SQL: ${SQL_FILE}" >&2
  exit 1
fi

query_args=(
  "--use_legacy_sql=false"
  "--project_id=${PROJECT_ID}"
)

if [[ -n "${BQ_LOCATION:-}" ]]; then
  query_args+=("--location=${BQ_LOCATION}")
fi

bq query "${query_args[@]}" < "${SQL_FILE}"
