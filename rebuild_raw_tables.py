from __future__ import annotations

import argparse
import sys
from pathlib import Path

from google.cloud import bigquery


DEFAULT_PROJECT_ID = "dev-utpbi-data-operation"
DEFAULT_SQL_FILE = Path(__file__).with_name("rebuild_raw_tables.sql")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ejecuta el script SQL para reconstruir tablas RAW en BigQuery."
    )
    parser.add_argument(
        "--project-id",
        default=DEFAULT_PROJECT_ID,
        help=f"Proyecto de BigQuery. Default: {DEFAULT_PROJECT_ID}",
    )
    parser.add_argument(
        "--sql-file",
        default=str(DEFAULT_SQL_FILE),
        help=f"Ruta del archivo SQL. Default: {DEFAULT_SQL_FILE}",
    )
    parser.add_argument(
        "--location",
        default=None,
        help="Ubicacion de BigQuery, por ejemplo US o southamerica-west1. Opcional.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sql_file = Path(args.sql_file)

    if not sql_file.is_file():
        print(f"No existe el archivo SQL: {sql_file}", file=sys.stderr)
        return 1

    sql = sql_file.read_text(encoding="utf-8")
    client = bigquery.Client(project=args.project_id, location=args.location)

    print(f"Proyecto: {args.project_id}")
    if args.location:
        print(f"Ubicacion: {args.location}")
    print(f"Ejecutando SQL: {sql_file}")

    job = client.query(sql)
    print(f"Job iniciado: {job.job_id}")

    job.result()
    print("Reconstruccion finalizada correctamente.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
