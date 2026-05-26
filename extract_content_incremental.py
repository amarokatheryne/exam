"""
Carga incremental de public.content (PostgreSQL RDS) hacia GCS y BigQuery landing.

Diferencias respecto al full mensual:
  - Filtra por columna `updated` (no por `created`).
  - El punto de partida es MAX(updated) en la tabla final de BQ, con lookback opcional.
  - Un solo archivo Parquet por ejecución (ventanas diarias internas si el rango es largo).

Variables de entorno requeridas:
  PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD
  (opcionales: PG_PORT, PG_SSLMODE)

Opcionales:
  INCREMENTAL_START  — ISO datetime; fuerza inicio y omite watermark de BQ.
  LOOKBACK_HOURS     — default 24; solapa el watermark para re-procesar actualizaciones tardías.
  SUBRANGE_DAYS, CHUNK_SIZE, OUTPUT_DIR, BUCKET_NAME, BQ_*, GCS_*_FOLDER, etc.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from dateutil.relativedelta import relativedelta

LIMA = ZoneInfo("America/Lima")

DB_CONFIG = {
    "host": os.environ.get("PG_HOST", ""),
    "port": int(os.environ.get("PG_PORT", "5432")),
    "database": os.environ.get("PG_DATABASE", "pao_course"),
    "user": os.environ.get("PG_USER", ""),
    "password": os.environ.get("PG_PASSWORD", ""),
    "sslmode": os.environ.get("PG_SSLMODE", "require"),
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}

BUCKET_NAME = os.environ.get(
    "GCS_BUCKET", "dev-utp-stg-pao-8m3p6t9x2v5q1z4w7k0j2b5ndf"
)
GCS_FOLDER_PENDING = os.environ.get(
    "GCS_FOLDER_PENDING",
    "data/input/pao_course/content/incremental/pending",
)
GCS_FOLDER_PROCESSED = os.environ.get(
    "GCS_FOLDER_PROCESSED",
    "data/input/pao_course/content/incremental/processed",
)
GCS_FOLDER_ERROR = os.environ.get(
    "GCS_FOLDER_ERROR",
    "data/input/pao_course/content/incremental/error",
)

BQ_PROJECT = os.environ.get("BQ_PROJECT", "dev-utpbi-data-operation")
BQ_DATASET = os.environ.get("BQ_DATASET", "landing_pao_course")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

SOURCE_SCHEMA = os.environ.get("SOURCE_SCHEMA", "public")
SOURCE_TABLE = os.environ.get("SOURCE_TABLE", "content")
BQ_TABLE_TMP = os.environ.get("BQ_TABLE_TMP", "content_tmp")
BQ_TABLE_FINAL = os.environ.get("BQ_TABLE_FINAL", "content")

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/tmp/pao_content_exports")
SUBRANGE_DAYS = int(os.environ.get("SUBRANGE_DAYS", "1"))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "100000"))
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "24"))

GSUTIL_PATH = ""
BQ_PATH = ""


def _resolve_cli(name: str) -> str:
    path = os.environ.get(name.upper(), "")
    if path and os.path.isfile(path):
        return path
    found = shutil.which(name)
    if found:
        return found
    win = os.environ.get(f"{name.upper()}_WIN_PATH", "")
    if win and os.path.isfile(win):
        return win
    raise FileNotFoundError(
        f"No se encontró '{name}' en PATH ni en {name.upper()}_WIN_PATH"
    )


def log(message: str) -> None:
    print(f"[{datetime.now(LIMA).strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def validate_config() -> None:
    global GSUTIL_PATH, BQ_PATH

    env_map = {"host": "PG_HOST", "user": "PG_USER", "password": "PG_PASSWORD"}
    missing = [env_map[k] for k in env_map if not DB_CONFIG.get(k)]
    if missing:
        raise ValueError("Faltan variables de entorno: " + ", ".join(missing))

    GSUTIL_PATH = _resolve_cli("gsutil")
    BQ_PATH = _resolve_cli("bq")
    log(f"Herramientas: gsutil={GSUTIL_PATH}, bq={BQ_PATH}")


def run_cmd(cmd: list[str]) -> str:
    log("Ejecutando comando:")
    log(" ".join([f'"{x}"' if " " in x else x for x in cmd]))

    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    if result.stdout.strip():
        log(result.stdout.strip())
    if result.stderr.strip():
        log(result.stderr.strip())

    if result.returncode != 0:
        raise RuntimeError(
            f"Error ejecutando comando.\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result.stdout


def bq_query(sql: str) -> str:
    sql_one_line = " ".join(sql.split())
    return run_cmd(
        [
            BQ_PATH,
            "--quiet",
            f"--location={BQ_LOCATION}",
            "query",
            "--use_legacy_sql=false",
            "--format=csv",
            sql_one_line,
        ]
    )


def parse_bq_scalar_csv(stdout: str) -> str | None:
    lines = [ln.strip() for ln in stdout.strip().splitlines() if ln.strip()]
    if len(lines) < 2:
        return None
    return lines[1] or None


def get_watermark_from_bq() -> datetime | None:
    sql = f"""
    SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', MAX(updated), 'America/Lima')
    FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_FINAL}`
    """
    out = bq_query(sql)
    raw = parse_bq_scalar_csv(out)
    if not raw or raw.upper() == "NULL":
        return None
    return datetime.strptime(raw.split(".")[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=LIMA)


def resolve_incremental_window() -> tuple[datetime, datetime, str]:
    end = datetime.now(LIMA).replace(microsecond=0)

    forced = os.environ.get("INCREMENTAL_START", "").strip()
    if forced:
        start = datetime.fromisoformat(forced)
        if start.tzinfo is None:
            start = start.replace(tzinfo=LIMA)
        else:
            start = start.astimezone(LIMA)
        log(f"INCREMENTAL_START forzado: {start}")
    else:
        watermark = get_watermark_from_bq()
        if watermark is None:
            raise RuntimeError(
                "La tabla final no tiene MAX(updated). "
                "Ejecute la carga full o defina INCREMENTAL_START."
            )
        start = watermark - relativedelta(hours=LOOKBACK_HOURS)
        log(f"Watermark BQ (Lima): {watermark}")
        log(f"Lookback {LOOKBACK_HOURS} h -> inicio: {start}")

    if start >= end:
        raise ValueError(f"Rango inválido: start={start} >= end={end}")

    period = f"incr_{end.strftime('%Y_%m_%d_%H%M%S')}"
    return start, end, period


def subrange_dates(start_date: datetime, end_date: datetime, days: int):
    current = start_date
    while current < end_date:
        next_date = min(current + relativedelta(days=days), end_date)
        yield current, next_date
        current = next_date


def build_gcs_file_path(folder: str, year: int, month: int, file_name: str) -> str:
    return f"gs://{BUCKET_NAME}/{folder}/year={year}/month={month:02d}/{file_name}"


def upload_to_gcs(local_file: str, gcs_path: str) -> None:
    run_cmd([GSUTIL_PATH, "cp", local_file, gcs_path])


def move_gcs_file(source_gcs: str, target_gcs: str) -> None:
    run_cmd([GSUTIL_PATH, "mv", source_gcs, target_gcs])


def gcs_file_exists(gcs_path: str) -> bool:
    result = subprocess.run(
        [GSUTIL_PATH, "ls", gcs_path], capture_output=True, text=True, check=False
    )
    return result.returncode == 0


def drop_tmp_table_if_exists() -> None:
    tmp_ref = f"{BQ_PROJECT}:{BQ_DATASET}.{BQ_TABLE_TMP}"
    result = subprocess.run(
        [BQ_PATH, "--quiet", "rm", "-f", "-t", tmp_ref],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout.strip():
        log(result.stdout.strip())
    if result.stderr.strip():
        log(result.stderr.strip())
    log(f"TMP limpiada o no existente: {tmp_ref}")


def get_content_arrow_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("content_id", pa.string()),
            pa.field("created", pa.timestamp("us", tz="UTC")),
            pa.field("updated", pa.timestamp("us", tz="UTC")),
            pa.field("dependent", pa.bool_()),
            pa.field("description", pa.string()),
            pa.field("metadata", pa.string()),
            pa.field("is_from_template", pa.bool_()),
            pa.field("order", pa.int64()),
            pa.field("published_date", pa.timestamp("us", tz="UTC")),
            pa.field("section_id", pa.string()),
            pa.field("status", pa.string()),
            pa.field("template_content_id", pa.string()),
            pa.field("template_id", pa.string()),
            pa.field("theme_id", pa.string()),
            pa.field("title", pa.string()),
            pa.field("type", pa.string()),
            pa.field("template_content_order", pa.int64()),
            pa.field("is_visible", pa.bool_()),
            pa.field("delivered_count", pa.int64()),
            pa.field("is_delivered", pa.bool_()),
            pa.field("is_visible_edited", pa.bool_()),
            pa.field("activity_id", pa.string()),
            pa.field("last_load_lima", pa.timestamp("us", tz="UTC")),
            pa.field("source_system", pa.string()),
            pa.field("extraction_period", pa.string()),
        ]
    )


def normalize_dataframe_types(df: pd.DataFrame) -> pd.DataFrame:
    for col in (
        "content_id",
        "description",
        "metadata",
        "section_id",
        "status",
        "template_content_id",
        "template_id",
        "theme_id",
        "title",
        "type",
        "activity_id",
        "source_system",
        "extraction_period",
    ):
        if col in df.columns:
            df[col] = df[col].astype("string")

    for col in ("order", "template_content_order", "delivered_count"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in (
        "dependent",
        "is_from_template",
        "is_visible",
        "is_delivered",
        "is_visible_edited",
    ):
        if col in df.columns:
            df[col] = df[col].astype("boolean")

    for col in ("created", "updated", "published_date", "last_load_lima"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            df[col] = df[col].astype("datetime64[us, UTC]")

    fixed_columns = [
        "content_id",
        "created",
        "updated",
        "dependent",
        "description",
        "metadata",
        "is_from_template",
        "order",
        "published_date",
        "section_id",
        "status",
        "template_content_id",
        "template_id",
        "theme_id",
        "title",
        "type",
        "template_content_order",
        "is_visible",
        "delivered_count",
        "is_delivered",
        "is_visible_edited",
        "activity_id",
        "last_load_lima",
        "source_system",
        "extraction_period",
    ]
    for col in fixed_columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df[fixed_columns]


def get_query(period: str) -> str:
    return f"""
        SELECT
            content_id,
            (created AT TIME ZONE 'America/Lima') AS created,
            (updated AT TIME ZONE 'America/Lima') AS updated,
            dependent,
            description,
            CAST(metadata AS TEXT) AS metadata,
            is_from_template,
            "order",
            (published_date AT TIME ZONE 'America/Lima') AS published_date,
            section_id,
            status,
            template_content_id,
            template_id,
            theme_id,
            title,
            type,
            template_content_order,
            is_visible,
            delivered_count,
            is_delivered,
            is_visible_edited,
            activity_id,
            now() AT TIME ZONE 'America/Lima' AS last_load_lima,
            'postgres_rds_pao' AS source_system,
            '{period}' AS extraction_period
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
        WHERE updated >= %s
          AND updated < %s
    """


def extract_incremental_to_parquet(
    start_date: datetime, end_date: datetime, period: str
) -> tuple[str | None, int]:
    file_name = f"{SOURCE_TABLE}_incr_{period}.parquet"
    local_file = os.path.join(OUTPUT_DIR, file_name)

    if os.path.exists(local_file):
        os.remove(local_file)

    log("=" * 90)
    log(f"INICIO INCREMENTAL {SOURCE_TABLE} periodo={period}")
    log(f"Rango Lima: {start_date} <= updated < {end_date}")

    total_rows = 0
    writer = None
    query = get_query(period)
    arrow_schema = get_content_arrow_schema()

    try:
        for sub_start, sub_end in subrange_dates(start_date, end_date, SUBRANGE_DAYS):
            log(f"Subrango: {sub_start} <= updated < {sub_end}")
            conn = None
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                for retry in range(3):
                    try:
                        for df in pd.read_sql_query(
                            query,
                            conn,
                            params=(sub_start, sub_end),
                            chunksize=CHUNK_SIZE,
                        ):
                            if df.empty:
                                continue
                            df = normalize_dataframe_types(df)
                            table = pa.Table.from_pandas(
                                df,
                                schema=arrow_schema,
                                preserve_index=False,
                                safe=False,
                            )
                            if writer is None:
                                writer = pq.ParquetWriter(
                                    local_file, arrow_schema, compression="snappy"
                                )
                            writer.write_table(table)
                            total_rows += len(df)
                            log(f"Filas acumuladas: {total_rows}")
                        break
                    except psycopg2.OperationalError as ssl_error:
                        log(f"Error SSL: {ssl_error}")
                        try:
                            conn.close()
                        except Exception:
                            pass
                        if retry < 2:
                            conn = psycopg2.connect(**DB_CONFIG)
                            continue
                        raise
            finally:
                if conn is not None:
                    conn.close()
    finally:
        if writer is not None:
            writer.close()

    if total_rows == 0:
        return None, 0
    return local_file, total_rows


def load_parquet_to_tmp(gcs_path: str) -> None:
    drop_tmp_table_if_exists()
    tmp_ref = f"{BQ_PROJECT}:{BQ_DATASET}.{BQ_TABLE_TMP}"
    run_cmd(
        [
            BQ_PATH,
            "--quiet",
            f"--location={BQ_LOCATION}",
            "load",
            "--replace",
            "--source_format=PARQUET",
            tmp_ref,
            gcs_path,
        ]
    )


def create_final_table_if_not_exists() -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_FINAL}`
    PARTITION BY DATE(created)
    CLUSTER BY section_id, theme_id, type, activity_id
    AS
    SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_TMP}` WHERE 1 = 0
    """
    bq_query(sql)


def merge_tmp_to_final() -> None:
    sql = f"""
    MERGE `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_FINAL}` T
    USING (
      SELECT content_id, created, updated, dependent, description, metadata,
        is_from_template, `order`, published_date, section_id, status,
        template_content_id, template_id, theme_id, title, type,
        template_content_order, is_visible, delivered_count, is_delivered,
        is_visible_edited, activity_id, last_load_lima, source_system, extraction_period
      FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_TMP}`
    ) S
    ON T.content_id = S.content_id
    WHEN MATCHED AND (
         IFNULL(T.updated, TIMESTAMP('1900-01-01')) <> IFNULL(S.updated, TIMESTAMP('1900-01-01'))
      OR IFNULL(T.published_date, TIMESTAMP('1900-01-01')) <> IFNULL(S.published_date, TIMESTAMP('1900-01-01'))
      OR IFNULL(T.dependent, FALSE) <> IFNULL(S.dependent, FALSE)
      OR IFNULL(T.description, '') <> IFNULL(S.description, '')
      OR IFNULL(T.metadata, '') <> IFNULL(S.metadata, '')
      OR IFNULL(T.is_from_template, FALSE) <> IFNULL(S.is_from_template, FALSE)
      OR IFNULL(T.`order`, -999999) <> IFNULL(S.`order`, -999999)
      OR IFNULL(T.section_id, '') <> IFNULL(S.section_id, '')
      OR IFNULL(T.status, '') <> IFNULL(S.status, '')
      OR IFNULL(T.template_content_id, '') <> IFNULL(S.template_content_id, '')
      OR IFNULL(T.template_id, '') <> IFNULL(S.template_id, '')
      OR IFNULL(T.theme_id, '') <> IFNULL(S.theme_id, '')
      OR IFNULL(T.title, '') <> IFNULL(S.title, '')
      OR IFNULL(T.type, '') <> IFNULL(S.type, '')
      OR IFNULL(T.template_content_order, -999999) <> IFNULL(S.template_content_order, -999999)
      OR IFNULL(T.is_visible, FALSE) <> IFNULL(S.is_visible, FALSE)
      OR IFNULL(T.delivered_count, -999999) <> IFNULL(S.delivered_count, -999999)
      OR IFNULL(T.is_delivered, FALSE) <> IFNULL(S.is_delivered, FALSE)
      OR IFNULL(T.is_visible_edited, FALSE) <> IFNULL(S.is_visible_edited, FALSE)
      OR IFNULL(T.activity_id, '') <> IFNULL(S.activity_id, '')
    )
    THEN UPDATE SET
      created = S.created, updated = S.updated, dependent = S.dependent,
      description = S.description, metadata = S.metadata,
      is_from_template = S.is_from_template, `order` = S.`order`,
      published_date = S.published_date, section_id = S.section_id, status = S.status,
      template_content_id = S.template_content_id, template_id = S.template_id,
      theme_id = S.theme_id, title = S.title, type = S.type,
      template_content_order = S.template_content_order, is_visible = S.is_visible,
      delivered_count = S.delivered_count, is_delivered = S.is_delivered,
      is_visible_edited = S.is_visible_edited, activity_id = S.activity_id,
      last_load_lima = S.last_load_lima, source_system = S.source_system,
      extraction_period = S.extraction_period
    WHEN NOT MATCHED THEN INSERT (
      content_id, created, updated, dependent, description, metadata,
      is_from_template, `order`, published_date, section_id, status,
      template_content_id, template_id, theme_id, title, type,
      template_content_order, is_visible, delivered_count, is_delivered,
      is_visible_edited, activity_id, last_load_lima, source_system, extraction_period
    )
    VALUES (
      S.content_id, S.created, S.updated, S.dependent, S.description, S.metadata,
      S.is_from_template, S.`order`, S.published_date, S.section_id, S.status,
      S.template_content_id, S.template_id, S.theme_id, S.title, S.type,
      S.template_content_order, S.is_visible, S.delivered_count, S.is_delivered,
      S.is_visible_edited, S.activity_id, S.last_load_lima, S.source_system,
      S.extraction_period
    )
    """
    bq_query(sql)


def process_incremental() -> None:
    start_date, end_date, period = resolve_incremental_window()
    run_ts = end_date
    pending_gcs_path = ""

    try:
        local_file, rows = extract_incremental_to_parquet(start_date, end_date, period)
        if not local_file:
            log("Sin registros nuevos/actualizados.")
            return

        file_name = os.path.basename(local_file)
        pending_gcs_path = build_gcs_file_path(
            GCS_FOLDER_PENDING, run_ts.year, run_ts.month, file_name
        )
        processed_gcs_path = build_gcs_file_path(
            GCS_FOLDER_PROCESSED, run_ts.year, run_ts.month, file_name
        )

        if gcs_file_exists(processed_gcs_path):
            log(f"Ya procesado: {processed_gcs_path}")
            return

        upload_to_gcs(local_file, pending_gcs_path)
        load_parquet_to_tmp(pending_gcs_path)
        create_final_table_if_not_exists()
        merge_tmp_to_final()
        log(f"Carga OK: {rows} filas, periodo {period}")
        move_gcs_file(pending_gcs_path, processed_gcs_path)

    except Exception:
        log(traceback.format_exc())
        if pending_gcs_path:
            try:
                file_name = os.path.basename(pending_gcs_path)
                error_gcs_path = build_gcs_file_path(
                    GCS_FOLDER_ERROR, run_ts.year, run_ts.month, file_name
                )
                if gcs_file_exists(pending_gcs_path):
                    move_gcs_file(pending_gcs_path, error_gcs_path)
            except Exception as move_error:
                log(f"No se pudo mover a error: {move_error}")
        raise


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    validate_config()
    log("INICIO CARGA INCREMENTAL content")
    process_incremental()
    log("FINALIZADO.")


if __name__ == "__main__":
    main()
