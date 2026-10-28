"""Loads Uber data exports into the raw tb_uber_*_rows tables (finance-backend
plans/uber-ingestion.md §3.3).

Runs on object finalize in the Uber bucket; only zips under inbox/ are read, so the move to
archive/ that ends every run does not start another one. For a zip:

  1. The trips, Eats orders and Eats order details files are read from it as received: every
     CSV row becomes one JSON record, every column kept, empty cells as "". Other files are
     never read. A zip with no trips or no Eats orders file fails, naming the files it holds.
  2. A zip whose MD5 already has a successful run in raw.tb_ingestion_log is only archived.
  3. Otherwise each file kind is loaded into a temp table and the three raw tables are written
     in one transaction, so a failed load writes nothing.
  4. The zip moves to archive/<export date>/, the date of the latest trip or order in it; a
     different zip already at that name is never overwritten.
  5. One row in raw.tb_ingestion_log. After a load that wrote rows, a Dataform run of the uber
     tag starts; a run that cannot start is logged and does not change the load's outcome.

A failure is logged and the zip stays in inbox/; the function then returns normally, because
a raised error would make Eventarc redeliver the event for a day. Moving the zip to a new
name inside inbox/ runs it again.
"""
import base64
import csv
import fnmatch
import hashlib
import io
import json
import os
import zipfile
from datetime import datetime, timezone

import functions_framework
import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery, storage

PROJECT_ID = os.environ.get("PROJECT_ID", "")
DATASET = os.environ.get("DATASET", "raw")
LOG_TABLE = "tb_ingestion_log"
TABLES = {"trips": "tb_uber_trip_rows",
          "eats_orders": "tb_uber_eats_order_rows",
          "eats_order_details": "tb_uber_eats_order_detail_rows"}
KINDS = {"trips": ["*/Rider/trips_data*.csv", "*/Rider/rider_lifetime_trips*.csv"],
         "eats_orders": ["*/Eats/user_orders*.csv"],
         "eats_order_details": ["*/Eats/eats_order_details*.csv"]}
REQUIRED = ["trips", "eats_orders"]
TIMES = {"trips": ["request_timestamp_utc", "request_time"], "eats_orders": ["Request_Time_Local"]}

DATAFORM_API = "https://dataform.googleapis.com/v1"
DATAFORM_LOCATION = "us-central1"
DATAFORM_REPOSITORY = "dataform-repo"
DATAFORM_RELEASE = os.environ.get("DATAFORM_RELEASE", "release-02")


class MissingFiles(Exception):
    pass


def read_export(content):
    """kind -> [(file_path, row_number, record)] for the three kinds of KINDS."""
    rows = {kind: [] for kind in KINDS}
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        names = [n for n in z.namelist() if not n.endswith("/")]
        for name in names:
            if name.startswith("__MACOSX/"):
                continue
            kind = next((k for k, patterns in KINDS.items()
                         if any(fnmatch.fnmatch(name, p) for p in patterns)), None)
            if kind is None:
                continue
            reader = csv.DictReader(io.StringIO(z.read(name).decode("utf-8-sig"), newline=""))
            rows[kind] += [(name, i, record) for i, record in enumerate(reader, start=1)]
    missing = [kind for kind in REQUIRED if not rows[kind]]
    if missing:
        raise MissingFiles(f"no {' or '.join(missing)} file in the export; it holds: {', '.join(names)}")
    return rows


def export_as_of(rows):
    """The latest trip or Eats order request time in the export."""
    times = [record[field] for kind, fields in TIMES.items() for _, _, record in rows[kind]
             for field in fields if record.get(field)]
    return max(datetime.fromisoformat(t).astimezone(timezone.utc) for t in times)


def archive_name(export_date, zip_name, export_id, taken):
    """archive/<date>/<zip name>, with the export id's first 8 characters added when a
    different zip (taken = its MD5) already has that name."""
    if taken and taken != export_id:
        stem, dot, extension = zip_name.rpartition(".")
        zip_name = f"{stem} ({export_id[:8]}){dot}{extension}" if dot else f"{zip_name} ({export_id[:8]})"
    return f"archive/{export_date.isoformat()}/{zip_name}"


@functions_framework.cloud_event
def ingest_uber(cloud_event):
    bucket_name, name = cloud_event.data["bucket"], cloud_event.data["name"]
    if not (name.startswith("inbox/") and name.endswith(".zip")):
        return
    bq = bigquery.Client(project=PROJECT_ID)
    bucket = storage.Client(project=PROJECT_ID).bucket(bucket_name)
    blob = bucket.blob(name)
    content = blob.download_as_bytes()
    export_id = hashlib.md5(content).hexdigest()
    zip_name = name.split("/", 1)[1]
    log = {"source_id": "uber", "bucket": bucket_name, "object_name": name, "file_hash": export_id,
           "rows_in_file": None, "rows_loaded": 0, "load_mode": "append", "error_message": None}
    try:
        rows = read_export(content)
        as_of = export_as_of(rows)
        log["rows_in_file"] = sum(len(r) for r in rows.values())
        if already_loaded(bq, export_id):
            archive(bucket, blob, as_of, zip_name, export_id)
            write_log(bq, log, "already_loaded")
            return
        load(bq, rows, export_id, zip_name, as_of)
        log["rows_loaded"] = log["rows_in_file"]
        archive(bucket, blob, as_of, zip_name, export_id)
    except Exception as e:  # noqa: BLE001 - logged, and the zip stays in inbox/ (module docstring)
        write_log(bq, {**log, "error_message": f"{type(e).__name__}: {e}"[:1000]}, "error")
        return
    write_log(bq, log, "success")
    start_uber_run(bq, log)


def already_loaded(bq, export_id):
    [row] = bq.query(f"""
        SELECT COUNT(*) AS loaded FROM `{PROJECT_ID}.{DATASET}.{LOG_TABLE}`
        WHERE source_id = 'uber' AND file_hash = @export_id AND status = 'success'
    """, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("export_id", "STRING", export_id)])).result()
    return row["loaded"] > 0


def load(bq, rows, export_id, zip_name, as_of):
    """Each kind into a temp table, then all three raw tables in one transaction."""
    temps = {kind: f"{PROJECT_ID}.{DATASET}._tmp_uber_{kind}_{export_id}" for kind in TABLES if rows[kind]}
    schema = [bigquery.SchemaField("file_path", "STRING"), bigquery.SchemaField("row_number", "INT64"),
              bigquery.SchemaField("record", "STRING")]
    try:
        for kind, temp in temps.items():
            bq.load_table_from_json(
                [{"file_path": f, "row_number": n, "record": json.dumps(r, ensure_ascii=False)}
                 for f, n, r in rows[kind]],
                temp, job_config=bigquery.LoadJobConfig(
                    schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)).result()
        inserts = "\n".join(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.{TABLES[kind]}`
              (export_id, export_name, export_as_of, file_path, row_number, record, loaded_at)
            SELECT @export_id, @export_name, @export_as_of, file_path, row_number, PARSE_JSON(record),
                   CURRENT_TIMESTAMP()
            FROM `{temp}`;""" for kind, temp in temps.items())
        bq.query(f"BEGIN TRANSACTION;{inserts}\nCOMMIT TRANSACTION;", job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("export_id", "STRING", export_id),
                              bigquery.ScalarQueryParameter("export_name", "STRING", zip_name),
                              bigquery.ScalarQueryParameter("export_as_of", "TIMESTAMP", as_of)])).result()
    finally:
        for temp in temps.values():
            bq.delete_table(temp, not_found_ok=True)


def archive(bucket, blob, as_of, zip_name, export_id):
    target = archive_name(as_of.date(), zip_name, export_id, None)
    existing = bucket.get_blob(target)
    if existing is not None:
        target = archive_name(as_of.date(), zip_name, export_id,
                              base64.b64decode(existing.md5_hash).hex())
    if bucket.get_blob(target) is None:
        bucket.copy_blob(blob, bucket, target)
    blob.delete()


def write_log(bq, log, status):
    errors = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET}.{LOG_TABLE}", [
        {**log, "status": status, "processed_at": datetime.now(timezone.utc).isoformat()}])
    if errors:
        print(f"tb_ingestion_log insert failed: {errors}")


def start_uber_run(bq, log):
    """Start a Dataform run of the uber tag from the release config's latest compilation, so the
    Uber models take in what was just loaded. Never raises: the load is done either way, and
    the daily run rebuilds the models too."""
    repository = f"projects/{PROJECT_ID}/locations/{DATAFORM_LOCATION}/repositories/{DATAFORM_REPOSITORY}"
    try:
        session = dataform_session()
        release = session.get(f"{DATAFORM_API}/{repository}/releaseConfigs/{DATAFORM_RELEASE}", timeout=30)
        release.raise_for_status()
        compilation = release.json().get("releaseCompilationResult")
        if not compilation:
            raise RuntimeError(f"{DATAFORM_RELEASE} has no compilation result")
        run = session.post(f"{DATAFORM_API}/{repository}/workflowInvocations", timeout=30, json={
            "compilationResult": compilation, "invocationConfig": {"includedTags": ["uber"]}})
        run.raise_for_status()
        write_log(bq, {**log, "error_message": run.json().get("name")}, "dataform_run_started")
    except Exception as e:  # noqa: BLE001 - see the docstring
        write_log(bq, {**log, "error_message": f"{type(e).__name__}: {e}"[:1000]}, "dataform_run_error")


def dataform_session():
    """An HTTP session authorised as the function's runtime account."""
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(credentials)
