"""Loads the Nordigen / GoCardless bank accounts into the raw tb_nordigen_* tables.

Runs once a day from Pub/Sub. For every active credential in trusted.tb_sheet_nordigen_account
it lists the accounts of the linked requisitions and, per account, calls metadata, details,
balances and transactions once each. Every record goes into a temp table and is merged into
its raw table on that table's key:

  tb_nordigen_meta, tb_nordigen_details   one row per account, updated in place
  tb_nordigen_balances                    insert only, one version per run (dtinsert)
  tb_nordigen_transactions                insert only, payload keeps the whole record

A table that fails does not stop the others, but the run then raises, so the execution is
reported as failed. Every row of a run carries the same dtinsert.
"""
import json
import os
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery, secretmanager

import gocardless

PROJECT_ID = os.environ.get("PROJECT_ID", "")
DATASET = os.environ.get("DATASET", "raw")
LOG_TABLE = "tb_nordigen_ingestion_log"

# raw table: (endpoint, path to the records in the answer, columns, key, update a matched row)
TABLES = {
    "tb_nordigen_meta": (
        "metadata", (),
        ["id", "created", "last_accessed", "iban", "institution_id", "status", "owner_name", "bban"],
        ["account_id"], True,
    ),
    "tb_nordigen_details": (
        "details", ("account",),
        ["iban", "bban", "currency", "name", "cashAccountType", "bic"],
        ["account_id"], True,
    ),
    "tb_nordigen_balances": (
        "balances", ("balances",),
        ["balanceType", "balanceAmount_amount", "balanceAmount_currency", "lastChangeDateTime"],
        ["account_id", "balanceType", "dtinsert"], False,
    ),
    "tb_nordigen_transactions": (
        "transactions", ("transactions", "booked"),
        ["transactionId", "bookingDate", "valueDate", "remittanceInformationUnstructured",
         "internalTransactionId", "transactionAmount_amount", "transactionAmount_currency", "payload"],
        ["account_id", "internalTransactionId"], False,
    ),
}


def main(event, context):
    if not PROJECT_ID:
        raise RuntimeError("PROJECT_ID is not set")

    bq = bigquery.Client(project=PROJECT_ID)
    dtinsert = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    failures = []
    rows_written = {table: 0 for table in TABLES}
    accounts = 0

    try:
        credentials = [dict(row) for row in bq.query(f"""
            SELECT b.id AS client_id, a.secret_ID AS secret_id
            FROM `{PROJECT_ID}.trusted.tb_sheet_nordigen_account` a
            INNER JOIN `{PROJECT_ID}.refined.dim_client` b ON a.person_ID = b.cpf
            WHERE a.active_FLG = 1
        """).result()]
        if not credentials:
            raise RuntimeError("No active Nordigen credentials in trusted.tb_sheet_nordigen_account")

        for credential in credentials:
            client_id = credential["client_id"]
            try:
                secret_key = secretmanager.SecretManagerServiceClient().access_secret_version(
                    name=f"projects/{PROJECT_ID}/secrets/Nordigen_{credential['secret_id']}/versions/latest"
                ).payload.data.decode("UTF-8")
                client = gocardless.Client(credential["secret_id"], secret_key)
                account_ids = client.linked_account_ids()
            except Exception as e:
                failures.append(f"client {client_id}: {e}")
                log_event(bq, "client_initialization_error", "error",
                          {"client_id": client_id, "error": str(e), "error_type": type(e).__name__})
                continue

            for account_id in account_ids:
                accounts += 1
                for table in TABLES:
                    try:
                        rows_written[table] += load(bq, client, table, account_id, client_id, dtinsert)
                    except Exception as e:
                        failures.append(f"{table} for account {account_id}: {e}")
                        log_event(bq, "table_processing_error", "error",
                                  {"table_id": table, "client_id": client_id, "account_id": account_id,
                                   "error": str(e), "error_type": type(e).__name__})

        log_event(bq, "ingestion_complete", "error" if failures else "success",
                  {"dtinsert": dtinsert, "accounts": accounts, "rows_written": rows_written,
                   "failures": failures})
    except Exception as e:
        log_event(bq, "fatal_error", "error", {"error": str(e), "error_type": type(e).__name__})
        raise

    if failures:
        raise RuntimeError("Nordigen ingestion finished with errors: " + "; ".join(failures))
    return f"Nordigen ingestion complete: {accounts} accounts, rows written {rows_written}"


def load(bq, client, table, account_id, client_id, dtinsert):
    """Fetch one endpoint for one account and merge it into its raw table. Returns rows written."""
    endpoint, path, columns, keys, update = TABLES[table]

    answer = client.fetch(account_id, endpoint)
    records = answer
    for step in path:
        if not isinstance(records, dict) or step not in records:
            raise ValueError(f"Unexpected {endpoint} answer: {str(answer)[:300]}")
        records = records[step]
    if isinstance(records, dict):
        records = [records]

    rows = {}
    for record in records:
        flat = flatten(record)
        # The API calls the bank's reference entryReference; the raw table and the
        # Dataform models call it transactionId
        if table == "tb_nordigen_transactions" and flat.get("transactionId") is None:
            flat["transactionId"] = flat.get("entryReference")
        row = {column: flat.get(column) for column in columns}
        row.update(account_id=account_id, client_id=client_id, dtinsert=dtinsert)
        if "payload" in columns:
            row["payload"] = json.dumps(record)
        if any(row[key] is None for key in keys):
            raise ValueError(f"A {endpoint} record has no value for {keys}: {str(record)[:300]}")
        rows.setdefault(tuple(row[key] for key in keys), row)

    if not rows:
        log("INFO", f"No records for {table}", account_id=account_id)
        return 0

    all_columns = columns + ["account_id", "client_id", "dtinsert"]
    target = f"{PROJECT_ID}.{DATASET}.{table}"
    temp = f"{target}_tmp_{uuid.uuid4().hex[:8]}"
    source = {c: f"PARSE_JSON(source.`{c}`)" if c == "payload" else f"source.`{c}`" for c in all_columns}
    matched = ""
    if update:
        matched = "WHEN MATCHED THEN UPDATE SET " + ", ".join(
            f"target.`{c}` = {source[c]}" for c in all_columns if c not in keys)
    merge = f"""
        MERGE `{target}` target
        USING `{temp}` source
        ON {" AND ".join(f"target.`{k}` = source.`{k}`" for k in keys)}
        {matched}
        WHEN NOT MATCHED THEN INSERT ({", ".join(f"`{c}`" for c in all_columns)})
        VALUES ({", ".join(source[c] for c in all_columns)})
    """

    try:
        bq.load_table_from_json(
            list(rows.values()), temp,
            job_config=bigquery.LoadJobConfig(
                schema=[bigquery.SchemaField(c, "STRING") for c in all_columns],
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        ).result()
        job = bq.query(merge)
        job.result()
    finally:
        bq.delete_table(temp, not_found_ok=True)

    log("INFO", f"Merged {table}", account_id=account_id, rows=len(rows), rows_written=job.num_dml_affected_rows)
    return job.num_dml_affected_rows or 0


def flatten(record, prefix=""):
    """{"a": {"b": 1}} -> {"a_b": "1"}. Values become strings, as every raw column is STRING."""
    flat = {}
    for key, value in record.items():
        if isinstance(value, dict):
            flat.update(flatten(value, f"{prefix}{key}_"))
        elif value is not None:
            flat[f"{prefix}{key}"] = value if isinstance(value, str) else json.dumps(value)
    return flat


def log(severity, message, **fields):
    """One JSON line per entry, which Cloud Logging reads as a structured entry with this severity."""
    print(json.dumps({"severity": severity, "message": message, **fields}, default=str), flush=True)


def log_event(bq, event_type, status, details):
    """Append to the ingestion log table, in its existing layout. Never raises."""
    log("ERROR" if status == "error" else "INFO", event_type, **details)
    row = {
        "ingestion_dt": datetime.now(timezone.utc).isoformat(),
        "type": "Error" if status == "error" else "Info",
        "error_code": event_type,
        "message": str(details.get("error", status))[:1000],
        "description": json.dumps(details, default=str)[:5000],
        "end_point": details.get("table_id"),
    }
    try:
        bq.load_table_from_json(
            [row], f"{PROJECT_ID}.{DATASET}.{LOG_TABLE}",
            job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND),
        ).result()
    except Exception as e:
        log("ERROR", "Could not write to the ingestion log table", error=str(e), event_type=event_type)
