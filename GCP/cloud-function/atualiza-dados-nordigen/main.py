"""Loads the Nordigen / GoCardless bank accounts into the raw tb_nordigen_* tables.

Runs from Pub/Sub, on two kinds of message (finance-backend plans/bank-refresh.md §3.2):

  {"account_ids": [...], "request_id": ...}   the app's refresh: only those accounts, and only
                                              balances and transactions (D-2). An account no
                                              linked requisition holds is skipped and logged.
  anything else                               the 08:00 scheduler's message: every account.

For every active credential in trusted.tb_sheet_nordigen_account it lists the accounts of the
linked requisitions and, per account, calls metadata, details, balances and transactions once
each (on a full run). Every record goes into a temp table and is merged into its raw table on
that table's key:

  tb_nordigen_meta, tb_nordigen_details   one row per account, updated in place
  tb_nordigen_balances                    insert only, one version per run (dtinsert)
  tb_nordigen_transactions                insert only, payload keeps the whole record

Every details, balances and transactions answer reports the account's allowance for that scope
(gocardless.allowance); one row per call goes into tb_nordigen_rate_limits, insert only (D-1).
A 429 means the account has no call left in its window: it is recorded there at 0 and logged as
rate_limited, and is not a failure of the run (D-10) - the account was read by a refresh earlier
in the same window.

A table that fails does not stop the others, but the run then raises, so the execution is
reported as failed. Every row of a run carries the same dtinsert. The run's log rows carry the
refresh's request_id, which is how the app tells that its refresh ended.

A run that wrote rows - even beside a table that failed - then starts a Dataform run of the bank
tag from release-02's compilation, so the tables the Bank tab reads take them in within minutes
(finance-backend plans/bank-reads-from-tables.md §3.2). It is logged as dataform_run_started, or
dataform_run_error when it cannot start; neither changes the load's outcome.
"""
import base64
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import google.auth
import requests
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery, secretmanager

import gocardless

PROJECT_ID = os.environ.get("PROJECT_ID", "")
DATASET = os.environ.get("DATASET", "raw")
LOG_TABLE = "tb_nordigen_ingestion_log"
LIMITS_TABLE = "tb_nordigen_rate_limits"
# As the table's DDL (finance-backend bigquery/ddl/nordigen/), modes included: a load whose schema
# says NULLABLE for a REQUIRED column is refused.
LIMITS_SCHEMA = [
    bigquery.SchemaField(name, kind, mode="REQUIRED" if required else "NULLABLE")
    for name, kind, required in [
        ("account_id", "STRING", True), ("client_id", "STRING", True), ("scope", "STRING", True),
        ("limit_count", "INT64", False), ("remaining", "INT64", False), ("reset_at", "TIMESTAMP", False),
        ("status", "STRING", True), ("observed_at", "TIMESTAMP", True), ("request_id", "STRING", False),
        ("dtinsert", "STRING", True),
    ]
]
# The endpoints GoCardless counts per account (metadata is not one), and what a refresh calls (D-2).
SCOPES = ("details", "balances", "transactions")
REFRESH_TABLES = ["tb_nordigen_balances", "tb_nordigen_transactions"]

# The Dataform run after a load (finance-backend plans/bank-reads-from-tables.md §3.2).
DATAFORM_API = "https://dataform.googleapis.com/v1"
DATAFORM_LOCATION = "us-central1"
DATAFORM_REPOSITORY = "dataform-repo"
DATAFORM_RELEASE = os.environ.get("DATAFORM_RELEASE", "release-02")

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
    allowances = []
    accounts = 0

    # The app's refresh names accounts; anything else (the scheduler's message) means every account.
    try:
        body = json.loads(base64.b64decode((event or {}).get("data") or ""))
    except ValueError:
        body = None
    body = body if isinstance(body, dict) else {}
    refresh = "account_ids" in body
    requested = body.get("account_ids") if isinstance(body.get("account_ids"), list) else []
    requested = list(dict.fromkeys(str(a) for a in requested))
    request_id = body.get("request_id") if refresh else None
    tables = REFRESH_TABLES if refresh else list(TABLES)
    run = {"dtinsert": dtinsert, "request_id": request_id, "requested": requested if refresh else None}

    try:
        credentials = [dict(row) for row in bq.query(f"""
            SELECT b.id AS client_id, a.secret_ID AS secret_id
            FROM `{PROJECT_ID}.trusted.tb_sheet_nordigen_account` a
            INNER JOIN `{PROJECT_ID}.refined.dim_client` b ON a.person_ID = b.cpf
            WHERE a.active_FLG = 1
        """).result()]
        if not credentials:
            raise RuntimeError("No active Nordigen credentials in trusted.tb_sheet_nordigen_account")

        linked = set()
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
                          {"client_id": client_id, "request_id": request_id,
                           "error": str(e), "error_type": type(e).__name__})
                continue
            linked.update(account_ids)
            if refresh:
                account_ids = [a for a in requested if a in account_ids]

            for account_id in account_ids:
                accounts += 1
                for table in tables:
                    endpoint = TABLES[table][0]
                    try:
                        answer, limits = client.fetch(account_id, endpoint)
                        if endpoint in SCOPES:
                            allowances.append(allowance_row(account_id, client_id, endpoint, limits, "ok", run))
                        rows_written[table] += load(bq, table, answer, account_id, client_id, dtinsert)
                    except Exception as e:
                        details = {"table_id": table, "client_id": client_id, "account_id": account_id,
                                   "request_id": request_id, "error": str(e), "error_type": type(e).__name__}
                        if isinstance(e, requests.HTTPError) and e.response is not None \
                                and e.response.status_code == 429:
                            if endpoint in SCOPES:
                                allowances.append(allowance_row(
                                    account_id, client_id, endpoint, gocardless.allowance(e.response.headers),
                                    "rate_limited", run))
                            log_event(bq, "rate_limited", "error", details)
                            continue
                        failures.append(f"{table} for account {account_id}: {e}")
                        log_event(bq, "table_processing_error", "error", details)

        skipped = [a for a in requested if a not in linked]
        for account_id in skipped:
            log_event(bq, "refresh_account_unknown", "error", {
                "account_id": account_id, "request_id": request_id,
                "error": "no linked requisition of an active credential holds this account: not read"})

        if allowances:
            try:
                bq.load_table_from_json(
                    allowances, f"{PROJECT_ID}.{DATASET}.{LIMITS_TABLE}",
                    job_config=bigquery.LoadJobConfig(
                        schema=LIMITS_SCHEMA, write_disposition=bigquery.WriteDisposition.WRITE_APPEND),
                ).result()
            except Exception as e:
                failures.append(f"{LIMITS_TABLE}: {e}")
                log_event(bq, "table_processing_error", "error", {
                    "table_id": LIMITS_TABLE, "request_id": request_id,
                    "error": str(e), "error_type": type(e).__name__})

        log_event(bq, "ingestion_complete", "error" if failures else "success",
                  {**run, "accounts": accounts, "skipped": skipped, "rows_written": rows_written,
                   "failures": failures})
        # Whatever was written reaches the Bank tab, even when another table failed.
        if sum(rows_written.values()):
            start_bank_run(bq, {"dtinsert": dtinsert, "request_id": request_id})
    except Exception as e:
        log_event(bq, "fatal_error", "error", {**run, "error": str(e), "error_type": type(e).__name__})
        raise

    if failures:
        raise RuntimeError("Nordigen ingestion finished with errors: " + "; ".join(failures))
    return f"Nordigen ingestion complete: {accounts} accounts, rows written {rows_written}"


def allowance_row(account_id, client_id, scope, limits, status, run):
    """A tb_nordigen_rate_limits row for one answered call. On a 429 nothing is left, and the reset
    is known only if the answer said so (the account success headers come with successes)."""
    observed = datetime.now(timezone.utc)
    return {
        "account_id": account_id, "client_id": client_id, "scope": scope,
        "limit_count": limits.get("limit"),
        "remaining": 0 if status == "rate_limited" else limits.get("remaining"),
        "reset_at": (observed + timedelta(seconds=limits["reset_seconds"])).isoformat()
        if "reset_seconds" in limits else None,
        "status": status, "observed_at": observed.isoformat(),
        "request_id": run["request_id"], "dtinsert": run["dtinsert"],
    }


def start_bank_run(bq, details):
    """Start a Dataform run of the bank tag, so the tables the Bank tab reads take in what was just
    loaded (finance-backend plans/bank-reads-from-tables.md D-1): from the release config's latest
    compilation (D-6), whether or not another run is going (D-5). Never raises: the load is done
    either way, and the daily run rebuilds the tables too."""
    repository = f"projects/{PROJECT_ID}/locations/{DATAFORM_LOCATION}/repositories/{DATAFORM_REPOSITORY}"
    try:
        session = dataform_session()
        release = session.get(f"{DATAFORM_API}/{repository}/releaseConfigs/{DATAFORM_RELEASE}", timeout=30)
        release.raise_for_status()
        compilation = release.json().get("releaseCompilationResult")
        if not compilation:
            raise RuntimeError(f"{DATAFORM_RELEASE} has no compilation result")
        run = session.post(f"{DATAFORM_API}/{repository}/workflowInvocations", timeout=30, json={
            "compilationResult": compilation, "invocationConfig": {"includedTags": ["bank"]}})
        run.raise_for_status()
        log_event(bq, "dataform_run_started", "success",
                  {**details, "invocation": run.json().get("name"), "compilation": compilation})
    except Exception as e:
        log_event(bq, "dataform_run_error", "error", {**details, "error": str(e), "error_type": type(e).__name__})


def dataform_session():
    """An HTTP session authorised as the function's runtime account."""
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(credentials)


def load(bq, table, answer, account_id, client_id, dtinsert):
    """Merge one endpoint's answer for one account into its raw table. Returns rows written."""
    endpoint, path, columns, keys, update = TABLES[table]

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
