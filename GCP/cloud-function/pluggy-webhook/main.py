"""Keeps raw.tb_pluggy_item_registry from Pluggy's item webhooks (plan pluggy-transactions
§3.3, D-9).

Pluggy's GET /v2/items is not available to Meu Pluggy accounts, so the connections cannot
be listed; Pluggy tells this endpoint about them instead. Registered with POST /webhooks
for the item events, with a header Pluggy sends on every call:

  item/created  -> the item is read back from Pluggy (GET /items/{id}, never trusted from the
                   payload) and MERGEd into the registry; the daily load reads it from the
                   next run.
  item/deleted  -> deleted_at is set; the row and the raw history stay.
  any other     -> last_event is noted on a registered item; nothing else.

A call without the shared secret header is refused with 403, which Pluggy never retries.
Anything that goes wrong after the header check is logged and answered 200: Pluggy retries
a failed delivery three times and nothing here needs it, since every write is idempotent.
The answer must come within 10 s.
"""
import json
import os
from datetime import datetime, timezone

from google.cloud import bigquery, secretmanager

import pluggy_api

PROJECT_ID = os.environ.get("PROJECT_ID", "")
DATASET = os.environ.get("DATASET", "raw")
SECRET_HEADER = "X-Finance-Webhook-Key"
EXPECTED_SECRET = os.environ.get("PLUGGY_WEBHOOK_SECRET", "")
REGISTRY = "tb_pluggy_item_registry"
LOG_TABLE = "tb_pluggy_ingestion_log"


def main(request):
    if not PROJECT_ID or not EXPECTED_SECRET:
        raise RuntimeError("PROJECT_ID and PLUGGY_WEBHOOK_SECRET must be set")
    if request.headers.get(SECRET_HEADER, "") != EXPECTED_SECRET:
        log("WARNING", "Webhook call refused: wrong or missing secret header")
        return ("Forbidden", 403)

    body = request.get_json(silent=True) or {}
    event, item_id = body.get("event"), body.get("itemId")
    log("INFO", "Webhook event", event=event, item_id=item_id, event_id=body.get("eventId"),
        triggered_by=body.get("triggeredBy"))
    if not event or not item_id:
        return ("Ignored", 200)

    bq = bigquery.Client(project=PROJECT_ID)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        if event == "item/created":
            item = pluggy_client().item(item_id)
            upsert(bq, item_id, now, event, connector=(item.get("connector") or {}).get("name"),
                   payload=json.dumps(item))
            log_event(bq, "item_registered", "success",
                      {"item_id": item_id, "connector": (item.get("connector") or {}).get("name")})
        elif event == "item/deleted":
            upsert(bq, item_id, now, event, deleted_at=now)
            log_event(bq, "item_deleted", "success", {"item_id": item_id})
        else:
            note_event(bq, item_id, now, event)
    except Exception as e:
        log_event(bq, "webhook_error", "error",
                  {"item_id": item_id, "event": event, "error": str(e), "error_type": type(e).__name__})
    return ("OK", 200)


def pluggy_client():
    secrets = secretmanager.SecretManagerServiceClient()
    client_id, client_secret = (
        secrets.access_secret_version(
            name=f"projects/{PROJECT_ID}/secrets/{name}/versions/latest"
        ).payload.data.decode("UTF-8").strip()
        for name in ("Pluggy_client_id", "Pluggy_client_secret")
    )
    return pluggy_api.Client(client_id, client_secret)


def upsert(bq, item_id, now, event, connector=None, payload=None, deleted_at=None):
    """One MERGE on item_id, parameterised: a registered item keeps its registered_at and
    source; an item/created for a known item clears deleted_at (the connection is back)."""
    sql = f"""
        MERGE `{PROJECT_ID}.{DATASET}.{REGISTRY}` target
        USING (SELECT @item_id AS item_id) source
        ON target.item_id = source.item_id
        WHEN MATCHED THEN UPDATE SET
          connector_name = COALESCE(@connector, target.connector_name),
          deleted_at = @deleted_at,
          last_event = @event,
          last_event_at = @now,
          payload = COALESCE(PARSE_JSON(@payload), target.payload)
        WHEN NOT MATCHED THEN INSERT
          (item_id, connector_name, source, registered_at, deleted_at, last_event, last_event_at, payload)
        VALUES (@item_id, @connector, 'webhook', @now, @deleted_at, @event, @now, PARSE_JSON(@payload))
    """
    run(bq, sql, item_id=item_id, connector=connector, deleted_at=deleted_at, event=event, now=now,
        payload=payload)


def note_event(bq, item_id, now, event):
    sql = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.{REGISTRY}`
        SET last_event = @event, last_event_at = @now
        WHERE item_id = @item_id
    """
    run(bq, sql, item_id=item_id, event=event, now=now)


def run(bq, sql, **params):
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter(name, "STRING", value) for name, value in params.items()]))
    job.result()
    return job.num_dml_affected_rows or 0


def log(severity, message, **fields):
    print(json.dumps({"severity": severity, "message": message, **fields}, default=str), flush=True)


def log_event(bq, event_type, status, details):
    """Append to the ingestion log table, in the tb_nordigen_ingestion_log layout. Never raises."""
    log("ERROR" if status == "error" else "INFO", event_type, **details)
    row = {
        "ingestion_dt": datetime.now(timezone.utc).isoformat(),
        "type": "Error" if status == "error" else "Info",
        "error_code": event_type,
        "message": str(details.get("error", status))[:1000],
        "description": json.dumps(details, default=str)[:5000],
        "end_point": details.get("item_id"),
        "gateway": "pluggy-webhook",
    }
    try:
        bq.load_table_from_json(
            [row], f"{PROJECT_ID}.{DATASET}.{LOG_TABLE}",
            job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND),
        ).result()
    except Exception as e:
        log("ERROR", "Could not write to the ingestion log table", error=str(e), event_type=event_type)
