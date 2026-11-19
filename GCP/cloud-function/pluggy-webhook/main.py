"""Keeps raw.tb_pluggy_item_registry from Pluggy's item webhooks (plan pluggy-transactions
§3.3, D-9) and asks atualiza-dados-pluggy to read an item as soon as Pluggy has synced it
(§3.4, D-11).

Pluggy's GET /v2/items is not available to Meu Pluggy accounts, so the connections cannot
be listed; Pluggy tells this endpoint about them instead. Registered with POST /webhooks
for the item events, with a header Pluggy sends on every call:

  item/created  -> the item is read back from Pluggy (GET /items/{id}, never trusted from the
                   payload) and MERGEd into the registry; the daily load reads it from the
                   next run.
  item/updated  -> last_event is noted on a registered item, which is then read back.
  item/deleted  -> deleted_at is set; the row and the raw history stay.
  any other     -> last_event is noted on a registered item; nothing else.

After item/created or item/updated, when the item read back has status UPDATED and
executionStatus SUCCESS, {"item_ids": [id], "event_id": <Pluggy's eventId>} is published to
LOAD_TOPIC, the topic that triggers atualiza-dados-pluggy, which then reads that item only.
An item still syncing, or whose sync failed, is left to its next item/updated or to the
08:15 run. An item/updated for an item the registry does not hold reads and asks nothing.

An item belongs to the integration it was connected through (finance-backend
plans/bank-integrations.md D-3): a row of raw.app_bank_integrations holding the names of that
Pluggy application's secrets. A new item says nothing of its application, so item/created
reads it with each integration in turn - another application's item answers 404 - and the
registry row takes the one that knows it. Later events read it with that integration alone.
Integrations with no secrets (simulated), switched off or deleted are never tried.

A call without the shared secret header is refused with 403, which Pluggy never retries.
Anything that goes wrong after the header check is logged and answered 200: Pluggy retries
a failed delivery three times and nothing here needs it, since every write is idempotent.
The answer must come within 10 s.
"""
import json
import os
from datetime import datetime, timezone

from google.cloud import bigquery, pubsub_v1, secretmanager

import pluggy_api

PROJECT_ID = os.environ.get("PROJECT_ID", "")
DATASET = os.environ.get("DATASET", "raw")
SECRET_HEADER = "X-Finance-Webhook-Key"
# Stripped: a secret created from a pipe carries a trailing newline, which the
# registration sent to Pluggy without, and one character off is a 403.
EXPECTED_SECRET = os.environ.get("PLUGGY_WEBHOOK_SECRET", "").strip()
LOAD_TOPIC = os.environ.get("LOAD_TOPIC", "")
# The whole answer must reach Pluggy within 10 s.
PUBLISH_TIMEOUT = 5
REGISTRY = "tb_pluggy_item_registry"
INTEGRATIONS = "app_bank_integrations"
LOG_TABLE = "tb_pluggy_ingestion_log"


def main(request):
    if not PROJECT_ID or not EXPECTED_SECRET or not LOAD_TOPIC:
        raise RuntimeError("PROJECT_ID, PLUGGY_WEBHOOK_SECRET and LOAD_TOPIC must be set")
    given = request.headers.get(SECRET_HEADER, "").strip()
    if given != EXPECTED_SECRET:
        log("WARNING", "Webhook call refused: wrong or missing secret header",
            header_present=bool(given), given_length=len(given), expected_length=len(EXPECTED_SECRET))
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
            integration_id, item = find_item(bq, item_id)
            upsert(bq, item_id, now, event, connector=(item.get("connector") or {}).get("name"),
                   payload=json.dumps(item), integration=integration_id)
            request_load(bq, item_id, item, body.get("eventId"))
            log_event(bq, "item_registered", "success",
                      {"item_id": item_id, "connector": (item.get("connector") or {}).get("name"),
                       "integration_id": integration_id})
        elif event == "item/deleted":
            upsert(bq, item_id, now, event, deleted_at=now)
            log_event(bq, "item_deleted", "success", {"item_id": item_id})
        elif event == "item/updated":
            if note_event(bq, item_id, now, event):
                its_own = integrations(bq, item_id)
                if its_own:
                    request_load(bq, item_id, pluggy_client(its_own[0]).item(item_id), body.get("eventId"))
                else:
                    log("INFO", "No load requested: the item's integration is not one to read", item_id=item_id)
        else:
            note_event(bq, item_id, now, event)
    except Exception as e:
        log_event(bq, "webhook_error", "error",
                  {"item_id": item_id, "event": event, "error": str(e), "error_type": type(e).__name__})
    return ("OK", 200)


def request_load(bq, item_id, item, event_id):
    """Publish the item to the load's topic when Pluggy has finished syncing it. A failed
    publish is logged; the 08:15 run still reads the item."""
    if item.get("status") != "UPDATED" or item.get("executionStatus") != "SUCCESS":
        log("INFO", "No load requested: the item has not synced cleanly", item_id=item_id,
            status=item.get("status"), execution_status=item.get("executionStatus"))
        return
    try:
        pubsub_v1.PublisherClient().publish(
            f"projects/{PROJECT_ID}/topics/{LOAD_TOPIC}",
            json.dumps({"item_ids": [item_id], "event_id": event_id}).encode(),
        ).result(timeout=PUBLISH_TIMEOUT)
    except Exception as e:
        log_event(bq, "publish_error", "error",
                  {"item_id": item_id, "event_id": event_id, "error": str(e), "error_type": type(e).__name__})
        return
    log_event(bq, "load_requested", "success", {"item_id": item_id, "event_id": event_id})


def integrations(bq, item_id=None):
    """The Pluggy integrations to read with, oldest first: the newest version of each, active,
    not deleted and holding both secret names. With item_id, only the one that item's registry
    row names - none when it names none, or one that is not to be read."""
    sql = f"""
        SELECT integration_id, client_id_secret, client_secret_secret
        FROM (
          SELECT * FROM `{PROJECT_ID}.{DATASET}.{INTEGRATIONS}`
          WHERE provider = 'pluggy'
          QUALIFY ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY version_ts DESC) = 1
        )
        WHERE is_active AND NOT is_deleted
          AND client_id_secret IS NOT NULL AND client_secret_secret IS NOT NULL
          AND (@item_id IS NULL OR integration_id = (
                 SELECT integration_id FROM `{PROJECT_ID}.{DATASET}.{REGISTRY}` WHERE item_id = @item_id))
        ORDER BY version_ts, integration_id
    """
    return [dict(row) for row in bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("item_id", "STRING", item_id)])).result()]


def find_item(bq, item_id):
    """(integration id, item) from the first integration whose application knows the item.
    One that cannot sign in, or that answers 404 for another application's item, gives way to
    the next; when none reads it, the last failure is raised and the item is not registered."""
    failure = RuntimeError("no Pluggy integration to read the item with")
    for integration in integrations(bq):
        try:
            return integration["integration_id"], pluggy_client(integration).item(item_id)
        except Exception as e:
            failure = e
    raise failure


def pluggy_client(integration):
    secrets = secretmanager.SecretManagerServiceClient()
    client_id, client_secret = (
        secrets.access_secret_version(
            name=f"projects/{PROJECT_ID}/secrets/{integration[name]}/versions/latest"
        ).payload.data.decode("UTF-8").strip()
        for name in ("client_id_secret", "client_secret_secret")
    )
    return pluggy_api.Client(client_id, client_secret)


def upsert(bq, item_id, now, event, connector=None, payload=None, deleted_at=None, integration=None):
    """One MERGE on item_id, parameterised: a registered item keeps its registered_at and
    source; an item/created for a known item clears deleted_at (the connection is back) and
    takes the integration that has just read it. An item/deleted keeps the integration."""
    sql = f"""
        MERGE `{PROJECT_ID}.{DATASET}.{REGISTRY}` target
        USING (SELECT @item_id AS item_id) source
        ON target.item_id = source.item_id
        WHEN MATCHED THEN UPDATE SET
          connector_name = COALESCE(@connector, target.connector_name),
          deleted_at = @deleted_at,
          last_event = @event,
          last_event_at = @now,
          payload = COALESCE(PARSE_JSON(@payload), target.payload),
          integration_id = COALESCE(@integration, target.integration_id)
        WHEN NOT MATCHED THEN INSERT
          (item_id, connector_name, source, registered_at, deleted_at, last_event, last_event_at, payload, integration_id)
        VALUES (@item_id, @connector, 'webhook', @now, @deleted_at, @event, @now, PARSE_JSON(@payload), @integration)
    """
    run(bq, sql, item_id=item_id, connector=connector, deleted_at=deleted_at, event=event, now=now,
        payload=payload, integration=integration)


def note_event(bq, item_id, now, event):
    """Returns the rows updated: 0 when the registry does not hold the item."""
    sql = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.{REGISTRY}`
        SET last_event = @event, last_event_at = @now
        WHERE item_id = @item_id
    """
    return run(bq, sql, item_id=item_id, event=event, now=now)


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
