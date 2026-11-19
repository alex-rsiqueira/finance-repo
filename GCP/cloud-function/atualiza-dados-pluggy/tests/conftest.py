import base64
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("PROJECT_ID", "test-project")

import main  # noqa: E402
import pluggy_api  # noqa: E402

LOG = "test-project.raw.tb_pluggy_ingestion_log"
REGISTRY = "test-project.raw.tb_pluggy_item_registry"
INTEGRATIONS = "test-project.raw.app_bank_integrations"
ITEM_1 = "item-1"
ITEM_2 = "item-2"
# The integration every test starts with, and a second household's.
INTEGRATION_1 = "integration-1"
INTEGRATION_2 = "integration-2"
# Secret Manager as the fakes hold it: the names an integration row carries, and their values.
SECRETS = {
    "Pluggy_client_id": "the-client-id\n", "Pluggy_client_secret": "the-client-secret",
    "second-client-id": "second-client-id-value", "second-client-secret": "second-client-secret-value\n",
}
# What POST /auth answers each pair with: Pluggy's API key is the application's.
API_KEYS = {
    ("the-client-id", "the-client-secret"): "api-key",
    ("second-client-id-value", "second-client-secret-value"): "second-api-key",
}


def response(status, body=None, retry_after=None):
    r = requests.Response()
    r.status_code = status
    r.reason = "fake"
    r.url = "https://api.pluggy.ai/fake"
    r._content = json.dumps(body if body is not None else {}).encode()
    if retry_after is not None:
        r.headers["Retry-After"] = str(retry_after)
    return r


class FakeApi:
    """What Pluggy answers. Data per item, account and investment; lists of pages where the
    API pages. failures[path] is a list consumed one call at a time before the real answer:
    an int is answered as that HTTP status, an exception is raised."""

    def __init__(self):
        self.items, self.accounts, self.investments = {}, {}, {}
        self.transaction_pages, self.bill_pages, self.investment_transaction_pages = {}, {}, {}
        self.failures = {}
        self.calls = []
        # item -> the API key of the application it was connected through; read with another
        # application's key Pluggy answers 404, as for an item it does not know.
        self.owners = {}
        self.refused = set()   # (client id, client secret) pairs POST /auth answers 401

    def add_item(self, item_id, status="UPDATED", consent="2027-10-09T18:47:45.492Z", api_key="api-key",
                 **fields):
        self.owners[item_id] = api_key
        self.items[item_id] = {"id": item_id, "connector": {"id": 200, "name": "MeuPluggy"},
                               "status": status, "executionStatus": "SUCCESS",
                               "lastUpdatedAt": "2026-10-10T23:33:19.737Z", "consentExpiresAt": consent,
                               **fields}
        self.accounts.setdefault(item_id, [])
        self.investments.setdefault(item_id, [[]])

    def add_account(self, item_id, account_id, type="BANK", transactions=((),), bills=((),)):
        subtype = "CREDIT_CARD" if type == "CREDIT" else "CHECKING_ACCOUNT"
        self.accounts[item_id].append({"id": account_id, "itemId": item_id, "type": type, "subtype": subtype,
                                       "name": f"Conta {account_id}", "currencyCode": "BRL", "balance": 1234.5})
        self.transaction_pages[account_id] = [list(page) for page in transactions]
        self.bill_pages[account_id] = [list(page) for page in bills]

    def add_investment(self, item_id, investment_id, transactions=((),), **fields):
        pages = self.investments[item_id]
        pages[-1].append({"id": investment_id, "itemId": item_id, "type": "FIXED_INCOME", "subtype": "CDB",
                          "name": "CDB", "currencyCode": "BRL", "balance": 100.0, "status": "ACTIVE",
                          **fields})
        self.investment_transaction_pages[investment_id] = [list(page) for page in transactions]

    def request(self, method, url, params=None, json=None, headers=None):
        parts = urlsplit(url)
        path = parts.path
        query = {k: v[0] for k, v in parse_qs(parts.query).items()}
        query.update({k: str(v) for k, v in (params or {}).items()})
        self.calls.append((method, path, query))

        queue = self.failures.get(path, [])
        if queue:
            failure = queue.pop(0)
            if isinstance(failure, Exception):
                raise failure
            return response(failure, retry_after=5 if failure in (429, 503) else None)

        if path == "/auth":
            pair = (json["clientId"], json["clientSecret"])
            if pair in self.refused:
                return response(401)
            return response(200, {"apiKey": API_KEYS[pair]})
        key = headers["X-API-KEY"]
        assert key in API_KEYS.values()
        if path.startswith("/items/"):
            item_id = path.split("/")[-1]
            return response(200, self.items[item_id]) if self.owners[item_id] == key else response(404)
        owner = (self.owners.get(query.get("itemId"))
                 or next((self.owners[a["itemId"]] for accounts in self.accounts.values() for a in accounts
                          if a["id"] == query.get("accountId")), None)
                 or next((self.owners[i["itemId"]] for pages in self.investments.values() for page in pages
                          for i in page if f"/investments/{i['id']}/" in path), None))
        assert owner == key, f"{path} read with another application's key"
        if path == "/accounts":
            return response(200, {"results": self.accounts[query["itemId"]]})
        if path == "/v2/transactions":
            account_id = query["accountId"]
            index = int(query.get("cursor", "0"))
            pages = self.transaction_pages[account_id]
            more = index + 1 < len(pages)
            return response(200, {"results": pages[index],
                                  "next": f"?accountId={account_id}&cursor={index + 1}" if more else None})
        if path == "/bills":
            return self.page(self.bill_pages[query["accountId"]], query)
        if path == "/investments":
            return self.page(self.investments[query["itemId"]], query)
        match = re.fullmatch(r"/investments/([^/]+)/transactions", path)
        if match:
            return self.page(self.investment_transaction_pages[match.group(1)], query)
        raise AssertionError(f"unexpected call {method} {url}")

    def page(self, pages, query):
        page = int(query["page"])
        return response(200, {"results": pages[page - 1], "page": page, "totalPages": len(pages),
                              "total": sum(len(p) for p in pages)})


class FakeSession:
    api = None

    def __init__(self):
        self.headers = {}

    def request(self, method, url, timeout=None, **kwargs):
        assert timeout == pluggy_api.TIMEOUT
        return self.api.request(method, url, headers=dict(self.headers), **kwargs)


class FakeSecrets:
    read = []

    def access_secret_version(self, name):
        secret = re.search(r"^projects/test-project/secrets/(.+)/versions/latest$", name).group(1)
        FakeSecrets.read.append(secret)
        if secret not in SECRETS:
            raise LookupError(f"Secret [{secret}] not found")
        return type("S", (), {"payload": type("P", (), {"data": SECRETS[secret].encode()})()})()


class Done:
    def __init__(self, rows=None, affected=None):
        self.rows, self.num_dml_affected_rows = rows, affected

    def result(self):
        return self.rows


class FakeBigQuery:
    """Tables are lists of dicts. MERGE is executed from its own SQL text: the ON keys, the
    columns of the WHEN MATCHED clause, and the INSERT column list."""

    def __init__(self, project=None):
        self.tables = FakeBigQuery.tables
        self.merges = FakeBigQuery.merges

    def query(self, sql, job_config=None):
        if job_config is not None and job_config.query_parameters:
            params = {p.name: p.value for p in job_config.query_parameters}
            if sql.strip().startswith("SELECT"):
                return self.previous_runs(sql, params)
            return self.registry_update(sql, params)
        if sql.strip().startswith("SELECT"):
            return self.registered_items(sql)
        assert sql.strip().startswith("MERGE"), sql
        assert "BY SOURCE" not in sql, "the fake runs WHEN MATCHED and WHEN NOT MATCHED only"
        self.merges.append(sql)
        target = re.search(r"MERGE `([^`]+)` target", sql).group(1)
        temp = re.search(r"USING `([^`]+)` source", sql).group(1)
        keys = re.findall(r"target\.`(\w+)` = source\.`\w+`", sql.split("ON", 1)[1].split("WHEN", 1)[0])
        insert_columns = re.findall(r"`(\w+)`", re.search(r"INSERT \(([^)]*)\)", sql).group(1))
        update = re.search(r"WHEN MATCHED THEN UPDATE SET (.*?)\n", sql)
        update_columns = re.findall(r"target\.`(\w+)` =", update.group(1)) if update else []
        rows = self.tables.setdefault(target, [])
        affected = 0
        for source in self.tables[temp]:
            value = {c: json.loads(source[c]) if c == "payload" and source[c] is not None else source[c]
                     for c in insert_columns}
            matches = [r for r in rows if all(r.get(k) == source[k] for k in keys)]
            if matches and update:
                assert len(matches) == 1, "a MERGE may update a target row from one source row only"
                matches[0].update({c: value[c] for c in update_columns})
                affected += 1
            elif not matches:
                rows.append(value)
                affected += 1
        return Done(affected=affected)

    def previous_runs(self, sql, params):
        """The redelivery lookup: the runs of one message, read from the log rows whose
        description contains the needle, as the SQL's STRPOS does."""
        assert f"`{LOG}`" in sql and "error_code IN ('ingestion_complete', 'fatal_error')" in sql, sql
        assert "STRPOS(description, @needle) > 0" in sql and "COUNTIF(type = 'Info') AS succeeded" in sql, sql
        runs = [r for r in self.tables.get(LOG, [])
                if r["error_code"] in ("ingestion_complete", "fatal_error") and params["needle"] in r["description"]]
        return Done(rows=[{"succeeded": sum(r["type"] == "Info" for r in runs), "runs": len(runs)}])

    def registered_items(self, sql):
        """The registry read, as its SQL joins it: live rows, oldest first, each LEFT JOINed to
        the newest version of the Pluggy integration it names, whose secret names show only
        while that version is active and not deleted."""
        flat = " ".join(sql.split())
        assert f"FROM `{REGISTRY}` r LEFT JOIN ( SELECT * FROM `{INTEGRATIONS}`" in flat, sql
        assert "WHERE provider = 'pluggy' QUALIFY ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY version_ts DESC) = 1" in flat, sql
        assert ") i ON i.integration_id = r.integration_id WHERE r.deleted_at IS NULL ORDER BY r.registered_at, r.item_id" in flat, sql
        for column in ("client_id_secret", "client_secret_secret"):
            assert f"IF(i.is_active AND NOT i.is_deleted, i.{column}, NULL) AS {column}" in flat, sql
        newest = {}
        for row in sorted(self.tables.get(INTEGRATIONS, []), key=lambda r: r["version_ts"]):
            if row["provider"] == "pluggy":
                newest[row["row_id"]] = row
        by_id = {row["integration_id"]: row for row in newest.values()}
        out = []
        for r in sorted(self.tables.get(REGISTRY, []), key=lambda r: (r["registered_at"], r["item_id"])):
            if r.get("deleted_at") is not None:
                continue
            i = by_id.get(r.get("integration_id"))
            readable = bool(i and i["is_active"] and not i["is_deleted"])
            out.append({"item_id": r["item_id"], "integration_id": i["integration_id"] if i else None,
                        "client_id_secret": i["client_id_secret"] if readable else None,
                        "client_secret_secret": i["client_secret_secret"] if readable else None})
        return Done(rows=out)

    def registry_update(self, sql, params):
        """mark_deleted: the item's row, deleted_at and the event set from the SQL."""
        flat = " ".join(sql.split())
        assert flat.startswith(f"UPDATE `{REGISTRY}` SET deleted_at = @now") and flat.endswith("WHERE item_id = @item_id"), sql
        self.merges.append(sql)
        event = re.search(r"last_event = '([^']+)'", sql).group(1)
        matches = [r for r in self.tables.get(REGISTRY, []) if r["item_id"] == params["item_id"]]
        for row in matches:
            row.update({"deleted_at": params["now"], "last_event": event, "last_event_at": params["now"]})
        return Done(affected=len(matches))

    def load_table_from_json(self, rows, ref, job_config=None):
        if ref == LOG:
            self.tables.setdefault(ref, []).extend(rows)
        else:
            assert "_tmp_" in ref
            assert {f.field_type for f in job_config.schema} == {"STRING"}
            names = [f.name for f in job_config.schema]
            assert all(list(r) == names for r in rows)
            assert all(v is None or isinstance(v, str) for r in rows for v in r.values())
            self.tables[ref] = [dict(r) for r in rows]
        return Done()

    def delete_table(self, ref, not_found_ok=False):
        self.tables.pop(ref, None)


class FakeDataform:
    """The Dataform API as start_bank_run calls it: the release config, then a workflow
    invocation. runs lists the invocations' bodies; failure, when set, is an HTTP status the
    release config answers with or an exception it raises; run_failure is a status the
    invocation answers with."""

    REPOSITORY = "https://dataform.googleapis.com/v1/projects/test-project/locations/us-central1/repositories/dataform-repo"
    COMPILATION = "projects/test-project/locations/us-central1/repositories/dataform-repo/compilationResults/c-1"

    def __init__(self):
        self.runs, self.failure, self.run_failure, self.compilation = [], None, None, self.COMPILATION

    @staticmethod
    def answer(status, body):
        r = requests.Response()
        r.status_code, r.reason, r.url = status, "fake", "https://dataform.googleapis.com/fake"
        r._content = json.dumps(body).encode()
        return r

    def get(self, url, timeout=None):
        assert url == f"{self.REPOSITORY}/releaseConfigs/release-02" and timeout
        if isinstance(self.failure, Exception):
            raise self.failure
        if self.failure:
            return self.answer(self.failure, {"releaseCompilationResult": self.compilation})
        return self.answer(200, {"name": "release-02", "releaseCompilationResult": self.compilation})

    def post(self, url, json=None, timeout=None):
        assert url == f"{self.REPOSITORY}/workflowInvocations" and timeout
        if self.run_failure:
            return self.answer(self.run_failure, {"name": "not started"})
        self.runs.append(json)
        return self.answer(200, {"name": f"{self.REPOSITORY[35:]}/workflowInvocations/run-{len(self.runs)}"})


@pytest.fixture
def env(monkeypatch):
    FakeBigQuery.tables, FakeBigQuery.merges = {}, []
    dataform = FakeDataform()
    monkeypatch.setattr(main, "dataform_session", lambda: dataform)
    api = FakeApi()
    FakeSession.api = api
    sleeps = []
    monkeypatch.setattr(main, "PROJECT_ID", "test-project")
    FakeSecrets.read = []
    monkeypatch.setattr(main.bigquery, "Client", FakeBigQuery)
    monkeypatch.setattr(main.secretmanager, "SecretManagerServiceClient", FakeSecrets)
    monkeypatch.setattr(pluggy_api.requests, "Session", FakeSession)
    monkeypatch.setattr(pluggy_api.time, "sleep", sleeps.append)

    class Env:
        def __init__(self):
            self.api, self.sleeps, self.tables, self.merges = api, sleeps, FakeBigQuery.tables, FakeBigQuery.merges
            self.dataform = dataform

            self.secrets_read = FakeSecrets.read
            # Every test starts as prod does: one Pluggy integration, and ITEM_1 connected through it.
            self.integration(INTEGRATION_1)
            self.register(ITEM_1)

        def items(self, *item_ids):
            """The registry holds exactly these, in this order, on the first integration."""
            self.tables[REGISTRY] = []
            self.register(*item_ids)

        def register(self, *item_ids, deleted=False, integration=INTEGRATION_1):
            rows = self.tables.setdefault(REGISTRY, [])
            for item_id in item_ids:
                rows.append({"item_id": item_id, "source": "webhook",
                             "registered_at": f"2026-10-20 00:00:{len(rows):02d}",
                             "deleted_at": "2026-10-21 00:00:00" if deleted else None,
                             "integration_id": integration})

        def integration(self, integration_id, client_id_secret="Pluggy_client_id",
                        client_secret_secret="Pluggy_client_secret", provider="pluggy", is_active=True,
                        is_deleted=False, version_ts="2026-11-19 22:00:00+00"):
            """One version of an integration; a later version_ts for the same id is its newer version."""
            self.tables.setdefault(INTEGRATIONS, []).append({
                "row_id": f"row-{integration_id}", "version_ts": version_ts, "is_deleted": is_deleted,
                "household_id": "HH_TEST", "integration_id": integration_id, "provider": provider,
                "label": integration_id, "client_id_secret": client_id_secret,
                "client_secret_secret": client_secret_secret, "is_active": is_active})

        def registry(self):
            return self.tables.get(REGISTRY, [])

        def run_at(self, when, event=None, context=None):
            ticks = []

            class Clock(datetime):
                """Advances one second per call, so a second now() in a run shows up."""
                @classmethod
                def now(cls, tz=None):
                    ticks.append(None)
                    return when + timedelta(seconds=len(ticks) - 1)
            monkeypatch.setattr(main, "datetime", Clock)
            return main.main(event or {}, context)

        def run(self):
            return self.run_at(datetime(2026, 10, 11, 7, 15, 0, tzinfo=timezone.utc))

        def deliver(self, message=None, message_id="msg-1", data=None):
            """A Pub/Sub delivery as the Functions Framework hands it to an event function:
            the message body base64 in event["data"], its message id as context.event_id.
            message is JSON-encoded; data is the raw body, for one that is not JSON."""
            if message is not None:
                data = json.dumps(message)
            event = {"@type": "type.googleapis.com/google.pubsub.v1.PubsubMessage", "attributes": None}
            if data is not None:
                event["data"] = base64.b64encode(data.encode()).decode()
            context = SimpleNamespace(event_id=message_id, timestamp="2026-10-22T21:24:40.000Z",
                                      event_type="google.pubsub.topic.publish", resource=None)
            return self.run_at(datetime(2026, 10, 22, 21, 25, 0, tzinfo=timezone.utc), event, context)

        def rows(self, table):
            return self.tables.get(f"test-project.raw.{table}", [])

        def log(self, event_type=None):
            return [r for r in self.tables.get(LOG, []) if event_type in (None, r["error_code"])]

        def calls_to(self, path):
            return [c for c in self.api.calls if c[1] == path]

        def merged(self, table):
            return [m for m in self.merges if f".{table}` target" in m]

    return Env()


def transaction(id, amount=-10.0, status="POSTED", **fields):
    record = {"id": id, "description": "SUPERMERCADO", "descriptionRaw": None, "currencyCode": "BRL",
              "amount": amount, "date": "2026-10-08T03:00:00.000Z", "type": "DEBIT" if amount < 0 else "CREDIT",
              "status": status, "category": "Groceries", "paymentData": None, "creditCardMetadata": None}
    record.update(fields)
    return record


def bill(id, closing="2026-10-09"):
    return {"id": id, "dueDate": "2026-10-16", "billClosingDate": closing, "totalAmount": 1500.25,
            "totalAmountCurrencyCode": "BRL", "minimumPaymentAmount": None, "payments": [], "financeCharges": []}


def investment_transaction(id, amount=1000.0):
    return {"id": id, "date": "2026-09-01T00:00:00.000Z", "type": "BUY", "movementType": "CREDIT",
            "amount": amount, "quantity": 1, "value": amount}
