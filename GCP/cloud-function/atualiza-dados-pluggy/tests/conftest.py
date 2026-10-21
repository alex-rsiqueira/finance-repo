import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("PROJECT_ID", "test-project")

import main  # noqa: E402
import pluggy_api  # noqa: E402

LOG = "test-project.raw.tb_pluggy_ingestion_log"
REGISTRY = "test-project.raw.tb_pluggy_item_registry"
ITEM_1 = "item-1"
ITEM_2 = "item-2"


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

    def add_item(self, item_id, status="UPDATED", consent="2027-10-09T18:47:45.492Z", **fields):
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
            assert json == {"clientId": "the-client-id", "clientSecret": "the-client-secret"}
            return response(200, {"apiKey": "api-key"})
        assert headers["X-API-KEY"] == "api-key"
        if path.startswith("/items/"):
            return response(200, self.items[path.split("/")[-1]])
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
    def access_secret_version(self, name):
        secret = re.search(r"secrets/(.+)/versions/latest", name).group(1)
        value = {"Pluggy_client_id": "the-client-id\n", "Pluggy_client_secret": "the-client-secret"}[secret]
        return type("S", (), {"payload": type("P", (), {"data": value.encode()})()})()


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
            return self.registry_merge(sql, {p.name: p.value for p in job_config.query_parameters})
        if sql.strip().startswith("SELECT"):
            assert f"`{REGISTRY}`" in sql and "deleted_at IS NULL" in sql, sql
            rows = [r for r in self.tables.get(REGISTRY, []) if r.get("deleted_at") is None]
            return Done(rows=sorted(rows, key=lambda r: (r["registered_at"], r["item_id"])))
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

    def registry_merge(self, sql, params):
        """mark_deleted: one row keyed on item_id, deleted_at and the event set from the SQL."""
        assert f"`{REGISTRY}` target" in sql and "ON target.item_id = source.item_id" in sql
        self.merges.append(sql)
        rows = self.tables.setdefault(REGISTRY, [])
        event = re.search(r"last_event = '([^']+)'", sql).group(1)
        match = next((r for r in rows if r["item_id"] == params["item_id"]), None)
        if match:
            match.update({"deleted_at": params["now"], "last_event": event, "last_event_at": params["now"]})
        else:
            rows.append({"item_id": params["item_id"], "source": "manual", "registered_at": params["now"],
                         "deleted_at": params["now"], "last_event": event, "last_event_at": params["now"]})
        return Done(affected=1)

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


@pytest.fixture
def env(monkeypatch):
    FakeBigQuery.tables, FakeBigQuery.merges = {}, []
    api = FakeApi()
    FakeSession.api = api
    sleeps = []
    monkeypatch.setattr(main, "PROJECT_ID", "test-project")
    monkeypatch.setattr(main, "ITEM_IDS", [ITEM_1])
    monkeypatch.setattr(main.bigquery, "Client", FakeBigQuery)
    monkeypatch.setattr(main.secretmanager, "SecretManagerServiceClient", FakeSecrets)
    monkeypatch.setattr(pluggy_api.requests, "Session", FakeSession)
    monkeypatch.setattr(pluggy_api.time, "sleep", sleeps.append)

    class Env:
        def __init__(self):
            self.api, self.sleeps, self.tables, self.merges = api, sleeps, FakeBigQuery.tables, FakeBigQuery.merges

        def items(self, *item_ids):
            monkeypatch.setattr(main, "ITEM_IDS", list(item_ids))

        def register(self, *item_ids, deleted=False):
            rows = self.tables.setdefault(REGISTRY, [])
            for n, item_id in enumerate(item_ids):
                rows.append({"item_id": item_id, "source": "webhook",
                             "registered_at": f"2026-10-20 00:00:0{n}",
                             "deleted_at": "2026-10-21 00:00:00" if deleted else None})

        def registry(self):
            return self.tables.get(REGISTRY, [])

        def run_at(self, when):
            ticks = []

            class Clock(datetime):
                """Advances one second per call, so a second now() in a run shows up."""
                @classmethod
                def now(cls, tz=None):
                    ticks.append(None)
                    return when + timedelta(seconds=len(ticks) - 1)
            monkeypatch.setattr(main, "datetime", Clock)
            return main.main({}, None)

        def run(self):
            return self.run_at(datetime(2026, 10, 11, 7, 15, 0, tzinfo=timezone.utc))

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
