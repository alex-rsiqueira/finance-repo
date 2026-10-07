import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("PROJECT_ID", "test-project")

import gocardless  # noqa: E402
import main  # noqa: E402

LOG = "test-project.raw.tb_nordigen_ingestion_log"


def http_error(status, retry_after=None):
    response = requests.Response()
    response.status_code = status
    if retry_after is not None:
        response.headers["Retry-After"] = str(retry_after)
    return requests.HTTPError({"status": status}, response=response)


class FakeBank:
    """What GoCardless answers. answers[(account_id, endpoint)] is a list consumed one call at
    a time; an exception in it is raised, anything else returned. The last answer repeats."""

    def __init__(self):
        self.requisitions = []
        self.answers = {}
        self.calls = []

    def account(self, account_id, transactions=(), balances=None, **extra):
        self.answers[(account_id, "metadata")] = [{
            "id": account_id, "created": "2026-01-01T00:00:00Z", "last_accessed": "2026-10-06T07:00:00Z",
            "iban": f"PT50{account_id}", "institution_id": "MILLENNIUMBCP_BCOMPTPL", "status": "READY",
            "owner_name": "OWNER"}]
        self.answers[(account_id, "details")] = [{"account": {
            "iban": f"PT50{account_id}", "currency": "EUR", "name": "Conta", "cashAccountType": "CACC"}}]
        self.answers[(account_id, "balances")] = [{"balances": balances if balances is not None else [
            {"balanceAmount": {"amount": "100.00", "currency": "EUR"}, "balanceType": "closingBooked"},
            {"balanceAmount": {"amount": "90.00", "currency": "EUR"}, "balanceType": "interimAvailable"},
        ]}]
        self.answers[(account_id, "transactions")] = [{"transactions": {"booked": list(transactions), "pending": []}}]
        self.answers.update({(account_id, k): v for k, v in extra.items()})

    def answer(self, account_id, endpoint):
        self.calls.append((account_id, endpoint))
        queue = self.answers[(account_id, endpoint)]
        value = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(value, Exception):
            raise value
        return value


class FakeNordigenClient:
    bank = None

    def __init__(self, secret_id, secret_key):
        assert secret_key == f"key-for-{secret_id}"
        bank = self.bank
        self.requisition = type("R", (), {"get_requisitions": lambda _self: {"results": bank.requisitions}})()

    def generate_token(self):
        return {"access": "token"}

    def account_api(self, id):
        bank = self.bank
        return type("A", (), {f"get_{e}": (lambda _self, e=e: bank.answer(id, e))
                              for e in ("metadata", "details", "balances", "transactions")})()


class FakeSecrets:
    def access_secret_version(self, name):
        secret_id = re.search(r"secrets/Nordigen_(.+)/versions", name).group(1)
        payload = type("P", (), {"data": f"key-for-{secret_id}".encode()})()
        return type("S", (), {"payload": payload})()


class Done:
    def __init__(self, rows=None, affected=None):
        self.rows, self.num_dml_affected_rows = rows, affected

    def result(self):
        return self.rows


class FakeBigQuery:
    """Tables are lists of dicts. MERGE is executed from its own SQL text: the ON keys,
    whether a WHEN MATCHED clause exists, and the INSERT column list."""

    credentials = [{"client_id": "client-1", "secret_id": "s1"}]

    def __init__(self, project=None):
        self.tables = FakeBigQuery.tables
        self.merges = FakeBigQuery.merges

    def query(self, sql):
        if "tb_sheet_nordigen_account" in sql:
            return Done(rows=list(self.credentials))
        assert sql.strip().startswith("MERGE"), sql
        self.merges.append(sql)
        target = re.search(r"MERGE `([^`]+)` target", sql).group(1)
        temp = re.search(r"USING `([^`]+)` source", sql).group(1)
        keys = re.findall(r"target\.`(\w+)` = source\.`\w+`", sql.split("ON", 1)[1].split("WHEN", 1)[0])
        insert_columns = re.findall(r"`(\w+)`", re.search(r"INSERT \(([^)]*)\)", sql).group(1))
        update = "WHEN MATCHED THEN UPDATE" in sql
        rows = self.tables.setdefault(target, [])
        affected = 0
        for source in self.tables[temp]:
            value = {c: json.loads(source[c]) if c == "payload" and source[c] is not None else source[c]
                     for c in insert_columns}
            matches = [r for r in rows if all(r.get(k) == source[k] for k in keys)]
            if matches and update:
                assert len(matches) == 1, "a MERGE may update a target row from one source row only"
                matches[0].update(value)
                affected += 1
            elif not matches:
                rows.append(value)
                affected += 1
        return Done(affected=affected)

    def load_table_from_json(self, rows, ref, job_config=None):
        if ref == LOG:
            self.tables.setdefault(ref, []).extend(rows)
        else:
            assert "_tmp_" in ref
            assert {f.field_type for f in job_config.schema} == {"STRING"}
            names = [f.name for f in job_config.schema]
            assert all(set(r) == set(names) for r in rows)
            assert all(v is None or isinstance(v, str) for r in rows for v in r.values())
            self.tables[ref] = [dict(r) for r in rows]
        return Done()

    def delete_table(self, ref, not_found_ok=False):
        self.tables.pop(ref, None)


@pytest.fixture
def env(monkeypatch):
    FakeBigQuery.tables, FakeBigQuery.merges = {}, []
    bank = FakeBank()
    FakeNordigenClient.bank = bank
    sleeps = []
    monkeypatch.setattr(main, "PROJECT_ID", "test-project")
    monkeypatch.setattr(main.bigquery, "Client", FakeBigQuery)
    monkeypatch.setattr(main.secretmanager, "SecretManagerServiceClient", FakeSecrets)
    monkeypatch.setattr(gocardless, "NordigenClient", FakeNordigenClient)
    monkeypatch.setattr(gocardless.time, "sleep", sleeps.append)

    class Env:
        def __init__(self):
            self.bank, self.sleeps, self.tables, self.merges = bank, sleeps, FakeBigQuery.tables, FakeBigQuery.merges

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
            return self.run_at(datetime(2026, 10, 7, 7, 0, 0, tzinfo=timezone.utc))

        def rows(self, table):
            return self.tables.get(f"test-project.raw.{table}", [])

        def log(self, event_type=None):
            return [r for r in self.tables.get(LOG, []) if event_type in (None, r["error_code"])]

    return Env()


def transaction(internal_id, amount="-10.00", **fields):
    record = {"transactionId": f"T{internal_id}", "internalTransactionId": internal_id,
              "bookingDate": "2026-10-01", "valueDate": "2026-10-01",
              "transactionAmount": {"amount": amount, "currency": "EUR"},
              "remittanceInformationUnstructured": "COMPRA CONTINENTE"}
    record.update(fields)
    return {k: v for k, v in record.items() if v is not None}
