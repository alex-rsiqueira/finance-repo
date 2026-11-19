import json
import os
import re
import sys

import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("PROJECT_ID", "test-project")
os.environ.setdefault("PLUGGY_WEBHOOK_SECRET", "the-secret")
os.environ.setdefault("LOAD_TOPIC", "the-topic")

import main  # noqa: E402
import pluggy_api  # noqa: E402

REGISTRY = "test-project.raw.tb_pluggy_item_registry"
INTEGRATIONS = "test-project.raw.app_bank_integrations"
LOG = "test-project.raw.tb_pluggy_ingestion_log"
# The integration every test starts with, and a second household's.
INTEGRATION_1 = "integration-1"
INTEGRATION_2 = "integration-2"
SECRETS = {
    "Pluggy_client_id": "the-client-id", "Pluggy_client_secret": "the-client-secret",
    "second-client-id": "second-client-id-value", "second-client-secret": "second-client-secret-value",
}
# What POST /auth answers each pair with: Pluggy's API key is the application's.
API_KEYS = {
    ("the-client-id", "the-client-secret"): "api-key",
    ("second-client-id-value", "second-client-secret-value"): "second-api-key",
}


def response(status, body=None):
    r = requests.Response()
    r.status_code = status
    r.reason = "fake"
    r.url = "https://api.pluggy.ai/fake"
    r._content = json.dumps(body if body is not None else {}).encode()
    return r


class FakeApi:
    def __init__(self):
        self.items, self.calls = {}, []
        # item -> the API key of the application it was connected through; read with another
        # application's key Pluggy answers 404, as for an item it does not know.
        self.owners = {}
        self.refused = set()   # (client id, client secret) pairs POST /auth answers 401
        self.keys_used = []    # the API key of every item read, in order

    def add_item(self, item_id, connector="Nubank", status="UPDATED", execution="SUCCESS", api_key="api-key"):
        self.owners[item_id] = api_key
        self.items[item_id] = {"id": item_id, "connector": {"id": 612, "name": connector},
                               "status": status, "executionStatus": execution,
                               "lastUpdatedAt": "2026-10-21T22:20:55.936Z"}

    def request(self, method, url, params=None, json=None, headers=None):
        path = url.split("api.pluggy.ai", 1)[1]
        self.calls.append((method, path))
        if path == "/auth":
            pair = (json["clientId"], json["clientSecret"])
            return response(401) if pair in self.refused else response(200, {"apiKey": API_KEYS[pair]})
        key = headers["X-API-KEY"]
        assert key in API_KEYS.values()
        if path.startswith("/items/"):
            item_id = path.split("/")[-1]
            self.keys_used.append(key)
            return response(200, self.items[item_id]) if self.owners.get(item_id) == key else response(404)
        raise AssertionError(f"unexpected call {method} {url}")


class FakeSession:
    api = None

    def __init__(self):
        self.headers = {}

    def request(self, method, url, timeout=None, **kwargs):
        return self.api.request(method, url, headers=dict(self.headers), **kwargs)


class FakeSecrets:
    def access_secret_version(self, name):
        secret = re.search(r"^projects/test-project/secrets/(.+)/versions/latest$", name).group(1)
        return type("S", (), {"payload": type("P", (), {"data": SECRETS[secret].encode()})()})()


class FakePublisher:
    """Pub/Sub: every message published, as (topic, decoded JSON), and the registry's item ids
    at that moment. failure, when set, is raised by the future's result(), as a publish that
    does not go through."""

    messages = []
    registered_when_published = []
    failure = None

    def publish(self, topic, data):
        FakePublisher.messages.append((topic, json.loads(data.decode())))
        FakePublisher.registered_when_published.append(
            [r["item_id"] for r in FakeBigQuery.tables.get(REGISTRY, [])])
        return self

    def result(self, timeout=None):
        assert timeout == main.PUBLISH_TIMEOUT
        if FakePublisher.failure:
            raise FakePublisher.failure


class Done:
    def __init__(self, affected=None, rows=None):
        self.num_dml_affected_rows, self.rows = affected, rows

    def result(self):
        return self.rows


class FakeBigQuery:
    """The registry is a list of dicts. The MERGE and UPDATE are executed from their own SQL
    text: each `column = expression` of the SET clause and each value of the INSERT, where an
    expression is @param, 'literal', COALESCE(x, target.column) or PARSE_JSON(x)."""

    tables = {}
    queries = []

    def __init__(self, project=None):
        self.tables = FakeBigQuery.tables

    @staticmethod
    def value(expression, params, row):
        expression = expression.strip()
        if expression.startswith("@"):
            return params[expression[1:]]
        if expression.startswith("'"):
            return expression.strip("'")
        coalesce = re.fullmatch(r"COALESCE\((.+), target\.(\w+)\)", expression)
        if coalesce:
            first = FakeBigQuery.value(coalesce.group(1), params, row)
            return first if first is not None else (row or {}).get(coalesce.group(2))
        parse = re.fullmatch(r"PARSE_JSON\((.+)\)", expression)
        if parse:
            text = FakeBigQuery.value(parse.group(1), params, row)
            return json.loads(text) if text is not None else None
        raise AssertionError(f"the fake cannot evaluate {expression}")

    def query(self, sql, job_config=None):
        params = {p.name: p.value for p in job_config.query_parameters}
        FakeBigQuery.queries.append((sql, params))
        if sql.strip().startswith("SELECT"):
            return self.integrations(sql, params)
        rows = self.tables.setdefault(REGISTRY, [])
        match = next((r for r in rows if r["item_id"] == params["item_id"]), None)
        if sql.strip().startswith("MERGE"):
            assert "USING (SELECT @item_id AS item_id)" in sql
            assert "ON target.item_id = source.item_id" in sql
            if match:
                assignments = sql.split("UPDATE SET", 1)[1].split("WHEN NOT MATCHED", 1)[0]
                for column, expression in re.findall(r"(\w+) = (.+?)(?:,\n|\n)", assignments):
                    match[column] = self.value(expression, params, match)
            else:
                columns = re.findall(r"\w+", re.search(r"INSERT\s*\(([^)]*)\)", sql).group(1))
                values = re.search(r"VALUES \((.*)\)", sql).group(1)
                expressions = re.split(r", (?![^(]*\))", values)
                assert len(columns) == len(expressions), (columns, expressions)
                rows.append({c: self.value(e, params, None) for c, e in zip(columns, expressions)})
            return Done(1)
        assert sql.strip().startswith("UPDATE")
        assert "WHERE item_id = @item_id" in sql
        if match:
            assignments = sql.split("SET", 1)[1].split("WHERE", 1)[0] + "\n"
            for column, expression in re.findall(r"(\w+) = (.+?)(?:,\s*|\n)", assignments):
                match[column] = self.value(expression, params, match)
        return Done(1 if match else 0)

    def integrations(self, sql, params):
        """The integrations read, as its SQL filters it: the newest version of each Pluggy
        integration, active, not deleted, with both secret names, oldest first; with @item_id,
        only the one that item's registry row names."""
        flat = " ".join(sql.split())
        assert f"FROM ( SELECT * FROM `{INTEGRATIONS}` WHERE provider = 'pluggy' QUALIFY ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY version_ts DESC) = 1 )" in flat, sql
        assert "WHERE is_active AND NOT is_deleted AND client_id_secret IS NOT NULL AND client_secret_secret IS NOT NULL" in flat, sql
        assert f"AND (@item_id IS NULL OR integration_id = ( SELECT integration_id FROM `{REGISTRY}` WHERE item_id = @item_id))" in flat, sql
        assert flat.endswith("ORDER BY version_ts, integration_id"), sql
        newest = {}
        for row in sorted(self.tables.get(INTEGRATIONS, []), key=lambda r: r["version_ts"]):
            if row["provider"] == "pluggy":
                newest[row["row_id"]] = row
        rows = [r for r in newest.values() if r["is_active"] and not r["is_deleted"]
                and r["client_id_secret"] is not None and r["client_secret_secret"] is not None]
        if params["item_id"] is not None:
            named = [r.get("integration_id") for r in self.tables.get(REGISTRY, []) if r["item_id"] == params["item_id"]]
            rows = [r for r in rows if named and r["integration_id"] == named[0]]
        rows.sort(key=lambda r: (r["version_ts"], r["integration_id"]))
        return Done(rows=[{k: r[k] for k in ("integration_id", "client_id_secret", "client_secret_secret")}
                          for r in rows])

    def load_table_from_json(self, rows, ref, job_config=None):
        assert ref == LOG
        self.tables.setdefault(ref, []).extend(rows)
        return Done()


class FakeRequest:
    def __init__(self, body, secret="the-secret"):
        self.headers = {} if secret is None else {main.SECRET_HEADER: secret}
        self.body = body

    def get_json(self, silent=False):
        return self.body


@pytest.fixture
def env(monkeypatch):
    FakeBigQuery.tables, FakeBigQuery.queries = {}, []
    FakePublisher.messages, FakePublisher.registered_when_published, FakePublisher.failure = [], [], None
    api = FakeApi()
    FakeSession.api = api
    monkeypatch.setattr(main, "PROJECT_ID", "test-project")
    monkeypatch.setattr(main, "EXPECTED_SECRET", "the-secret")
    monkeypatch.setattr(main, "LOAD_TOPIC", "the-topic")
    monkeypatch.setattr(main.pubsub_v1, "PublisherClient", FakePublisher)
    monkeypatch.setattr(main.bigquery, "Client", FakeBigQuery)
    monkeypatch.setattr(main.secretmanager, "SecretManagerServiceClient", FakeSecrets)
    monkeypatch.setattr(pluggy_api.requests, "Session", FakeSession)
    monkeypatch.setattr(pluggy_api.time, "sleep", lambda s: None)

    class Env:
        def __init__(self):
            self.api, self.tables, self.queries = api, FakeBigQuery.tables, FakeBigQuery.queries
            self.published = FakePublisher.messages
            # Every test starts as prod does: one Pluggy integration.
            self.integration(INTEGRATION_1)

        def integration(self, integration_id, client_id_secret="Pluggy_client_id",
                        client_secret_secret="Pluggy_client_secret", provider="pluggy", is_active=True,
                        is_deleted=False, version_ts="2026-11-19 22:00:00+00"):
            """One version of an integration; a later version_ts for the same id is its newer version."""
            self.tables.setdefault(INTEGRATIONS, []).append({
                "row_id": f"row-{integration_id}", "version_ts": version_ts, "is_deleted": is_deleted,
                "household_id": "HH_TEST", "integration_id": integration_id, "provider": provider,
                "label": integration_id, "client_id_secret": client_id_secret,
                "client_secret_secret": client_secret_secret, "is_active": is_active})

        def call(self, event, item_id="item-1", secret="the-secret", **more):
            body = {"event": event, "eventId": "evt-1", "itemId": item_id, "triggeredBy": "USER", **more}
            return main.main(FakeRequest(body, secret))

        def registry(self):
            return self.tables.get(REGISTRY, [])

        def log(self, event_type=None):
            return [r for r in self.tables.get(LOG, []) if event_type in (None, r["error_code"])]

    return Env()
