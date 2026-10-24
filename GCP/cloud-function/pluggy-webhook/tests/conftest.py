import copy
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
LOG = "test-project.raw.tb_pluggy_ingestion_log"


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

    def add_item(self, item_id, connector="Nubank", status="UPDATED", execution="SUCCESS"):
        self.items[item_id] = {"id": item_id, "connector": {"id": 612, "name": connector},
                               "status": status, "executionStatus": execution,
                               "lastUpdatedAt": "2026-10-21T22:20:55.936Z"}

    def request(self, method, url, params=None, json=None, headers=None):
        path = url.split("api.pluggy.ai", 1)[1]
        self.calls.append((method, path))
        if path == "/auth":
            assert json == {"clientId": "the-client-id", "clientSecret": "the-client-secret"}
            return response(200, {"apiKey": "api-key"})
        assert headers["X-API-KEY"] == "api-key"
        if path.startswith("/items/"):
            item_id = path.split("/")[-1]
            return response(200, self.items[item_id]) if item_id in self.items else response(404)
        raise AssertionError(f"unexpected call {method} {url}")


class FakeSession:
    api = None

    def __init__(self):
        self.headers = {}

    def request(self, method, url, timeout=None, **kwargs):
        return self.api.request(method, url, headers=dict(self.headers), **kwargs)


class FakeSecrets:
    def access_secret_version(self, name):
        secret = re.search(r"secrets/(.+)/versions/latest", name).group(1)
        value = {"Pluggy_client_id": "the-client-id", "Pluggy_client_secret": "the-client-secret"}[secret]
        return type("S", (), {"payload": type("P", (), {"data": value.encode()})()})()


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


class FakeThread:
    """threading.Thread: started threads wait in `pending` until the test runs them, so a test
    sees what had happened when the webhook answered."""

    pending = []

    def __init__(self, target, args=(), kwargs=None, daemon=None):
        self.target, self.args, self.kwargs, self.daemon = target, args, kwargs or {}, daemon

    def start(self):
        FakeThread.pending.append(self)


class Done:
    def __init__(self, affected=None):
        self.num_dml_affected_rows = affected

    def result(self):
        return None


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
    FakeThread.pending = []
    api = FakeApi()
    FakeSession.api = api
    monkeypatch.setattr(main, "PROJECT_ID", "test-project")
    monkeypatch.setattr(main, "EXPECTED_SECRET", "the-secret")
    monkeypatch.setattr(main, "LOAD_TOPIC", "the-topic")
    monkeypatch.setattr(main.pubsub_v1, "PublisherClient", FakePublisher)
    monkeypatch.setattr(main.threading, "Thread", FakeThread)
    monkeypatch.setattr(main.bigquery, "Client", FakeBigQuery)
    monkeypatch.setattr(main.secretmanager, "SecretManagerServiceClient", FakeSecrets)
    monkeypatch.setattr(pluggy_api.requests, "Session", FakeSession)
    monkeypatch.setattr(pluggy_api.time, "sleep", lambda s: None)

    class Env:
        def __init__(self):
            self.api, self.tables, self.queries = api, FakeBigQuery.tables, FakeBigQuery.queries
            self.published = FakePublisher.messages

        def call(self, event, item_id="item-1", secret="the-secret", **more):
            """The webhook's answer. What had happened when it answered is kept in
            self.at_answer; work it left to a thread then runs, as it would after the answer."""
            body = {"event": event, "eventId": "evt-1", "itemId": item_id, "triggeredBy": "USER", **more}
            answer = main.main(FakeRequest(body, secret))
            self.at_answer = {"registry": copy.deepcopy(self.registry()), "published": list(self.published),
                              "pluggy_calls": list(self.api.calls), "log": list(self.log()),
                              "threads": len(FakeThread.pending)}
            while FakeThread.pending:
                thread = FakeThread.pending.pop(0)
                assert thread.daemon is not True, "a daemon thread is killed with the process"
                thread.target(*thread.args, **thread.kwargs)
            return answer

        def registry(self):
            return self.tables.get(REGISTRY, [])

        def log(self, event_type=None):
            return [r for r in self.tables.get(LOG, []) if event_type in (None, r["error_code"])]

    return Env()
