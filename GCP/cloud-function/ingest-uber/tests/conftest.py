import base64
import hashlib
import io
import os
import re
import sys
import zipfile
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("PROJECT_ID", "test-project")

import main  # noqa: E402

BUCKET = "uber_data"
LOG = "test-project.raw.tb_ingestion_log"

TRIPS_V1 = "city,product_type,status,request_time,fare_currency,fare_amount\n" \
           "Lisbon,uberX,completed,2024-10-14T07:13:54.000Z,Euro,9.5\n" \
           "Sao Paulo,uberX,completed,2018-12-25T07:49:18.000Z,Brazilian Real,20.47\n"
ORDERS = "City_Name,Restaurant_Name,Request_Time_Local,Order_Status,Item_Name,Item_Price,Order_Price,Currency\n" \
         "Lisbon,Lupita,2024-10-18T20:33:13.000Z,completed,Pizza,12.5,15.0,EUR\n"
TRIPS_V3 = "city_name,request_timestamp_utc,status,original_fare_local,card_number\n" \
           "Lisbon,2026-10-13T10:26:14.000Z,completed,8.2,1234\n"
ORDERS_V3 = "City_Name,Restaurant_Name,Request_Time_Local,Order_Status,Item_Name,Item_Price,Order_Price,Currency\n" \
            "Ericeira,Continente,2026-10-26T13:50:47.000Z,completed,Ovos,3.74,62.86,EUR\n"
DETAILS_V3 = "Order_UUID,Request_Time_Local,Restaurant_Name,Item_Name\n" \
             "3977fccc-aba0-4a11-a494-4dcb25c35733,2026-10-26T13:50:47.000Z,Continente,Ovos\n"


def make_zip(files):
    """files: path inside the zip -> text (or bytes)."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        for path, text in files.items():
            z.writestr(path, text if isinstance(text, bytes) else text.encode("utf-8"))
    return buffer.getvalue()


OLD_EXPORT = {"Uber Data/Rider/trips_data-0.csv": TRIPS_V1,
              "Uber Data/Eats/user_orders-0.csv": ORDERS,
              "Uber Data/Eats/eats_app_analytics-0.csv": "a,b\n1,2\n",
              "Uber Data/readme.html": "<html></html>"}
NEW_EXPORT = {"Uber Data/Rider/rider_lifetime_trips-0.csv": TRIPS_V3,
              "Uber Data/Eats/user_orders-0.csv": ORDERS_V3,
              "Uber Data/Eats/eats_order_details-0.csv": DETAILS_V3}


def md5_base64(content):
    return base64.b64encode(hashlib.md5(content).digest()).decode()


class FakeBlob:
    def __init__(self, bucket, name, content):
        self.bucket, self.name, self.content = bucket, name, content
        self.md5_hash = md5_base64(content)

    def download_as_bytes(self):
        return self.content

    def delete(self):
        del self.bucket.objects[self.name]


class FakeBucket:
    def __init__(self, name):
        self.name, self.objects = name, {}

    def put(self, name, content):
        self.objects[name] = FakeBlob(self, name, content)

    def blob(self, name):
        return self.objects[name]

    def get_blob(self, name):
        return self.objects.get(name)

    def copy_blob(self, blob, destination_bucket, new_name):
        destination_bucket.put(new_name, blob.content)
        return destination_bucket.objects[new_name]


class FakeStorage:
    def __init__(self, bucket):
        self._bucket = bucket

    def bucket(self, name):
        assert name == self._bucket.name
        return self._bucket


class Done:
    def __init__(self, rows=None):
        self.rows = rows or []

    def result(self):
        return self.rows


class FakeBigQuery:
    """Tables are lists of dicts. Load jobs fill temp tables; the transaction's INSERT ... SELECT
    statements copy a temp table into its raw table with the export's parameters; the log is
    streamed with insert_rows_json."""

    def __init__(self):
        self.tables, self.queries, self.fail_transaction = {}, [], False

    def load_table_from_json(self, rows, table, job_config=None):
        self.tables[table] = [dict(r) for r in rows]
        return Done()

    def delete_table(self, table, not_found_ok=False):
        self.tables.pop(table, None)

    def insert_rows_json(self, table, rows):
        self.tables.setdefault(table, []).extend(rows)
        return []

    def query(self, sql, job_config=None):
        params = {p.name: p.value for p in (job_config.query_parameters if job_config else [])}
        self.queries.append(sql)
        if sql.strip().startswith("SELECT"):
            assert f"`{LOG}`" in sql and "status = 'success'" in sql, sql
            hits = [r for r in self.tables.get(LOG, [])
                    if r["file_hash"] == params["export_id"] and r["status"] == "success"]
            return Done(rows=[{"loaded": len(hits)}])
        assert "BEGIN TRANSACTION" in sql and "COMMIT TRANSACTION" in sql, sql
        if self.fail_transaction:
            raise RuntimeError("transaction failed")
        for target, temp in re.findall(r"INSERT INTO `([^`]+)`.*?FROM `([^`]+)`", sql, re.S):
            self.tables.setdefault(target, []).extend(
                {"export_id": params["export_id"], "export_name": params["export_name"],
                 "export_as_of": params["export_as_of"], "file_path": r["file_path"],
                 "row_number": r["row_number"], "record": r["record"]}
                for r in self.tables[temp])
        return Done()


class FakeDataform:
    def __init__(self):
        self.runs, self.fail = [], False

    def get(self, url, timeout=None):
        if self.fail:
            return SimpleNamespace(raise_for_status=lambda: (_ for _ in ()).throw(RuntimeError("500")))
        return SimpleNamespace(raise_for_status=lambda: None,
                               json=lambda: {"releaseCompilationResult": "compilations/c1"})

    def post(self, url, timeout=None, json=None):
        self.runs.append(json)
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"name": "invocations/i1"})


@pytest.fixture
def env(monkeypatch):
    bucket = FakeBucket(BUCKET)
    bq = FakeBigQuery()
    dataform = FakeDataform()
    monkeypatch.setattr(main.storage, "Client", lambda project=None: FakeStorage(bucket))
    monkeypatch.setattr(main.bigquery, "Client", lambda project=None: bq)
    monkeypatch.setattr(main, "dataform_session", lambda: dataform)
    return SimpleNamespace(bucket=bucket, bq=bq, dataform=dataform)


def drop(env, name, files):
    """Puts a zip at `name` in the bucket and runs the function on its finalize event."""
    content = make_zip(files) if isinstance(files, dict) else files
    env.bucket.put(name, content)
    main.ingest_uber(SimpleNamespace(data={"bucket": BUCKET, "name": name, "md5Hash": md5_base64(content)}))
    return content
