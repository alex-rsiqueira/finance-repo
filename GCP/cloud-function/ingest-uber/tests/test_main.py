from datetime import date, datetime, timezone

import pytest

import main
from conftest import (LOG, NEW_EXPORT, OLD_EXPORT, ORDERS, TRIPS_V1, drop, make_zip)

TRIPS = "test-project.raw.tb_uber_trip_rows"
ORDERS_TABLE = "test-project.raw.tb_uber_eats_order_rows"
DETAILS_TABLE = "test-project.raw.tb_uber_eats_order_detail_rows"


def test_old_format_zip_reads_trips_and_orders():
    rows = main.read_export(make_zip(OLD_EXPORT))
    assert [r[0] for r in rows["trips"]] == ["Uber Data/Rider/trips_data-0.csv"] * 2
    assert [r[0] for r in rows["eats_orders"]] == ["Uber Data/Eats/user_orders-0.csv"]
    assert rows["eats_order_details"] == []
    assert set(rows) == {"trips", "eats_orders", "eats_order_details"}


def test_new_format_zip_reads_three_kinds():
    rows = main.read_export(make_zip(NEW_EXPORT))
    assert rows["trips"][0][0] == "Uber Data/Rider/rider_lifetime_trips-0.csv"
    assert rows["eats_order_details"][0][2]["Order_UUID"] == "3977fccc-aba0-4a11-a494-4dcb25c35733"


def test_macosx_entries_are_skipped():
    # macOS's resource fork shadows the trips file's own path under __MACOSX/
    files = dict(OLD_EXPORT, **{"__MACOSX/Uber Data/Rider/trips_data-0.csv": b"\x00\x05\x16\x07\xff\xfe"})
    rows = main.read_export(make_zip(files))
    assert len(rows["trips"]) == 2
    assert all(not r[0].startswith("__MACOSX/") for kind in rows.values() for r in kind)


def test_zip_without_trips_fails_and_names_its_files(env):
    files = {"Uber Data/Eats/user_orders-0.csv": ORDERS, "Uber Data/Rider/rider_app_analytics-0.csv": "a\n1\n"}
    with pytest.raises(main.MissingFiles) as error:
        main.read_export(make_zip(files))
    assert "Uber Data/Eats/user_orders-0.csv" in str(error.value)
    assert "Uber Data/Rider/rider_app_analytics-0.csv" in str(error.value)

    drop(env, "inbox/broken.zip", files)
    assert TRIPS not in env.bq.tables and ORDERS_TABLE not in env.bq.tables
    [log] = env.bq.tables[LOG]
    assert log["status"] == "error" and "rider_app_analytics-0.csv" in log["error_message"]
    assert "inbox/broken.zip" in env.bucket.objects


def test_record_keeps_every_column_and_empty_cells():
    trips = 'city_name,request_timestamp_utc,card_number,note\n' \
            'Lisbon,2026-10-13T10:26:14.000Z,,"two\nlines"\n' \
            'Porto,2026-10-14T10:26:14.000Z,1234,x\n'
    rows = main.read_export(make_zip({"U/Rider/rider_lifetime_trips-0.csv": trips,
                                      "U/Eats/user_orders-0.csv": ORDERS}))
    first, second = rows["trips"]
    assert first[2] == {"city_name": "Lisbon", "request_timestamp_utc": "2026-10-13T10:26:14.000Z",
                        "card_number": "", "note": "two\nlines"}
    assert (first[1], second[1]) == (1, 2)
    assert second[2]["card_number"] == "1234"


def test_byte_order_mark_is_not_in_the_first_column():
    rows = main.read_export(make_zip({"U/Rider/trips_data-0.csv": "﻿" + TRIPS_V1,
                                      "U/Eats/user_orders-0.csv": "﻿" + ORDERS}))
    assert list(rows["trips"][0][2])[0] == "city"
    assert list(rows["eats_orders"][0][2])[0] == "City_Name"


def test_export_as_of_is_the_latest_trip_or_order():
    rows = main.read_export(make_zip(OLD_EXPORT))
    # trips end 2024-10-14T07:13:54, the order is 2024-10-18T20:33:13
    assert main.export_as_of(rows) == datetime(2024, 10, 18, 20, 33, 13, tzinfo=timezone.utc)
    later_trip = TRIPS_V1 + "Lisbon,uberX,completed,2024-10-20T08:00:00.000Z,Euro,7\n"
    rows = main.read_export(make_zip(dict(OLD_EXPORT, **{"Uber Data/Rider/trips_data-0.csv": later_trip})))
    assert main.export_as_of(rows) == datetime(2024, 10, 20, 8, 0, 0, tzinfo=timezone.utc)


def test_names_outside_inbox_are_ignored(env):
    drop(env, "archive/2026-10-26/x.zip", NEW_EXPORT)
    drop(env, "inbox/notes.txt", b"not a zip")
    assert env.bq.tables == {} and env.bq.queries == []
    assert set(env.bucket.objects) == {"archive/2026-10-26/x.zip", "inbox/notes.txt"}


def test_a_new_export_loads_three_tables_and_is_archived(env):
    content = drop(env, "inbox/Uber Data Request A895E707 2.zip", NEW_EXPORT)
    assert len(env.bq.tables[TRIPS]) == 1 and len(env.bq.tables[ORDERS_TABLE]) == 1
    assert len(env.bq.tables[DETAILS_TABLE]) == 1
    row = env.bq.tables[TRIPS][0]
    assert row["export_name"] == "Uber Data Request A895E707 2.zip"
    assert row["export_as_of"] == datetime(2026, 10, 26, 13, 50, 47, tzinfo=timezone.utc)
    assert '"card_number": "1234"' in row["record"]
    assert set(env.bucket.objects) == {"archive/2026-10-26/Uber Data Request A895E707 2.zip"}
    log = env.bq.tables[LOG][0]
    assert log["status"] == "success" and log["source_id"] == "uber" and log["rows_loaded"] == 3
    assert not [t for t in env.bq.tables if "_tmp_" in t]


def test_zip_already_loaded_is_only_archived(env):
    drop(env, "inbox/a.zip", NEW_EXPORT)
    queries = len(env.bq.queries)
    drop(env, "inbox/a.zip", NEW_EXPORT)
    assert len(env.bq.tables[TRIPS]) == 1
    assert [q for q in env.bq.queries[queries:] if "TRANSACTION" in q] == []
    assert set(env.bucket.objects) == {"archive/2026-10-26/a.zip"}


def test_archive_never_overwrites_a_different_zip(env):
    first = drop(env, "inbox/a.zip", NEW_EXPORT)
    other = dict(NEW_EXPORT, **{"Uber Data/Rider/rider_lifetime_trips-0.csv":
                                "city_name,request_timestamp_utc\nPorto,2026-10-01T10:00:00.000Z\n"})
    second = drop(env, "inbox/a.zip", other)
    export_id = main.hashlib.md5(second).hexdigest()
    assert env.bucket.objects["archive/2026-10-26/a.zip"].content == first
    assert env.bucket.objects[f"archive/2026-10-26/a ({export_id[:8]}).zip"].content == second
    assert main.archive_name(date(2026, 10, 26), "a.zip", export_id, None) == "archive/2026-10-26/a.zip"


def test_failed_insert_writes_nothing_and_keeps_the_zip(env):
    env.bq.fail_transaction = True
    drop(env, "inbox/a.zip", NEW_EXPORT)
    assert TRIPS not in env.bq.tables and ORDERS_TABLE not in env.bq.tables
    [log] = env.bq.tables[LOG]
    assert log["status"] == "error" and "transaction failed" in log["error_message"]
    assert set(env.bucket.objects) == {"inbox/a.zip"}
    assert not [t for t in env.bq.tables if "_tmp_" in t]


def test_dataform_run_failure_does_not_fail_the_load(env):
    drop(env, "inbox/a.zip", NEW_EXPORT)
    assert env.dataform.runs[0]["invocationConfig"] == {"includedTags": ["uber"]}
    assert [r["status"] for r in env.bq.tables[LOG]] == ["success", "dataform_run_started"]

    env.dataform.fail = True
    drop(env, "inbox/b.zip", OLD_EXPORT)
    assert [r["status"] for r in env.bq.tables[LOG]][2:] == ["success", "dataform_run_error"]
