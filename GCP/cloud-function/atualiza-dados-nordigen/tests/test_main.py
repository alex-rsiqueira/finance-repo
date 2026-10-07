import json
from datetime import datetime, timezone

import pytest
import requests

from conftest import http_error, transaction

A1 = "4b4a4651-acc1"
A2 = "4b4a4651-acc2"


def linked(*accounts, status="LN"):
    return {"id": f"req-{status}-{len(accounts)}", "status": status, "accounts": list(accounts)}


def test_no_linked_requisition_fails_and_logs_once(env):
    env.bank.requisitions = [linked(A1, status="EX")]
    with pytest.raises(RuntimeError, match="authorised again"):
        env.run()
    assert len(env.log("client_initialization_error")) == 1
    assert env.bank.calls == []
    assert env.merges == []


def test_only_linked_requisitions_load(env):
    env.bank.requisitions = [linked(A2, status="EX"), linked(A1)]
    env.bank.account(A1, transactions=[transaction("1")])
    env.run()
    assert {account for account, _ in env.bank.calls} == {A1}
    assert {r["account_id"] for r in env.rows("tb_nordigen_transactions")} == {A1}


def test_rate_limit_is_not_retried_and_the_run_fails(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1, transactions=[transaction("1")])
    env.bank.answers[(A1, "transactions")] = [http_error(429, retry_after=5)]
    with pytest.raises(RuntimeError, match="tb_nordigen_transactions"):
        env.run()
    assert env.bank.calls.count((A1, "transactions")) == 1
    assert env.sleeps == []
    assert env.rows("tb_nordigen_transactions") == []
    assert len(env.rows("tb_nordigen_meta")) == 1
    assert len(env.rows("tb_nordigen_details")) == 1
    assert len(env.rows("tb_nordigen_balances")) == 2
    assert len(env.log("table_processing_error")) == 1
    assert env.log("ingestion_complete")[0]["type"] == "Error"


def test_expired_access_is_not_retried(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1)
    env.bank.answers[(A1, "balances")] = [http_error(401)]
    with pytest.raises(RuntimeError):
        env.run()
    assert env.bank.calls.count((A1, "balances")) == 1
    assert env.sleeps == []


def test_server_error_is_retried_then_loads(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1, transactions=[transaction("1")])
    env.bank.answers[(A1, "transactions")] = [
        http_error(503, retry_after=7),
        {"transactions": {"booked": [transaction("1")], "pending": []}},
    ]
    env.run()
    assert env.bank.calls.count((A1, "transactions")) == 2
    assert env.sleeps == [7]
    assert len(env.rows("tb_nordigen_transactions")) == 1


def test_connection_error_is_retried_and_gives_up_after_three_attempts(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1)
    env.bank.answers[(A1, "details")] = [requests.ConnectionError("reset")]
    with pytest.raises(RuntimeError, match="tb_nordigen_details"):
        env.run()
    assert env.bank.calls.count((A1, "details")) == 3
    assert env.sleeps == [2, 4]


def test_no_transactions_is_a_success_without_a_merge(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1, transactions=[])
    env.run()
    assert not any("tb_nordigen_transactions`" in m.split("USING")[0] for m in env.merges)
    assert env.rows("tb_nordigen_transactions") == []
    assert env.log("ingestion_complete")[0]["type"] == "Info"
    assert not any("_tmp_" in name for name in env.tables)


def test_same_day_rerun_keeps_both_balance_positions_and_adds_no_transaction(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1, transactions=[transaction("1"), transaction("2")])
    env.run_at(datetime(2026, 10, 7, 7, 0, 0, tzinfo=timezone.utc))
    first = [dict(r) for r in env.rows("tb_nordigen_balances")]

    env.bank.answers[(A1, "balances")] = [{"balances": [
        {"balanceAmount": {"amount": "80.00", "currency": "EUR"}, "balanceType": "closingBooked"},
        {"balanceAmount": {"amount": "70.00", "currency": "EUR"}, "balanceType": "interimAvailable"},
    ]}]
    env.run_at(datetime(2026, 10, 7, 15, 30, 0, tzinfo=timezone.utc))

    balances = env.rows("tb_nordigen_balances")
    assert len(balances) == 4
    assert balances[:2] == first
    assert {(r["dtinsert"], r["balanceAmount_amount"]) for r in balances[2:]} == {
        ("2026-10-07 15:30:00", "80.00"), ("2026-10-07 15:30:00", "70.00")}
    assert len(env.rows("tb_nordigen_transactions")) == 2
    assert len(env.rows("tb_nordigen_meta")) == 1
    assert env.rows("tb_nordigen_meta")[0]["dtinsert"] == "2026-10-07 15:30:00"


def test_every_row_of_a_run_carries_the_same_dtinsert(env):
    env.bank.requisitions = [linked(A1, A2)]
    env.bank.account(A1, transactions=[transaction("1")])
    env.bank.account(A2, transactions=[transaction("2")])
    env.run_at(datetime(2026, 10, 7, 7, 0, 0, tzinfo=timezone.utc))
    written = [r for t in ("meta", "details", "balances", "transactions") for r in env.rows(f"tb_nordigen_{t}")]
    assert len(written) == 2 + 2 + 4 + 2
    assert {r["dtinsert"] for r in written} == {"2026-10-07 07:00:00"}


def test_transaction_without_any_reference_still_loads(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1, transactions=[
        transaction("1", transactionId=None),
        transaction("2", transactionId=None, entryReference="E2"),
    ])
    env.run()
    by_id = {r["internalTransactionId"]: r for r in env.rows("tb_nordigen_transactions")}
    assert by_id["1"]["transactionId"] is None
    assert by_id["2"]["transactionId"] == "E2"


def test_two_accounts_on_one_requisition_do_not_collide(env):
    env.bank.requisitions = [linked(A1, A2)]
    env.bank.account(A1, transactions=[transaction("same-id")])
    env.bank.account(A2, transactions=[transaction("same-id")])
    env.run()
    for table, per_account in (("meta", 1), ("details", 1), ("balances", 2), ("transactions", 1)):
        rows = env.rows(f"tb_nordigen_{table}")
        assert sorted(r["account_id"] for r in rows) == sorted([A1] * per_account + [A2] * per_account), table


def test_fields_outside_the_columns_are_kept_in_payload(env):
    env.bank.requisitions = [linked(A1)]
    record = transaction("1", creditorName="CONTINENTE", bankTransactionCode="PMNT",
                         remittanceInformationUnstructuredArray=["COMPRA", "CONTINENTE"])
    env.bank.account(A1, transactions=[record])
    env.run()
    row = env.rows("tb_nordigen_transactions")[0]
    assert row["payload"] == record
    assert row["transactionAmount_amount"] == "-10.00"
    assert row["client_id"] == "client-1"
    assert "creditorName" not in row


def test_merge_sql_matches_each_table_on_its_key(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1, transactions=[transaction("1")])
    env.run()
    merge = {m.split("`")[1].split(".")[-1]: m for m in env.merges}
    assert "WHEN MATCHED" in merge["tb_nordigen_meta"] and "WHEN MATCHED" in merge["tb_nordigen_details"]
    assert "WHEN MATCHED" not in merge["tb_nordigen_balances"]
    assert "WHEN MATCHED" not in merge["tb_nordigen_transactions"]
    assert "target.`dtinsert` = source.`dtinsert`" in merge["tb_nordigen_balances"]
    assert "PARSE_JSON(source.`payload`)" in merge["tb_nordigen_transactions"]


def test_a_failing_log_table_does_not_hide_the_result(env, monkeypatch):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1)
    import conftest

    original = conftest.FakeBigQuery.load_table_from_json

    def refuse_log(self, rows, ref, job_config=None):
        if ref == conftest.LOG:
            raise RuntimeError("Not found: Table tb_nordigen_ingestion_log")
        return original(self, rows, ref, job_config)

    monkeypatch.setattr(conftest.FakeBigQuery, "load_table_from_json", refuse_log)
    assert "1 accounts" in env.run()
    assert len(env.rows("tb_nordigen_balances")) == 2


def test_no_credentials_is_a_failed_run(env, monkeypatch):
    import conftest

    monkeypatch.setattr(conftest.FakeBigQuery, "credentials", [])
    with pytest.raises(RuntimeError, match="No active Nordigen credentials"):
        env.run()
    assert len(env.log("fatal_error")) == 1


def test_flatten_turns_nested_values_into_strings():
    import main

    assert main.flatten({"a": {"b": 1, "c": None}, "d": [1, 2], "e": "x", "f": True}) == {
        "a_b": "1", "d": "[1, 2]", "e": "x", "f": "true"}
    assert json.loads(main.flatten({"d": [1, 2]})["d"]) == [1, 2]
