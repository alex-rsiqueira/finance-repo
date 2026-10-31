import json
from datetime import datetime, timedelta, timezone

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


def test_rate_limit_is_not_retried_recorded_at_zero_and_not_a_failure(env):
    """plans/bank-refresh.md D-10: the account was read by a refresh earlier in its window."""
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1, transactions=[transaction("1")])
    env.bank.answers[(A1, "transactions")] = [http_error(429, retry_after=5)]
    assert "1 accounts" in env.run()
    assert env.bank.calls.count((A1, "transactions")) == 1
    assert env.sleeps == []
    assert env.rows("tb_nordigen_transactions") == []
    assert len(env.rows("tb_nordigen_meta")) == 1
    assert len(env.rows("tb_nordigen_details")) == 1
    assert len(env.rows("tb_nordigen_balances")) == 2
    [limited] = env.log("rate_limited")
    assert json.loads(limited["description"])["account_id"] == A1
    assert env.log("table_processing_error") == []
    assert env.log("ingestion_complete")[0]["type"] == "Info"
    [row] = [r for r in env.rows("tb_nordigen_rate_limits") if r["scope"] == "transactions"]
    assert (row["status"], row["remaining"], row["limit_count"], row["reset_at"]) == ("rate_limited", 0, None, None)


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


# --- the bank run after a load (finance-backend plans/bank-reads-from-tables.md §3.2) ------

BANK_RUN = {"compilationResult": "projects/test-project/locations/us-central1/repositories/dataform-repo/"
                                 "compilationResults/c-1", "invocationConfig": {"includedTags": ["bank"]}}


def test_a_load_that_wrote_rows_starts_one_bank_run_from_the_release(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1, transactions=[transaction("1")])
    env.run()
    assert env.dataform.runs == [BANK_RUN]
    [started] = env.log("dataform_run_started")
    details = json.loads(started["description"])
    assert started["type"] == "Info" and details["invocation"].endswith("/workflowInvocations/run-1")
    assert [r["error_code"] for r in env.log()][-2:] == ["ingestion_complete", "dataform_run_started"]


def test_a_load_where_every_table_failed_starts_no_run(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1)
    for endpoint in ("metadata", "details", "balances", "transactions"):
        env.bank.answers[(A1, endpoint)] = [http_error(500)] * 4
    with pytest.raises(RuntimeError):
        env.run()
    assert env.dataform.runs == []


def test_a_table_that_failed_beside_others_that_loaded_still_starts_the_run(env):
    """What did load reaches the Bank tab (decided 2026-10-24); the run is still reported failed."""
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1, transactions=[transaction("1")])
    env.bank.answers[(A1, "transactions")] = [http_error(401)]
    with pytest.raises(RuntimeError, match="tb_nordigen_transactions"):
        env.run()
    assert env.dataform.runs == [BANK_RUN]


def test_a_run_that_failed_before_loading_starts_none(env, monkeypatch):
    import conftest

    monkeypatch.setattr(conftest.FakeBigQuery, "credentials", [])
    with pytest.raises(RuntimeError):
        env.run()
    assert env.dataform.runs == []


@pytest.mark.parametrize("failure", [403, requests.ConnectionError("dataform unreachable")])
def test_a_run_that_cannot_start_is_logged_and_the_load_still_succeeds(env, failure):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1, transactions=[transaction("1")])
    env.dataform.failure = failure
    assert "1 accounts" in env.run()
    assert env.dataform.runs == []
    [error] = env.log("dataform_run_error")
    assert error["type"] == "Error"
    if isinstance(failure, int):
        assert str(failure) in error["message"], "the answer's status is what is logged"


def test_an_invocation_the_api_refuses_is_logged_and_the_load_still_succeeds(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1, transactions=[transaction("1")])
    env.dataform.run_failure = 403
    assert "1 accounts" in env.run()
    assert env.log("dataform_run_started") == []
    assert "403" in env.log("dataform_run_error")[0]["message"]


def test_a_release_with_no_compilation_is_logged_and_starts_nothing(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1, transactions=[transaction("1")])
    env.dataform.compilation = None
    env.run()
    assert env.dataform.runs == []
    assert "release-02 has no compilation result" in env.log("dataform_run_error")[0]["message"]


# --- the allowance and the app's refresh (finance-backend plans/bank-refresh.md) -------------

def limits(env, **match):
    return [r for r in env.rows("tb_nordigen_rate_limits") if all(r[k] == v for k, v in match.items())]


def test_every_counted_call_records_the_allowance_it_reported(env):
    env.bank.requisitions = [linked(A1, A2)]
    env.bank.account(A1, transactions=[transaction("1")])
    env.bank.account(A2, transactions=[transaction("2")])
    env.run()
    rows = env.rows("tb_nordigen_rate_limits")
    assert sorted((r["account_id"], r["scope"]) for r in rows) == sorted(
        (a, s) for a in (A1, A2) for s in ("details", "balances", "transactions")), "no row for metadata"
    for r in rows:
        assert (r["limit_count"], r["remaining"], r["status"], r["client_id"]) == (4, 3, "ok", "client-1")
        assert r["request_id"] is None and r["dtinsert"] == "2026-10-07 07:00:00"
        observed = datetime.fromisoformat(r["observed_at"])
        assert datetime.fromisoformat(r["reset_at"]) - observed == timedelta(seconds=21243)


def test_a_second_run_records_what_the_first_left(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1)
    env.run()
    env.run({"account_ids": [A1], "request_id": "r-1"})
    assert [r["remaining"] for r in limits(env, scope="balances")] == [3, 2]


def test_an_answer_without_the_headers_records_unknown(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1)
    env.bank.headers = False
    env.run()
    assert {(r["limit_count"], r["remaining"], r["reset_at"], r["status"])
            for r in env.rows("tb_nordigen_rate_limits")} == {(None, None, None, "ok")}


def test_a_rate_limit_answer_that_reports_the_allowance_keeps_its_reset(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1)
    env.bank.answers[(A1, "balances")] = [http_error(429, headers={
        "http_x_ratelimit_account_success_limit": "4", "http_x_ratelimit_account_success_remaining": "0",
        "http_x_ratelimit_account_success_reset": "600"})]
    env.run()
    [row] = limits(env, scope="balances")
    assert (row["status"], row["remaining"], row["limit_count"]) == ("rate_limited", 0, 4)
    assert datetime.fromisoformat(row["reset_at"]) - datetime.fromisoformat(row["observed_at"]) == timedelta(seconds=600)


def test_a_refresh_reads_only_its_accounts_balances_and_transactions(env):
    env.bank.requisitions = [linked(A1, A2)]
    env.bank.account(A1, transactions=[transaction("1")])
    env.bank.account(A2, transactions=[transaction("2")])
    assert "1 accounts" in env.run({"account_ids": [A1], "request_id": "r-1"})
    assert sorted(env.bank.calls) == [(A1, "balances"), (A1, "transactions")]
    assert env.rows("tb_nordigen_meta") == [] and env.rows("tb_nordigen_details") == []
    assert {r["account_id"] for r in env.rows("tb_nordigen_transactions")} == {A1}
    assert {(r["scope"], r["request_id"]) for r in env.rows("tb_nordigen_rate_limits")} == {
        ("balances", "r-1"), ("transactions", "r-1")}
    [complete] = env.log("ingestion_complete")
    details = json.loads(complete["description"])
    assert (details["request_id"], details["requested"], details["skipped"]) == ("r-1", [A1], [])
    assert json.loads(env.log("dataform_run_started")[0]["description"])["request_id"] == "r-1"


def test_a_refresh_skips_an_account_no_linked_requisition_holds(env):
    env.bank.requisitions = [linked(A2, status="EX"), linked(A1)]
    env.bank.account(A1)
    env.bank.account(A2)
    env.run({"account_ids": [A2, A1, A1], "request_id": "r-2"})
    assert {a for a, _ in env.bank.calls} == {A1}
    assert env.bank.calls.count((A1, "balances")) == 1, "a repeated id is read once"
    [unknown] = env.log("refresh_account_unknown")
    assert json.loads(unknown["description"]) == {
        "account_id": A2, "request_id": "r-2",
        "error": "no linked requisition of an active credential holds this account: not read"}
    details = json.loads(env.log("ingestion_complete")[0]["description"])
    assert details["skipped"] == [A2] and env.log("ingestion_complete")[0]["type"] == "Info"


@pytest.mark.parametrize("message", [{"account_ids": [], "request_id": "r-3"}, {"account_ids": "acc"}])
def test_a_refresh_naming_no_account_reads_nothing(env, message):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1)
    env.run(message)
    assert env.bank.calls == []
    assert env.dataform.runs == []


@pytest.mark.parametrize("message", [None, {}, "trigger"])
def test_any_other_message_is_a_full_run(env, message):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1)
    env.run(message)
    assert sorted(e for _, e in env.bank.calls) == ["balances", "details", "metadata", "transactions"]
    assert {r["request_id"] for r in env.rows("tb_nordigen_rate_limits")} == {None}


def test_a_failure_writing_the_allowance_fails_the_run_and_keeps_the_bank_rows(env, monkeypatch):
    import conftest

    original = conftest.FakeBigQuery.load_table_from_json

    def refuse_limits(self, rows, ref, job_config=None):
        if ref == conftest.LIMITS:
            raise RuntimeError("Not found: Table tb_nordigen_rate_limits")
        return original(self, rows, ref, job_config)

    monkeypatch.setattr(conftest.FakeBigQuery, "load_table_from_json", refuse_limits)
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1, transactions=[transaction("1")])
    with pytest.raises(RuntimeError, match="tb_nordigen_rate_limits"):
        env.run()
    assert len(env.rows("tb_nordigen_transactions")) == 1
    assert env.dataform.runs == [BANK_RUN]


def test_an_error_answer_that_is_not_json_is_raised_with_its_text(env):
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1)
    env.bank.answers[(A1, "details")] = [http_error(403, body="<html>Forbidden</html>")]
    with pytest.raises(RuntimeError, match="Forbidden"):
        env.run()
    assert env.bank.calls.count((A1, "details")) == 1


def test_a_rate_limit_from_bigquery_is_not_read_as_the_banks(env, monkeypatch):
    """Only GoCardless's 429 (a requests.HTTPError) is D-10's; anything else stays a failure."""
    import main
    from types import SimpleNamespace

    from google.api_core.exceptions import TooManyRequests

    def busy(*args, **kwargs):
        raise TooManyRequests("rate limited by BigQuery", response=SimpleNamespace(status_code=429, headers={}))

    monkeypatch.setattr(main, "load", busy)
    env.bank.requisitions = [linked(A1)]
    env.bank.account(A1)
    with pytest.raises(RuntimeError, match="BigQuery"):
        env.run()
    assert env.log("rate_limited") == []


def test_allowance_reads_only_the_account_success_headers():
    import gocardless

    assert gocardless.allowance({
        "http_x_ratelimit_limit": "100", "http_x_ratelimit_remaining": "99",
        "http_x_ratelimit_account_success_limit": "4", "http_x_ratelimit_account_success_remaining": "2",
        "http_x_ratelimit_account_success_reset": "21243"}) == {"limit": 4, "remaining": 2, "reset_seconds": 21243}
    assert gocardless.allowance({"http_x_ratelimit_account_success_remaining": ""}) == {}
    assert gocardless.allowance({}) == {}
