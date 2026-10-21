from datetime import datetime, timezone

import pytest
import requests

from conftest import ITEM_1, ITEM_2, bill, investment_transaction, transaction

CHECKING = "acc-checking"
CARD = "acc-card"
FIRST = datetime(2026, 10, 11, 7, 15, 0, tzinfo=timezone.utc)
SECOND = datetime(2026, 10, 12, 7, 15, 0, tzinfo=timezone.utc)


def one_item(env, transactions=None):
    env.api.add_item(ITEM_1)
    env.api.add_account(ITEM_1, CHECKING, transactions=transactions or [[transaction("t1")]])


def test_auth_refused_fails_at_once_without_retry(env):
    one_item(env)
    env.api.failures["/auth"] = [401]
    with pytest.raises(requests.HTTPError):
        env.run()
    assert len(env.calls_to("/auth")) == 1
    assert env.sleeps == []
    assert len(env.log("fatal_error")) == 1
    assert env.merges == []


def test_item_not_updated_writes_its_snapshot_only_and_the_others_load(env):
    env.items(ITEM_1, ITEM_2)
    env.api.add_item(ITEM_1, status="LOGIN_ERROR")
    env.api.add_account(ITEM_1, "acc-of-item-1", transactions=[[transaction("x")]])
    env.api.add_item(ITEM_2)
    env.api.add_account(ITEM_2, CHECKING, transactions=[[transaction("t1")]])
    with pytest.raises(RuntimeError, match="item item-1: status LOGIN_ERROR"):
        env.run()
    assert [r["status"] for r in env.rows("tb_pluggy_items")] == ["LOGIN_ERROR", "UPDATED"]
    assert not any(q.get("itemId") == ITEM_1 for _, path, q in env.api.calls if path == "/accounts")
    assert {r["account_id"] for r in env.rows("tb_pluggy_transactions")} == {CHECKING}
    error = env.log("item_error")
    assert len(error) == 1 and "authorise again in Meu Pluggy" in error[0]["message"]
    assert error[0]["end_point"] == ITEM_1
    assert env.log("ingestion_complete")[0]["type"] == "Error"


def test_consent_ending_within_30_days_logs_an_error_and_the_run_succeeds(env):
    env.api.add_item(ITEM_1, consent="2026-11-10T07:14:59.000Z")
    env.api.add_account(ITEM_1, CHECKING, transactions=[[transaction("t1")]])
    env.run_at(FIRST)
    warning = env.log("consent_expiring")
    assert len(warning) == 1 and "2026-11-10" in warning[0]["message"]
    assert "authorise again in Meu Pluggy" in warning[0]["message"]
    assert len(env.rows("tb_pluggy_transactions")) == 1
    assert env.log("ingestion_complete")[0]["type"] == "Info"


def test_consent_ending_after_30_days_logs_nothing(env):
    env.api.add_item(ITEM_1, consent="2026-11-10T07:15:01.000Z")
    env.api.add_account(ITEM_1, CHECKING)
    env.run_at(FIRST)
    assert env.log("consent_expiring") == []


def test_an_item_refreshed_exactly_36_hours_ago_is_not_reported(env):
    env.api.add_item(ITEM_1, lastUpdatedAt="2026-10-09T19:15:00.000Z")
    env.api.add_account(ITEM_1, CHECKING)
    env.run_at(FIRST)
    assert env.log("item_stale") == []


def test_an_item_not_refreshed_for_36_hours_and_a_second_is_reported_and_still_loads(env):
    env.api.add_item(ITEM_1, lastUpdatedAt="2026-10-09T19:14:59.000Z")
    env.api.add_account(ITEM_1, CHECKING, transactions=[[transaction("t1")]])
    env.run_at(FIRST)
    stale = env.log("item_stale")
    assert len(stale) == 1 and stale[0]["end_point"] == ITEM_1
    assert stale[0]["message"] == ("not refreshed since 2026-10-09 19:14 UTC; "
                                   "check the connection in Meu Pluggy")
    assert len(env.rows("tb_pluggy_transactions")) == 1
    assert env.log("ingestion_complete")[0]["type"] == "Info"


def test_a_stale_item_beside_a_healthy_one_is_the_only_one_reported(env):
    env.items(ITEM_1, ITEM_2)
    env.api.add_item(ITEM_1, lastUpdatedAt="2026-10-08T07:15:00.000Z")
    env.api.add_account(ITEM_1, "acc-of-item-1", transactions=[[transaction("x")]])
    env.api.add_item(ITEM_2)
    env.api.add_account(ITEM_2, CHECKING, transactions=[[transaction("t1")]])
    env.run_at(FIRST)
    assert [r["end_point"] for r in env.log("item_stale")] == [ITEM_1]
    assert {r["account_id"] for r in env.rows("tb_pluggy_transactions")} == {"acc-of-item-1", CHECKING}
    assert env.log("ingestion_complete")[0]["type"] == "Info"


def test_server_error_is_retried_honouring_retry_after_then_loads(env):
    one_item(env)
    env.api.failures["/v2/transactions"] = [503]
    env.run()
    assert len(env.calls_to("/v2/transactions")) == 2
    assert env.sleeps == [5]
    assert len(env.rows("tb_pluggy_transactions")) == 1


def test_rate_limit_is_not_retried_and_the_run_fails(env):
    one_item(env)
    env.api.failures["/v2/transactions"] = [429]
    with pytest.raises(RuntimeError, match="item item-1: 429"):
        env.run()
    assert len(env.calls_to("/v2/transactions")) == 1
    assert env.sleeps == []
    assert env.rows("tb_pluggy_transactions") == []
    assert env.rows("tb_pluggy_accounts") == []
    assert len(env.rows("tb_pluggy_items")) == 1


def test_connection_error_is_retried_and_gives_up_after_three_attempts(env):
    one_item(env)
    env.api.failures["/accounts"] = [requests.ConnectionError("reset")] * 3
    with pytest.raises(RuntimeError, match="item item-1"):
        env.run()
    assert len(env.calls_to("/accounts")) == 3
    assert env.sleeps == [2, 4]


def test_transactions_over_two_cursor_pages_all_load_and_next_is_appended_as_is(env):
    one_item(env, transactions=[[transaction("t1"), transaction("t2")], [transaction("t3")]])
    env.run()
    assert sorted(r["id"] for r in env.rows("tb_pluggy_transactions")) == ["t1", "t2", "t3"]
    calls = env.calls_to("/v2/transactions")
    assert [q for _, _, q in calls] == [{"accountId": CHECKING}, {"accountId": CHECKING, "cursor": "1"}]


def test_bills_and_investments_over_two_pages(env):
    env.api.add_item(ITEM_1)
    env.api.add_account(ITEM_1, CARD, type="CREDIT", transactions=[[]],
                        bills=[[bill("b1"), bill("b2")], [bill("b3")]])
    env.api.add_investment(ITEM_1, "inv-1",
                           transactions=[[investment_transaction("it1")], [investment_transaction("it2")]])
    env.api.investments[ITEM_1].append([])
    env.api.add_investment(ITEM_1, "inv-2")
    env.run()
    assert sorted(r["id"] for r in env.rows("tb_pluggy_bills")) == ["b1", "b2", "b3"]
    assert {r["account_id"] for r in env.rows("tb_pluggy_bills")} == {CARD}
    assert sorted(r["investment_id"] for r in env.rows("tb_pluggy_investments")) == ["inv-1", "inv-2"]
    assert sorted((r["investment_id"], r["id"]) for r in env.rows("tb_pluggy_investment_transactions")) == [
        ("inv-1", "it1"), ("inv-1", "it2")]
    assert [q["page"] for _, _, q in env.calls_to("/bills")] == ["1", "2"]


def test_bills_are_read_for_credit_cards_only(env):
    env.api.add_item(ITEM_1)
    env.api.add_account(ITEM_1, CHECKING)
    env.api.add_account(ITEM_1, CARD, type="CREDIT", bills=[[bill("b1")]])
    env.run()
    assert [q["accountId"] for _, _, q in env.calls_to("/bills")] == [CARD]


def test_pending_transaction_posted_later_under_the_same_id_is_one_row_updated(env):
    one_item(env, transactions=[[transaction("t1", status="PENDING", date="2026-10-10T03:00:00.000Z")]])
    env.run_at(FIRST)
    env.api.transaction_pages[CHECKING] = [[transaction("t1", status="POSTED", date="2026-10-11T03:00:00.000Z")]]
    env.run_at(SECOND)
    rows = env.rows("tb_pluggy_transactions")
    assert len(rows) == 1
    assert (rows[0]["status"], rows[0]["date"], rows[0]["payload"]["status"]) == (
        "POSTED", "2026-10-11T03:00:00.000Z", "POSTED")
    assert (rows[0]["dtinsert"], rows[0]["last_seen_dtinsert"]) == ("2026-10-11 07:15:00", "2026-10-12 07:15:00")


def test_pending_transaction_not_returned_again_is_kept_with_its_last_seen_run(env):
    one_item(env, transactions=[[transaction("t1", status="PENDING"), transaction("t2")]])
    env.run_at(FIRST)
    env.api.transaction_pages[CHECKING] = [[transaction("t2")]]
    env.run_at(SECOND)
    by_id = {r["id"]: r for r in env.rows("tb_pluggy_transactions")}
    assert by_id["t1"]["status"] == "PENDING"
    assert by_id["t1"]["last_seen_dtinsert"] == "2026-10-11 07:15:00"
    assert by_id["t2"]["last_seen_dtinsert"] == "2026-10-12 07:15:00"


def test_same_day_rerun_adds_snapshots_but_no_transaction_rows(env):
    env.api.add_item(ITEM_1)
    env.api.add_account(ITEM_1, CHECKING, transactions=[[transaction("t1"), transaction("t2")]])
    env.api.add_account(ITEM_1, CARD, type="CREDIT", transactions=[[transaction("c1", amount=50.0)]],
                        bills=[[bill("b1")]])
    env.api.add_investment(ITEM_1, "inv-1", transactions=[[investment_transaction("it1")]])
    env.run_at(datetime(2026, 10, 11, 7, 15, 0, tzinfo=timezone.utc))
    env.api.accounts[ITEM_1][0]["balance"] = 999.0
    env.run_at(datetime(2026, 10, 11, 15, 30, 0, tzinfo=timezone.utc))

    assert len(env.rows("tb_pluggy_transactions")) == 3
    assert len(env.rows("tb_pluggy_bills")) == 1
    assert len(env.rows("tb_pluggy_investment_transactions")) == 1
    assert [r["dtinsert"] for r in env.rows("tb_pluggy_items")] == ["2026-10-11 07:15:00", "2026-10-11 15:30:00"]
    assert len(env.rows("tb_pluggy_investments")) == 2
    accounts = [(r["account_id"], r["dtinsert"], r["balance"]) for r in env.rows("tb_pluggy_accounts")]
    assert sorted(accounts) == [(CARD, "2026-10-11 07:15:00", "1234.5"), (CARD, "2026-10-11 15:30:00", "1234.5"),
                                (CHECKING, "2026-10-11 07:15:00", "1234.5"),
                                (CHECKING, "2026-10-11 15:30:00", "999.0")]


def test_account_without_transactions_and_card_without_bills_succeed_without_a_merge(env):
    env.api.add_item(ITEM_1)
    env.api.add_account(ITEM_1, CHECKING, transactions=[[]])
    env.api.add_account(ITEM_1, CARD, type="CREDIT", transactions=[[]], bills=[[]])
    assert "1 items" in env.run()
    for table in ("tb_pluggy_transactions", "tb_pluggy_bills", "tb_pluggy_investments",
                  "tb_pluggy_investment_transactions"):
        assert env.merged(table) == [], table
        assert env.rows(table) == [], table
    assert len(env.rows("tb_pluggy_accounts")) == 2
    assert env.log("ingestion_complete")[0]["type"] == "Info"
    assert not any("_tmp_" in name for name in env.tables)


def test_payment_data_with_payer_and_receiver_is_kept_whole_in_payload(env):
    payment = {"payer": {"name": "ALEXSANDER", "documentNumber": {"type": "CPF", "value": "000.000.000-00"},
                         "branchNumber": "0001", "accountNumber": "12345-6"},
               "receiver": {"name": "MERCADO LTDA", "documentNumber": {"type": "CNPJ", "value": "00.000.000/0001-00"}},
               "paymentMethod": "PIX", "referenceNumber": "E123"}
    record = transaction("t1", paymentData=payment)
    one_item(env, transactions=[[record]])
    env.run()
    assert env.rows("tb_pluggy_transactions")[0]["payload"] == record


def test_a_field_pluggy_adds_later_is_kept_in_payload(env):
    record = transaction("t1", newField={"nested": [1, 2]})
    one_item(env, transactions=[[record]])
    env.api.items[ITEM_1]["anotherNewField"] = "x"
    env.run()
    row = env.rows("tb_pluggy_transactions")[0]
    assert row["payload"]["newField"] == {"nested": [1, 2]}
    assert "newField" not in row
    assert env.rows("tb_pluggy_items")[0]["payload"]["anotherNewField"] == "x"


def test_every_row_of_a_run_carries_the_same_dtinsert(env):
    env.items(ITEM_1, ITEM_2)
    env.api.add_item(ITEM_1)
    env.api.add_account(ITEM_1, CARD, type="CREDIT", transactions=[[transaction("c1", amount=50.0)]],
                        bills=[[bill("b1")]])
    env.api.add_investment(ITEM_1, "inv-1", transactions=[[investment_transaction("it1")]])
    env.api.add_item(ITEM_2)
    env.api.add_account(ITEM_2, CHECKING, transactions=[[transaction("t1")]])
    env.run_at(FIRST)
    tables = ("items", "accounts", "transactions", "bills", "investments", "investment_transactions")
    written = [r for t in tables for r in env.rows(f"tb_pluggy_{t}")]
    assert len(written) == 2 + 2 + 2 + 1 + 1 + 1
    assert {r["dtinsert"] for r in written} == {"2026-10-11 07:15:00"}


def test_columns_are_read_from_the_record_as_strings(env):
    one_item(env, transactions=[[transaction("t1", amount=-12.3)]])
    env.run()
    item = env.rows("tb_pluggy_items")[0]
    assert (item["item_id"], item["connector_name"], item["status"]) == (ITEM_1, "MeuPluggy", "UPDATED")
    account = env.rows("tb_pluggy_accounts")[0]
    assert (account["account_id"], account["item_id"], account["balance"]) == (CHECKING, ITEM_1, "1234.5")
    row = env.rows("tb_pluggy_transactions")[0]
    assert (row["account_id"], row["id"], row["amount"], row["type"]) == (CHECKING, "t1", "-12.3", "DEBIT")


def test_merge_sql_matches_each_table_on_its_key(env):
    env.api.add_item(ITEM_1)
    env.api.add_account(ITEM_1, CARD, type="CREDIT", transactions=[[transaction("c1")]], bills=[[bill("b1")]])
    env.api.add_investment(ITEM_1, "inv-1", transactions=[[investment_transaction("it1")]])
    env.run()
    merge = {m.split("`")[1].split(".")[-1]: m for m in env.merges}
    for table, keys in (("tb_pluggy_items", ("item_id", "dtinsert")),
                        ("tb_pluggy_accounts", ("account_id", "dtinsert")),
                        ("tb_pluggy_investments", ("investment_id", "dtinsert"))):
        assert "WHEN MATCHED" not in merge[table], table
        assert all(f"target.`{k}` = source.`{k}`" in merge[table] for k in keys), table
    for table, keys in (("tb_pluggy_transactions", ("account_id", "id")),
                        ("tb_pluggy_bills", ("account_id", "id")),
                        ("tb_pluggy_investment_transactions", ("investment_id", "id"))):
        on = merge[table].split("ON", 1)[1].split("WHEN", 1)[0]
        assert all(f"target.`{k}` = source.`{k}`" in on for k in keys), table
        assert "dtinsert" not in on, table
        updated = merge[table].split("WHEN MATCHED THEN UPDATE SET", 1)[1].split("WHEN NOT MATCHED", 1)[0]
        assert "target.`last_seen_dtinsert`" in updated and "target.`dtinsert`" not in updated, table
    assert all("PARSE_JSON(source.`payload`)" in m for m in env.merges)


def test_no_item_anywhere_is_a_failed_run(env):
    env.items()
    with pytest.raises(RuntimeError, match="registry is empty and PLUGGY_ITEM_IDS is not set"):
        env.run()
    assert env.log("fatal_error")


# --- the item registry (plan pluggy-transactions §3.3) -----------------------------

def test_registered_items_load_without_the_variable(env):
    env.items()
    env.api.add_item(ITEM_1)
    env.api.add_account(ITEM_1, CHECKING, transactions=[[transaction("t1")]])
    env.register(ITEM_1)
    env.run()
    assert [r["item_id"] for r in env.rows("tb_pluggy_items")] == [ITEM_1]
    assert env.log("ingestion_complete")[0]["type"] == "Info"


def test_an_item_in_the_registry_and_the_variable_loads_once(env):
    one_item(env)
    env.register(ITEM_1)
    env.run()
    assert len(env.calls_to(f"/items/{ITEM_1}")) == 1
    assert len(env.rows("tb_pluggy_items")) == 1


def test_registry_items_come_first_then_the_variables_own(env):
    env.api.add_item(ITEM_1)
    env.api.add_item(ITEM_2)
    env.register(ITEM_2)
    env.items(ITEM_1)
    env.run()
    assert [c[1] for c in env.api.calls if c[1].startswith("/items/")] == [f"/items/{ITEM_2}", f"/items/{ITEM_1}"]


def test_a_registered_item_marked_deleted_is_not_read(env):
    one_item(env)
    env.api.add_item(ITEM_2)
    env.register(ITEM_2, deleted=True)
    env.run()
    assert env.calls_to(f"/items/{ITEM_2}") == []


def test_a_404_marks_the_item_deleted_and_the_run_succeeds(env):
    one_item(env)
    env.api.add_item(ITEM_2)
    env.register(ITEM_2)
    env.api.failures[f"/items/{ITEM_2}"] = [404]
    env.run()
    [gone] = [r for r in env.registry() if r["item_id"] == ITEM_2]
    assert gone["deleted_at"] == "2026-10-11 07:15:00" and gone["last_event"] == "load/404"
    assert [r["item_id"] for r in env.rows("tb_pluggy_items")] == [ITEM_1]
    assert env.log("item_error") == [] and env.log("item_gone")[0]["end_point"] == ITEM_2
    assert env.log("ingestion_complete")[0]["type"] == "Info"


def test_a_404_on_an_item_the_variable_names_writes_a_deleted_registry_row(env):
    one_item(env)
    env.items(ITEM_1, ITEM_2)
    env.api.failures[f"/items/{ITEM_2}"] = [404]
    env.run()
    [row] = env.registry()
    assert (row["item_id"], row["source"], row["deleted_at"]) == (ITEM_2, "manual", "2026-10-11 07:15:00")


def test_a_403_is_still_a_failure_not_a_deletion(env):
    one_item(env)
    env.api.add_item(ITEM_2)
    env.register(ITEM_2)
    env.api.failures[f"/items/{ITEM_2}"] = [403]
    with pytest.raises(RuntimeError, match=ITEM_2):
        env.run()
    assert env.registry()[0]["deleted_at"] is None


def test_a_failing_log_table_does_not_hide_the_result(env, monkeypatch):
    one_item(env)
    import conftest

    original = conftest.FakeBigQuery.load_table_from_json

    def refuse_log(self, rows, ref, job_config=None):
        if ref == conftest.LOG:
            raise RuntimeError("Not found: Table tb_pluggy_ingestion_log")
        return original(self, rows, ref, job_config)

    monkeypatch.setattr(conftest.FakeBigQuery, "load_table_from_json", refuse_log)
    assert "1 items" in env.run()
    assert len(env.rows("tb_pluggy_transactions")) == 1
