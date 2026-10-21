import pytest

from conftest import FakeRequest
import main


def test_a_call_without_the_secret_header_is_refused_and_nothing_is_read_or_written(env):
    env.api.add_item("item-1")
    assert env.call("item/created", secret=None) == ("Forbidden", 403)
    assert env.call("item/created", secret="wrong") == ("Forbidden", 403)
    assert env.api.calls == [] and env.registry() == [] and env.log() == []


def test_item_created_reads_the_item_back_and_registers_it(env):
    env.api.add_item("item-1", connector="Inter")
    assert env.call("item/created") == ("OK", 200)
    [row] = env.registry()
    assert (row["item_id"], row["connector_name"], row["source"], row["deleted_at"], row["last_event"]) == \
        ("item-1", "Inter", "webhook", None, "item/created")
    assert row["payload"]["connector"]["name"] == "Inter"
    assert [c[1] for c in env.api.calls] == ["/auth", "/items/item-1"]
    assert [r["end_point"] for r in env.log("item_registered")] == ["item-1"]


def test_an_item_pluggy_does_not_answer_is_not_registered(env):
    """A forged or stale item id: the registry only ever holds what GET /items confirmed."""
    assert env.call("item/created", item_id="ghost") == ("OK", 200)
    assert env.registry() == []
    [error] = env.log("webhook_error")
    assert "404" in error["message"] and error["end_point"] == "ghost"


def test_item_deleted_marks_the_row_and_keeps_it(env):
    env.api.add_item("item-1")
    env.call("item/created")
    env.call("item/deleted")
    [row] = env.registry()
    assert row["deleted_at"] is not None and row["last_event"] == "item/deleted"
    assert row["connector_name"] == "Nubank" and row["payload"] is not None


def test_item_deleted_for_an_unknown_item_still_records_it_as_deleted(env):
    """A connection made before the webhook existed: the row says it is gone, and the daily
    load, which reads only rows with no deleted_at, never tries it."""
    env.call("item/deleted", item_id="old")
    [row] = env.registry()
    assert row["item_id"] == "old" and row["deleted_at"] is not None and row["payload"] is None


def test_item_created_again_clears_deleted_at(env):
    env.api.add_item("item-1")
    env.call("item/created")
    env.call("item/deleted")
    env.call("item/created")
    [row] = env.registry()
    assert row["deleted_at"] is None


def test_another_event_only_notes_itself_on_a_registered_item(env):
    env.api.add_item("item-1")
    env.call("item/created")
    env.call("item/error")
    [row] = env.registry()
    assert row["last_event"] == "item/error" and row["deleted_at"] is None
    env.call("item/updated", item_id="unknown")
    assert len(env.registry()) == 1
    assert [c[1] for c in env.api.calls] == ["/auth", "/items/item-1"]


def test_a_body_without_event_or_item_is_ignored(env):
    assert main.main(FakeRequest({"hello": 1})) == ("Ignored", 200)
    assert main.main(FakeRequest({"event": "item/created"})) == ("Ignored", 200)
    assert env.registry() == [] and env.queries == []


def test_a_failing_write_is_answered_200_and_logged(env, monkeypatch):
    env.api.add_item("item-1")

    def refuse(self, sql, job_config=None):
        raise RuntimeError("quota")
    import conftest
    monkeypatch.setattr(conftest.FakeBigQuery, "query", refuse)
    assert env.call("item/created") == ("OK", 200)
    [error] = env.log("webhook_error")
    assert error["message"] == "quota"


def test_missing_configuration_fails_loudly(env, monkeypatch):
    monkeypatch.setattr(main, "EXPECTED_SECRET", "")
    with pytest.raises(RuntimeError, match="PLUGGY_WEBHOOK_SECRET"):
        env.call("item/created")
