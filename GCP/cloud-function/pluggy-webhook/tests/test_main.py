import json

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


def test_a_trailing_newline_on_either_side_of_the_secret_still_matches(env, monkeypatch):
    """A secret stored from a pipe ends in a newline; Pluggy was given it stripped."""
    env.api.add_item("item-1")
    monkeypatch.setattr(main, "EXPECTED_SECRET", "the-secret")
    assert env.call("item/created", secret="the-secret\n") == ("OK", 200)
    assert len(env.registry()) == 1


def test_the_secret_is_stripped_when_read_from_the_environment(monkeypatch):
    import importlib
    monkeypatch.setenv("PLUGGY_WEBHOOK_SECRET", "abc123\n")
    assert importlib.reload(main).EXPECTED_SECRET == "abc123"


TOPIC = "projects/test-project/topics/the-topic"


def test_item_created_and_synced_asks_the_load_to_read_it(env):
    env.api.add_item("item-1")
    assert env.call("item/created") == ("OK", 200)
    assert env.published == [(TOPIC, {"item_ids": ["item-1"], "event_id": "evt-1"})]
    assert [r["end_point"] for r in env.log("load_requested")] == ["item-1"]


def test_a_new_item_is_in_the_registry_before_the_load_is_asked_to_read_it(env):
    """The load skips an item the registry does not hold."""
    import conftest
    env.api.add_item("item-1")
    env.call("item/created")
    assert conftest.FakePublisher.registered_when_published == [["item-1"]]


def test_item_created_still_syncing_is_registered_and_not_loaded(env):
    """Its item/updated, when the sync ends, asks for the read."""
    env.api.add_item("item-1", status="UPDATING", execution="LOGIN_IN_PROGRESS")
    assert env.call("item/created") == ("OK", 200)
    assert len(env.registry()) == 1
    assert env.published == [] and env.log("load_requested") == []


def test_item_created_that_pluggy_does_not_answer_asks_for_nothing(env):
    env.call("item/created", item_id="ghost")
    assert env.published == []


def test_item_updated_with_a_successful_sync_notes_the_event_and_asks_the_load_to_read_it(env):
    env.api.add_item("item-1")
    env.call("item/created")
    env.call("item/updated", eventId="evt-2")
    [row] = env.registry()
    assert row["last_event"] == "item/updated"
    assert env.published[-1] == (TOPIC, {"item_ids": ["item-1"], "event_id": "evt-2"})
    assert len(env.published) == 2


def test_item_updated_is_judged_from_the_item_read_back_never_from_the_payload(env):
    env.api.add_item("item-1")
    env.call("item/created")
    env.api.add_item("item-1", execution="ERROR")
    env.call("item/updated", executionStatus="SUCCESS", status="UPDATED")
    assert len(env.published) == 1
    assert [c[1] for c in env.api.calls].count("/items/item-1") == 2


@pytest.mark.parametrize("status, execution", [
    ("UPDATED", "PARTIAL_SUCCESS"),
    ("UPDATED", "ERROR"),
    ("LOGIN_ERROR", "SUCCESS"),
    ("OUTDATED", "SUCCESS"),
])
def test_item_updated_whose_item_has_not_synced_cleanly_asks_for_nothing(env, status, execution):
    env.api.add_item("item-1")
    env.call("item/created")
    env.api.add_item("item-1", status=status, execution=execution)
    env.call("item/updated")
    assert len(env.published) == 1
    assert env.registry()[0]["last_event"] == "item/updated"


def test_item_updated_for_an_item_the_registry_does_not_hold_reads_nothing_and_asks_for_nothing(env):
    """Meu Pluggy's own item behind a bank (no accounts of its own) sends item/updated; the
    load would skip it as unregistered, so it is not asked for."""
    env.api.add_item("wrapper")
    assert env.call("item/updated", item_id="wrapper") == ("OK", 200)
    assert env.api.calls == [] and env.published == [] and env.registry() == []


@pytest.mark.parametrize("event", ["item/error", "item/waiting_user_input", "item/login_succeeded"])
def test_other_events_ask_for_nothing_and_read_nothing(env, event):
    env.api.add_item("item-1")
    env.call("item/created")
    env.call(event)
    assert len(env.published) == 1
    assert [c[1] for c in env.api.calls] == ["/auth", "/items/item-1"]


def test_item_deleted_asks_for_nothing(env):
    env.api.add_item("item-1")
    env.call("item/deleted")
    assert env.published == []


def test_a_failed_publish_is_answered_200_logged_and_the_item_stays_registered(env):
    import conftest
    env.api.add_item("item-1")
    conftest.FakePublisher.failure = TimeoutError("deadline exceeded")
    assert env.call("item/created") == ("OK", 200)
    [error] = env.log("publish_error")
    assert error["type"] == "Error" and error["end_point"] == "item-1" and "deadline" in error["message"]
    assert env.log("load_requested") == [] and env.log("webhook_error") == []
    assert len(env.registry()) == 1 and len(env.log("item_registered")) == 1


def test_a_missing_load_topic_fails_loudly(env, monkeypatch):
    monkeypatch.setattr(main, "LOAD_TOPIC", "")
    with pytest.raises(RuntimeError, match="LOAD_TOPIC"):
        env.call("item/created")


def test_item_updated_is_answered_before_its_work_and_the_work_then_runs(env):
    """Pluggy waits 10 s for the answer; the registry note, the read back and the publish
    take about 6 s on a warm instance, more after a cold start."""
    env.api.add_item("item-1")
    env.call("item/created")
    calls, published = len(env.api.calls), len(env.published)
    assert env.call("item/updated", eventId="evt-2") == ("OK", 200)
    assert env.at_answer["threads"] == 1
    assert env.at_answer["registry"][0]["last_event"] == "item/created"
    assert len(env.at_answer["published"]) == published and len(env.at_answer["pluggy_calls"]) == calls
    assert env.registry()[0]["last_event"] == "item/updated"
    assert env.published[-1] == (TOPIC, {"item_ids": ["item-1"], "event_id": "evt-2"})


def test_item_created_does_its_work_before_answering(env):
    """A new bank must be registered: if the answer is late, Pluggy sends the event again."""
    env.api.add_item("item-1")
    env.call("item/created")
    assert env.at_answer["threads"] == 0
    assert [r["item_id"] for r in env.at_answer["registry"]] == ["item-1"]
    assert env.at_answer["published"] == [(TOPIC, {"item_ids": ["item-1"], "event_id": "evt-1"})]


@pytest.mark.parametrize("event", ["item/deleted", "item/error", "item/waiting_user_input"])
def test_other_events_do_their_work_before_answering(env, event):
    env.api.add_item("item-1")
    env.call("item/created")
    env.call(event)
    assert env.at_answer["threads"] == 0
    assert env.at_answer["registry"][0]["last_event"] == event


def test_a_refused_or_ignored_call_starts_no_work(env):
    env.call("item/updated", secret="wrong")
    assert env.at_answer["threads"] == 0
    assert main.main(FakeRequest({"event": "item/updated"})) == ("Ignored", 200)
    import conftest
    assert conftest.FakeThread.pending == []


def test_item_updated_whose_work_fails_after_the_answer_is_logged(env, monkeypatch):
    env.api.add_item("item-1")
    env.call("item/created")

    def refuse(self, sql, job_config=None):
        raise RuntimeError("quota")
    import conftest
    monkeypatch.setattr(conftest.FakeBigQuery, "query", refuse)
    assert env.call("item/updated") == ("OK", 200)
    [error] = env.log("webhook_error")
    assert error["message"] == "quota" and error["end_point"] == "item-1"
    assert json.loads(error["description"])["event"] == "item/updated"
