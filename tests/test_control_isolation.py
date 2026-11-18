"""Regression tests for control isolation between collector rounds.

These cover the reported interleaving failures:
* resume succeeds, then the *old* round wakes up and pauses the job again
* a paused round keeps issuing upstream requests / rewrites the 429 wait time
* pause during the start/resume window loses the handle to a live worker, so a
  later pause cannot stop it

Every write a round performs (record commit, job state, event log, visible
``retry_after_at``) is fenced by ``run_epoch``; these tests pin that down.
"""

from __future__ import annotations

import json
import threading
import time

from app import db
from app.collector import CollectorManager, Job

PAGE_0 = {"records": [{"id": "a", "body": "A"}], "next_cursor": "1"}
LAST_PAGE = {"records": [], "next_cursor": None}


def wait_for(predicate, timeout=15.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError("condition not met in time")


def detail(client, job_id):
    return client.get(f"/api/jobs/{job_id}").get_json()


def row_and_ids(app, job_id):
    conn = db.connect(app.config["DATABASE"])
    try:
        row = conn.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        ids = [
            r[0]
            for r in conn.execute(
                "SELECT record_id FROM records WHERE job_id = ? ORDER BY seq",
                (job_id,),
            )
        ]
        events = [
            r[0]
            for r in conn.execute(
                "SELECT message FROM events WHERE job_id = ? ORDER BY id", (job_id,)
            )
        ]
    finally:
        conn.close()
    return row, ids, events


def test_stale_round_cannot_repause_after_resume(collector, monkeypatch):
    """Resume wins: the old round's late response must not pause again."""
    app, client = collector
    calls: list[str] = []
    in_flight = threading.Event()
    release = threading.Event()

    def fake(self, url, *, timeout):
        calls.append(url)
        if len(calls) == 1:
            in_flight.set()
            release.wait(10)
        payload = LAST_PAGE if "cursor=" in url else PAGE_0
        return 200, {}, json.dumps(payload).encode()

    monkeypatch.setattr(CollectorManager, "_http_once", fake)

    job_id = client.post(
        "/api/jobs", json={"url": "http://upstream.test/records"}
    ).get_json()["id"]
    wait_for(in_flight.is_set)

    assert client.post(f"/api/jobs/{job_id}/pause").get_json()["state"] == "paused"
    assert client.post(f"/api/jobs/{job_id}/resume").status_code == 200
    epoch_after_resume = row_and_ids(app, job_id)[0]["run_epoch"]

    # Let the old round's fenced response land *after* the resume.
    release.set()
    wait_for(lambda: detail(client, job_id)["state"] == "done")
    time.sleep(0.4)  # give the stale thread time to fully exit

    row, ids, events = row_and_ids(app, job_id)
    assert row["state"] == "done"
    assert row["run_epoch"] == epoch_after_resume, "stale round changed the epoch"
    assert ids == ["a"]
    # Exactly three requests: the old blocked one plus the resumed round's two
    # pages.  The fenced round issued nothing further.
    assert calls == [
        "http://upstream.test/records",
        "http://upstream.test/records",
        "http://upstream.test/records?cursor=1",
    ], calls
    # Only the pause that the operator requested was confirmed.
    assert sum("暂停已确认" in e for e in events) == 1


def test_stale_round_cannot_touch_next_runs_wait_or_log(collector):
    """A superseded round's DB writes are all no-ops, even without its event."""
    app, _client = collector
    manager = app.extensions["collector_manager"]

    with app.app_context():
        conn = db.connect(app.config["DATABASE"])
        try:
            job_id = db.create_job(conn, "http://upstream.test/records")
            epoch1 = db.start_job(conn, job_id)
            assert epoch1 is not None
            assert db.pause_job(conn, job_id) == "paused"
            epoch2 = db.start_job(conn, job_id)
            assert epoch2 is not None and epoch2 != epoch1

            future = time.time() + 30
            assert db.set_retry_at(conn, job_id, epoch2, future) is True

            # A live-but-stale round (pause event deliberately NOT set) tries
            # to wait, i.e. it would overwrite and later clear retry_after_at.
            stale = Job(job_id, "http://upstream.test/records", epoch1)
            assert manager._wait(conn, stale, 0.05) is False
            assert db.get_job(conn, job_id)["retry_after_at"] == future

            assert (
                db.add_run_event(conn, job_id, epoch1, "warning", "stale log line")
                is False
            )
            assert (
                db.add_run_event(conn, job_id, epoch2, "info", "current log line")
                is True
            )
            assert db.set_retry_at(conn, job_id, epoch1, None) is False
            assert db.get_job(conn, job_id)["retry_after_at"] == future

            messages = [
                r[0]
                for r in conn.execute(
                    "SELECT message FROM events WHERE job_id = ? ORDER BY id", (job_id,)
                )
            ]
            assert "stale log line" not in messages
            assert "current log line" in messages
        finally:
            conn.close()


def test_round_started_into_paused_state_does_no_io(collector, monkeypatch):
    """Pause confirmed before the thread is scheduled: it must exit quietly."""
    app, _client = collector
    manager = app.extensions["collector_manager"]

    def fail(*_args, **_kwargs):
        raise AssertionError("a paused round must not request upstream")

    monkeypatch.setattr(CollectorManager, "_http_once", fail)

    with app.app_context():
        conn = db.connect(app.config["DATABASE"])
        try:
            job_id = db.create_job(conn, "http://upstream.test/records")
            epoch = db.start_job(conn, job_id)
            assert epoch is not None
            assert db.pause_job(conn, job_id) == "paused"
            job = Job(job_id, "http://upstream.test/records", epoch)
            manager._run(conn, job)  # returns without raising or fetching
            row = db.get_job(conn, job_id)
            assert row["state"] == "paused"
            assert row["pages_done"] == 0
        finally:
            conn.close()


def test_second_pause_fences_old_and_new_rounds(collector, monkeypatch):
    """Pause must reach every live round, not just the registry's current one."""
    app, client = collector
    calls: list[str] = []
    gates = [threading.Event(), threading.Event()]

    def fake(self, url, *, timeout):
        index = len(calls)
        calls.append(url)
        if index < len(gates):
            gates[index].wait(10)
        return 200, {}, json.dumps(PAGE_0).encode()

    monkeypatch.setattr(CollectorManager, "_http_once", fake)
    manager = app.extensions["collector_manager"]

    job_id = client.post(
        "/api/jobs", json={"url": "http://upstream.test/records"}
    ).get_json()["id"]
    wait_for(lambda: len(calls) >= 1)
    assert client.post(f"/api/jobs/{job_id}/pause").get_json()["state"] == "paused"

    # Resuming while the old round is still blocked creates a second round;
    # the first is now "stale" but still alive and must stay under control.
    assert client.post(f"/api/jobs/{job_id}/resume").status_code == 200
    wait_for(lambda: len(calls) >= 2)
    assert client.post(f"/api/jobs/{job_id}/pause").get_json()["state"] == "paused"

    for gate in gates:
        gate.set()
    wait_for(lambda: not manager.is_active(job_id))
    time.sleep(0.3)

    row, ids, _events = row_and_ids(app, job_id)
    assert row["state"] == "paused"
    assert row["pages_done"] == 0
    assert row["next_cursor"] is None
    assert ids == []
    assert len(calls) == 2, "a fenced round issued another request"

    # A fresh resume still collects correctly from the (unchanged) checkpoint.
    def working(self, url, *, timeout):
        payload = LAST_PAGE if "cursor=" in url else PAGE_0
        return 200, {}, json.dumps(payload).encode()

    monkeypatch.setattr(CollectorManager, "_http_once", working)
    assert client.post(f"/api/jobs/{job_id}/resume").status_code == 200
    wait_for(lambda: detail(client, job_id)["state"] == "done")
    row, ids, _events = row_and_ids(app, job_id)
    assert ids == ["a"]
    assert row["pages_done"] == 2


def test_delete_during_flight_discards_late_response(collector, monkeypatch):
    """Deleting an in-flight job must not be undone by the late response."""
    app, client = collector
    started = threading.Event()
    release = threading.Event()

    def fake(self, url, *, timeout):
        started.set()
        release.wait(10)
        return 200, {}, json.dumps(PAGE_0).encode()

    monkeypatch.setattr(CollectorManager, "_http_once", fake)

    job_id = client.post(
        "/api/jobs", json={"url": "http://upstream.test/records"}
    ).get_json()["id"]
    wait_for(started.is_set)
    assert client.delete(f"/api/jobs/{job_id}").status_code == 200
    assert client.get(f"/api/jobs/{job_id}").status_code == 404

    release.set()
    time.sleep(0.4)
    assert client.get(f"/api/jobs/{job_id}").status_code == 404
    conn = db.connect(app.config["DATABASE"])
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM records WHERE job_id = ?", (job_id,)
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM events WHERE job_id = ?", (job_id,)
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_resume_while_pause_request_inflight_keeps_single_live_round(
    collector, monkeypatch
):
    """Interleaved start/pause must not lose the handle to the live worker."""
    app, client = collector
    manager = app.extensions["collector_manager"]
    calls: list[str] = []
    release = threading.Event()

    def fake(self, url, *, timeout):
        calls.append(url)
        release.wait(10)
        return 200, {}, json.dumps(LAST_PAGE if "cursor=" in url else PAGE_0).encode()

    monkeypatch.setattr(CollectorManager, "_http_once", fake)

    job_id = client.post(
        "/api/jobs", json={"url": "http://upstream.test/records"}
    ).get_json()["id"]
    wait_for(lambda: calls)

    outcomes = {}

    def pause():
        outcomes["pause"] = client.post(f"/api/jobs/{job_id}/pause").status_code

    def resume():
        outcomes["resume"] = client.post(f"/api/jobs/{job_id}/resume").status_code

    threads = [threading.Thread(target=pause), threading.Thread(target=resume)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    release.set()
    wait_for(lambda: not manager.is_active(job_id))
    time.sleep(0.2)

    row, ids, _events = row_and_ids(app, job_id)
    state = detail(client, job_id)["state"]
    # Whichever request ran second decided the state, and the outcome is
    # consistent: paused leaves the checkpoint untouched, running finishes.
    if outcomes.get("resume") == 200 and state == "done":
        assert ids == ["a"] and row["pages_done"] == 2
    else:
        assert state == "paused"
        assert row["pages_done"] == 0 and ids == []
    # No orphaned round keeps running after the job settled.
    assert manager.rounds(job_id) == []
