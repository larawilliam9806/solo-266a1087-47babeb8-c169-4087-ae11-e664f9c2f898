"""Functional tests for the archive collector.

Covers the acceptance-critical behaviours:
* complete collection + JSONL download, deduplicated by record ID
* duplicate records in the upstream feed
* pause with an in-flight request: the stale response must not write records
  or advance progress/cursor
* resume after pause continues from the saved checkpoint without gaps/dupes
* 429 Retry-After wait, pausable during the wait
* restart recovery: a job left "running" is parked as paused and resumes at
  the saved cursor
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time

from app import db

TOTAL = 23  # 5 pages of 5 + a final page of 3


def wait_for(predicate, timeout=15.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError("condition not met in time")


def job_state(client, job_id):
    return client.get(f"/api/jobs/{job_id}").get_json()


def start(client, url=None):
    url = url or client.application.config["DEMO_API_URL"]
    res = client.post("/api/jobs", json={"url": url})
    assert res.status_code in (200, 201), res.get_data(as_text=True)
    return res.get_json()["id"]


def snapshot(app, job_id):
    """Read durable state straight from SQLite (not via HTTP)."""
    conn = sqlite3.connect(app.config["DATABASE"])
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT state, next_cursor, pages_done, records_seen, duplicates, run_epoch "
            "FROM jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        ids = [
            r[0]
            for r in conn.execute(
                "SELECT record_id FROM records WHERE job_id = ? ORDER BY seq", (job_id,)
            )
        ]
    finally:
        conn.close()
    return row, ids


def download_ids(client, job_id):
    res = client.get(f"/api/jobs/{job_id}/download")
    assert res.status_code == 200
    lines = [line for line in res.get_data(as_text=True).splitlines() if line]
    ids = [json.loads(line)["id"] for line in lines]
    bodies = {json.loads(line)["id"]: json.loads(line)["body"] for line in lines}
    return ids, bodies


def test_full_collection_and_jsonl_download(collector):
    app, client = collector
    job_id = start(client)
    detail = wait_for(lambda: job_state(client, job_id)
                      if job_state(client, job_id)["state"] == "done" else None)

    ids, bodies = download_ids(client, job_id)
    assert ids == [f"rec-{i:04d}" for i in range(1, TOTAL + 1)]
    assert len(ids) == len(set(ids)) == TOTAL
    assert len(bodies) == TOTAL
    assert detail["pages_done"] == 5
    assert detail["record_count"] == TOTAL
    assert detail["next_cursor"] is None
    assert "attachment" in client.get(f"/api/jobs/{job_id}/download").headers[
        "Content-Disposition"
    ]


def test_duplicate_records_are_deduped(collector, demo):
    app, client = collector
    import requests

    requests.post(f"{demo.url}/demo/config", json={"duplicates": True}, timeout=5)
    job_id = start(client)
    detail = wait_for(lambda: job_state(client, job_id)
                      if job_state(client, job_id)["state"] == "done" else None)

    ids, _ = download_ids(client, job_id)
    assert ids == [f"rec-{i:04d}" for i in range(1, TOTAL + 1)]
    assert len(ids) == len(set(ids)) == TOTAL
    assert detail["duplicates"] > 0
    assert detail["record_count"] == TOTAL


def test_pause_discards_inflight_result_and_resume_continues(collector, demo):
    app, client = collector
    import requests

    # Make every page take 1s so we can pause while page 0 is in flight.
    requests.post(f"{demo.url}/demo/config", json={"slow_ms": 1000}, timeout=5)
    job_id = start(client)

    wait_for(lambda: any(
        entry["page"] == 0 for entry in requests.get(f"{demo.url}/demo/stats", timeout=5).json()["log"]
    ))

    paused = client.post(f"/api/jobs/{job_id}/pause")
    assert paused.status_code == 200
    assert paused.get_json()["state"] == "paused"

    # Pause must be confirmed without waiting for the in-flight HTTP request.
    assert client.get(f"/api/jobs/{job_id}").get_json()["state"] == "paused"
    before, _ = snapshot(app, job_id)

    # Wait well past the in-flight response; it must be fenced off.
    time.sleep(2.5)
    after, ids = snapshot(app, job_id)
    assert after["state"] == "paused"
    assert after["pages_done"] == before["pages_done"] == 0
    assert after["next_cursor"] is None
    assert ids == []

    requests.post(f"{demo.url}/demo/config", json={"slow_ms": 0}, timeout=5)
    resumed = client.post(f"/api/jobs/{job_id}/resume")
    assert resumed.status_code == 200
    detail = wait_for(lambda: job_state(client, job_id)
                      if job_state(client, job_id)["state"] == "done" else None)

    ids, _ = download_ids(client, job_id)
    assert ids == [f"rec-{i:04d}" for i in range(1, TOTAL + 1)]
    assert detail["record_count"] == TOTAL


def test_pause_between_pages_keeps_checkpoint(collector, demo):
    app, client = collector
    import requests

    requests.post(f"{demo.url}/demo/config", json={"slow_ms": 400}, timeout=5)
    job_id = start(client)

    # Let a couple of pages land, then pause.
    wait_for(lambda: job_state(client, job_id)["pages_done"] >= 2)
    assert client.post(f"/api/jobs/{job_id}/pause").get_json()["state"] == "paused"
    time.sleep(1.2)
    row, ids = snapshot(app, job_id)
    assert row["state"] == "paused"
    pages = row["pages_done"]
    cursor = row["next_cursor"]
    assert pages >= 2
    assert len(ids) == row["records_seen"]
    assert cursor is not None

    requests.post(f"{demo.url}/demo/config", json={"slow_ms": 0}, timeout=5)
    client.post(f"/api/jobs/{job_id}/resume")
    wait_for(lambda: job_state(client, job_id)["state"] == "done")

    final_ids, _ = download_ids(client, job_id)
    assert final_ids == [f"rec-{i:04d}" for i in range(1, TOTAL + 1)]


def test_rate_limit_waits_and_can_pause_during_wait(collector, demo):
    app, client = collector
    import requests

    # First request (page 0) triggers an immediate 429 with Retry-After: 4.
    requests.post(
        f"{demo.url}/demo/config",
        json={"rate_limit_every": 1, "retry_after": 4},
        timeout=5,
    )
    job_id = start(client)
    wait_for(lambda: job_state(client, job_id)["retry_after_at"] is not None)

    started = time.monotonic()
    paused = client.post(f"/api/jobs/{job_id}/pause")
    elapsed = time.monotonic() - started
    assert paused.get_json()["state"] == "paused"
    assert elapsed < 2.0, "pause must not wait for Retry-After"

    row, ids = snapshot(app, job_id)
    assert row["state"] == "paused"
    assert row["pages_done"] == 0 and ids == []

    # Turn off rate limiting; resume completes normally.
    requests.post(
        f"{demo.url}/demo/config", json={"rate_limit_every": 0}, timeout=5
    )
    client.post(f"/api/jobs/{job_id}/resume")
    wait_for(lambda: job_state(client, job_id)["state"] == "done")
    final_ids, _ = download_ids(client, job_id)
    assert final_ids == [f"rec-{i:04d}" for i in range(1, TOTAL + 1)]


def test_rate_limit_retries_and_completes(collector, demo):
    app, client = collector
    import requests

    # Every 3rd request is throttled with a 2s Retry-After.
    requests.post(
        f"{demo.url}/demo/config",
        json={"rate_limit_every": 3, "retry_after": 2},
        timeout=5,
    )
    job_id = start(client)
    wait_for(lambda: job_state(client, job_id)["state"] == "done", timeout=40)

    ids, _ = download_ids(client, job_id)
    assert ids == [f"rec-{i:04d}" for i in range(1, TOTAL + 1)]
    stats = requests.get(f"{demo.url}/demo/stats", timeout=5).json()
    assert any(entry["rate_limited"] for entry in stats["log"])


def test_restart_parks_running_job_and_resumes_from_checkpoint(collector, demo):
    app, client = collector
    import requests

    requests.post(f"{demo.url}/demo/config", json={"slow_ms": 300}, timeout=5)
    job_id = start(client)
    wait_for(lambda: job_state(client, job_id)["pages_done"] >= 2)
    client.post(f"/api/jobs/{job_id}/pause")
    time.sleep(0.6)
    row, ids_before = snapshot(app, job_id)
    saved_cursor = row["next_cursor"]
    saved_pages = row["pages_done"]
    assert saved_cursor is not None

    # Simulate a crash that leaves the row "running", then boot a fresh app.
    with app.app_context():
        conn = db.connect(app.config["DATABASE"])
        conn.execute(
            "UPDATE jobs SET state = 'running', run_epoch = run_epoch + 1 WHERE id = ?",
            (job_id,),
        )
        recovered = db.recover_running_jobs(conn)
        conn.close()
    assert recovered == 1

    from app import create_app

    app2 = create_app({"TESTING": True, "DATABASE": app.config["DATABASE"],
                       "DEMO_UPSTREAM_URL": demo.url,
                       "DEMO_API_URL": f"{app.config['DEMO_API_URL']}"})
    client2 = app2.test_client()
    with app2.app_context():
        db.init_db()
    try:
        state = client2.get(f"/api/jobs/{job_id}").get_json()
        assert state["state"] == "paused"
        assert state["next_cursor"] == saved_cursor
        assert state["pages_done"] == saved_pages

        requests.post(f"{demo.url}/demo/config", json={"slow_ms": 0}, timeout=5)
        assert client2.post(f"/api/jobs/{job_id}/resume").status_code == 200
        wait_for(lambda: client2.get(f"/api/jobs/{job_id}").get_json()["state"] == "done")

        ids_after, _ = download_ids(client2, job_id)
        assert ids_after == [f"rec-{i:04d}" for i in range(1, TOTAL + 1)]
        assert len(ids_after) == len(set(ids_after)) == TOTAL
        assert ids_after[: len(ids_before)] == ids_before
    finally:
        app2.extensions["collector_manager"].shutdown()


def test_no_duplicate_workers_on_resume(collector, demo):
    app, client = collector
    import requests

    requests.post(f"{demo.url}/demo/config", json={"slow_ms": 300}, timeout=5)
    job_id = start(client)
    wait_for(lambda: job_state(client, job_id)["pages_done"] >= 1)
    client.post(f"/api/jobs/{job_id}/pause")
    time.sleep(0.5)
    # Two concurrent resumes: only one may start.
    results = []

    def resume():
        results.append(client.post(f"/api/jobs/{job_id}/resume").status_code)

    threads = [threading.Thread(target=resume) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count(200) == 1
    wait_for(lambda: job_state(client, job_id)["state"] == "done")
    ids, _ = download_ids(client, job_id)
    assert ids == [f"rec-{i:04d}" for i in range(1, TOTAL + 1)]


def test_invalid_url_rejected(collector):
    _app, client = collector
    for bad in ("", "ftp://x/y", "not a url", "javascript:alert(1)"):
        res = client.post("/api/jobs", json={"url": bad})
        assert res.status_code == 400, bad


def test_upstream_error_marks_job_error(collector):
    _app, client = collector
    job_id = start(client, "http://127.0.0.1:1/records")
    detail = wait_for(lambda: job_state(client, job_id)
                      if job_state(client, job_id)["state"] == "error" else None,
                      timeout=40)
    assert "网络错误" in detail["last_error"]
    assert detail["pages_done"] == 0


def test_stale_response_cannot_write_after_pause(collector, demo, monkeypatch):
    """Fence check: an in-flight page that lands after pause is discarded."""
    app, client = collector
    import requests

    requests.post(f"{demo.url}/demo/config", json={"slow_ms": 1200}, timeout=5)
    job_id = start(client)

    def demo_has_inflight():
        log = requests.get(f"{demo.url}/demo/stats", timeout=5).json()["log"]
        return any(not entry["finished"] for entry in log)

    wait_for(demo_has_inflight)

    paused = client.post(f"/api/jobs/{job_id}/pause")
    assert paused.get_json()["state"] == "paused"
    row_before, ids_before = snapshot(app, job_id)

    # Wait until the demo actually finished serving the fenced request.
    wait_for(lambda: all(
        entry["finished"]
        for entry in requests.get(f"{demo.url}/demo/stats", timeout=5).json()["log"]
    ), timeout=10)
    time.sleep(0.4)  # let the collector's stale handler run

    row_after, ids_after = snapshot(app, job_id)
    assert row_after["state"] == "paused"
    assert row_after["pages_done"] == row_before["pages_done"] == 0
    assert row_after["records_seen"] == 0
    assert row_after["next_cursor"] is None
    assert ids_after == ids_before == []

    # Resuming still produces the full, correctly ordered dataset.
    requests.post(f"{demo.url}/demo/config", json={"slow_ms": 0}, timeout=5)
    client.post(f"/api/jobs/{job_id}/resume")
    wait_for(lambda: job_state(client, job_id)["state"] == "done")
    final_ids, _ = download_ids(client, job_id)
    assert final_ids == [f"rec-{i:04d}" for i in range(1, TOTAL + 1)]


def test_cursor_never_regresses_across_pauses(collector, demo):
    """Repeated pause/resume must not rewind or skip pages."""
    app, client = collector
    import requests

    requests.post(f"{demo.url}/demo/config", json={"slow_ms": 250}, timeout=5)
    job_id = start(client)
    seen_cursors = []
    for target in (1, 2, 3):
        wait_for(lambda: job_state(client, job_id)["pages_done"] >= target)
        state = job_state(client, job_id)
        seen_cursors.append(state["next_cursor"])
        client.post(f"/api/jobs/{job_id}/pause")
        time.sleep(0.5)
        paused_state = job_state(client, job_id)
        seen_cursors.append(paused_state["next_cursor"])
        client.post(f"/api/jobs/{job_id}/resume")

    wait_for(lambda: job_state(client, job_id)["state"] == "done", timeout=30)
    ids, _ = download_ids(client, job_id)
    assert ids == [f"rec-{i:04d}" for i in range(1, TOTAL + 1)]

    numeric = [int(c) for c in seen_cursors if c is not None]
    assert numeric == sorted(numeric), f"cursor regressed: {seen_cursors}"
    stats = requests.get(f"{demo.url}/demo/stats", timeout=5).json()
    served = [entry["page"] for entry in stats["log"] if not entry["rate_limited"]]
    assert max(served) <= 4
