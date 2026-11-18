"""SQLite storage for jobs, records and checkpoints.

The SQLite file is the durable source of truth.  Every fetched page commits its
records and the *next* cursor in a single transaction, so a crashed or paused
job resumes from the last completed page without losing or duplicating records.

``run_epoch`` is a fencing token.  Pausing/resuming increments it; a worker
whose HTTP request was already in flight can only write if BOTH the job is still
``running`` and its epoch is unchanged.  A stale response therefore cannot write
records, advance progress or move the cursor after a pause has been confirmed.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid

import click
from flask import current_app, g

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id             TEXT PRIMARY KEY,
    source_url     TEXT NOT NULL,
    state          TEXT NOT NULL DEFAULT 'pending',
    next_cursor    TEXT,
    pages_done     INTEGER NOT NULL DEFAULT 0,
    records_seen   INTEGER NOT NULL DEFAULT 0,
    duplicates     INTEGER NOT NULL DEFAULT 0,
    run_epoch      INTEGER NOT NULL DEFAULT 0,
    last_error     TEXT,
    retry_after_at REAL,
    created_at     REAL NOT NULL,
    updated_at     REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS records (
    job_id     TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    record_id  TEXT NOT NULL,
    payload    TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    PRIMARY KEY (job_id, record_id)
);
CREATE TABLE IF NOT EXISTS events (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id  TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    at      REAL NOT NULL,
    level   TEXT NOT NULL,
    message TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_job_seq ON records(job_id, seq);
"""


def connect(path: str) -> sqlite3.Connection:
    """Open a connection suitable for one thread/worker."""
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = connect(current_app.config["DATABASE"])
    return g.db


def close_db(_exc=None) -> None:
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


def init_db() -> None:
    get_db().executescript(SCHEMA)


@click.command("init-db")
def init_db_command() -> None:
    init_db()
    click.echo("Initialised the database.")


def init_app(app) -> None:
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

def create_job(conn: sqlite3.Connection, source_url: str, state: str = "pending") -> str:
    job_id = uuid.uuid4().hex
    now = time.time()
    conn.execute(
        "INSERT INTO jobs (id, source_url, state, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        (job_id, source_url, state, now, now),
    )
    add_event(conn, job_id, "info", "任务已创建")
    return job_id


def get_job(conn: sqlite3.Connection, job_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()


def list_jobs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()


def add_event(conn: sqlite3.Connection, job_id: str, level: str, message: str) -> None:
    try:
        conn.execute(
            "INSERT INTO events (job_id, at, level, message) VALUES (?, ?, ?, ?)",
            (job_id, time.time(), level, message),
        )
    except sqlite3.IntegrityError:
        # The job was deleted concurrently; logging is best-effort.
        pass


def get_events(conn: sqlite3.Connection, job_id: str, limit: int = 80) -> list[sqlite3.Row]:
    rows = conn.execute(
        "SELECT * FROM events WHERE job_id = ? ORDER BY id DESC LIMIT ?", (job_id, limit)
    ).fetchall()
    return list(reversed(rows))


def start_job(conn: sqlite3.Connection, job_id: str) -> int | None:
    """Move a pending/paused/error job to running and return its new epoch.

    Returns None when the job is already running or finished.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = get_job(conn, job_id)
        if row is None:
            conn.execute("ROLLBACK")
            raise KeyError(job_id)
        if row["state"] not in ("pending", "paused", "error"):
            conn.execute("ROLLBACK")
            return None
        epoch = row["run_epoch"] + 1
        conn.execute(
            "UPDATE jobs SET state = 'running', run_epoch = ?, last_error = NULL, "
            "retry_after_at = NULL, updated_at = ? WHERE id = ?",
            (epoch, time.time(), job_id),
        )
        add_event(
            conn,
            job_id,
            "info",
            "任务继续" if row["state"] != "pending" else "任务启动",
        )
        conn.execute("COMMIT")
        return epoch
    except Exception:
        conn.execute("ROLLBACK")
        raise


def pause_job(
    conn: sqlite3.Connection, job_id: str, *, expected_epoch: int | None = None
) -> str | None:
    """Atomically confirm a pause.  Returns the resulting state (None if absent).

    The epoch bump fences off any request that is still in flight.  When
    ``expected_epoch`` is given, the pause only happens if that run still owns
    the job — this is how a worker confirms the pause Event the API set, without
    ever being able to pause a *newer* run that already resumed.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = get_job(conn, job_id)
        if row is None:
            conn.execute("ROLLBACK")
            return None
        if expected_epoch is not None and row["run_epoch"] != expected_epoch:
            conn.execute("COMMIT")
            return row["state"]
        if row["state"] in ("running", "pending"):
            conn.execute(
                "UPDATE jobs SET state = 'paused', run_epoch = run_epoch + 1, "
                "retry_after_at = NULL, updated_at = ? WHERE id = ?",
                (time.time(), job_id),
            )
            add_event(conn, job_id, "info", "暂停已确认，在途请求的结果将被忽略")
        state = get_job(conn, job_id)["state"]
        conn.execute("COMMIT")
        return state
    except Exception:
        conn.execute("ROLLBACK")
        raise


def recover_running_jobs(conn: sqlite3.Connection) -> int:
    """After a process restart no worker exists; park orphaned jobs as paused."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        rows = conn.execute("SELECT id FROM jobs WHERE state = 'running'").fetchall()
        for row in rows:
            conn.execute(
                "UPDATE jobs SET state = 'paused', run_epoch = run_epoch + 1, "
                "retry_after_at = NULL, updated_at = ? WHERE id = ?",
                (time.time(), row["id"]),
            )
            add_event(
                conn, row["id"], "warning", "服务重启，任务已暂停；请手动继续"
            )
        conn.execute("COMMIT")
        return len(rows)
    except Exception:
        conn.execute("ROLLBACK")
        raise


def is_run_current(conn: sqlite3.Connection, job_id: str, epoch: int) -> bool:
    """True only while this exact worker run still owns the job."""
    row = conn.execute(
        "SELECT 1 FROM jobs WHERE id = ? AND state = 'running' AND run_epoch = ?",
        (job_id, epoch),
    ).fetchone()
    return row is not None


def add_run_event(
    conn: sqlite3.Connection,
    job_id: str,
    epoch: int,
    level: str,
    message: str,
) -> bool:
    """Log an event only if ``epoch`` is still the current run.

    A paused/restarted run must not leave log lines behind that claim it did
    work: they would interleave with the next run's log and confuse the
    operator.  The conditional INSERT is one statement, so the epoch check and
    the write cannot be reordered by another thread.
    """
    try:
        cur = conn.execute(
            "INSERT INTO events (job_id, at, level, message) "
            "SELECT ?, ?, ?, ? WHERE EXISTS ("
            "SELECT 1 FROM jobs WHERE id = ? AND run_epoch = ? "
            "AND state IN ('running', 'done'))",
            (job_id, time.time(), level, message, job_id, epoch),
        )
        return bool(cur.rowcount)
    except sqlite3.IntegrityError:
        # The job was deleted concurrently; logging is best-effort.
        return False


def set_retry_at(
    conn: sqlite3.Connection, job_id: str, epoch: int, at: float | None
) -> bool:
    """Update the visible 429 wait deadline, fenced by the run epoch.

    Returns False when the caller's run is stale.  In particular a worker that
    was paused mid-wait must not clear or overwrite the ``retry_after_at`` of
    the run that replaced it.
    """
    cur = conn.execute(
        "UPDATE jobs SET retry_after_at = ?, updated_at = ? "
        "WHERE id = ? AND state = 'running' AND run_epoch = ?",
        (at, time.time(), job_id, epoch),
    )
    return bool(cur.rowcount)


def mark_error(
    conn: sqlite3.Connection, job_id: str, epoch: int, message: str
) -> bool:
    """Fail the job only if this exact worker run is still current."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "UPDATE jobs SET state = 'error', last_error = ?, updated_at = ?, "
            "retry_after_at = NULL WHERE id = ? AND state = 'running' AND run_epoch = ?",
            (message, time.time(), job_id, epoch),
        )
        if cur.rowcount:
            add_event(conn, job_id, "error", message)
        conn.execute("COMMIT")
        return bool(cur.rowcount)
    except Exception:
        conn.execute("ROLLBACK")
        raise


# ---------------------------------------------------------------------------
# Page commit
# ---------------------------------------------------------------------------

def commit_page(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    expected_cursor: str | None,
    expected_epoch: int,
    records: list[dict],
    next_cursor: str | None,
    finished: bool,
) -> tuple[int, int] | None:
    """Atomically persist a fetched page and advance the checkpoint.

    Returns ``(new_records, duplicates)`` or ``None`` when the write is fenced
    off (job paused/resumed, or another run already advanced the cursor).
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT next_cursor, state, run_epoch FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        if row is None:
            # Job was deleted while the request was in flight: write nothing.
            conn.execute("ROLLBACK")
            return None
        if row["state"] != "running" or row["run_epoch"] != expected_epoch:
            # Paused/restarted while the request was in flight.
            conn.execute("ROLLBACK")
            return None
        if (row["next_cursor"] or None) != (expected_cursor or None):
            # A resume or another worker already advanced past this page.
            conn.execute("ROLLBACK")
            return None

        seq_row = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS m FROM records WHERE job_id = ?", (job_id,)
        ).fetchone()
        seq = seq_row["m"]
        added = 0
        dup = 0
        for record in records:
            cur = conn.execute(
                "INSERT OR IGNORE INTO records (job_id, record_id, payload, seq) "
                "VALUES (?, ?, ?, ?)",
                (job_id, record["id"], json.dumps(record, ensure_ascii=False), seq + 1),
            )
            if cur.rowcount:
                added += 1
                seq += 1
            else:
                dup += 1
        now = time.time()
        conn.execute(
            "UPDATE jobs SET next_cursor = ?, pages_done = pages_done + 1, "
            "records_seen = records_seen + ?, duplicates = duplicates + ?, "
            "retry_after_at = NULL, updated_at = ?, state = ? "
            "WHERE id = ? AND state = 'running' AND run_epoch = ?",
            (
                next_cursor,
                added,
                dup,
                now,
                "done" if finished else "running",
                job_id,
                expected_epoch,
            ),
        )
        conn.execute("COMMIT")
        return added, dup
    except Exception:
        conn.execute("ROLLBACK")
        raise


def count_records(conn: sqlite3.Connection, job_id: str) -> int:
    return conn.execute(
        "SELECT COUNT(*) AS c FROM records WHERE job_id = ?", (job_id,)
    ).fetchone()["c"]


def iter_records(conn: sqlite3.Connection, job_id: str):
    return conn.execute(
        "SELECT record_id, payload FROM records WHERE job_id = ? ORDER BY seq", (job_id,)
    )


def delete_job(conn: sqlite3.Connection, job_id: str) -> None:
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM events WHERE job_id = ?", (job_id,))
        conn.execute("DELETE FROM records WHERE job_id = ?", (job_id,))
        conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
