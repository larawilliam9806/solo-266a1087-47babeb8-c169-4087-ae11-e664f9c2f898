"""Background collection worker and its in-process registry.

Design notes
------------
* One daemon thread per running *round*.  A round is identified by the job's
  ``run_epoch`` (the fencing token in SQLite).  Flask stays responsive while
  the worker performs blocking HTTP calls.
* Pause is *confirmed in the database* (state=paused + epoch bump) before the
  response to the pause request is sent.  The worker additionally checks a
  per-round ``threading.Event`` so it stops promptly, but correctness does not
  depend on it: every write it performs is fenced by the epoch check in
  :mod:`app.db`.
* Rounds never signal each other.  A pause that was confirmed for round N only
  affects round N: the worker's self-pause is conditional on ``run_epoch`` and
  every event/``retry_after_at`` write is conditional on it too.  A round that
  has been superseded (paused, then resumed while its HTTP request was still in
  flight) may finish that one request, but it cannot start another one, write a
  record, log an event, change the job state or alter the visible 429 wait of
  the round that replaced it.
* ``start``/``pause`` for one job are serialised by a per-job operation lock.
  The registry keeps superseded rounds until their threads exit, so a pause or
  shutdown still reaches every live worker instead of losing control of the
  one that is no longer "current".
* After a 429 the worker sleeps Retry-After in small slices and checks the
  pause flag and its epoch between slices, so a pause during the wait is
  immediate and a stale wait aborts instead of clearing the new round's
  ``retry_after_at``.
* A page is retried on transient failures (timeouts, connection errors, 5xx)
  with backoff.  Because the cursor only advances on a successful commit, a
  failed page is always fetched again from the same cursor and never skipped.
* Records are stored keyed by their upstream ID, so replaying a page after a
  crash is idempotent.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from . import db

log = logging.getLogger("collector")

TRANSIENT_HTTP = {408, 425, 500, 502, 503, 504}


class Paused(Exception):
    """Raised internally when the job was paused mid-flight."""


class _PausedDuringWait(Exception):
    pass


class _RetriesExhausted(Exception):
    pass


class Job:
    """One worker round.

    ``closed`` is set as soon as the round is superseded (pause confirmed,
    resume started, delete or shutdown).  After that no new HTTP request may be
    issued, even if the database check races with the request setup.
    """

    def __init__(self, job_id: str, url: str, epoch: int):
        self.id = job_id
        self.url = url
        self.epoch = epoch
        self.pause = threading.Event()
        self.thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._closed = False
        self._inflight = 0

    def stop_new_requests(self) -> None:
        """Fence off this round and wake it up from waits."""
        with self._state_lock:
            self._closed = True
        self.pause.set()

    def begin_request(self) -> bool:
        with self._state_lock:
            if self._closed:
                return False
            self._inflight += 1
            return True

    def end_request(self) -> None:
        with self._state_lock:
            if self._inflight > 0:
                self._inflight -= 1

    @property
    def inflight(self) -> int:
        with self._state_lock:
            return self._inflight

    @property
    def closed(self) -> bool:
        with self._state_lock:
            return self._closed


class CollectorManager:
    def __init__(self, app):
        self.app = app
        # Current round per job id.
        self._jobs: dict[str, Job] = {}
        # Superseded rounds whose threads have not exited yet.  They are kept so
        # pause/shutdown can still signal them (and so we never silently lose a
        # control handle for a running thread).
        self._stale: dict[str, set[Job]] = {}
        self._lock = threading.Lock()
        self._op_locks: dict[str, threading.Lock] = {}

    # -- lifecycle ---------------------------------------------------------
    def _op_lock(self, job_id: str) -> threading.Lock:
        with self._lock:
            return self._op_locks.setdefault(job_id, threading.Lock())

    def start(self, job_id: str) -> bool:
        """Start (or resume) a job.  Returns False if it cannot be started."""
        # Serialise with pause() so a pause request cannot observe a half
        # started round (DB flipped to running but the registry not yet
        # updated) and vice versa.
        with self._op_lock(job_id):
            with self.app.app_context():
                conn = db.connect(self.app.config["DATABASE"])
                try:
                    row = db.get_job(conn, job_id)
                    if row is None:
                        return False
                    epoch = db.start_job(conn, job_id)
                    if epoch is None:
                        return False
                    job = Job(job_id, row["source_url"], epoch)
                finally:
                    conn.close()

            previous = None
            with self._lock:
                previous = self._jobs.get(job_id)
                self._jobs[job_id] = job
                if (
                    previous is not None
                    and previous is not job
                    and previous.thread is not None
                    and previous.thread.is_alive()
                ):
                    self._stale.setdefault(job_id, set()).add(previous)

            if previous is not None and previous is not job:
                # A superseded round may finish the request it already sent,
                # but it must not issue another one.
                previous.stop_new_requests()
                # Its DB-level epoch is older, so the remaining fences are in
                # db.commit_page / db.mark_error / db.set_retry_at.

            thread = threading.Thread(
                target=self._run_guarded,
                args=(job,),
                name=f"collector-{job_id[:8]}-e{epoch}",
                daemon=True,
            )
            job.thread = thread
            thread.start()
            return True

    def pause(self, job_id: str) -> str | None:
        """Request a pause and confirm it in the database synchronously.

        Never waits for in-flight I/O: only the thread/DB bookkeeping is
        serialised.  Every live round for the job (current and superseded) is
        fenced so none of them can send another request.
        """
        with self._op_lock(job_id):
            with self._lock:
                live = []
                current = self._jobs.get(job_id)
                if current is not None:
                    live.append(current)
                live.extend(self._stale.get(job_id, ()))
            for job in live:
                job.stop_new_requests()

            with self.app.app_context():
                conn = db.connect(self.app.config["DATABASE"])
                try:
                    return db.pause_job(conn, job_id)
                finally:
                    conn.close()

    def is_active(self, job_id: str) -> bool:
        with self._lock:
            jobs = []
            current = self._jobs.get(job_id)
            if current is not None:
                jobs.append(current)
            jobs.extend(self._stale.get(job_id, ()))
        return any(job.thread is not None and job.thread.is_alive() for job in jobs)

    def rounds(self, job_id: str) -> list[dict]:
        """Diagnostics: every live round for a job, current or superseded."""
        with self._lock:
            current = self._jobs.get(job_id)
            entries = []
            if current is not None:
                entries.append((current, True))
            entries.extend((job, False) for job in self._stale.get(job_id, ()))
        return [
            {
                "epoch": job.epoch,
                "current": is_current,
                "closed": job.closed,
                "alive": bool(job.thread is not None and job.thread.is_alive()),
                "inflight": job.inflight,
            }
            for job, is_current in entries
        ]

    def shutdown(self) -> None:
        with self._lock:
            jobs = list(self._jobs.values())
            for stale in self._stale.values():
                jobs.extend(stale)
        for job in jobs:
            job.stop_new_requests()

    def delete(self, job_id: str) -> bool:
        """Fence all rounds, then delete the job under the per-job op lock.

        Serialised with start/pause so a concurrent resume cannot create a
        fresh round against a row that is about to disappear.
        """
        with self._op_lock(job_id):
            with self._lock:
                live = []
                current = self._jobs.get(job_id)
                if current is not None:
                    live.append(current)
                live.extend(self._stale.get(job_id, ()))
            for job in live:
                job.stop_new_requests()
            with self.app.app_context():
                conn = db.connect(self.app.config["DATABASE"])
                try:
                    if db.get_job(conn, job_id) is None:
                        return False
                    db.delete_job(conn, job_id)
                    return True
                finally:
                    conn.close()

    # -- registry bookkeeping ---------------------------------------------
    def _forget(self, job: Job) -> None:
        with self._lock:
            if self._jobs.get(job.id) is job:
                self._jobs.pop(job.id, None)
            stale = self._stale.get(job.id)
            if stale is not None:
                stale.discard(job)
                if not stale:
                    self._stale.pop(job.id, None)

    # -- worker ------------------------------------------------------------
    def _run_guarded(self, job: Job) -> None:
        with self.app.app_context():
            conn = db.connect(self.app.config["DATABASE"])
            try:
                self._run(conn, job)
            except (Paused, _PausedDuringWait):
                log.info("job %s epoch %s paused", job.id, job.epoch)
            except Exception as exc:  # pragma: no cover - defensive
                log.exception("job %s crashed", job.id)
                try:
                    # Fenced: a stale round cannot turn a running/paused job
                    # into "error".
                    db.mark_error(conn, job.id, job.epoch, f"任务异常终止：{exc}")
                except Exception:
                    log.exception("could not record failure for %s", job.id)
            finally:
                conn.close()
                self._forget(job)

    def _run(self, conn, job: Job) -> None:
        cfg = self.app.config
        row = db.get_job(conn, job.id)
        if row is None:
            return
        if row["state"] != "running" or row["run_epoch"] != job.epoch:
            # Started and superseded before this thread got scheduled.
            return
        cursor = row["next_cursor"]
        same_cursor = 0
        db.add_run_event(
            conn,
            job.id,
            job.epoch,
            "info",
            f"开始采集，游标：{cursor or '（首页）'}",
        )

        for page_no in range(1, cfg["MAX_PAGES"] + 1):
            if self._stop_requested(conn, job):
                raise Paused()

            url = self._page_url(job.url, cursor)
            try:
                status, headers, body = self._fetch(
                    conn, job, url, timeout=cfg["UPSTREAM_TIMEOUT"]
                )
            except _PausedDuringWait:
                raise Paused()
            except _RetriesExhausted as exc:
                db.mark_error(conn, job.id, job.epoch, str(exc))
                return

            if job.closed or job.pause.is_set():
                # Fenced while the response was in flight (pause or takeover).
                # The one request may complete, but its result is not ours.
                raise Paused()

            if status != 200:
                db.mark_error(
                    conn, job.id, job.epoch, f"上游返回 HTTP {status}（已重试）"
                )
                return

            try:
                payload = json.loads(body)
                raw_records = payload["records"]
                next_cursor = payload.get("next_cursor")
                if not isinstance(raw_records, list):
                    raise ValueError("records 不是数组")
                records: list[dict] = []
                for item in raw_records:
                    if (
                        not isinstance(item, dict)
                        or "id" not in item
                        or "body" not in item
                        or item["id"] is None
                    ):
                        raise ValueError("记录缺少 id/body 字段")
                    records.append({"id": str(item["id"]), "body": item["body"]})
            except (ValueError, KeyError, TypeError) as exc:
                db.mark_error(conn, job.id, job.epoch, f"响应格式错误：{exc}")
                return

            # An absent/null/empty cursor signals the last page.
            finished = next_cursor is None or str(next_cursor) == ""
            if not finished and str(next_cursor) == (cursor or ""):
                # Guard against an upstream stuck on the same cursor.
                same_cursor += 1
                if same_cursor >= 5:
                    db.mark_error(
                        conn, job.id, job.epoch, f"游标连续 5 页未前进（{next_cursor}）"
                    )
                    return
            else:
                same_cursor = 0
            result = db.commit_page(
                conn,
                job.id,
                expected_cursor=cursor,
                expected_epoch=job.epoch,
                records=records,
                next_cursor=None if finished else str(next_cursor),
                finished=finished,
            )
            if result is None:
                # Fenced off: the job was paused/resumed while this request was
                # in flight, so its result must not be written.
                raise Paused()
            added, dup = result
            db.add_run_event(
                conn,
                job.id,
                job.epoch,
                "info",
                f"第 {page_no} 页完成：新增 {added} 条，重复 {dup} 条（本次响应 {len(records)} 条）",
            )
            if finished:
                db.add_run_event(conn, job.id, job.epoch, "info", "采集完成")
                return
            cursor = str(next_cursor)

        db.mark_error(conn, job.id, job.epoch, "达到单次运行的最大页数限制")

    def _stop_requested(self, conn, job: Job) -> bool:
        """True if this round must not continue.

        Also confirms an in-process pause in the database when the event was
        set but the API had not committed it yet.  The confirmation is fenced
        by ``job.epoch``, so a superseded round can never pause the round that
        replaced it.
        """
        if not db.is_run_current(conn, job.id, job.epoch):
            return True
        if job.pause.is_set() or job.closed:
            if job.pause.is_set():
                db.pause_job(conn, job.id, expected_epoch=job.epoch)
            return True
        return False

    @staticmethod
    def _page_url(base: str, cursor: str | None) -> str:
        if not cursor:
            return base
        sep = "&" if urllib.parse.urlsplit(base).query else "?"
        return f"{base}{sep}cursor={urllib.parse.quote(cursor, safe='')}"

    def _http_once(self, url: str, *, timeout: float):
        """One HTTP request.  Returns ``(status, headers, body)``.

        HTTP error statuses are returned rather than raised; transport errors
        propagate to the caller.
        """
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "archive-collector/1.0",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, dict(resp.headers), resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers or {}), exc.read()

    def _fetch(self, conn, job: Job, url: str, *, timeout: float):
        """Fetch one page, retrying 429 and transient failures.

        Returns ``(status, headers, body)``; raises Paused-family exceptions to
        abort the run.
        """
        attempt = 0
        backoff = 1.0
        while True:
            # Check *before* opening a socket: once pause/takeover has fenced
            # this round, it must not issue more requests.
            if self._stop_requested(conn, job):
                raise _PausedDuringWait()
            if not job.begin_request():
                raise _PausedDuringWait()
            if job.closed:
                # Pause/takeover landed between the gate and the send.
                job.end_request()
                raise _PausedDuringWait()
            attempt += 1
            try:
                status, headers, body = self._http_once(url, timeout=timeout)
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
                if self._stop_requested(conn, job):
                    raise _PausedDuringWait()
                if attempt < 6:
                    db.add_run_event(
                        conn,
                        job.id,
                        job.epoch,
                        "warning",
                        f"网络错误：{exc}；{backoff:.1f}s 后重试",
                    )
                    if not self._wait(conn, job, backoff):
                        raise _PausedDuringWait()
                    backoff = min(backoff * 2, 30.0)
                    continue
                raise _RetriesExhausted(f"网络错误，重试 {attempt} 次仍失败：{exc}")
            finally:
                job.end_request()

            if status == 429:
                delay = self._retry_after(headers)
                db.add_run_event(
                    conn,
                    job.id,
                    job.epoch,
                    "warning",
                    f"429 限流，按 Retry-After 等待 {delay:.1f}s 后重试",
                )
                if not self._wait(conn, job, delay):
                    raise _PausedDuringWait()
                continue
            if status in TRANSIENT_HTTP and attempt < 6:
                db.add_run_event(
                    conn,
                    job.id,
                    job.epoch,
                    "warning",
                    f"HTTP {status}，{backoff:.1f}s 后重试",
                )
                if not self._wait(conn, job, backoff):
                    raise _PausedDuringWait()
                backoff = min(backoff * 2, 30.0)
                continue
            return status, headers, body

    def _retry_after(self, headers: dict) -> float:
        raw = headers.get("Retry-After") or headers.get("retry-after")
        if raw:
            try:
                return min(max(float(raw.strip()), 0.0), self.app.config["MAX_RETRY_AFTER"])
            except ValueError:
                pass
        return 30.0

    def _wait(self, conn, job: Job, seconds: float) -> bool:
        """Sleep in small slices, honouring pause and epoch.  False = abort."""
        # Fenced: a superseded round must not advertise (or later clear) the
        # new round's 429 wait.
        if not db.set_retry_at(conn, job.id, job.epoch, time.time() + seconds):
            return False
        try:
            deadline = time.monotonic() + seconds
            next_db_check = 0.0
            while True:
                if job.pause.is_set() or job.closed:
                    return False
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return db.is_run_current(conn, job.id, job.epoch)
                now = time.monotonic()
                if now >= next_db_check:
                    if not db.is_run_current(conn, job.id, job.epoch):
                        return False
                    next_db_check = now + 1.0
                job.pause.wait(min(0.2, remaining))
        finally:
            db.set_retry_at(conn, job.id, job.epoch, None)
