"""Background collection worker and its in-process registry.

Design notes
------------
* One daemon thread per running job.  Flask stays responsive while the worker
  performs blocking HTTP calls.
* Pause is *confirmed in the database* (state=paused + epoch bump) before the
  response to the pause request is sent.  The worker additionally checks a
  per-job ``threading.Event`` so it stops promptly, but correctness does not
  depend on it: any stale in-flight response is fenced off in
  :func:`app.db.commit_page` by the epoch/cursor check.
* After a 429 the worker sleeps Retry-After in small slices and checks the
  pause flag between slices, so a pause during the wait is immediate.
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
    def __init__(self, job_id: str, url: str, epoch: int):
        self.id = job_id
        self.url = url
        self.epoch = epoch
        self.pause = threading.Event()
        self.thread: threading.Thread | None = None


class CollectorManager:
    def __init__(self, app):
        self.app = app
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()

    # -- lifecycle ---------------------------------------------------------
    def start(self, job_id: str) -> bool:
        """Start (or resume) a job.  Returns False if it cannot be started."""
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
        with self._lock:
            self._jobs[job_id] = job
        thread = threading.Thread(
            target=self._run_guarded,
            args=(job,),
            name=f"collector-{job_id[:8]}",
            daemon=True,
        )
        job.thread = thread
        thread.start()
        return True

    def pause(self, job_id: str) -> str | None:
        """Request a pause and confirm it in the database synchronously."""
        with self._lock:
            job = self._jobs.get(job_id)
        if job is not None:
            job.pause.set()
        with self.app.app_context():
            conn = db.connect(self.app.config["DATABASE"])
            try:
                return db.pause_job(conn, job_id)
            finally:
                conn.close()

    def is_active(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
        return bool(job and job.thread and job.thread.is_alive())

    def shutdown(self) -> None:
        with self._lock:
            jobs = list(self._jobs.values())
        for job in jobs:
            job.pause.set()

    # -- worker ------------------------------------------------------------
    def _run_guarded(self, job: Job) -> None:
        with self.app.app_context():
            conn = db.connect(self.app.config["DATABASE"])
            try:
                self._run(conn, job)
            except (Paused, _PausedDuringWait):
                log.info("job %s paused", job.id)
            except Exception as exc:  # pragma: no cover - defensive
                log.exception("job %s crashed", job.id)
                try:
                    db.mark_error(conn, job.id, job.epoch, f"任务异常终止：{exc}")
                except Exception:
                    log.exception("could not record failure for %s", job.id)
            finally:
                conn.close()
                with self._lock:
                    if self._jobs.get(job.id) is job:
                        self._jobs.pop(job.id, None)

    def _run(self, conn, job: Job) -> None:
        cfg = self.app.config
        row = db.get_job(conn, job.id)
        if row is None:
            return
        cursor = row["next_cursor"]
        same_cursor = 0
        db.add_event(conn, job.id, "info", f"开始采集，游标：{cursor or '（首页）'}")

        for page_no in range(1, cfg["MAX_PAGES"] + 1):
            if self._check_pause(conn, job):
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
            db.add_event(
                conn,
                job.id,
                "info",
                f"第 {page_no} 页完成：新增 {added} 条，重复 {dup} 条（本次响应 {len(records)} 条）",
            )
            if finished:
                db.add_event(conn, job.id, "info", "采集完成")
                return
            cursor = str(next_cursor)

        db.mark_error(conn, job.id, job.epoch, "达到单次运行的最大页数限制")

    def _check_pause(self, conn, job: Job) -> bool:
        if not job.pause.is_set():
            return False
        row = db.get_job(conn, job.id)
        if row and row["state"] == "running" and row["run_epoch"] == job.epoch:
            # The pause event was set but the API had not yet committed it.
            db.pause_job(conn, job.id)
        return True

    @staticmethod
    def _page_url(base: str, cursor: str | None) -> str:
        if not cursor:
            return base
        sep = "&" if urllib.parse.urlsplit(base).query else "?"
        return f"{base}{sep}cursor={urllib.parse.quote(cursor, safe='')}"

    def _fetch(self, conn, job: Job, url: str, *, timeout: float):
        """Fetch one page, retrying 429 and transient failures.

        Returns ``(status, headers, body)``; raises Paused-family exceptions to
        abort the run.
        """
        attempt = 0
        backoff = 1.0
        while True:
            if job.pause.is_set():
                raise _PausedDuringWait()
            attempt += 1
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
                body = exc.read()
                headers = dict(exc.headers or {})
                if exc.code == 429:
                    delay = self._retry_after(headers)
                    db.add_event(
                        conn,
                        job.id,
                        "warning",
                        f"429 限流，按 Retry-After 等待 {delay:.1f}s 后重试",
                    )
                    if not self._wait(conn, job, delay):
                        raise _PausedDuringWait()
                    continue
                if exc.code in TRANSIENT_HTTP and attempt < 6:
                    db.add_event(
                        conn,
                        job.id,
                        "warning",
                        f"HTTP {exc.code}，{backoff:.1f}s 后重试",
                    )
                    if not self._wait(conn, job, backoff):
                        raise _PausedDuringWait()
                    backoff = min(backoff * 2, 30.0)
                    continue
                return exc.code, headers, body
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
                if attempt < 6:
                    db.add_event(
                        conn,
                        job.id,
                        "warning",
                        f"网络错误：{exc}；{backoff:.1f}s 后重试",
                    )
                    if not self._wait(conn, job, backoff):
                        raise _PausedDuringWait()
                    backoff = min(backoff * 2, 30.0)
                    continue
                raise _RetriesExhausted(f"网络错误，重试 {attempt} 次仍失败：{exc}")

    def _retry_after(self, headers: dict) -> float:
        raw = headers.get("Retry-After") or headers.get("retry-after")
        if raw:
            try:
                return min(max(float(raw.strip()), 0.0), self.app.config["MAX_RETRY_AFTER"])
            except ValueError:
                pass
        return 30.0

    def _wait(self, conn, job: Job, seconds: float) -> bool:
        """Sleep in small slices, honouring pause.  False means paused."""
        deadline = time.monotonic() + seconds
        db.set_retry_at(conn, job.id, time.time() + seconds)
        try:
            while True:
                if job.pause.is_set():
                    return False
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return True
                job.pause.wait(min(0.2, remaining))
        finally:
            db.set_retry_at(conn, job.id, None)
