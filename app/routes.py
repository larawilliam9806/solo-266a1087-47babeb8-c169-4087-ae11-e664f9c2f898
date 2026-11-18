"""HTTP routes for the collection console."""

from __future__ import annotations

import sqlite3
import urllib.parse
from datetime import datetime, timezone

from flask import (
    Blueprint,
    Response,
    current_app,
    jsonify,
    render_template,
    request,
    stream_with_context,
)

from . import db

bp = Blueprint("main", __name__)


def _job_to_dict(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "source_url": row["source_url"],
        "state": row["state"],
        "next_cursor": row["next_cursor"],
        "pages_done": row["pages_done"],
        "records_seen": row["records_seen"],
        "duplicates": row["duplicates"],
        "last_error": row["last_error"],
        "retry_after_at": row["retry_after_at"],
        "run_epoch": row["run_epoch"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _valid_url(value: str) -> bool:
    if not isinstance(value, str):
        return False
    value = value.strip()
    if not value or len(value) > 2048:
        return False
    parsed = urllib.parse.urlsplit(value)
    return parsed.scheme in ("http", "https") and bool(parsed.hostname)


def _job_or_404(conn, job_id: str):
    row = db.get_job(conn, job_id)
    if row is None:
        return None, (jsonify({"error": "任务不存在"}), 404)
    return row, None


@bp.get("/")
def index():
    conn = db.get_db()
    jobs = [_job_to_dict(row) for row in db.list_jobs(conn)]
    return render_template(
        "index.html",
        jobs=jobs,
        demo_api_url=current_app.config.get("DEMO_API_URL", ""),
        demo_control_url=current_app.config.get("DEMO_CONTROL_URL", ""),
    )


@bp.post("/api/jobs")
def create_job():
    payload = request.get_json(silent=True) or {}
    url = (payload.get("url") or "").strip()
    if not _valid_url(url):
        return jsonify({"error": "请输入合法的 http(s) 接口地址"}), 400
    conn = db.get_db()
    job_id = db.create_job(conn, url)
    started = current_app.extensions["collector_manager"].start(job_id)
    if not started:  # pragma: no cover - guard
        db.mark_error(conn, job_id, 0, "无法启动任务")
        return jsonify({"error": "无法启动任务"}), 500
    return jsonify(_job_to_dict(db.get_job(conn, job_id))), 201


@bp.get("/api/jobs")
def list_jobs():
    conn = db.get_db()
    return jsonify([_job_to_dict(row) for row in db.list_jobs(conn)])


@bp.get("/api/jobs/<job_id>")
def get_job(job_id: str):
    conn = db.get_db()
    row, err = _job_or_404(conn, job_id)
    if err:
        return err
    data = _job_to_dict(row)
    data["record_count"] = db.count_records(conn, job_id)
    data["events"] = [
        {
            "at": event["at"],
            "level": event["level"],
            "message": event["message"],
            "time": datetime.fromtimestamp(event["at"], timezone.utc).isoformat(),
        }
        for event in db.get_events(conn, job_id)
    ]
    data["server_time"] = datetime.now(timezone.utc).timestamp()
    data["rounds"] = current_app.extensions["collector_manager"].rounds(job_id)
    return jsonify(data)


@bp.post("/api/jobs/<job_id>/pause")
def pause_job(job_id: str):
    conn = db.get_db()
    row, err = _job_or_404(conn, job_id)
    if err:
        return err
    if row["state"] == "done":
        return jsonify({"error": "任务已完成"}), 409
    # Fetches the pause event (for in-process workers) and confirms the new
    # state + epoch in SQLite before responding.  No waiting on in-flight I/O.
    state = current_app.extensions["collector_manager"].pause(job_id)
    return jsonify({"state": state})


@bp.post("/api/jobs/<job_id>/resume")
def resume_job(job_id: str):
    conn = db.get_db()
    row, err = _job_or_404(conn, job_id)
    if err:
        return err
    if row["state"] == "done":
        return jsonify({"error": "任务已完成，无需继续"}), 409
    if row["state"] == "running":
        return jsonify({"error": "任务正在运行"}), 409
    started = current_app.extensions["collector_manager"].start(job_id)
    if not started:
        return jsonify({"error": "当前状态无法继续"}), 409
    return jsonify(_job_to_dict(db.get_job(conn, job_id)))


@bp.delete("/api/jobs/<job_id>")
def remove_job(job_id: str):
    conn = db.get_db()
    _row, err = _job_or_404(conn, job_id)
    if err:
        return err
    # Fences every live round (current and superseded), then deletes under the
    # same per-job lock that start/pause use, so no new round can appear
    # between the check and the delete.
    manager = current_app.extensions["collector_manager"]
    if not manager.delete(job_id):
        return jsonify({"error": "任务不存在"}), 404
    return jsonify({"deleted": job_id})


@bp.get("/api/jobs/<job_id>/download")
def download(job_id: str):
    conn = db.get_db()
    row, err = _job_or_404(conn, job_id)
    if err:
        return err
    path = current_app.config["DATABASE"]
    filename = f"archive-{job_id[:12]}.jsonl"

    def generate():
        worker_conn = db.connect(path)
        try:
            for record in db.iter_records(worker_conn, job_id):
                yield record["payload"] + "\n"
        finally:
            worker_conn.close()

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Cache-Control": "no-store",
    }
    return Response(
        stream_with_context(generate()),
        mimetype="application/x-ndjson",
        headers=headers,
    )


@bp.get("/healthz")
def healthz():
    conn = db.get_db()
    conn.execute("SELECT 1")
    return jsonify({"ok": True})
