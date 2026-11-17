"""Deterministic local demo upstream for the archive collector.

Endpoints
---------
GET  /records?cursor=<n>   page `n` of a fixed synthetic dataset
GET  /demo/config          current fault-injection settings
POST /demo/config          update settings (JSON; missing keys keep old value)
POST /demo/reset           clear request log and counters
GET  /demo/stats           request log, used by the demo UI/tests

Fault injection
---------------
slow_ms          delay every page response by N ms (test in-flight pause)
duplicates       re-serve the previous page's records alongside each page
                 (same IDs and identical bodies, so dedupe can be verified)
rate_limit_every every N-th page request returns 429 + Retry-After
retry_after      seconds advertised in the Retry-After header

The dataset is generated deterministically, so a record ID always maps to the
same body even across page retries.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import deque

from flask import Flask, Response, jsonify, request

PAGE_SIZE = 5
TOTAL_RECORDS = int(os.environ.get("DEMO_TOTAL_RECORDS", "23"))

app = Flask(__name__)

_lock = threading.Lock()
_config = {
    "slow_ms": 0,
    "duplicates": False,
    "rate_limit_every": 0,
    "retry_after": 2,
}
_log: deque[dict] = deque(maxlen=500)
_page_requests = 0
_request_seq = 0


def _records_for_page(page: int) -> list[dict]:
    start = page * PAGE_SIZE
    return [
        {
            "id": f"rec-{index + 1:04d}",
            "body": f"档案正文 #{index + 1:04d}：这是可重复校验的固定内容。",
        }
        for index in range(start, min(start + PAGE_SIZE, TOTAL_RECORDS))
    ]


def _next_cursor(page: int) -> str | None:
    return str(page + 1) if (page + 1) * PAGE_SIZE < TOTAL_RECORDS else None


@app.get("/records")
def records():
    global _page_requests, _request_seq
    raw = request.args.get("cursor")
    try:
        page = int(raw) if raw not in (None, "") else 0
        if page < 0:
            raise ValueError
    except ValueError:
        return jsonify({"error": "invalid cursor"}), 400

    with _lock:
        config = dict(_config)
        _page_requests += 1
        number = _page_requests
        _request_seq += 1
        seq = _request_seq
        entry = {
            "seq": seq,
            "at": time.time(),
            "cursor": raw,
            "page": page,
            "slow_ms": config["slow_ms"],
            "rate_limited": False,
            "finished": False,
            "served_ids": [],
        }
        # Log at request start so the in-flight request is observable.
        _log.append(entry)

    if config["slow_ms"] > 0:
        time.sleep(config["slow_ms"] / 1000.0)

    if config["rate_limit_every"] > 0 and number % config["rate_limit_every"] == 0:
        entry["rate_limited"] = True
        entry["finished"] = True
        retry_after = max(1, int(config["retry_after"]))
        resp = jsonify({"error": "rate limited"})
        resp.status_code = 429
        resp.headers["Retry-After"] = str(retry_after)
        return resp

    entry["finished"] = True
    if page * PAGE_SIZE >= TOTAL_RECORDS:
        return jsonify({"records": [], "next_cursor": None})

    items = _records_for_page(page)
    if config["duplicates"]:
        # Re-serve the previous page's records with identical IDs and bodies.
        items = _records_for_page(max(page - 1, 0)) + items

    entry["served_ids"] = [item["id"] for item in items]
    return Response(
        json.dumps({"records": items, "next_cursor": _next_cursor(page)}, ensure_ascii=False),
        mimetype="application/json",
    )


@app.get("/demo/config")
def get_config():
    with _lock:
        return jsonify(dict(_config))


@app.post("/demo/config")
def set_config():
    payload = request.get_json(silent=True) or {}
    with _lock:
        for key in ("slow_ms", "duplicates", "rate_limit_every", "retry_after"):
            if key not in payload:
                continue
            value = payload[key]
            if key == "duplicates":
                _config[key] = bool(value)
            elif key == "retry_after":
                _config[key] = max(0.0, float(value))
            else:
                _config[key] = max(0, int(value))
        return jsonify(dict(_config))


@app.post("/demo/reset")
def reset():
    global _page_requests, _request_seq
    with _lock:
        _log.clear()
        _page_requests = 0
        _request_seq = 0
    return jsonify({"ok": True})


@app.get("/demo/stats")
def stats():
    with _lock:
        return jsonify(
            {
                "config": dict(_config),
                "requests": _page_requests,
                "log": list(_log),
                "total_records": TOTAL_RECORDS,
                "page_size": PAGE_SIZE,
            }
        )


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "9000")), threaded=True)
