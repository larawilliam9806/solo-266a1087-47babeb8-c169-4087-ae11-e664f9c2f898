"""Shared fixtures: run the demo upstream in a thread and boot the collector."""

from __future__ import annotations

import socket
import sys
import threading
import time
from pathlib import Path

import pytest
from werkzeug.serving import make_server

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app import create_app  # noqa: E402
from demo import upstream  # noqa: E402


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ThreadedServer:
    def __init__(self, wsgi_app):
        self.port = _free_port()
        self.server = make_server("127.0.0.1", self.port, wsgi_app, threaded=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.thread.join(timeout=5)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


@pytest.fixture()
def demo():
    upstream._config.update(
        slow_ms=0, duplicates=False, rate_limit_every=0, retry_after=1
    )
    upstream._log.clear()
    upstream._page_requests = 0
    with ThreadedServer(upstream.app) as server:
        yield server


@pytest.fixture()
def collector(tmp_path, demo):
    app = create_app(
        {
            "TESTING": True,
            "DATABASE": str(tmp_path / "test.sqlite3"),
            "UPSTREAM_TIMEOUT": 5,
            "MAX_RETRY_AFTER": 10,
            "DEMO_UPSTREAM_URL": demo.url,
            "DEMO_API_URL": f"{demo.url}/records",
        }
    )
    with app.app_context():
        from app import db

        db.init_db()
    client = app.test_client()
    yield app, client
    app.extensions["collector_manager"].shutdown()
    time.sleep(0.05)
