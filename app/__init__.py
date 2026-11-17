"""Paged archive collector application factory."""

from __future__ import annotations

import os

from flask import Flask

from . import db
from .collector import CollectorManager
from .routes import bp


def create_app(test_config: dict | None = None) -> Flask:
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        DATABASE=os.environ.get(
            "COLLECTOR_DATABASE", os.path.join(app.instance_path, "collector.sqlite3")
        ),
        UPSTREAM_TIMEOUT=float(os.environ.get("UPSTREAM_TIMEOUT", "30")),
        MAX_RETRY_AFTER=float(os.environ.get("MAX_RETRY_AFTER", "300")),
        MAX_PAGES=int(os.environ.get("MAX_PAGES", "100000")),
        DEMO_API_URL=os.environ.get("DEMO_API_URL", "http://127.0.0.1:9000/records"),
        DEMO_UPSTREAM_URL=os.environ.get(
            "DEMO_UPSTREAM_URL", "http://127.0.0.1:9000"
        ),
        DEMO_CONTROL_URL="/demo",
    )
    if test_config:
        app.config.update(test_config)

    # Keep Chinese text readable in API responses and JSONL.
    app.json.ensure_ascii = False

    os.makedirs(app.instance_path, exist_ok=True)
    os.makedirs(os.path.dirname(app.config["DATABASE"]) or ".", exist_ok=True)

    db.init_app(app)
    app.extensions["collector_manager"] = CollectorManager(app)
    app.register_blueprint(bp)

    if app.config["DEMO_CONTROL_URL"]:
        from .demo_proxy import bp as demo_bp

        app.register_blueprint(demo_bp)
    return app
