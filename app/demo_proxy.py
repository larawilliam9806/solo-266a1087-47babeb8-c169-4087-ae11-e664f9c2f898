"""Same-origin proxy to the local demo upstream's control plane.

Serving the demo controls through the collector's own origin means the browser
never needs cross-origin access to port 9000, and the demo can be toggled for
the containerised stack without extra CORS configuration.  When no demo
upstream is configured the proxy returns 503 and the UI hides the controls.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request

from flask import Blueprint, Response, current_app, request

bp = Blueprint("demo_proxy", __name__, url_prefix="/demo")

ALLOWED = {"config", "reset", "stats"}


def _upstream(path: str, method: str, body: bytes | None):
    base = current_app.config.get("DEMO_UPSTREAM_URL") or ""
    if not base:
        return Response(
            json.dumps({"error": "未配置演示源"}),
            status=503,
            mimetype="application/json",
        )
    req = urllib.request.Request(
        f"{base.rstrip('/')}/demo/{path}",
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return Response(resp.read(), status=resp.status, mimetype="application/json")
    except urllib.error.HTTPError as exc:
        return Response(exc.read(), status=exc.code, mimetype="application/json")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return Response(
            json.dumps({"error": f"演示源不可用：{exc}"}),
            status=503,
            mimetype="application/json",
        )


@bp.get("/<action>")
def demo_get(action: str):
    if action not in ALLOWED:
        return Response(json.dumps({"error": "not found"}), status=404, mimetype="application/json")
    return _upstream(action, "GET", None)


@bp.post("/<action>")
def demo_post(action: str):
    if action not in ALLOWED:
        return Response(json.dumps({"error": "not found"}), status=404, mimetype="application/json")
    return _upstream(action, "POST", request.get_data() or b"{}")
