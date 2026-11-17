"""WSGI entry-point used by Docker/gunicorn and local development.

Run with a single worker process and several threads: the worker registry lives
in-process, while pause/resume correctness is enforced by SQLite.
"""

from app import create_app
from app import db as _db

app = create_app()

# Ensure schema exists and park jobs orphaned by a previous process.  A process
# restart cannot have a live worker, so a job left "running" is paused and must
# be resumed manually.
with app.app_context():
    _db.init_db()
    recovered = _db.recover_running_jobs(_db.get_db())
    if recovered:
        app.logger.warning("recovered %s orphaned running job(s)", recovered)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True, use_reloader=False)
