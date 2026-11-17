FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    COLLECTOR_DATABASE=/data/collector.sqlite3

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY demo ./demo
COPY wsgi.py .

RUN useradd --create-home --uid 10001 collector \
    && mkdir -p /data \
    && chown -R collector:collector /data /srv
USER collector

EXPOSE 8000 9000

# A single process owns the in-process worker registry; threads keep the UI
# responsive.  Do NOT raise --workers: one worker run per job is enforced by
# SQLite fencing, but the in-process pause Event would only reach one worker.
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", "--threads", "16", \
     "--timeout", "120", "--graceful-timeout", "5", "--access-logfile", "-", "wsgi:app"]
