FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_VOLUME_DB=/data/volume.db

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# Single-threaded dev server is sufficient for this simulation and makes the
# crash/recovery behavior deterministic (no concurrent requests mid-crash).
# --no-reload is essential: the simulated crash calls os._exit() and must kill
# the whole process instead of being revived by the reloader supervisor.
CMD ["python", "-m", "flask", "--app", "app.server", "run", "--host", "0.0.0.0", "--port", "8000", "--without-threads", "--no-reload", "--no-debugger"]
