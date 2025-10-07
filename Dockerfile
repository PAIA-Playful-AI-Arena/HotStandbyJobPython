FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY operate_hsj_parallelism.py /app/operate.py
COPY pod_status_manager.py /app/pod_status_manager.py

RUN useradd -u 10001 -r -s /sbin/nologin kopfuser
USER 10001

ENTRYPOINT ["kopf", "run", "-A", "/app/operate.py"]
