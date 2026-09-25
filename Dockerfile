FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 非 root 运行；/data 用于挂载卷持久化 SQLite 数据库
RUN useradd --system --uid 10001 appuser \
    && mkdir -p /data \
    && chown appuser:appuser /data
USER appuser

ENV TELEMETRY_DB_PATH=/data/telemetry.db

EXPOSE 8000

# 镜像内建健康检查（仅依赖镜像自带 Python，无需 curl/wget）；
# docker compose（以及 `docker run --healthcheck`）可直接据此报告 healthy。
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD python -c "import sys, urllib.request; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health', timeout=2).status == 200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
