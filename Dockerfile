# syntax=docker/dockerfile:1.7
#
# BOSSman — one image: control-plane API + dashboard on a single port (7070),
# plus the `bossman` CLI and Python SDK (so managed Python agents can run
# inside the same image).
#
#   docker build -t bossman/bossman:0.2.0 .
#   docker run -p 7070:7070 -e BOSSMAN_ADMIN_TOKEN=... bossman/bossman:0.2.0

# ── 1. dashboard ─────────────────────────────────────────────────────────────
FROM node:22-alpine AS dashboard
WORKDIR /ui
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

# ── 2. python wheels ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS build
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /src
COPY backend/pyproject.toml backend/README.md backend/
COPY backend/bossman_server backend/bossman_server
COPY sdk/python sdk/python
RUN pip wheel --wheel-dir /wheels "./backend[llm,otel]" ./sdk/python

# ── 3. runtime ───────────────────────────────────────────────────────────────
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin bossman
COPY --from=build /wheels /wheels
RUN pip install /wheels/*.whl && rm -rf /wheels
COPY --from=dashboard /ui/dist /app/static
RUN mkdir -p /data && chown bossman:bossman /data
USER bossman
WORKDIR /data
ENV API_HOST_BIND=0.0.0.0 \
    API_PORT=7070 \
    BOSSMAN_ENV=production \
    BOSSMAN_DASHBOARD_DIR=/app/static \
    FORWARDED_ALLOW_IPS=*
EXPOSE 7070
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:7070/healthz', timeout=3).status == 200 else 1)"
ENTRYPOINT ["bossmand"]
CMD ["serve"]
