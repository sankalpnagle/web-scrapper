FROM python:3.11-slim

# ── System deps ───────────────────────────────────────────────────────────
# BUG-14 FIX: libasound2 was renamed to libasound2-dev in Debian bookworm
#             (python:3.11-slim is bookworm-based). Using the wrong name
#             causes apt-get to fail and silently break the build.
# BUG-15 FIX: Removed all inline # comments from inside the RUN block.
#             Shell line-continuation (\) + inline comments cause apt-get
#             to try to install '#' as a package name, failing the build.
RUN apt-get update && apt-get install -y --no-install-recommends \
        supervisor \
        libnss3 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libcups2 \
        libdrm2 \
        libxkbcommon0 \
        libxcomposite1 \
        libxdamage1 \
        libxfixes3 \
        libxrandr2 \
        libgbm1 \
        libasound2-dev \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── Python deps ───────────────────────────────────────────────────────────
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium + its OS-level deps
RUN playwright install chromium --with-deps

# ── App source ────────────────────────────────────────────────────────────
COPY database.py                      .
COPY fetchcanocaialLink.py            .
COPY fetchCanonicalLinkWithConsent.py .
COPY fast_pipeline.py                 .
COPY retry_pipeline.py                .

# ── Supervisor config ─────────────────────────────────────────────────────
COPY supervisor.conf /etc/supervisor/conf.d/supervisor.conf

# ── Log directories ───────────────────────────────────────────────────────
RUN mkdir -p /root/log /var/log/supervisor

# ── Healthcheck: both programs must be RUNNING ────────────────────────────
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD supervisorctl -c /etc/supervisor/conf.d/supervisor.conf status \
        fast_pipeline retry_pipeline \
        | awk '{if($2!="RUNNING") exit 1}'

# ── Entrypoint ────────────────────────────────────────────────────────────
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisor.conf"]
