# syntax=docker/dockerfile:1
#
# One image, two processes (see supervisord.conf): the signal-cli JSON-RPC daemon and the
# Python gateway. signal-cli 0.14.x is compiled for Java 25 (class file v69), and its bundled
# native libsignal needs glibc — so the base is a Java 25 JRE on Debian/Ubuntu (Temurin on
# noble), NOT alpine/musl.
#
# Pin >=0.14.5: a 2026-06-10 Signal server change dropped the `serverGuid` string field on
# sealed-sender envelopes; signal-cli <=0.14.4.1 NPEs and silently drops ALL incoming
# sealed-sender messages (AsamK/signal-cli#2059). 0.14.5 fixes it.

ARG SIGNAL_CLI_VERSION=0.14.5

FROM eclipse-temurin:25-jre-noble

ARG SIGNAL_CLI_VERSION

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    UV_PYTHON_DOWNLOADS=0 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH=/app/.venv/bin:$PATH

# System deps: Python 3.12 (default on noble), supervisor (process manager), and curl/certs
# for the signal-cli download.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        supervisor \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# signal-cli (pinned). The release tarball ships its own native libsignal for glibc.
RUN curl -fsSL "https://github.com/AsamK/signal-cli/releases/download/v${SIGNAL_CLI_VERSION}/signal-cli-${SIGNAL_CLI_VERSION}.tar.gz" \
        | tar -xz -C /opt \
    && ln -s "/opt/signal-cli-${SIGNAL_CLI_VERSION}/bin/signal-cli" /usr/local/bin/signal-cli \
    && signal-cli --version

# uv. Dependency versions are pinned by uv.lock + `--frozen`, so the uv binary itself need
# not be version-pinned for reproducibility.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Non-root runtime user. /data is the persistent volume mount holding signal-cli state.
RUN useradd --create-home --uid 10001 app \
    && mkdir -p /data /app \
    && chown -R app:app /data /app

WORKDIR /app

# Install dependencies first (this layer is cached until the lockfile changes), then the
# project itself once the source is copied.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY . .
RUN uv sync --frozen --no-dev

COPY supervisord.conf /etc/supervisor/supervisord.conf
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Only the gateway's callback server is exposed; signal-cli's HTTP daemon stays on loopback.
EXPOSE 8081

# Runs as root only long enough to chown the volume, then supervisord drops each program to
# the unprivileged `app` user.
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
