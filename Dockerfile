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

# System deps: Python 3.12 (default on noble), supervisor (process manager), curl/certs
# for the signal-cli download, and media-types for /etc/mime.types — signal-cli stamps
# outbound attachment content types via Java's Files.probeContentType, which reads
# /etc/mime.types on Linux. Without it every voice note goes out as
# application/octet-stream and Signal clients render a generic file card instead of the
# inline voice-note player, even with the voiceNote flag set (issue #49). The greps
# fail the build if a future base-image/package change drops the audio mappings we
# depend on (.opus/.ogg for worker TTS replies, .m4a for the extension-less fallback).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        supervisor \
        curl \
        ca-certificates \
        media-types \
    && grep -Eq '^audio/ogg[[:space:]].*[[:space:]]opus([[:space:]]|$)' /etc/mime.types \
    && grep -Eq '^audio/ogg[[:space:]].*[[:space:]]ogg([[:space:]]|$)' /etc/mime.types \
    && grep -Eq '^audio/mp4[[:space:]].*[[:space:]]m4a([[:space:]]|$)' /etc/mime.types \
    && rm -rf /var/lib/apt/lists/*

# signal-cli (pinned). The release tarball ships its own native libsignal for glibc.
RUN curl -fsSL "https://github.com/AsamK/signal-cli/releases/download/v${SIGNAL_CLI_VERSION}/signal-cli-${SIGNAL_CLI_VERSION}.tar.gz" \
        | tar -xz -C /opt \
    && ln -s "/opt/signal-cli-${SIGNAL_CLI_VERSION}/bin/signal-cli" /usr/local/bin/signal-cli \
    && signal-cli --version

# uv, pinned to a concrete version for a reproducible build tool (no mutable `latest`);
# dependency versions are additionally locked by uv.lock + `--frozen`.
COPY --from=ghcr.io/astral-sh/uv:0.11.23 /uv /uvx /usr/local/bin/

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
