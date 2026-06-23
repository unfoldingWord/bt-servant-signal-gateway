#!/bin/sh
set -e

# Fly mounts volumes root-owned; make the signal-cli state dir writable by the unprivileged
# `app` user before supervisord drops privileges to it. (Local compose volumes are already
# app-owned, so this is a no-op there.)
chown -R app:app /data 2>/dev/null || true

exec supervisord -c /etc/supervisor/supervisord.conf
