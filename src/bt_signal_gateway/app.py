"""Async application entrypoint.

Stub: the full wiring (run the SSE listener + the uvicorn callback server
together, with graceful shutdown) lands in the "Config & async app
entrypoint" issue. For now this just serves the health endpoint so the
container/process has something to run.
"""

from __future__ import annotations

import uvicorn

from bt_signal_gateway.callback_server import app


def main() -> None:
    # Bind all interfaces: this runs as a container service behind Fly's proxy.
    uvicorn.run(app, host="0.0.0.0", port=8081)


if __name__ == "__main__":
    main()
