"""Async application entrypoint.

Runs the two long-lived halves of the gateway in one process: the inbound
Signal SSE listener and the uvicorn callback server. Both are launched as tasks
and torn down together on SIGINT/SIGTERM so ``Ctrl-C`` exits cleanly.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import suppress

import uvicorn

from bt_signal_gateway.callback_server import create_app
from bt_signal_gateway.config import get_settings
from bt_signal_gateway.engine_client import EngineClient
from bt_signal_gateway.envelope import InboundMessage
from bt_signal_gateway.logging_config import configure_logging
from bt_signal_gateway.signal_client import SignalClient
from bt_signal_gateway.signal_listener import InboundHandler, run_listener

logger = logging.getLogger(__name__)


def _make_inbound_handler(engine_client: EngineClient) -> InboundHandler:
    """Build the listener handler that relays accepted messages to the worker."""

    async def _relay(message: InboundMessage) -> None:
        if not await engine_client.submit(message):
            logger.warning(
                "inbound message not relayed to worker",
                extra={"user_id": message.user_id, "chat_id": message.chat_id},
            )

    return _relay


class _Server(uvicorn.Server):
    """uvicorn server that leaves signal handling to the entrypoint.

    The default ``Server`` installs its own SIGINT/SIGTERM handlers, which would
    stop the HTTP server while leaving the listener task running. We own signals
    here so both halves shut down together.
    """

    def install_signal_handlers(self) -> None:
        return None


async def run() -> None:
    settings = get_settings()
    configure_logging()
    logger.info(
        "starting bt-servant-signal-gateway",
        extra={
            "account": settings.signal_account,
            "host": settings.host,
            "port": settings.port,
            "engine_base_url": settings.engine_base_url,
            "progress_callback_url": settings.progress_callback_url,
        },
    )

    signal_client = SignalClient(settings)
    engine_client = EngineClient(settings)

    config = uvicorn.Config(
        create_app(signal_client=signal_client, settings=settings),
        host=settings.host,
        port=settings.port,
        log_config=None,
    )
    server = _Server(config)
    listener_task = asyncio.create_task(
        run_listener(
            settings,
            handler=_make_inbound_handler(engine_client),
            signal_client=signal_client,
        ),
        name="signal-listener",
    )
    server_task = asyncio.create_task(server.serve(), name="callback-server")

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):  # add_signal_handler is POSIX-only
            loop.add_signal_handler(sig, stop.set)

    stop_task = asyncio.create_task(stop.wait(), name="shutdown-signal")
    await asyncio.wait(
        {stop_task, listener_task, server_task},
        return_when=asyncio.FIRST_COMPLETED,
    )

    if stop_task.done():
        logger.info("shutdown signal received; stopping")
    else:
        logger.warning("a core task exited unexpectedly; shutting down")

    # Tear both halves down and drain their cancellations.
    server.should_exit = True
    listener_task.cancel()
    stop_task.cancel()
    server_result, listener_result, _ = await asyncio.gather(
        server_task, listener_task, stop_task, return_exceptions=True
    )
    await signal_client.aclose()
    await engine_client.aclose()

    # We requested the listener's cancellation, so a CancelledError there is
    # expected; anything else — or any exception from the server — is a real
    # failure that must surface (non-zero exit + traceback) rather than be
    # swallowed into a clean-looking shutdown.
    failures = [
        (task.get_name(), result)
        for task, result in ((server_task, server_result), (listener_task, listener_result))
        if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError)
    ]
    for name, exc in failures:
        logger.error("core task failed", extra={"task": name}, exc_info=exc)

    logger.info("shutdown complete")

    if failures:
        raise failures[0][1]


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
