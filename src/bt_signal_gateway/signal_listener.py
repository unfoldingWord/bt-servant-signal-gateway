"""Inbound SSE listener against the signal-cli daemon.

Stub: the real SSE connection (reconnect/backoff, envelope parsing) lands in the
"Inbound SSE listener & envelope parsing" issue. For now :func:`run_listener` is
a cancellable placeholder so the app entrypoint can run it as a long-lived task
alongside the callback server.
"""

from __future__ import annotations

import asyncio
import logging

from bt_signal_gateway.config import Settings

logger = logging.getLogger(__name__)


async def run_listener(settings: Settings) -> None:
    """Run the inbound listener until cancelled.

    Currently a no-op that blocks forever; replaced by the real SSE loop in a
    later issue. Wired now so the entrypoint's concurrency + shutdown path is
    exercised end to end.
    """
    logger.info("signal listener started (stub)", extra={"account": settings.signal_account})
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        logger.info("signal listener stopped")
        raise
