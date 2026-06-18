"""Tests for the async entrypoint's shutdown / failure handling."""

from __future__ import annotations

import asyncio

import pytest

from bt_signal_gateway import app as app_module
from bt_signal_gateway.config import get_settings

REQUIRED_ENV = {
    "SIGNAL_ACCOUNT": "+15551234567",
    "ENGINE_BASE_URL": "https://api.btservant.ai",
    "ENGINE_API_KEY": "secret-token",
    "GATEWAY_PUBLIC_URL": "https://gw.fly.dev",
}


@pytest.fixture
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    # Keep the test from clobbering pytest's logging config.
    monkeypatch.setattr(app_module, "configure_logging", lambda *a, **k: None)


async def test_run_propagates_core_task_failure(
    monkeypatch: pytest.MonkeyPatch, _env: None
) -> None:
    """A crashing core task must make run() raise, not exit cleanly."""

    async def boom(_settings: object) -> None:
        raise RuntimeError("listener crashed")

    async def fake_serve(self: object) -> None:
        # Mimic uvicorn's own serve loop: return once should_exit is set
        # (graceful stop). noqa: emulating uvicorn's poll, not production code.
        while not getattr(self, "should_exit", False):  # noqa: ASYNC110
            await asyncio.sleep(0.01)

    monkeypatch.setattr(app_module, "run_listener", boom)
    monkeypatch.setattr(app_module._Server, "serve", fake_serve)

    # wait_for guards against a regression that would otherwise hang the suite.
    with pytest.raises(RuntimeError, match="listener crashed"):
        await asyncio.wait_for(app_module.run(), timeout=5)
