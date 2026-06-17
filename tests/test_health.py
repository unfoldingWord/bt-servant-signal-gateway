"""Smoke test for the health endpoint — the one piece of real behavior in scaffolding."""

from __future__ import annotations

from fastapi.testclient import TestClient

from bt_signal_gateway.callback_server import app


def test_health_ok() -> None:
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "service": "bt-servant-signal-gateway"}
