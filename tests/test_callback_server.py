"""Tests for the ``/progress-callback`` endpoint (auth, dedup, dispatch)."""

from __future__ import annotations

from typing import cast

import httpx
from fastapi.testclient import TestClient

from bt_signal_gateway.callback_server import create_app
from bt_signal_gateway.config import Settings
from bt_signal_gateway.signal_client import SignalClient

ACCOUNT = "+15551234567"
API_KEY = "secret-token"
USER_ID = "11111111-2222-3333-4444-555555555555"
GROUP_CHAT_ID = "group:dGVzdGdyb3Vw"


def _settings() -> Settings:
    return Settings(
        _env_file=None,  # type: ignore  # pydantic-settings runtime kwarg, not a model field
        signal_account=ACCOUNT,
        engine_base_url="https://api.btservant.ai",
        engine_api_key=API_KEY,
        gateway_public_url="https://gw.fly.dev",
    )


class _FakeSignalClient:
    def __init__(self) -> None:
        self.sends: list[tuple[str, str]] = []

    async def send(
        self,
        chat_id: str,
        message: str,
        attachments: list[str] | None = None,
        text_styles: list[str] | None = None,
    ) -> bool:
        self.sends.append((chat_id, message))
        return True


def _client() -> tuple[_FakeSignalClient, TestClient]:
    fake = _FakeSignalClient()
    app = create_app(signal_client=cast(SignalClient, fake), settings=_settings())
    # TestClient runs background tasks before returning, so sends are observable.
    return fake, TestClient(app)


def _complete_body(message_key: str = "k1") -> dict[str, object]:
    return {
        "type": "complete",
        "user_id": USER_ID,
        "message_key": message_key,
        "text": "hello there",
    }


def _post(client: TestClient, body: object, *, token: str | None = API_KEY) -> httpx.Response:
    headers = {"X-Engine-Token": token} if token is not None else {}
    return client.post("/progress-callback", json=body, headers=headers)


def test_missing_token_is_rejected() -> None:
    fake, client = _client()
    resp = _post(client, _complete_body(), token=None)
    assert resp.status_code == 401
    assert fake.sends == []


def test_bad_token_is_rejected() -> None:
    fake, client = _client()
    resp = _post(client, _complete_body(), token="wrong")
    assert resp.status_code == 401
    assert fake.sends == []


def test_complete_delivers_chunked_reply() -> None:
    fake, client = _client()
    resp = _post(client, _complete_body())
    assert resp.status_code == 200
    assert fake.sends == [(USER_ID, "hello there")]


def test_group_complete_routes_to_chat_id() -> None:
    fake, client = _client()
    body = {**_complete_body(), "chat_id": GROUP_CHAT_ID}
    resp = _post(client, body)
    assert resp.status_code == 200
    assert fake.sends == [(GROUP_CHAT_ID, "hello there")]


def test_duplicate_complete_delivered_once() -> None:
    fake, client = _client()
    assert _post(client, _complete_body("dup")).status_code == 200
    assert _post(client, _complete_body("dup")).status_code == 200
    assert fake.sends == [(USER_ID, "hello there")]


def test_error_sends_fallback() -> None:
    fake, client = _client()
    body = {"type": "error", "user_id": USER_ID, "message_key": "k1", "error": "boom"}
    resp = _post(client, body)
    assert resp.status_code == 200
    assert len(fake.sends) == 1
    assert fake.sends[0][0] == USER_ID


def test_status_and_progress_are_acked_without_sending() -> None:
    fake, client = _client()
    for type_ in ("status", "progress"):
        body = {"type": type_, "user_id": USER_ID, "message_key": "k1", "text": "..."}
        resp = _post(client, body)
        assert resp.status_code == 200
    assert fake.sends == []


def test_unrecognized_payload_is_bad_request() -> None:
    fake, client = _client()
    resp = _post(client, {"type": "complete"})  # missing user_id/message_key
    assert resp.status_code == 400
    assert fake.sends == []


def test_malformed_json_is_bad_request() -> None:
    _, client = _client()
    resp = client.post(
        "/progress-callback",
        content=b"not json",
        headers={"X-Engine-Token": API_KEY, "Content-Type": "application/json"},
    )
    assert resp.status_code == 400
