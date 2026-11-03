"""Tests for the ``/progress-callback`` endpoint (auth, dedup, dispatch)."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import httpx
import pytest
from fastapi.testclient import TestClient

from bt_signal_gateway.callback_server import create_app
from bt_signal_gateway.config import Settings
from bt_signal_gateway.signal_client import SignalClient
from bt_signal_gateway.typing_indicator import TypingIndicators

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
    """Records ``send`` and typing calls; returns ``True`` (or a queued per-call result)."""

    def __init__(
        self,
        results: list[bool] | None = None,
        *,
        typing_raises: bool = False,
        typing_ok: bool = True,
    ) -> None:
        self.sends: list[tuple[str, str]] = []
        self.reactions: list[tuple[str, str, str, int]] = []
        self.typing: list[tuple[str, bool]] = []
        self._results = list(results) if results is not None else None
        self._typing_raises = typing_raises
        self._typing_ok = typing_ok

    async def send(
        self,
        chat_id: str,
        message: str,
        attachments: list[str] | None = None,
        text_styles: list[str] | None = None,
    ) -> bool:
        self.sends.append((chat_id, message))
        if self._results is None:
            return True
        return self._results.pop(0) if self._results else True

    async def send_reaction(
        self, chat_id: str, emoji: str, target_author: str, target_timestamp: int
    ) -> bool:
        self.reactions.append((chat_id, emoji, target_author, target_timestamp))
        return True

    async def send_typing(self, chat_id: str, *, stop: bool = False) -> bool:
        self.typing.append((chat_id, stop))
        if self._typing_raises:
            raise RuntimeError("typing rpc down")
        return self._typing_ok


def _client(
    results: list[bool] | None = None,
    *,
    typing_raises: bool = False,
    typing_ok: bool = True,
    now: Callable[[], float] | None = None,
) -> tuple[_FakeSignalClient, TestClient]:
    fake = _FakeSignalClient(results, typing_raises=typing_raises, typing_ok=typing_ok)
    typing = TypingIndicators(now=now) if now is not None else None
    app = create_app(
        signal_client=cast(SignalClient, fake),
        settings=_settings(),
        typing=typing,
    )
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


def test_failed_delivery_leaves_key_redeliverable() -> None:
    # First send fails (key stays uncompleted), retry succeeds, third is a dupe.
    fake, client = _client(results=[False, True])
    assert _post(client, _complete_body("retry")).status_code == 200
    assert len(fake.sends) == 1  # one failed attempt, key NOT marked complete

    assert _post(client, _complete_body("retry")).status_code == 200
    assert len(fake.sends) == 2  # re-delivered because the first attempt failed

    assert _post(client, _complete_body("retry")).status_code == 200
    assert len(fake.sends) == 2  # now completed → subsequent duplicate ignored


def test_error_sends_fallback() -> None:
    fake, client = _client()
    body = {"type": "error", "user_id": USER_ID, "message_key": "k1", "error": "boom"}
    resp = _post(client, body)
    assert resp.status_code == 200
    assert len(fake.sends) == 1
    assert fake.sends[0][0] == USER_ID


def test_status_is_acked_without_sending() -> None:
    fake, client = _client()
    body = {"type": "status", "user_id": USER_ID, "message_key": "k1", "text": "..."}
    resp = _post(client, body)
    assert resp.status_code == 200
    assert fake.sends == []


def test_status_shows_typing_for_the_dm_and_sends_no_message() -> None:
    fake, client = _client()
    body = {
        "type": "status",
        "user_id": USER_ID,
        "message_key": "k1",
        "text": "Looking up the verse",
    }
    resp = _post(client, body)
    assert resp.status_code == 200
    assert fake.sends == []
    assert fake.typing == [(USER_ID, False)]


def test_status_typing_is_throttled_to_once_per_ten_seconds() -> None:
    clock = {"now": 1_000.0}
    fake, client = _client(now=lambda: clock["now"])
    body = {"type": "status", "user_id": USER_ID, "message_key": "k1", "text": "working"}

    assert _post(client, body).status_code == 200
    assert _post(client, body).status_code == 200
    assert fake.typing == [(USER_ID, False)]

    clock["now"] = 1_009.9
    assert _post(client, body).status_code == 200
    assert fake.typing == [(USER_ID, False)]

    clock["now"] = 1_010.0
    assert _post(client, body).status_code == 200
    assert fake.typing == [(USER_ID, False), (USER_ID, False)]


def test_typing_throttle_is_per_chat() -> None:
    fake, client = _client(now=lambda: 0.0)
    dm = {"type": "status", "user_id": USER_ID, "message_key": "k1"}
    group = {
        "type": "status",
        "user_id": USER_ID,
        "message_key": "k1",
        "chat_id": GROUP_CHAT_ID,
    }
    assert _post(client, dm).status_code == 200
    assert _post(client, group).status_code == 200
    assert fake.typing == [(USER_ID, False), (GROUP_CHAT_ID, False)]


def test_status_shows_typing_for_the_group_chat() -> None:
    fake, client = _client()
    body = {
        "type": "status",
        "user_id": USER_ID,
        "message_key": "k1",
        "chat_id": GROUP_CHAT_ID,
        "text": "Looking up the verse",
    }
    resp = _post(client, body)
    assert resp.status_code == 200
    assert fake.sends == []
    assert fake.typing == [(GROUP_CHAT_ID, False)]


def test_complete_stops_typing_and_a_late_status_does_not_restart_it() -> None:
    fake, client = _client()
    assert _post(client, _complete_body()).status_code == 200
    assert fake.sends == [(USER_ID, "hello there")]
    assert fake.typing == [(USER_ID, True)]

    late = {"type": "status", "user_id": USER_ID, "message_key": "k1", "text": "still working"}
    assert _post(client, late).status_code == 200
    assert fake.sends == [(USER_ID, "hello there")]
    assert fake.typing == [(USER_ID, True)]


def test_error_stops_typing_and_a_late_status_does_not_restart_it() -> None:
    fake, client = _client()
    body = {"type": "error", "user_id": USER_ID, "message_key": "k1", "error": "boom"}
    assert _post(client, body).status_code == 200
    assert len(fake.sends) == 1
    assert fake.sends[0][0] == USER_ID
    assert fake.typing == [(USER_ID, True)]

    late = {"type": "status", "user_id": USER_ID, "message_key": "k1"}
    assert _post(client, late).status_code == 200
    assert len(fake.sends) == 1
    assert fake.typing == [(USER_ID, True)]


def test_status_then_complete_then_late_status_sends_one_start_and_one_stop() -> None:
    # The late status is past the 10s throttle, so only the finished turn can
    # keep it from starting the indicator again.
    clock = {"now": 0.0}
    fake, client = _client(now=lambda: clock["now"])
    status = {"type": "status", "user_id": USER_ID, "message_key": "k1", "text": "working"}
    assert _post(client, status).status_code == 200
    assert _post(client, _complete_body()).status_code == 200
    clock["now"] = 30.0
    assert _post(client, status).status_code == 200
    assert fake.sends == [(USER_ID, "hello there")]
    assert fake.typing == [(USER_ID, False), (USER_ID, True)]


def test_finished_turn_does_not_block_typing_for_a_new_message() -> None:
    fake, client = _client()
    assert _post(client, _complete_body("k1")).status_code == 200
    new_status = {"type": "status", "user_id": USER_ID, "message_key": "k2", "text": "next"}
    assert _post(client, new_status).status_code == 200
    assert fake.typing == [(USER_ID, True), (USER_ID, False)]


def test_group_complete_stops_typing_in_the_group() -> None:
    fake, client = _client()
    body = {**_complete_body(), "chat_id": GROUP_CHAT_ID}
    assert _post(client, body).status_code == 200
    assert fake.sends == [(GROUP_CHAT_ID, "hello there")]
    assert fake.typing == [(GROUP_CHAT_ID, True)]


def test_failed_typing_is_logged_and_does_not_break_the_callback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake, client = _client(typing_raises=True)
    status = {"type": "status", "user_id": USER_ID, "message_key": "k1", "text": "working"}
    with caplog.at_level("WARNING"):
        assert _post(client, status).status_code == 200
    assert fake.sends == []
    assert any("typing indicator failed" in record.message for record in caplog.records)

    caplog.clear()
    with caplog.at_level("WARNING"):
        assert _post(client, _complete_body()).status_code == 200
    assert fake.sends == [(USER_ID, "hello there")]
    assert any("typing indicator failed" in record.message for record in caplog.records)


def test_failed_typing_start_can_be_retried() -> None:
    fake, client = _client(typing_ok=False)
    status = {"type": "status", "user_id": USER_ID, "message_key": "k1"}
    assert _post(client, status).status_code == 200
    assert _post(client, status).status_code == 200
    assert fake.typing == [(USER_ID, False), (USER_ID, False)]
    assert fake.sends == []


def test_progress_does_not_send_typing() -> None:
    fake, client = _client()
    body = {"type": "progress", "user_id": USER_ID, "message_key": "k1", "text": "working…"}
    assert _post(client, body).status_code == 200
    assert fake.sends == [(USER_ID, "working…")]
    assert fake.typing == []


def test_progress_streams_text_as_new_message() -> None:
    # issue #28: progress is relayed as a new Signal message (not dropped).
    fake, client = _client()
    body = {"type": "progress", "user_id": USER_ID, "message_key": "k1", "text": "working…"}
    resp = _post(client, body)
    assert resp.status_code == 200
    assert fake.sends == [(USER_ID, "working…")]


def test_duplicate_progress_is_not_deduped() -> None:
    # Only terminal `complete` is deduped; progress fires every time.
    fake, client = _client()
    body = {"type": "progress", "user_id": USER_ID, "message_key": "samekey", "text": "step"}
    assert _post(client, body).status_code == 200
    assert _post(client, body).status_code == 200
    assert fake.sends == [(USER_ID, "step"), (USER_ID, "step")]


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
