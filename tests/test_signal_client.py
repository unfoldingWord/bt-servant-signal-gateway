"""Unit tests for the signal-cli JSON-RPC client.

Drives :class:`SignalClient` against an ``httpx.MockTransport`` standing in for
the signal-cli daemon — no live daemon, filesystem, or network required.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Iterator

import httpx
import pytest

from bt_signal_gateway.config import Settings
from bt_signal_gateway.signal_client import SignalClient, markdown_to_signal
from bt_signal_gateway.signal_rate_limit import _reset_scheduler

ACCOUNT = "+15551234567"
PEER = "+15559998888"
PEER_UUID = "11111111-2222-3333-4444-555555555555"


def _settings() -> Settings:
    return Settings(
        _env_file=None,  # type: ignore  # pydantic-settings runtime kwarg, not a model field
        signal_account=ACCOUNT,
        engine_base_url="https://api.btservant.ai",
        engine_api_key="secret",
        gateway_public_url="https://gw.fly.dev",
    )


class FakeDaemon:
    """Records JSON-RPC requests and replies from a per-method response script."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        # method -> list of result/error specs, popped in order; falls back to
        # a generic success once exhausted.
        self._scripts: dict[str, list[dict]] = {}

    def script(self, method: str, *responses: dict) -> None:
        self._scripts[method] = list(responses)

    def _next(self, method: str) -> dict:
        queue = self._scripts.get(method)
        if queue:
            return queue.pop(0)
        return {"result": {"timestamp": 1}}

    def calls(self, method: str) -> list[dict]:
        return [r for r in self.requests if r.get("method") == method]

    def handler(self, request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        self.requests.append(payload)
        spec = self._next(payload["method"])
        body = {"jsonrpc": "2.0", "id": payload.get("id")}
        if "error" in spec:
            body["error"] = spec["error"]
        else:
            body["result"] = spec.get("result", {"timestamp": 1})
        return httpx.Response(200, json=body)


@pytest.fixture(autouse=True)
def _reset() -> Iterator[None]:
    _reset_scheduler()
    yield
    _reset_scheduler()


def _make_client(daemon: FakeDaemon) -> SignalClient:
    transport = httpx.MockTransport(daemon.handler)
    return SignalClient(_settings(), client=httpx.AsyncClient(transport=transport))


# ---------------------------------------------------------------------------
# send — routing
# ---------------------------------------------------------------------------


async def test_send_dm_sets_recipient() -> None:
    daemon = FakeDaemon()
    client = _make_client(daemon)
    ok = await client.send(PEER, "hello")
    assert ok is True

    (call,) = daemon.calls("send")
    params = call["params"]
    assert params["recipient"] == [PEER]
    assert "groupId" not in params
    assert params["message"] == "hello"
    assert params["account"] == ACCOUNT


async def test_send_group_sets_group_id() -> None:
    daemon = FakeDaemon()
    client = _make_client(daemon)
    ok = await client.send("group:abc123==", "hi team")
    assert ok is True

    (call,) = daemon.calls("send")
    params = call["params"]
    assert params["groupId"] == "abc123=="
    assert "recipient" not in params


async def test_send_returns_false_on_rpc_error() -> None:
    daemon = FakeDaemon()
    daemon.script("send", {"error": {"code": 1, "message": "boom"}})
    client = _make_client(daemon)
    assert await client.send(PEER, "hello") is False


# ---------------------------------------------------------------------------
# send — markdown styling
# ---------------------------------------------------------------------------


async def test_send_applies_single_text_style() -> None:
    daemon = FakeDaemon()
    client = _make_client(daemon)
    await client.send(PEER, "**bold**")

    params = daemon.calls("send")[0]["params"]
    assert params["message"] == "bold"
    assert params["textStyle"] == "0:4:BOLD"
    assert "textStyles" not in params


async def test_send_applies_multiple_text_styles() -> None:
    daemon = FakeDaemon()
    client = _make_client(daemon)
    await client.send(PEER, "**a** and _b_")

    params = daemon.calls("send")[0]["params"]
    assert params["message"] == "a and b"
    assert params["textStyles"] == ["0:1:BOLD", "6:1:ITALIC"]


async def test_explicit_text_styles_override_markdown() -> None:
    daemon = FakeDaemon()
    client = _make_client(daemon)
    await client.send(PEER, "**not converted**", text_styles=[])

    params = daemon.calls("send")[0]["params"]
    # text_styles=[] suppresses conversion: message stays literal, no styles.
    assert params["message"] == "**not converted**"
    assert "textStyle" not in params
    assert "textStyles" not in params


def test_markdown_to_signal_styles() -> None:
    assert markdown_to_signal("# Title") == ("Title", ["0:5:BOLD"])
    assert markdown_to_signal("say `x` now") == ("say x now", ["4:1:MONOSPACE"])
    assert markdown_to_signal("~~gone~~") == ("gone", ["0:4:STRIKETHROUGH"])
    assert markdown_to_signal("plain text") == ("plain text", [])


def test_markdown_inline_code_preserves_markers() -> None:
    # Markdown markers inside an inline code span must stay literal, not be
    # stripped as bold/italic.
    assert markdown_to_signal("`**x**`") == ("**x**", ["0:5:MONOSPACE"])


def test_markdown_fenced_code_preserves_markers() -> None:
    # Fenced code content must not be re-parsed by the inline formatting pass.
    assert markdown_to_signal("```**x**```") == ("**x**", ["0:5:MONOSPACE"])


def test_markdown_to_signal_utf16_offsets() -> None:
    # An emoji outside the BMP is two UTF-16 code units; the bold run must
    # report length 2, and a following style must start past it.
    plain, styles = markdown_to_signal("**😀** ok")
    assert plain == "😀 ok"
    assert styles == ["0:2:BOLD"]


# ---------------------------------------------------------------------------
# UUID resolution
# ---------------------------------------------------------------------------


async def test_send_upgrades_e164_to_uuid_via_contacts() -> None:
    daemon = FakeDaemon()
    daemon.script(
        "listContacts",
        {"result": [{"number": PEER, "uuid": PEER_UUID}]},
    )
    client = _make_client(daemon)

    await client.send(PEER, "first")
    await client.send(PEER, "second")

    # listContacts is queried once; the mapping is cached for the second send.
    assert len(daemon.calls("listContacts")) == 1
    sends = daemon.calls("send")
    assert sends[0]["params"]["recipient"] == [PEER_UUID]
    assert sends[1]["params"]["recipient"] == [PEER_UUID]


async def test_remember_identifiers_seeds_cache() -> None:
    daemon = FakeDaemon()
    client = _make_client(daemon)
    client.remember_identifiers(PEER, PEER_UUID)

    await client.send(PEER, "hi")
    # Cache hit means no listContacts round-trip.
    assert daemon.calls("listContacts") == []
    assert daemon.calls("send")[0]["params"]["recipient"] == [PEER_UUID]


async def test_send_to_uuid_passes_through_unresolved() -> None:
    daemon = FakeDaemon()
    client = _make_client(daemon)
    await client.send(PEER_UUID, "hi")
    assert daemon.calls("listContacts") == []
    assert daemon.calls("send")[0]["params"]["recipient"] == [PEER_UUID]


# ---------------------------------------------------------------------------
# Attachments + rate-limit backoff
# ---------------------------------------------------------------------------


async def test_send_with_attachment_succeeds() -> None:
    daemon = FakeDaemon()
    client = _make_client(daemon)
    ok = await client.send(PEER, "see file", attachments=["/tmp/a.png"])
    assert ok is True
    params = daemon.calls("send")[0]["params"]
    assert params["attachments"] == ["/tmp/a.png"]


async def test_attachment_send_retries_after_rate_limit() -> None:
    daemon = FakeDaemon()
    # First attempt 429 (tiny retry_after keeps the scheduler pause ~10ms),
    # second attempt succeeds.
    daemon.script(
        "send",
        {"error": {"code": -5, "message": "Retry after 0.01 seconds"}},
        {"result": {"timestamp": 99}},
    )
    client = _make_client(daemon)

    ok = await client.send(PEER, "x", attachments=["/tmp/a.png"], text_styles=[])
    assert ok is True
    assert len(daemon.calls("send")) == 2


async def test_attachment_send_gives_up_after_max_attempts() -> None:
    daemon = FakeDaemon()
    daemon.script(
        "send",
        {"error": {"code": -5, "message": "Retry after 0.01 seconds"}},
        {"error": {"code": -5, "message": "Retry after 0.01 seconds"}},
    )
    client = _make_client(daemon)

    ok = await client.send(PEER, "x", attachments=["/tmp/a.png"], text_styles=[])
    assert ok is False


# ---------------------------------------------------------------------------
# Reactions
# ---------------------------------------------------------------------------


async def test_send_reaction_params() -> None:
    daemon = FakeDaemon()
    client = _make_client(daemon)
    ok = await client.send_reaction(PEER, "👀", PEER, 1700000000000)
    assert ok is True

    params = daemon.calls("sendReaction")[0]["params"]
    assert params["emoji"] == "👀"
    assert params["targetAuthor"] == PEER
    assert params["targetTimestamp"] == 1700000000000
    assert params["recipient"] == [PEER]


async def test_remove_reaction_sets_remove_flag() -> None:
    daemon = FakeDaemon()
    client = _make_client(daemon)
    ok = await client.remove_reaction("group:gid", PEER, 1700000000000)
    assert ok is True

    params = daemon.calls("sendReaction")[0]["params"]
    assert params["remove"] is True
    assert params["emoji"] == ""
    assert params["groupId"] == "gid"


# ---------------------------------------------------------------------------
# get_attachment
# ---------------------------------------------------------------------------


async def test_get_attachment_decodes_data() -> None:
    raw = b"\x89PNG\r\n\x1a\n payload"
    daemon = FakeDaemon()
    daemon.script(
        "getAttachment",
        {"result": {"data": base64.b64encode(raw).decode()}},
    )
    client = _make_client(daemon)

    assert await client.get_attachment("att-1") == raw


async def test_get_attachment_handles_bare_base64() -> None:
    raw = b"hello"
    daemon = FakeDaemon()
    daemon.script("getAttachment", {"result": base64.b64encode(raw).decode()})
    client = _make_client(daemon)
    assert await client.get_attachment("att-1") == raw


async def test_get_attachment_missing_returns_none() -> None:
    daemon = FakeDaemon()
    daemon.script("getAttachment", {"result": None})
    client = _make_client(daemon)
    assert await client.get_attachment("att-1") is None
