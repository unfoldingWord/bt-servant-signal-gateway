"""Unit tests for Signal envelope parsing + filtering.

Drives :func:`parse_envelope` over representative signal-cli envelope shapes —
no live daemon, filesystem, or network required.
"""

from __future__ import annotations

from typing import Any

from bt_signal_gateway.config import Settings
from bt_signal_gateway.envelope import InboundMessage, parse_envelope

ACCOUNT = "+15551234567"
PEER = "+15559998888"
PEER_UUID = "11111111-2222-3333-4444-555555555555"
GROUP_ID = "abcdGROUPid=="
NOW = 1_700_000_000.0  # fixed wall clock for deterministic age-cutoff tests
NOW_MS = int(NOW * 1000)


def _settings(**overrides: Any) -> Settings:
    base: dict[str, Any] = dict(
        signal_account=ACCOUNT,
        engine_base_url="https://api.btservant.ai",
        engine_api_key="secret",
        gateway_public_url="https://gw.fly.dev",
    )
    base.update(overrides)
    return Settings(_env_file=None, **base)  # type: ignore  # pydantic-settings runtime kwarg


def _dm(message: str = "hello", **data: Any) -> dict[str, Any]:
    """A direct-message envelope from PEER."""
    return {
        "envelope": {
            "sourceNumber": PEER,
            "sourceUuid": PEER_UUID,
            "sourceName": "Peer",
            "timestamp": NOW_MS,
            "dataMessage": {"message": message, **data},
        }
    }


def _group(message: str = "hello", **data: Any) -> dict[str, Any]:
    """A group-message envelope from PEER."""
    env = _dm(message, **data)
    env["envelope"]["dataMessage"]["groupInfo"] = {"groupId": GROUP_ID, "groupName": "Translators"}
    return env


# --------------------------------------------------------------------------
# Accepted messages
# --------------------------------------------------------------------------


def test_dm_text_parses() -> None:
    msg = parse_envelope(_dm("hi there"), _settings(), now=NOW)
    assert isinstance(msg, InboundMessage)
    assert msg.text == "hi there"
    assert msg.user_id == PEER_UUID
    assert msg.chat_id == PEER_UUID
    assert msg.is_group is False
    assert msg.source_number == PEER
    assert msg.sender_name == "Peer"
    assert msg.timestamp_ms == NOW_MS


def test_unwrapped_envelope() -> None:
    """signal-cli sometimes delivers the envelope without the outer wrapper."""
    raw = _dm("hi")["envelope"]
    msg = parse_envelope(raw, _settings(), now=NOW)
    assert msg is not None and msg.text == "hi"


def test_group_text_allowed() -> None:
    settings = _settings(signal_group_allowed_users=[GROUP_ID], signal_require_mention=False)
    msg = parse_envelope(_group("team update"), settings, now=NOW)
    assert msg is not None
    assert msg.is_group is True
    assert msg.group_id == GROUP_ID
    assert msg.group_name == "Translators"
    assert msg.chat_id == f"group:{GROUP_ID}"


def test_group_wildcard_allowlist() -> None:
    settings = _settings(signal_group_allowed_users=["*"], signal_require_mention=False)
    assert parse_envelope(_group(), settings, now=NOW) is not None


def test_edit_message_data_is_used() -> None:
    raw = {
        "envelope": {
            "sourceUuid": PEER_UUID,
            "timestamp": NOW_MS,
            "editMessage": {"dataMessage": {"message": "edited"}},
        }
    }
    msg = parse_envelope(raw, _settings(), now=NOW)
    assert msg is not None and msg.text == "edited"


def test_quote_reply_fields() -> None:
    msg = parse_envelope(
        _dm("re", quote={"id": 1700000000111, "text": "original"}), _settings(), now=NOW
    )
    assert msg is not None
    assert msg.reply_to_id == "1700000000111"
    assert msg.reply_to_text == "original"


def test_attachment_refs_captured() -> None:
    msg = parse_envelope(
        _dm(
            "see this",
            attachments=[
                {"id": "att-1", "contentType": "image/jpeg", "size": 1234, "filename": "p.jpg"},
                {"contentType": "image/png"},  # no id → skipped
            ],
        ),
        _settings(),
        now=NOW,
    )
    assert msg is not None
    assert len(msg.attachments) == 1
    att = msg.attachments[0]
    assert att.id == "att-1"
    assert att.content_type == "image/jpeg"
    assert att.size == 1234
    assert att.filename == "p.jpg"


def test_attachment_only_message_is_kept() -> None:
    """No text but an attachment present → still a real message."""
    msg = parse_envelope(
        _dm("", attachments=[{"id": "att-1", "contentType": "audio/ogg"}]), _settings(), now=NOW
    )
    assert msg is not None and msg.text == ""
    assert msg.attachments[0].id == "att-1"


def test_mention_rendering() -> None:
    msg = parse_envelope(
        _dm("hey ￼ look", mentions=[{"start": 4, "length": 1, "name": "Bob"}]),
        _settings(),
        now=NOW,
    )
    assert msg is not None and msg.text == "hey @Bob look"


def test_mention_rendering_falls_back_to_number() -> None:
    msg = parse_envelope(
        _dm("￼", mentions=[{"start": 0, "length": 1, "number": PEER}]),
        _settings(),
        now=NOW,
    )
    assert msg is not None and msg.text == f"@{PEER}"


def test_group_require_mention_with_mention() -> None:
    settings = _settings(signal_group_allowed_users=["*"], signal_require_mention=True)
    msg = parse_envelope(
        _group("￼ help", mentions=[{"start": 0, "length": 1, "number": ACCOUNT}]),
        settings,
        now=NOW,
    )
    assert msg is not None and msg.text == f"@{ACCOUNT} help"


# --------------------------------------------------------------------------
# Filtered envelopes (return None)
# --------------------------------------------------------------------------


def test_sync_message_dropped() -> None:
    raw = {"envelope": {"sourceUuid": PEER_UUID, "timestamp": NOW_MS, "syncMessage": {}}}
    assert parse_envelope(raw, _settings(), now=NOW) is None


def test_receipt_message_dropped() -> None:
    raw = {"envelope": {"sourceUuid": PEER_UUID, "timestamp": NOW_MS, "receiptMessage": {}}}
    assert parse_envelope(raw, _settings(), now=NOW) is None


def test_typing_message_dropped() -> None:
    raw = {"envelope": {"sourceUuid": PEER_UUID, "timestamp": NOW_MS, "typingMessage": {}}}
    assert parse_envelope(raw, _settings(), now=NOW) is None


def test_story_message_dropped() -> None:
    raw = {"envelope": {"sourceUuid": PEER_UUID, "timestamp": NOW_MS, "storyMessage": {"x": 1}}}
    assert parse_envelope(raw, _settings(), now=NOW) is None


def test_self_source_dropped() -> None:
    """An envelope whose sender is the bot's own account is an echo → drop."""
    raw = {
        "envelope": {
            "sourceNumber": ACCOUNT,
            "timestamp": NOW_MS,
            "dataMessage": {"message": "echo"},
        }
    }
    assert parse_envelope(raw, _settings(), now=NOW) is None


def test_no_sender_dropped() -> None:
    raw = {"envelope": {"timestamp": NOW_MS, "dataMessage": {"message": "x"}}}
    assert parse_envelope(raw, _settings(), now=NOW) is None


def test_no_data_message_dropped() -> None:
    raw = {"envelope": {"sourceUuid": PEER_UUID, "timestamp": NOW_MS}}
    assert parse_envelope(raw, _settings(), now=NOW) is None


def test_contentless_dropped() -> None:
    """A dataMessage with only whitespace text and no attachments is metadata."""
    assert parse_envelope(_dm("   "), _settings(), now=NOW) is None


def test_group_no_allowlist_dropped() -> None:
    """Default (empty allowlist) disables groups entirely."""
    assert parse_envelope(_group(), _settings(), now=NOW) is None


def test_group_not_in_allowlist_dropped() -> None:
    settings = _settings(signal_group_allowed_users=["other-group"], signal_require_mention=False)
    assert parse_envelope(_group(), settings, now=NOW) is None


def test_group_require_mention_without_mention_dropped() -> None:
    settings = _settings(signal_group_allowed_users=["*"], signal_require_mention=True)
    assert parse_envelope(_group("just chatting"), settings, now=NOW) is None


def test_old_message_dropped() -> None:
    old = {
        "envelope": {
            "sourceUuid": PEER_UUID,
            "timestamp": NOW_MS - 7200 * 1000,  # 2h old, default cutoff is 1h
            "dataMessage": {"message": "stale"},
        }
    }
    assert parse_envelope(old, _settings(), now=NOW) is None


def test_recent_message_within_cutoff_kept() -> None:
    recent = {
        "envelope": {
            "sourceUuid": PEER_UUID,
            "timestamp": NOW_MS - 600 * 1000,  # 10 min old
            "dataMessage": {"message": "fresh"},
        }
    }
    assert parse_envelope(recent, _settings(), now=NOW) is not None
