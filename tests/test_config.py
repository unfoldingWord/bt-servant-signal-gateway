"""Unit tests for typed configuration parsing and validation.

Construct ``Settings`` through the environment (the real load path) rather than
passing kwargs, so the string -> typed coercion and validators are exercised.
``_env_file=None`` keeps a developer's local ``.env`` out of the test.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from bt_signal_gateway.config import Settings

# Minimal env to construct a valid Settings.
REQUIRED_ENV = {
    "SIGNAL_ACCOUNT": "+15551234567",
    "ENGINE_BASE_URL": "https://api.btservant.ai",
    "ENGINE_API_KEY": "secret-token",
    "GATEWAY_PUBLIC_URL": "https://gw.fly.dev",
}

ALL_ENV_KEYS = [
    *REQUIRED_ENV,
    "SIGNAL_HTTP_URL",
    "ENGINE_ORG",
    "HOST",
    "PORT",
    "CHUNK_SIZE",
    "MESSAGE_AGE_CUTOFF_SECONDS",
    "SIGNAL_GROUP_ALLOWED_USERS",
    "SIGNAL_REQUIRE_MENTION",
]


def _build(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> Settings:
    """Set a clean env (required + overrides) and construct Settings."""
    for key in ALL_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in {**REQUIRED_ENV, **overrides}.items():
        monkeypatch.setenv(key, value)
    return Settings(_env_file=None)  # type: ignore  # fields come from the environment


def test_defaults_applied(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _build(monkeypatch)
    assert settings.signal_http_url == "http://127.0.0.1:8080"
    assert settings.engine_org == "unfoldingWord"
    assert settings.host == "0.0.0.0"
    assert settings.port == 8081
    assert settings.chunk_size == 1500
    assert settings.message_age_cutoff_seconds == 3600
    assert settings.signal_group_allowed_users == []
    assert settings.signal_require_mention is True


def test_required_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ALL_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in REQUIRED_ENV.items():
        if key != "ENGINE_API_KEY":
            monkeypatch.setenv(key, value)
    with pytest.raises(ValidationError):
        Settings(_env_file=None)  # type: ignore  # intentionally missing a required var


@pytest.mark.parametrize("good", ["+15551234567", "+447911123456", "+12"])
def test_e164_accepts_valid(monkeypatch: pytest.MonkeyPatch, good: str) -> None:
    assert _build(monkeypatch, SIGNAL_ACCOUNT=good).signal_account == good


@pytest.mark.parametrize("bad", ["15551234567", "+0123456789", "not-a-number", "+", "+1"])
def test_e164_rejects_invalid(monkeypatch: pytest.MonkeyPatch, bad: str) -> None:
    with pytest.raises(ValidationError):
        _build(monkeypatch, SIGNAL_ACCOUNT=bad)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("a,b", ["a", "b"]),
        (" a , b , ", ["a", "b"]),
        ("", []),
        ("*", ["*"]),
    ],
)
def test_group_allowed_users_comma_parsing(
    monkeypatch: pytest.MonkeyPatch, raw: str, expected: list[str]
) -> None:
    settings = _build(monkeypatch, SIGNAL_GROUP_ALLOWED_USERS=raw)
    assert settings.signal_group_allowed_users == expected


def test_progress_callback_url_strips_trailing_slash(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _build(monkeypatch, GATEWAY_PUBLIC_URL="https://gw.fly.dev/")
    assert settings.progress_callback_url == "https://gw.fly.dev/progress-callback"
