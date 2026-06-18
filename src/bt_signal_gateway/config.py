"""Typed configuration.

Loads and validates environment into a typed :class:`Settings` object so the
rest of the gateway reads config instead of ``os.environ``. Required values
have no default, so a missing one fails fast with a ``ValidationError`` at
startup.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Annotated

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

# E.164: leading '+', first digit 1-9, up to 15 digits total.
_E164_RE = re.compile(r"^\+[1-9]\d{1,14}$")


class Settings(BaseSettings):
    """Gateway configuration, loaded from the environment (and ``.env``)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Signal (signal-cli daemon) ---
    signal_account: str
    signal_http_url: str = "http://127.0.0.1:8080"

    # --- BT Servant worker (the "brain") ---
    engine_base_url: str
    engine_org: str = "unfoldingWord"
    engine_api_key: str

    # --- This gateway ---
    gateway_public_url: str
    host: str = "0.0.0.0"
    port: int = 8081

    # --- Behavior ---
    chunk_size: int = 1500
    message_age_cutoff_seconds: int = 3600

    # --- Groups ---
    # NoDecode: keep the raw env string so the comma-splitting validator below
    # runs instead of pydantic-settings JSON-decoding it (which would reject
    # plain "a,b" input).
    signal_group_allowed_users: Annotated[list[str], NoDecode] = []
    signal_require_mention: bool = True

    @field_validator("signal_account")
    @classmethod
    def _validate_e164(cls, value: str) -> str:
        if not _E164_RE.match(value):
            raise ValueError(f"SIGNAL_ACCOUNT must be E.164 (e.g. +15551234567), got {value!r}")
        return value

    @field_validator("signal_group_allowed_users", mode="before")
    @classmethod
    def _split_comma_list(cls, value: object) -> object:
        """Parse a comma-separated string into a trimmed, non-empty list.

        Mirrors hermes ``_parse_comma_list``: ``"a, b,"`` -> ``["a", "b"]``,
        ``""`` -> ``[]``. Already-list values pass through unchanged.
        """
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @property
    def progress_callback_url(self) -> str:
        """URL the worker calls back into, derived once from the public URL."""
        return f"{self.gateway_public_url.rstrip('/')}/progress-callback"


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings instance (constructed once)."""
    return Settings()  # type: ignore  # values come from the environment
