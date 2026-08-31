"""Unit tests for Discord notifier and unified notifier wiring."""

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import src.notifications.config as notif_config
from src.notifications.config import (
    DiscordSettings,
    NotificationSettings,
)
from src.notifications.discord import DiscordNotifier
from src.notifications.notifier import IngestionNotifier, create_notifier

# --- DiscordNotifier payload structure ---


@patch("src.notifications.discord.httpx.Client")
def test_notify_failure_builds_embed_payload(mock_client_cls):
    """Test failure notification builds a Discord embed payload."""
    mock_response = MagicMock()
    mock_response.status_code = 204
    mock_response.text = ""
    mock_client = MagicMock()
    mock_client.post.return_value = mock_response
    mock_client_cls.return_value.__enter__.return_value = mock_client

    notifier = DiscordNotifier(
        webhook_url="https://discord.com/api/webhooks/123/abc",
    )

    result = notifier.notify_failure(
        error_category="API_TIMEOUT",
        error_message="Request timed out",
        record_id=42,
        timestamp=datetime(2025, 12, 28, 12, 0, 0, tzinfo=timezone.utc),
    )

    assert result is True

    # Verify payload
    _, kwargs = mock_client.post.call_args
    payload = kwargs["json"]

    assert payload["content"] is None
    assert len(payload["embeds"]) == 1

    embed = payload["embeds"][0]
    assert embed["title"] == "🚨 Ingestion Failed"
    assert embed["color"] == 0xFF0000

    fields = {f["name"]: f["value"] for f in embed["fields"]}
    assert fields["Environment"] == "development"
    assert fields["Service"] == "ingestion-service"
    assert fields["Error Category"] == "`API_TIMEOUT`"
    assert fields["Record ID"] == "42"
    assert "Request timed out" in fields["Error Message"]

    assert embed["footer"]["text"] == "⏰ 2025-12-28 12:00:00 UTC"


@patch("src.notifications.discord.httpx.Client")
def test_notify_failure_record_id_none(mock_client_cls):
    """Test record_id defaults to N/A when not provided."""
    mock_response = MagicMock()
    mock_response.status_code = 204
    mock_response.text = ""
    mock_client = MagicMock()
    mock_client.post.return_value = mock_response
    mock_client_cls.return_value.__enter__.return_value = mock_client

    notifier = DiscordNotifier(webhook_url="https://discord.com/api/webhooks/123/abc")
    notifier.notify_failure(error_category="RATE_LIMIT", error_message="Quota exceeded")

    _, kwargs = mock_client.post.call_args
    fields = {f["name"]: f["value"] for f in kwargs["json"]["embeds"][0]["fields"]}
    assert fields["Record ID"] == "N/A"


# --- DiscordNotifier enabled/disabled behavior ---


@pytest.fixture
def clean_notification_env():
    """Clear Discord env vars and reset the settings singleton for env-independent tests."""
    saved = {
        k: os.environ.get(k) for k in
        ("DISCORD_WEBHOOK_URL", "DISCORD_ENABLED", "NOTIFY_ENVIRONMENT", "NOTIFY_SERVICE_NAME")
        if k in os.environ
    }
    for k in saved:
        os.environ.pop(k, None)
    # Rebuild the singleton WITHOUT env vars, and point discord.py at the fresh one
    notif_config._notification_settings = None
    fresh = notif_config.get_notification_settings()
    with patch("src.notifications.discord.notification_settings", fresh):
        yield
    # Restore
    for k, v in saved.items():
        os.environ[k] = v
    notif_config._notification_settings = None


def test_notifier_disabled_without_webhook(clean_notification_env):
    """Test notifier is disabled when no webhook URL is configured."""
    notifier = DiscordNotifier(webhook_url=None)
    assert notifier.enabled is False


@patch("src.notifications.discord.httpx.Client")
def test_send_disabled_skips_request(mock_client_cls, clean_notification_env):
    """Test _send returns False and does not POST when disabled."""
    notifier = DiscordNotifier(webhook_url=None)
    result = notifier._send({"content": "test"})

    assert result is False
    mock_client_cls.assert_not_called()


@patch("src.notifications.discord.httpx.Client")
def test_webhook_error_returns_false(mock_client_cls):
    """Test non-204 response returns False."""
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_client = MagicMock()
    mock_client.post.return_value = mock_response
    mock_client_cls.return_value.__enter__.return_value = mock_client

    notifier = DiscordNotifier(webhook_url="https://discord.com/api/webhooks/123/abc")
    result = notifier.notify_failure(error_category="API_TIMEOUT", error_message="x")

    assert result is False


# --- Config ---


@patch.dict("os.environ", {}, clear=True)
def test_discord_settings_defaults():
    """Test DiscordSettings default values (independent of real env)."""
    settings = DiscordSettings()
    assert settings.enabled is True
    assert settings.webhook_url is None


def test_notification_settings_contains_discord(clean_notification_env):
    """Test NotificationSettings nests DiscordSettings (independent of real env)."""
    settings = NotificationSettings()
    assert isinstance(settings.discord, DiscordSettings)
    assert settings.environment == "development"
    assert settings.service_name == "ingestion-service"


# --- Unified notifier ---


def test_create_notifier_returns_ingestion_notifier():
    """Test create_notifier builds an IngestionNotifier with a Discord notifier."""
    notifier = create_notifier()
    assert isinstance(notifier, IngestionNotifier)
    assert isinstance(notifier.discord, DiscordNotifier)


def test_on_failure_delegates_to_discord():
    """Test on_failure forwards to the Discord notifier."""
    mock_discord = MagicMock()
    notifier = IngestionNotifier(discord=mock_discord)

    notifier.on_failure(
        record_id=7,
        error_category="S3_UPLOAD",
        error_message="Upload failed",
    )

    mock_discord.notify_failure.assert_called_once_with(
        error_category="S3_UPLOAD",
        error_message="Upload failed",
        record_id=7,
    )


def test_notify_from_record_failure_extracts_category():
    """Test notify_from_record parses [CATEGORY] prefix from error message."""
    from src.ingestion.db import IngestionStatus

    mock_discord = MagicMock()
    notifier = IngestionNotifier(discord=mock_discord)

    record = MagicMock()
    record.status = IngestionStatus.FAILED
    record.id = 9
    record.error_message = "[API_TIMEOUT] Request took too long"
    record.s3_path = None
    record.record_count = 0

    notifier.notify_from_record(record)

    mock_discord.notify_failure.assert_called_once_with(
        error_category="API_TIMEOUT",
        error_message="Request took too long",
        record_id=9,
    )


def test_notify_from_record_success_does_not_notify():
    """Test notify_from_record logs success but does not alert."""
    from src.ingestion.db import IngestionStatus

    mock_discord = MagicMock()
    notifier = IngestionNotifier(discord=mock_discord)

    record = MagicMock()
    record.status = IngestionStatus.SUCCESS
    record.id = 3
    record.error_message = None
    record.s3_path = "s3://bucket/key"
    record.record_count = 26

    notifier.notify_from_record(record)

    mock_discord.notify_failure.assert_not_called()
