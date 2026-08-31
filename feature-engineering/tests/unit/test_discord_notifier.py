"""Unit tests for the daily report Discord notifier."""

from datetime import date
from unittest.mock import MagicMock, patch

from src.features.report.discord_notifier import DiscordNotifier, create_discord_notifier


@patch("src.features.report.discord_notifier.httpx.Client")
def test_notify_report_ready_builds_embed(mock_client_cls):
    """Test report-ready notification builds a Discord embed with download links."""
    mock_response = MagicMock()
    mock_response.status_code = 204
    mock_response.text = ""
    mock_client = MagicMock()
    mock_client.post.return_value = mock_response
    mock_client_cls.return_value.__enter__.return_value = mock_client

    notifier = DiscordNotifier(webhook_url="https://discord.com/api/webhooks/123/abc")
    result = notifier.notify_report_ready(
        report_date=date(2025, 12, 28),
        record_count=1234,
        aircraft_count=56,
        pdf_url="https://example.com/report.pdf",
        data_url="https://example.com/data.parquet",
    )

    assert result is True

    _, kwargs = mock_client.post.call_args
    payload = kwargs["json"]

    assert payload["content"] is None
    assert len(payload["embeds"]) == 1

    embed = payload["embeds"][0]
    assert embed["title"] == "📊 Daily Flight Report Ready"
    assert embed["color"] == 0x00FF00

    fields = {f["name"]: f["value"] for f in embed["fields"]}
    assert fields["Date"] == "December 28, 2025"
    assert fields["Region"] == "Mumbai Airspace"
    assert fields["Total Records"] == "1,234"
    assert fields["Unique Aircraft"] == "56"
    assert fields["PDF Report"] == "[📄 Download](https://example.com/report.pdf)"
    assert fields["Cleaned Data"] == "[📦 Download](https://example.com/data.parquet)"
    assert "Links valid for 24 hours" in embed["footer"]["text"]


@patch("src.features.report.discord_notifier.httpx.Client")
def test_notify_report_failed_builds_embed(mock_client_cls):
    """Test report-failed notification builds a red embed with error details."""
    mock_response = MagicMock()
    mock_response.status_code = 204
    mock_response.text = ""
    mock_client = MagicMock()
    mock_client.post.return_value = mock_response
    mock_client_cls.return_value.__enter__.return_value = mock_client

    notifier = DiscordNotifier(webhook_url="https://discord.com/api/webhooks/123/abc")
    result = notifier.notify_report_failed(
        report_date=date(2025, 12, 28),
        error_message="No data available for 2025-12-28",
    )

    assert result is True

    _, kwargs = mock_client.post.call_args
    embed = kwargs["json"]["embeds"][0]

    assert embed["title"] == "🚨 Daily Report Failed"
    assert embed["color"] == 0xFF0000

    fields = {f["name"]: f["value"] for f in embed["fields"]}
    assert fields["Date"] == "December 28, 2025"
    assert fields["Region"] == "Mumbai Airspace"
    assert "No data available" in fields["Error"]


def test_notifier_disabled_without_webhook():
    """Test notifier is disabled when no webhook URL configured."""
    with patch("src.features.report.discord_notifier.settings") as mock_settings:
        mock_settings.discord_webhook_url = None
        notifier = DiscordNotifier(webhook_url=None)
        assert notifier.enabled is False


@patch("src.features.report.discord_notifier.httpx.Client")
def test_send_disabled_skips_request(mock_client_cls):
    """Test _send returns False and does not POST when disabled."""
    with patch("src.features.report.discord_notifier.settings") as mock_settings:
        mock_settings.discord_webhook_url = None
        notifier = DiscordNotifier(webhook_url=None)
        result = notifier._send({"content": "test"})

        assert result is False
    mock_client_cls.assert_not_called()


@patch("src.features.report.discord_notifier.httpx.Client")
def test_webhook_error_returns_false(mock_client_cls):
    """Test non-204 response returns False."""
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_client = MagicMock()
    mock_client.post.return_value = mock_response
    mock_client_cls.return_value.__enter__.return_value = mock_client

    notifier = DiscordNotifier(webhook_url="https://discord.com/api/webhooks/123/abc")
    result = notifier.notify_report_ready(
        report_date=date(2025, 12, 28),
        record_count=1,
        aircraft_count=1,
        pdf_url="https://example.com/report.pdf",
        data_url="https://example.com/data.parquet",
    )

    assert result is False


def test_create_discord_notifier():
    """Test factory returns a DiscordNotifier."""
    notifier = create_discord_notifier()
    assert isinstance(notifier, DiscordNotifier)
