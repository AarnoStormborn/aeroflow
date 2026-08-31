"""Unit tests for OpenSky client, local storage, and failure capture."""

import tempfile
from unittest.mock import patch

from src.ingestion.components.client import OpenSkyClient
from src.ingestion.components.local_storage import LocalStorage


def test_local_storage_states_to_dataframe():
    """Test parsing raw OpenSky state vector array into typed Polars DataFrame."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalStorage(base_dir=tmpdir)

        mock_states_response = {
            "time": 1766880000,
            "states": [
                [
                    "800c42",
                    "SEJ502",
                    "India",
                    1766880000,
                    1766880000,
                    72.8656,
                    19.0896,
                    1200.0,
                    False,
                    140.5,
                    270.0,
                    -2.5,
                    None,
                    1250.0,
                    "4211",
                    False,
                    0,
                    0,
                ]
            ]
        }

        df = storage.states_to_dataframe(mock_states_response)

        assert len(df) == 1
        assert "icao24" in df.columns
        assert "callsign" in df.columns
        assert "capture_time" in df.columns
        assert df["icao24"][0] == "800c42"
        assert df["callsign"][0] == "SEJ502"
        assert df["latitude"][0] == 19.0896
        assert df["longitude"][0] == 72.8656


def test_opensky_client_bounding_box_param():
    """Test client URL formatting with bounding box."""
    client = OpenSkyClient(base_url="https://opensky-network.org/api")

    with patch("httpx.Client.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"time": 1234567890, "states": []}

        # lamin, lomin, lamax, lomax
        bbox = (18.0, 71.5, 20.0, 74.0)
        res = client.get_states(bounding_box=bbox)

        assert res["states"] == []
        mock_get.assert_called_once()
        params = mock_get.call_args[1].get("params", {})
        assert params.get("lamin") == 18.0
        assert params.get("lomin") == 71.5
        assert params.get("lamax") == 20.0
        assert params.get("lomax") == 74.0


def test_opensky_client_refreshes_token_on_401():
    """Test client re-fetches OAuth token on 401 and retries once."""
    client = OpenSkyClient(
        base_url="https://opensky-network.org/api",
        client_id="test-client",
        client_secret="test-secret",
    )

    # First call returns 401 (expired token), retry returns 200
    responses = [
        {"status_code": 401, "json.return_value": {"detail": "Unauthorized"}},
        {"status_code": 200, "json.return_value": {"time": 123, "states": []}},
    ]

    with patch("httpx.Client.get") as mock_get:
        def side_effect(*args, **kwargs):
            resp = responses.pop(0)
            mock = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()
            mock.status_code = resp["status_code"]
            mock.json.return_value = resp["json.return_value"]
            mock.text = ""
            return mock

        mock_get.side_effect = side_effect

        # Token re-fetch also needs to succeed
        with patch("httpx.Client.post") as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {"access_token": "new-token"}

            res = client.get_states(bounding_box=(18.0, 71.5, 20.0, 74.0))

    assert res["states"] == []
    # Token was refreshed
    assert client._token == "new-token"
    # 2 GETs: original 401 + retry
    assert mock_get.call_count == 2


def test_opensky_client_raises_on_persistent_401():
    """Test client raises OpenSkyAPIError if re-fetch fails or still 401."""
    from src.ingestion.components.client import OpenSkyAPIError

    client = OpenSkyClient(
        base_url="https://opensky-network.org/api",
        client_id="test-client",
        client_secret="test-secret",
    )

    # Both attempts return 401
    with patch("httpx.Client.get") as mock_get:
        mock_get.return_value.status_code = 401
        mock_get.return_value.json.return_value = {"detail": "Unauthorized"}
        mock_get.return_value.text = "Unauthorized"

        with patch("httpx.Client.post") as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {"access_token": "new-token"}

            try:
                client.get_states(bounding_box=(18.0, 71.5, 20.0, 74.0))
                raise AssertionError("Should have raised OpenSkyAPIError")
            except OpenSkyAPIError:
                pass


def test_opensky_client_timeout_includes_timeout_value():
    """Test timeout error message includes the configured timeout (not 'None')."""
    from src.ingestion.components.client import OpenSkyClient
    from src.utils.exceptions import APITimeoutError

    client = OpenSkyClient(
        base_url="https://opensky-network.org/api",
        timeout=60,
    )

    with patch("httpx.Client.get") as mock_get:
        mock_get.side_effect = httpx_timeout()

        try:
            client.get_states(bounding_box=(18.0, 71.5, 20.0, 74.0))
            raise AssertionError("Should have raised APITimeoutError")
        except APITimeoutError as e:
            assert e.timeout == 60
            assert "after 60s" in str(e)


def httpx_timeout():
    import httpx
    return httpx.TimeoutException("Request timed out")
