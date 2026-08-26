"""Unit tests for OpenSky client, local storage, and failure capture."""

from datetime import datetime, timezone
from unittest.mock import Mock, patch
import tempfile
from pathlib import Path
import polars as pl
import pytest

from src.ingestion.components.client import OpenSkyClient
from src.ingestion.components.local_storage import LocalStorage
from src.ingestion.db import IngestionRepository, IngestionStatus
from src.utils.exceptions import (
    RateLimitError,
    APIConnectionError,
    APITimeoutError,
    S3UploadError,
)


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
