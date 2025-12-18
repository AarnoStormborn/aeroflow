"""
OpenSky API client for fetching flight data.

Provides methods to fetch flight data from the OpenSky Network API.
Documentation: https://openskynetwork.github.io/opensky-api/
"""

import time
from datetime import datetime, timezone
from typing import Any

import httpx

from src.utils import logger
from src.utils.exceptions import (
    OpenSkyAPIError,
    RateLimitError,
    APIConnectionError,
    APITimeoutError,
)
from src.ingestion.config import settings


class OpenSkyClient:
    """
    Client for interacting with the OpenSky Network API.
    
    The OpenSky API provides flight tracking data including:
    - Real-time state vectors (position, velocity, etc.)
    - Historical flight data
    - Aircraft metadata
    
    Supports OAuth2 client credentials for higher rate limits.
    """
    
    def __init__(
        self,
        base_url: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: int | None = None,
    ):
        """
        Initialize the OpenSky client.
        
        Args:
            base_url: API base URL (defaults to settings)
            client_id: OAuth2 client ID
            client_secret: OAuth2 client secret
            username: Legacy basic auth username
            password: Legacy basic auth password
            timeout: Request timeout in seconds
        """
        self.base_url = base_url or settings.opensky.base_url
        self.auth_url = settings.opensky.auth_url
        self.client_id = client_id or settings.opensky.client_id
        self.client_secret = client_secret or settings.opensky.client_secret
        self.username = username or settings.opensky.username
        self.password = password or settings.opensky.password
        self.timeout = timeout or settings.opensky.timeout_seconds
        
        # Auth headers/token
        self._auth = None
        self._token = None
        
        # Try OAuth first, then fall back to basic auth
        if self.client_id and self.client_secret:
            self._fetch_oauth_token()
        elif self.username and self.password:
            self._auth = (self.username, self.password)
            logger.info("OpenSky client initialized with basic authentication")
        else:
            logger.info("OpenSky client initialized without authentication (lower rate limits)")
    
    def _fetch_oauth_token(self) -> None:
        """Fetch OAuth2 access token using client credentials."""
        try:
            logger.info("Fetching OAuth2 token from OpenSky...")
            with httpx.Client(timeout=30) as client:
                response = client.post(
                    self.auth_url,
                    data={
                        "grant_type": "client_credentials",
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                    },
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                
                if response.status_code != 200:
                    logger.warning(f"OAuth token fetch failed: {response.status_code} - {response.text}")
                    logger.warning("Falling back to unauthenticated mode")
                    return
                
                token_data = response.json()
                self._token = token_data.get("access_token")
                
                if self._token:
                    logger.info("OpenSky client initialized with OAuth2 authentication")
                else:
                    logger.warning("OAuth response missing access_token, falling back to unauthenticated")
                    
        except Exception as e:
            logger.warning(f"Failed to fetch OAuth token: {e}")
            logger.warning("Falling back to unauthenticated mode")
    
    def _make_request(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """
        Make a request to the OpenSky API.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            Parsed JSON response
            
        Raises:
            OpenSkyAPIError: On API errors
            RateLimitError: When rate limit exceeded
            APIConnectionError: On connection failures
            APITimeoutError: On request timeout
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            logger.debug(f"Making request to {url} with params: {params}")
            
            # Build headers with Bearer token if available
            headers = {}
            if self._token:
                headers["Authorization"] = f"Bearer {self._token}"
            
            with httpx.Client(timeout=self.timeout) as client:
                response = client.get(
                    url,
                    params=params,
                    headers=headers if headers else None,
                    auth=self._auth if not self._token else None,
                )
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                raise RateLimitError(
                    message="OpenSky API rate limit exceeded",
                    retry_after=int(retry_after) if retry_after else None,
                )
            
            # Handle other errors
            if response.status_code != 200:
                raise OpenSkyAPIError(
                    message=f"API request failed: {response.status_code}",
                    status_code=response.status_code,
                    response_body=response.text,
                )
            
            data = response.json()
            logger.debug(f"Received response with {len(data) if isinstance(data, list) else 'object'} items")
            return data
            
        except httpx.ConnectError as e:
            raise APIConnectionError(f"Failed to connect to OpenSky API: {e}")
        except httpx.TimeoutException as e:
            raise APITimeoutError(f"Request to OpenSky API timed out: {e}")
        except httpx.HTTPError as e:
            raise OpenSkyAPIError(f"HTTP error occurred: {e}")
    
    def get_states(
        self,
        time_secs: int | None = None,
        icao24: str | list[str] | None = None,
        bounding_box: tuple[float, float, float, float] | None = None,
    ) -> dict[str, Any]:
        """
        Get current state vectors of aircraft.
        
        Args:
            time_secs: Unix timestamp for historical data (only for authenticated users)
            icao24: Filter by ICAO24 transponder address(es)
            bounding_box: Geographic bounding box (lamin, lomin, lamax, lomax)
            
        Returns:
            State vectors response containing 'time' and 'states' fields
        """
        params = {}
        
        if time_secs:
            params["time"] = time_secs
        
        if icao24:
            if isinstance(icao24, list):
                params["icao24"] = ",".join(icao24)
            else:
                params["icao24"] = icao24
        
        if bounding_box:
            params["lamin"] = bounding_box[0]
            params["lomin"] = bounding_box[1]
            params["lamax"] = bounding_box[2]
            params["lomax"] = bounding_box[3]
        
        logger.info("Fetching current state vectors from OpenSky API")
        return self._make_request("/states/all", params)
    
    def get_flights_by_time(
        self,
        begin: int,
        end: int,
    ) -> list[dict[str, Any]]:
        """
        Get flights within a time interval.
        
        Args:
            begin: Start of time interval (Unix timestamp)
            end: End of time interval (Unix timestamp)
            
        Returns:
            List of flight records
            
        Note:
            Maximum time interval is 2 hours (7200 seconds)
        """
        if end - begin > 7200:
            logger.warning("Time interval exceeds 2 hours, API may return partial results")
        
        params = {
            "begin": begin,
            "end": end,
        }
        
        logger.info(f"Fetching flights from {datetime.fromtimestamp(begin, tz=timezone.utc)} to {datetime.fromtimestamp(end, tz=timezone.utc)}")
        return self._make_request("/flights/all", params)
    
    def get_flights_by_aircraft(
        self,
        icao24: str,
        begin: int,
        end: int,
    ) -> list[dict[str, Any]]:
        """
        Get flights for a specific aircraft.
        
        Args:
            icao24: ICAO24 transponder address
            begin: Start of time interval (Unix timestamp)
            end: End of time interval (Unix timestamp)
            
        Returns:
            List of flight records for the aircraft
        """
        params = {
            "icao24": icao24.lower(),
            "begin": begin,
            "end": end,
        }
        
        logger.info(f"Fetching flights for aircraft {icao24}")
        return self._make_request("/flights/aircraft", params)
    
    def get_arrivals_by_airport(
        self,
        airport: str,
        begin: int,
        end: int,
    ) -> list[dict[str, Any]]:
        """
        Get arrivals at an airport.
        
        Args:
            airport: ICAO airport code
            begin: Start of time interval (Unix timestamp)
            end: End of time interval (Unix timestamp)
            
        Returns:
            List of arrival flight records
        """
        params = {
            "airport": airport.upper(),
            "begin": begin,
            "end": end,
        }
        
        logger.info(f"Fetching arrivals at {airport}")
        return self._make_request("/flights/arrival", params)
    
    def get_departures_by_airport(
        self,
        airport: str,
        begin: int,
        end: int,
    ) -> list[dict[str, Any]]:
        """
        Get departures from an airport.
        
        Args:
            airport: ICAO airport code
            begin: Start of time interval (Unix timestamp)
            end: End of time interval (Unix timestamp)
            
        Returns:
            List of departure flight records
        """
        params = {
            "airport": airport.upper(),
            "begin": begin,
            "end": end,
        }
        
        logger.info(f"Fetching departures from {airport}")
        return self._make_request("/flights/departure", params)


# Convenience function for quick access
def create_client() -> OpenSkyClient:
    """Create a new OpenSky client with default settings."""
    return OpenSkyClient()


__all__ = ["OpenSkyClient", "create_client"]
