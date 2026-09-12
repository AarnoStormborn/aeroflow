"""Tests for the forecasting HTTP API's API-key requirement.

`/forecast` performs real work on a publicly reachable URL, so the key check is
a security control: these tests pin the fail-closed behaviour.
"""

import pytest
from fastapi.testclient import TestClient
from src.forecasting.api import app
from src.forecasting.config import settings

KEY = "test-key-abc123"


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(settings, "api_key", KEY)
    return TestClient(app)


def test_missing_header_is_rejected(client):
    assert client.get("/health").status_code == 401
    assert client.get("/models").status_code == 401


def test_wrong_key_is_rejected(client):
    r = client.get("/health", headers={"Authorization": "Bearer nope"})
    assert r.status_code == 401


def test_malformed_scheme_is_rejected(client):
    """A raw key with no scheme must not be accepted."""
    for value in (KEY, f"Basic {KEY}", f"Token {KEY}", "Bearer", "Bearer "):
        r = client.get("/health", headers={"Authorization": value})
        assert r.status_code == 401, value


def test_correct_key_is_accepted(client):
    r = client.get("/health", headers={"Authorization": f"Bearer {KEY}"})
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_scheme_is_case_insensitive(client):
    """RFC 7235: the auth scheme is case-insensitive."""
    r = client.get("/health", headers={"Authorization": f"bearer {KEY}"})
    assert r.status_code == 200


def test_rejection_advertises_bearer(client):
    r = client.get("/health")
    assert r.headers.get("WWW-Authenticate") == "Bearer"


def test_unset_key_fails_closed(monkeypatch):
    """An unconfigured key must reject, never fall open."""
    monkeypatch.setattr(settings, "api_key", None)
    c = TestClient(app)
    assert c.get("/health").status_code == 503
    assert c.get("/health", headers={"Authorization": "Bearer anything"}).status_code == 503


def test_models_route_also_protected(client):
    assert client.get("/models").status_code == 401
    assert client.get("/models", headers={"Authorization": f"Bearer {KEY}"}).status_code == 200
