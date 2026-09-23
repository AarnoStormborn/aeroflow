"""Tests for keyless external attribution of traffic anomalies.

Two things matter here and neither is the prose.

First, the gate. Most flagged days in this feed are measurement artifacts, and
attributing one to weather or a holiday is fabrication - the 2026-09-07
capture-rate collapse made a week of days look like a shortfall that no weather
report could explain.

Second, direction. Rain suppresses traffic; it does not produce a surge.
Offering rain against a rise invites a causal reading the evidence cannot
support.

Network access is stubbed, so the suite stays offline.
"""

from datetime import date

import httpx
import pytest
from src.dashboard import attribution as A


@pytest.fixture(autouse=True)
def _clear_memo():
    """The module memoises weather and calendars; keep tests independent."""
    A._MEMO.clear()
    yield
    A._MEMO.clear()


# ---- holidays (no network: the package ships the calendar) ---------------

def test_holiday_note_names_ganesh_chaturthi():
    """A major Mumbai holiday, and inside our data window."""
    assert A.holiday_note(date(2026, 9, 14)).startswith("Ganesh Chaturthi")


def test_holiday_note_includes_the_state_context():
    assert "Maharashtra" in A.holiday_note(date(2026, 9, 14))


def test_holiday_note_is_none_on_an_ordinary_day():
    assert A.holiday_note(date(2026, 9, 22)) is None


def test_holiday_note_takes_the_primary_name_when_several_apply():
    """The calendar joins observances with '; ' - we want just the first."""
    note = A.holiday_note(date(2026, 9, 4))
    assert note is not None
    assert ";" not in note.split(" (public holiday")[0]


# ---- the gate ------------------------------------------------------------

@pytest.mark.parametrize(
    "cause",
    [
        "incomplete ingestion",
        "baseline spans a data change",
        "sustained level shift",
    ],
)
def test_no_attribution_for_anything_that_is_not_a_transient_deviation(cause):
    """Every one of these is a measurement or structure artifact.

    A real example: 2026-09-14 was Ganesh Chaturthi AND had a broken ingestion
    hour. Blaming the holiday alone would be a confident half-truth.
    """
    assert A.attribute(date(2026, 9, 14), -18.0, cause) is None


def test_unknown_cause_is_not_attributed():
    assert A.attribute(date(2026, 9, 14), -18.0, "something new") is None


# ---- direction -----------------------------------------------------------

def test_rain_is_not_offered_against_a_rise():
    weather = {"rain_mm": 40.0, "gust_kmh": 70.0}
    assert A._weather_note(weather, 20.0) is None


def test_rain_is_offered_against_a_dip():
    note = A._weather_note({"rain_mm": 40.0, "gust_kmh": 20.0}, -20.0)
    assert note is not None and "40mm rain" in note


def test_unremarkable_weather_is_not_evidence():
    """A few mm through the monsoon is not an explanation for anything."""
    assert A._weather_note({"rain_mm": 1.0, "gust_kmh": 20.0}, -20.0) is None


def test_strong_gusts_alone_are_evidence():
    note = A._weather_note({"rain_mm": 0.0, "gust_kmh": 75.0}, -20.0)
    assert note is not None and "75km/h" in note


# ---- end-to-end attribution ---------------------------------------------

def test_attribute_combines_holiday_and_weather_and_names_sources(monkeypatch):
    monkeypatch.setattr(
        A, "_fetch_weather", lambda: {"2026-09-14": {"rain_mm": 11.0, "gust_kmh": 45.0}}
    )

    got = A.attribute(date(2026, 9, 14), -18.0, A.ATTRIBUTABLE_CAUSE)

    assert got is not None
    assert "Ganesh Chaturthi" in got["text"]
    assert "11mm rain" in got["text"]
    assert got["sources"] == ["public holidays (IN/MH)", "open-meteo.com"]


def test_attribute_is_none_when_there_is_nothing_verifiable(monkeypatch):
    monkeypatch.setattr(A, "_fetch_weather", lambda: {"2026-09-22": {"rain_mm": 0.5, "gust_kmh": 12.0}})
    assert A.attribute(date(2026, 9, 22), -18.0, A.ATTRIBUTABLE_CAUSE) is None


def test_attribute_works_with_no_weather_but_a_holiday(monkeypatch):
    monkeypatch.setattr(A, "_fetch_weather", lambda: {})
    got = A.attribute(date(2026, 9, 14), -18.0, A.ATTRIBUTABLE_CAUSE)
    assert got is not None
    assert got["sources"] == ["public holidays (IN/MH)"]


# ---- fail-soft -----------------------------------------------------------

def test_weather_lookup_returns_empty_on_a_connection_error(monkeypatch):
    def boom(*args, **kwargs):
        raise httpx.ConnectError("no route to host")

    monkeypatch.setattr(A.httpx, "get", boom)
    assert A._fetch_weather() == {}


def test_weather_lookup_returns_empty_on_a_malformed_payload(monkeypatch):
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"unexpected": True}

    monkeypatch.setattr(A.httpx, "get", lambda *a, **k: Response())
    assert A._fetch_weather() == {}


def test_weather_by_day_is_empty_for_no_days():
    assert A.weather_by_day([]) == {}


def test_at_handles_missing_and_short_lists():
    assert A._at(None, 0) is None
    assert A._at([], 0) is None
    assert A._at([1.0], 5) is None
    assert A._at([1.0, 2.0], 1) == 2.0


def test_weather_by_day_filters_to_the_requested_days(monkeypatch):
    monkeypatch.setattr(
        A,
        "_fetch_weather",
        lambda: {
            "2026-09-14": {"rain_mm": 1.0, "gust_kmh": 1.0},
            "2026-09-22": {"rain_mm": 2.0, "gust_kmh": 2.0},
        },
    )

    got = A.weather_by_day([date(2026, 9, 22)])

    assert list(got) == ["2026-09-22"]
