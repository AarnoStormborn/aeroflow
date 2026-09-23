"""Tests for keeping undercounted hours out of the accuracy stats.

An hour's flight_count is the union of unique aircraft across that hour's polls,
so an hour polled fewer times than the 15-minute cadence reports fewer aircraft
than were actually there. Scoring those hours measures the ingester rather than
the model: five such hours out of ninety-two were inflating the headline MAPE
from 17.2% to 27.9% and inventing a +12.6% bias.

They still appear on the chart - they are real observations - they just do not
score.
"""

from datetime import date, datetime, timezone


def test_capture_hour_parses_a_raw_key():
    from src.dashboard.data import _capture_hour

    key = "raw/flights/states/year=2026/month=09/day=20/20260920_071207.parquet"
    assert _capture_hour(key) == datetime(2026, 9, 20, 7, 12, 7, tzinfo=timezone.utc)


def test_capture_hour_rejects_a_malformed_key():
    from src.dashboard.data import _capture_hour

    assert _capture_hour("raw/flights/states/not-a-timestamp.parquet") is None


def test_sparse_keys_flags_the_real_outage_hours():
    """The Sep 22 reboot left hours 18 and 19 with a single poll each."""
    from src.dashboard.data import _sparse_keys_from_polls

    polls = {h: 4 for h in range(24)}
    polls[18] = 1
    polls[19] = 1

    assert _sparse_keys_from_polls({date(2026, 9, 22): polls}) == {
        "2026-09-22T18:00",
        "2026-09-22T19:00",
    }


def test_sparse_keys_accepts_a_three_poll_hour():
    """Three polls is a scheduling boundary artifact, not a data gap."""
    from src.dashboard.data import _sparse_keys_from_polls

    polls = {h: 4 for h in range(24)}
    polls[19] = 3

    assert _sparse_keys_from_polls({date(2026, 9, 22): polls}) == set()


def test_sparse_keys_survives_a_phantom_hour_clock_change():
    """India is UTC+5:30 with no DST; a 23-hour day must not shift keys."""
    from src.dashboard.data import _sparse_keys_from_polls

    polls = {h: 4 for h in range(23)}
    polls[23] = 1

    assert _sparse_keys_from_polls({date(2026, 9, 22): polls}) == {"2026-09-22T23:00"}


def test_sparse_keys_ignores_wholly_missed_hours():
    """A missed hour has no actual to score, so there is nothing to exclude."""
    from src.dashboard.data import _sparse_keys_from_polls

    polls = {h: 4 for h in range(24) if h != 3}

    assert _sparse_keys_from_polls({date(2026, 9, 22): polls}) == set()


def test_sparse_keys_are_empty_for_a_healthy_day():
    from src.dashboard.data import _sparse_keys_from_polls

    assert _sparse_keys_from_polls({date(2026, 9, 22): {h: 4 for h in range(24)}}) == set()


def test_sparse_key_format_matches_the_actuals_lookup():
    """The exclusion is a set-membership test against _hour_key.

    If the two produced different shapes the filter would silently never match
    and the whole exclusion would be a no-op, which is exactly the kind of
    failure that looks like it works.
    """
    from src.dashboard.data import _hour_key, _sparse_keys_from_polls

    sparse = _sparse_keys_from_polls({date(2026, 9, 22): {0: 1}})

    assert sparse == {_hour_key(datetime(2026, 9, 22, 0, tzinfo=timezone.utc))}


def test_sparse_key_format_matches_a_naive_datetime():
    """Parquet hands back naive datetimes, so the join must survive that too."""
    from src.dashboard.data import _hour_key, _sparse_keys_from_polls

    sparse = _sparse_keys_from_polls({date(2026, 9, 22): {7: 1}})

    assert sparse == {_hour_key(datetime(2026, 9, 22, 7))}
