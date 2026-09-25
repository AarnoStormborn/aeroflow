"""Tests for excluding undercounted hours from the promotion comparison.

Both the candidate and the incumbent are scored on the same validation split, so
a thinly-polled hour there is not just a bad row - it is noise injected straight
into the decision to replace the served model. The 2026-09-25 run compared over a
92-hour split containing two such hours.

All of this is offline; the S3 client is faked.
"""

from datetime import date, datetime, timezone

from src.training.quality import (
    POLLS_PER_HOUR_ALERT,
    _capture_hour,
    hour_key,
    sparse_hour_keys,
    sparse_keys_from_polls,
)

# ---- hour key format -----------------------------------------------------


def test_hour_key_uses_the_dashboard_format():
    """The dashboard's _hour_key produces exactly this shape.

    If the two ever disagree the filter becomes a silent no-op that looks like it
    works, so pin the literal string rather than trusting both to stay aligned.
    """
    assert hour_key(datetime(2026, 9, 22, 7, tzinfo=timezone.utc)) == "2026-09-22T07:00"


def test_hour_key_treats_naive_datetimes_as_utc():
    """Parquet hands back naive datetimes, and the host may not be UTC."""
    assert hour_key(datetime(2026, 9, 22, 7)) == "2026-09-22T07:00"


def test_hour_key_normalises_a_non_utc_offset():
    from datetime import timedelta

    ist = timezone(timedelta(hours=5, minutes=30))
    assert hour_key(datetime(2026, 9, 22, 12, 30, tzinfo=ist)) == "2026-09-22T07:00"


# ---- key parsing ---------------------------------------------------------


def test_capture_hour_parses_a_raw_key():
    key = "raw/flights/states/year=2026/month=09/day=20/20260920_071207.parquet"
    assert _capture_hour(key) == datetime(2026, 9, 20, 7, 12, 7, tzinfo=timezone.utc)


def test_capture_hour_rejects_a_malformed_key():
    assert _capture_hour("raw/flights/states/nonsense.parquet") is None


# ---- the rule ------------------------------------------------------------


def test_flags_the_real_reboot_hours():
    """2026-09-22 18:00 and 19:00 got a single poll each when the Pi rebooted."""
    polls = {h: 4 for h in range(24)}
    polls[18] = 1
    polls[19] = 1
    assert sparse_keys_from_polls({date(2026, 9, 22): polls}) == {
        "2026-09-22T18:00",
        "2026-09-22T19:00",
    }


def test_accepts_a_three_poll_hour():
    """Three polls is a scheduling boundary artifact, not a data gap."""
    polls = {h: 4 for h in range(24)}
    polls[19] = 3
    assert sparse_keys_from_polls({date(2026, 9, 22): polls}) == set()


def test_ignores_wholly_missed_hours():
    """A missed hour has no target at all, so there is nothing to exclude."""
    polls = {h: 4 for h in range(24) if h != 3}
    assert sparse_keys_from_polls({date(2026, 9, 22): polls}) == set()


def test_a_healthy_day_yields_nothing():
    assert sparse_keys_from_polls({date(2026, 9, 22): {h: 4 for h in range(24)}}) == set()


def test_threshold_is_a_plausible_cadence():
    assert 1 <= POLLS_PER_HOUR_ALERT <= 4


# ---- the S3-backed lookup ------------------------------------------------


class FakeS3:
    def __init__(self, keys):
        self.keys = keys

    def get_paginator(self, _name):
        outer = self

        class _Paginator:
            def paginate(self, Bucket=None, Prefix=""):
                yield {"Contents": [{"Key": k} for k in outer.keys if k.startswith(Prefix)]}

        return _Paginator()


def _key(day: date, hour: int, minute: int) -> str:
    return (
        f"raw/flights/states/year={day.year}/month={day.month:02d}/day={day.day:02d}/"
        f"{day:%Y%m%d}_{hour:02d}{minute:02d}00.parquet"
    )


def test_sparse_hour_keys_reads_polls_from_the_window():
    day = date(2026, 9, 22)
    keys = [_key(day, h, m) for h in range(24) for m in (5, 20, 35, 50) if h != 18]
    keys.append(_key(day, 18, 5))  # one lonely poll

    got = sparse_hour_keys(FakeS3(keys), "bucket", day, day, now=datetime(2026, 9, 23, 0, 30, tzinfo=timezone.utc))

    assert got == {"2026-09-22T18:00"}


def test_sparse_hour_keys_excludes_the_still_running_hour():
    """A partially ingested hour is not evidence of sparse polling."""
    day = date(2026, 9, 22)
    keys = [_key(day, h, m) for h in range(23) for m in (5, 20, 35, 50)]
    keys.append(_key(day, 23, 5))  # only one poll so far this hour

    got = sparse_hour_keys(FakeS3(keys), "bucket", day, day, now=datetime(2026, 9, 22, 23, 10, tzinfo=timezone.utc))

    assert got == set()


def test_sparse_hour_keys_is_empty_without_any_raw_data():
    got = sparse_hour_keys(
        FakeS3([]),
        "bucket",
        date(2026, 9, 22),
        date(2026, 9, 22),
        now=datetime(2026, 9, 23, 0, 30, tzinfo=timezone.utc),
    )
    assert got == set()
