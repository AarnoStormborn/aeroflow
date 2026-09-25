"""Which hours are too thinly polled to be trusted as a training target.

An hour's flight_count is the union of unique aircraft across that hour's polls,
so an hour sampled below the 15-minute cadence reports fewer aircraft than were
actually there. That matters twice over here.

For training it means undercounted labels: the model is taught to predict a
number that was never the real traffic level.

For the promotion decision it is worse, because the decision is a comparison.
The 2026-09-25 retrain scored the candidate and the incumbent over a 92-hour
split that contained two such hours (2026-09-22 18:00 and 19:00 - the Pi reboot).
The served model scores 21.95% on that split but 17.1% on the dashboard's
otherwise-identical window, so those two hours alone moved the comparison by
roughly 4.9 MAPE points and helped widen the paired interval to 19 points - far
too wide to resolve the 2.6% difference the guard was asked to judge.

Mirrors POLLS_PER_HOUR_ALERT in forecasting/models/health.py and
dashboard/data.py. They are separate deployables with no shared dependency, so a
root test asserts all three definitions agree.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

# An hour polled fewer times than this cannot be trusted. The ingester runs every
# 15 minutes (4 polls/hour); an occasional 3 is a scheduling boundary artifact.
POLLS_PER_HOUR_ALERT = 2

RAW_PREFIX = "raw/flights/states"


def _capture_hour(key: str) -> datetime | None:
    """Capture time encoded in a raw key, e.g. .../20260920_071207.parquet."""
    stamp = key.rsplit("/", 1)[-1].removesuffix(".parquet")
    try:
        return datetime.strptime(stamp, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def hour_key(value: datetime) -> str:
    """Canonical hour key. Must match the dashboard's _hour_key format."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:00")


def sparse_keys_from_polls(day_polls: dict[date, dict[int, int]]) -> set[str]:
    """Canonical hour keys for hours polled fewer than the cadence requires.

    Hours that were missed entirely are absent from the input and so are not
    listed: they have no target at all, so there is nothing to exclude.
    """
    sparse: set[str] = set()
    for day, polls in day_polls.items():
        for hour, count in polls.items():
            if count < POLLS_PER_HOUR_ALERT:
                sparse.add(hour_key(datetime(day.year, day.month, day.day, hour, tzinfo=timezone.utc)))
    return sparse


def sparse_hour_keys(client, bucket: str, start: date, end: date, now: datetime | None = None) -> set[str]:
    """Sparse hour keys across [start, end], from the timestamps in raw keys.

    Args:
        client: boto3 S3 client
        bucket: bucket holding the raw archive
        start: first day of the window
        end: last day of the window
        now: reference time, so the still-running hour can be excluded

    Returns:
        Set of canonical hour keys that must not be scored or trained on
    """
    now = now or datetime.now(timezone.utc)
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    day_polls: dict[date, dict[int, int]] = {}

    day = start
    while day <= end:
        prefix = f"{RAW_PREFIX}/year={day.year}/month={day.month:02d}/day={day.day:02d}/"
        polls: dict[int, int] = {}
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                captured = _capture_hour(obj["Key"])
                if captured is None or captured >= current_hour:
                    continue  # unparseable, or the running hour's partial polling
                polls[captured.hour] = polls.get(captured.hour, 0) + 1
        day_polls[day] = polls
        day += timedelta(days=1)

    return sparse_keys_from_polls(day_polls)


__all__ = [
    "POLLS_PER_HOUR_ALERT",
    "hour_key",
    "sparse_hour_keys",
    "sparse_keys_from_polls",
]
