"""Tests for the duplicate-forecast guard.

Motivated by the Sep 20 ingestion outage: with no new data the forecaster kept
anchoring on the last complete pre-gap hour, so it re-wrote byte-identical
predictions every hour for ~28 hours. Those files carried no information and
inflated the archive.
"""

import io

from src.forecasting.models.forecaster import ForecastEngine, forecast_signature

ANCHOR = "2026-09-20T07:00:00+00:00"


class FakeS3:
    """Minimal stand-in for the boto3 client ForecastEngine holds."""

    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.puts = []

    def get_paginator(self, _name):
        outer = self

        class _Paginator:
            def paginate(self, Bucket=None, Prefix=""):
                keys = sorted(k for k in outer.objects if k.startswith(Prefix))
                yield {"Contents": [{"Key": k} for k in keys]}

        return _Paginator()

    def get_object(self, Bucket=None, Key=None):
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket=None, Key=None, Body=None):
        self.puts.append(Key)
        self.objects[Key] = Body


def _engine(fake):
    """Build an engine without __init__, so tests need no AWS credentials."""
    engine = ForecastEngine.__new__(ForecastEngine)
    engine._s3 = fake
    engine.bucket = "test-bucket"
    return engine


def _forecast(anchor=ANCHOR, version="8", stage="Production", generated="2026-09-20T08:15:00+00:00"):
    return {
        "schema_version": 2,
        "generated_at": generated,
        "last_actual_hour": anchor,
        "models": {
            "flight-traffic-hourly": {
                "model_stage": stage,
                "model_version": version,
                "hourly": {"hour_start": anchor, "predicted_flight_count": 34.13},
                "quarter_daily": [],
            }
        },
    }


# ---- signature -----------------------------------------------------------


def test_signature_is_equal_for_identical_runs():
    """Same anchor and same served model means identical predictions."""
    assert forecast_signature(_forecast()) == forecast_signature(_forecast())


def test_signature_ignores_generation_time():
    """Two runs at different times but the same anchor are duplicates."""
    a = _forecast(generated="2026-09-20T08:15:00+00:00")
    b = _forecast(generated="2026-09-20T09:16:00+00:00")
    assert forecast_signature(a) == forecast_signature(b)


def test_signature_changes_when_anchor_advances():
    a = _forecast(anchor=ANCHOR)
    b = _forecast(anchor="2026-09-20T08:00:00+00:00")
    assert forecast_signature(a) != forecast_signature(b)


def test_signature_changes_when_a_new_model_version_is_served():
    """A promotion changes predictions even though the anchor has not moved."""
    a = _forecast(version="8")
    b = _forecast(version="9")
    assert forecast_signature(a) != forecast_signature(b)


def test_signature_survives_a_missing_model_version():
    """An unresolvable version must not break comparability."""
    unknown = _forecast(version=None)
    assert forecast_signature(unknown) == forecast_signature(unknown)
    assert forecast_signature(unknown) != forecast_signature(_forecast(version="8"))


# ---- write behaviour -----------------------------------------------------


def test_first_forecast_is_written():
    fake = FakeS3()
    url = _engine(fake).save_forecast(_forecast())
    assert url is not None
    assert len(fake.puts) == 1


def test_duplicate_forecast_is_skipped():
    """The outage case: anchor unchanged, so nothing new to store."""
    fake = FakeS3()
    engine = _engine(fake)

    assert engine.save_forecast(_forecast(generated="2026-09-20T08:15:00+00:00")) is not None
    first_count = len(fake.puts)

    skipped = engine.save_forecast(_forecast(generated="2026-09-20T09:16:00+00:00"))

    assert skipped is None
    assert len(fake.puts) == first_count  # no new object written


def test_duplicates_are_skipped_repeatedly():
    """A 28-hour stall must not accumulate 28 copies."""
    fake = FakeS3()
    engine = _engine(fake)
    engine.save_forecast(_forecast(generated="2026-09-20T08:15:00+00:00"))

    for hour in range(9, 20):
        engine.save_forecast(_forecast(generated=f"2026-09-20T{hour:02d}:16:00+00:00"))

    assert len(fake.puts) == 1


def test_advanced_anchor_is_written():
    fake = FakeS3()
    engine = _engine(fake)
    engine.save_forecast(_forecast(generated="2026-09-20T08:15:00+00:00"))

    url = engine.save_forecast(_forecast(anchor="2026-09-20T08:00:00+00:00", generated="2026-09-20T09:16:00+00:00"))

    assert url is not None
    assert len(fake.puts) == 2


def test_promoted_model_is_written_at_the_same_anchor():
    fake = FakeS3()
    engine = _engine(fake)
    engine.save_forecast(_forecast(version="8", generated="2026-09-20T08:15:00+00:00"))

    url = engine.save_forecast(_forecast(version="9", generated="2026-09-20T09:16:00+00:00"))

    assert url is not None
    assert len(fake.puts) == 2


def test_write_proceeds_when_the_guard_cannot_read_the_previous_forecast():
    """The guard is an optimisation, so a failed check must not block writes."""
    prefix = "forecasts/hourly/year=2026/month=09"
    fake = FakeS3({f"{prefix}/forecast_20260920_081500.json": b"{not valid json"})

    url = _engine(fake).save_forecast(_forecast(generated="2026-09-20T09:16:00+00:00"))

    assert url is not None


def test_dedupe_can_be_disabled():
    fake = FakeS3()
    engine = _engine(fake)
    engine.save_forecast(_forecast(generated="2026-09-20T08:15:00+00:00"))

    url = engine.save_forecast(_forecast(generated="2026-09-20T09:16:00+00:00"), dedupe=False)

    assert url is not None
    assert len(fake.puts) == 2
