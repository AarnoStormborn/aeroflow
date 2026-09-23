"""The sparse-hour threshold lives in two packages; keep them in step.

The dashboard decides which hours are too thinly polled to score, and the
forecasting health check decides which ones to alert on. Both encode the same
15-minute polling cadence, so changing one alone would leave the dashboard's
accuracy and the health alert disagreeing about the same hour.

They are separate deployables with no shared dependency, so this asserts on the
source text rather than importing across packages - the same approach the
Modal image-drift guard takes.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NAME = "POLLS_PER_HOUR_ALERT"

SOURCES = {
    "dashboard": ROOT / "dashboard/src/dashboard/data.py",
    "health": ROOT / "forecasting/src/forecasting/models/health.py",
}


def _value(path: Path) -> int:
    match = re.search(rf"^{NAME}\s*=\s*(\d+)", path.read_text(), re.MULTILINE)
    assert match, f"{NAME} not found in {path}"
    return int(match.group(1))


def test_sparse_poll_threshold_agrees_across_packages():
    values = {pkg: _value(path) for pkg, path in SOURCES.items()}
    assert len(set(values.values())) == 1, (
        f"{NAME} has drifted apart: {values}. The dashboard accuracy filter and "
        "the health alert have to agree about which hours are unreliable."
    )


def test_sparse_poll_threshold_is_a_plausible_cadence():
    """Catches a typo turning the filter into a no-op or a blanket skip.

    At 0 nothing is ever excluded; at 5 every hour of a healthy 4-poll-per-hour
    day would be, and the accuracy panel would report no scored hours at all.
    """
    for pkg, path in SOURCES.items():
        value = _value(path)
        assert 1 <= value <= 4, f"{pkg}: {NAME}={value} is outside a 15-minute cadence"
