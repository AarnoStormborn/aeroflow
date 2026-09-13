"""Guard against deploying the app into the wrong Modal workspace.

`modal deploy` targets whichever profile is active in ~/.modal.toml. That
silently flipped to another workspace during this project, and deploying there
would have created the app *and* its Modal secrets (AWS keys, OpenSky
credentials, Discord webhook) in the wrong workspace.

These tests run the import in a subprocess so they exercise the real guard
rather than a reimplementation, and they assert on the guard's own message so
they do not depend on credentials or on the rest of the module import
succeeding.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BLOCKED = "Refusing to run under Modal profile"


def _import_modal_app(env_extra: dict[str, str]) -> subprocess.CompletedProcess:
    env = {**os.environ, **env_extra}
    env.pop("AEROFLOW_SKIP_PROFILE_CHECK", None)
    env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-c", "import modal_app"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        timeout=180,
    )


def test_blocks_wrong_profile():
    """The real failure mode: sticky profile pointing at another workspace."""
    r = _import_modal_app({"MODAL_PROFILE": "some-other-workspace"})
    out = r.stdout + r.stderr
    assert BLOCKED in out
    assert r.returncode != 0


def test_message_names_the_expected_profile_and_a_fix():
    """The error must be actionable, not just a failure."""
    r = _import_modal_app({"MODAL_PROFILE": "some-other-workspace"})
    out = r.stdout + r.stderr
    assert "harshsingh90220" in out
    assert "modal profile activate" in out


def test_allows_expected_profile():
    r = _import_modal_app({"MODAL_PROFILE": "harshsingh90220"})
    assert BLOCKED not in (r.stdout + r.stderr)


def test_can_be_deliberately_overridden():
    """There must be an escape hatch, or the guard becomes a footgun."""
    r = _import_modal_app(
        {"MODAL_PROFILE": "some-other-workspace", "AEROFLOW_SKIP_PROFILE_CHECK": "1"}
    )
    assert BLOCKED not in (r.stdout + r.stderr)
