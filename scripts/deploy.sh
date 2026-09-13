#!/usr/bin/env bash
# Deploy the Modal app with the workspace pinned.
#
# `modal deploy` uses whichever profile is *active* in ~/.modal.toml, and that
# silently flipped to a different workspace more than once during this project.
# Deploying under the wrong profile would create this app and its Modal secrets
# (AWS keys, OpenSky credentials, Discord webhook) in the wrong workspace.
#
# Pinning MODAL_PROFILE here means the target is explicit and independent of the
# ambient setting. modal_app.py also refuses to run under a wrong profile, so
# both paths are covered.
#
# Usage:  scripts/deploy.sh [extra modal deploy args]
set -euo pipefail

EXPECTED_PROFILE="${AEROFLOW_MODAL_PROFILE:-harshsingh90220}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

MODAL=".venv/bin/modal"
[ -x "$MODAL" ] || MODAL="modal"

echo "→ deploying with MODAL_PROFILE=$EXPECTED_PROFILE"
MODAL_PROFILE="$EXPECTED_PROFILE" "$MODAL" deploy modal_app.py "$@"
