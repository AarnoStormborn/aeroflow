#!/bin/bash
# Daily report script for feature-engineering service
# Generates and publishes daily flight analysis report

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load environment variables from .env if exists
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Create virtualenv if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    uv venv
fi

# Activate virtualenv
if [ -d ".venv" ]; then
    source .venv/bin/activate
elif [ -d "venv" ]; then
    source venv/bin/activate
else
    echo "ERROR: No virtualenv found"
    exit 1
fi

# Install/sync dependencies
uv sync

# Run daily report (defaults to yesterday)
echo "Starting daily report generation..."
uv run python -m src.features.daily_report "$@"

echo "Daily report complete"
