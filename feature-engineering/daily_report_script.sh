#!/bin/bash
# Daily pipeline script for feature-engineering service
# Runs feature engineering pipeline AND report generation
# Should be scheduled to run daily via cron

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
    python -m venv .venv
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

# Install dependencies
pip install -q -e .

# 1. Run feature engineering pipeline
echo "========================================"
echo "Step 1: Running feature pipeline..."
echo "========================================"
python -m src.pipeline.run "$@"

# 2. Run daily report generation
echo "========================================"
echo "Step 2: Running daily report..."
echo "========================================"
python -m src.features.daily_report "$@"

echo "========================================"
echo "Daily pipeline complete!"
echo "========================================"
