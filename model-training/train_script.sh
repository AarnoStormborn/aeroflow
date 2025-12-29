#!/bin/bash
# Model training script
# Runs every 3 days via cron to train new model with last 14 days of data

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

# Run training pipeline
echo "========================================"
echo "Starting model training..."
echo "========================================"
uv run python -m src.training.train "$@"

echo "========================================"
echo "Training complete!"
echo "========================================"
