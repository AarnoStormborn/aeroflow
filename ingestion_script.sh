#!/bin/bash
# Ingestion script for flights-forecasting service
# Activates virtualenv and runs single ingestion

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Activate virtualenv
if [ -d ".venv" ]; then
    source .venv/bin/activate
elif [ -d "venv" ]; then
    source venv/bin/activate
else
    echo "WARNING: No virtualenv found, using system Python"
fi

# Run ingestion once
echo "Starting ingestion..."
uv run main.py --run-once

echo "Ingestion complete"
