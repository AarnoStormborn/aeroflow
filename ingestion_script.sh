#!/bin/bash
# Ingestion script for flights-forecasting service
# Gets OAuth token and runs single ingestion

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load environment variables from .env if exists
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Check required variables
if [ -z "$CLIENT_ID" ] || [ -z "$CLIENT_SECRET" ]; then
    echo "ERROR: CLIENT_ID and CLIENT_SECRET must be set"
    exit 1
fi

# Get OAuth token
echo "Fetching OAuth token..."
export TOKEN=$(curl -s -X POST "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=client_credentials" \
    -d "client_id=$CLIENT_ID" \
    -d "client_secret=$CLIENT_SECRET" | jq -r .access_token)

if [ "$TOKEN" == "null" ] || [ -z "$TOKEN" ]; then
    echo "ERROR: Failed to get OAuth token"
    exit 1
fi

echo "Token obtained successfully"

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
