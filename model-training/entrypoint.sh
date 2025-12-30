#!/bin/bash
set -e

echo "========================================"
echo "Starting MLflow Server"
echo "========================================"

# Default values
BACKEND_STORE_URI=${MLFLOW_BACKEND_STORE_URI:-"sqlite:////app/db/mlflow.db"}
ARTIFACT_ROOT=${MLFLOW_ARTIFACT_ROOT:-"s3://flights-forecasting/mlflow"}
HOST=${MLFLOW_HOST:-"0.0.0.0"}
PORT=${MLFLOW_PORT:-5000}

echo "Backend Store: $BACKEND_STORE_URI"
echo "Artifact Root: $ARTIFACT_ROOT"
echo "Host: $HOST:$PORT"
echo "========================================"

# Start MLflow server
exec mlflow server \
    --backend-store-uri "$BACKEND_STORE_URI" \
    --default-artifact-root "$ARTIFACT_ROOT" \
    --host "$HOST" \
    --port "$PORT"
