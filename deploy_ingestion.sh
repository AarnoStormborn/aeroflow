#!/bin/bash
# Deploy aeroflow ingestion service to Ubuntu 24.04 EC2
# Usage: bash deploy_ingestion.sh
set -euo pipefail

echo "========================================"
echo "AEROFLOW INGESTION DEPLOY"
echo "========================================"

# 1. System packages
echo "[1/6] Installing system packages..."
sudo apt-get update -qq
sudo apt-get install -y -qq python3.12 python3.12-venv python3-pip git curl > /dev/null

# 2. Install uv
echo "[2/6] Installing uv..."
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# 3. Clone repo (HTTPS — no GitHub key on server)
echo "[3/6] Cloning aeroflow..."
if [ ! -d "$HOME/aeroflow" ]; then
    git clone https://github.com/AarnoStormborn/aeroflow.git "$HOME/aeroflow"
fi
cd "$HOME/aeroflow"

# 4. Set up .env
echo "[4/6] Configuring .env..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo
    echo ">>> Edit $HOME/aeroflow/.env and fill in:"
    echo "    OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET (required)"
    echo "    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (or leave blank if IAM role)"
    echo "    AWS_S3_BUCKET_NAME=flights-forecasting"
    echo "    DISCORD_WEBHOOK_URL / DISCORD_ENABLED=true"
    echo "    SCHEDULER_INTERVAL_SECONDS=900"
    echo "    DB_PATH=/home/ubuntu/aeroflow/ingestion/data/ingestion.db"
    echo
    read -p "Press Enter once .env is configured..."
fi

# 5. Sync dependencies (workspace, ingestion package)
echo "[5/6] Installing dependencies..."
export PATH="$HOME/.local/bin:$PATH"
uv sync --package ingestion

# 6. Systemd service (runs from workspace root; .env is at repo root)
echo "[6/6] Creating systemd service..."
sudo tee /etc/systemd/system/aeroflow-ingestion.service > /dev/null <<EOF
[Unit]
Description=Aeroflow Ingestion Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/aeroflow
EnvironmentFile=/home/ubuntu/aeroflow/.env
ExecStart=/home/ubuntu/.local/bin/uv run --package ingestion --directory ingestion python main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable aeroflow-ingestion
sudo systemctl start aeroflow-ingestion

echo "========================================"
echo "DEPLOY COMPLETE"
echo "========================================"
echo "Check status: sudo systemctl status aeroflow-ingestion"
echo "View logs:    sudo journalctl -u aeroflow-ingestion -f"
