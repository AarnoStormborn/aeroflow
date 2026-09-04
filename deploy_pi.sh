#!/bin/bash
# Deploy aeroflow ingestion service to Raspberry Pi (DietPi, 1GB RAM)
# Uses pip (not uv) + minimal runtime requirements for low footprint.
# Usage: bash deploy_pi.sh
set -euo pipefail

echo "========================================"
echo "AEROFLOW INGESTION — RASPBERRY PI DEPLOY"
echo "========================================"

# 1. System deps (DietPi is minimal; ensure python3 + pip)
echo "[1/6] Installing system packages..."
sudo apt-get update -qq
sudo apt-get install -y -qq python3 python3-venv python3-pip git curl > /dev/null

# 2. Clone repo (HTTPS)
echo "[2/6] Cloning aeroflow..."
if [ ! -d "$HOME/aeroflow" ]; then
    git clone --depth 1 https://github.com/AarnoStormborn/aeroflow.git "$HOME/aeroflow"
fi
cd "$HOME/aeroflow"

# 3. Create venv + install MINIMAL runtime deps with pip
echo "[3/6] Creating venv + installing minimal deps..."
if [ ! -d "$HOME/aeroflow/.venv-pi" ]; then
    python3 -m venv "$HOME/aeroflow/.venv-pi"
fi
source "$HOME/aeroflow/.venv-pi/bin/activate"
pip install --no-cache-dir -q --upgrade pip
pip install --no-cache-dir -q -r requirements-pi.txt
echo "Installed: $(pip list --format=freeze 2>/dev/null | wc -l) packages"

# 4. Set up .env
echo "[4/6] Configuring .env..."
if [ ! -f "$HOME/aeroflow/.env" ]; then
    cp "$HOME/aeroflow/.env.example" "$HOME/aeroflow/.env"
    echo
    echo ">>> Edit $HOME/aeroflow/.env and fill in:"
    echo "    OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET (required)"
    echo "    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY"
    echo "    AWS_S3_BUCKET_NAME=flights-forecasting"
    echo "    DISCORD_WEBHOOK_URL / DISCORD_ENABLED=true"
    echo "    SCHEDULER_INTERVAL_SECONDS=900"
    echo "    DB_PATH=/root/aeroflow/ingestion/data/ingestion.db"
    echo
    read -p "Press Enter once .env is configured..."
fi

# 5. systemd service (runs from ingestion/ with pip venv)
echo "[5/6] Creating systemd service..."
sudo tee /etc/systemd/system/aeroflow-ingestion.service > /dev/null <<EOF
[Unit]
Description=Aeroflow Ingestion Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$HOME/aeroflow/ingestion
EnvironmentFile=$HOME/aeroflow/.env
ExecStart=$HOME/aeroflow/.venv-pi/bin/python main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable aeroflow-ingestion
sudo systemctl start aeroflow-ingestion

# 6. Install daily summary + cleanup timers
sudo cp $HOME/aeroflow/deploy/systemd/aeroflow-summary.service /etc/systemd/system/
sudo cp $HOME/aeroflow/deploy/systemd/aeroflow-summary.timer /etc/systemd/system/
sudo cp $HOME/aeroflow/deploy/systemd/aeroflow-cleanup.service /etc/systemd/system/
sudo cp $HOME/aeroflow/deploy/systemd/aeroflow-cleanup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable aeroflow-summary.timer aeroflow-cleanup.timer
sudo systemctl start aeroflow-summary.timer aeroflow-cleanup.timer

echo "========================================"
echo "DEPLOY COMPLETE"
echo "========================================"
echo "Check: sudo systemctl status aeroflow-ingestion"
echo "Logs:  sudo journalctl -u aeroflow-ingestion -f"
echo "Memory: systemctl show aeroflow-ingestion -p MemoryCurrent"
echo "Timers: systemctl list-timers aeroflow-*"
