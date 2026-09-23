#!/bin/bash
# Update the aeroflow ingestion on the Pi with new scripts + timers.
# Run ON the Pi after pulling latest code.
set -euo pipefail

cd "$HOME/aeroflow"
echo "=== Pulling latest code ==="
git pull origin main

echo "=== Installing daily summary + cleanup timers ==="
sudo cp deploy/systemd/aeroflow-summary.service /etc/systemd/system/
sudo cp deploy/systemd/aeroflow-summary.timer /etc/systemd/system/
sudo cp deploy/systemd/aeroflow-cleanup.service /etc/systemd/system/
sudo cp deploy/systemd/aeroflow-cleanup.timer /etc/systemd/system/

# Power/thermal watchdog. DietPi keeps /var/log on a tmpfs and clears it hourly,
# so the system journal cannot explain the reboots this box keeps having; this
# writes to real disk instead.
echo "=== Installing power/thermal watchdog ==="
sudo cp deploy/aeroflow-powerwatch /usr/local/bin/
sudo cp deploy/aeroflow-bootmark /usr/local/bin/
sudo chmod +x /usr/local/bin/aeroflow-powerwatch /usr/local/bin/aeroflow-bootmark
sudo cp deploy/systemd/aeroflow-powerwatch.service /etc/systemd/system/
sudo cp deploy/systemd/aeroflow-powerwatch.timer /etc/systemd/system/
sudo cp deploy/systemd/aeroflow-bootmark.service /etc/systemd/system/
sudo cp deploy/systemd/aeroflow-bootmark.timer /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable aeroflow-summary.timer aeroflow-cleanup.timer
sudo systemctl start aeroflow-summary.timer aeroflow-cleanup.timer
sudo systemctl enable --now aeroflow-powerwatch.timer aeroflow-bootmark.timer

echo "=== Restarting ingestion (picks up any code changes) ==="
sudo systemctl restart aeroflow-ingestion

echo "=== Status ==="
systemctl list-timers aeroflow-* --no-pager
systemctl status aeroflow-ingestion --no-pager | head -5
