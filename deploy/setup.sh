#!/usr/bin/env bash
# One-time provisioning for the TSX watcher on an Ubuntu always-on host
# (Oracle Cloud Always Free VM or any VPS). Idempotent — safe to re-run.
#
# Run it from inside the cloned repo:
#   cd ~/tsx-watcher && bash deploy/setup.sh
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"
echo "Repo dir: $REPO_DIR"

# 1. System deps -------------------------------------------------------------
echo "==> Installing system packages..."
sudo apt-get update -y
sudo apt-get install -y python3 python3-venv python3-pip git

# 2. Python venv + deps ------------------------------------------------------
echo "==> Creating virtualenv + installing requirements..."
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip
./.venv/bin/pip install -r requirements.txt

# 3. Secrets file ------------------------------------------------------------
if [ ! -f deploy/tsx-watcher.env ]; then
  cp deploy/tsx-watcher.env.example deploy/tsx-watcher.env
  chmod 600 deploy/tsx-watcher.env
  echo "==> Created deploy/tsx-watcher.env — EDIT IT NOW with your real secrets:"
  echo "    nano deploy/tsx-watcher.env"
else
  echo "==> deploy/tsx-watcher.env already exists — leaving it."
fi

# 4. Install systemd unit + timer -------------------------------------------
# Rewrite the User= and paths in the unit to match THIS user/clone location,
# so it works regardless of username or clone path.
echo "==> Installing systemd service + timer..."
SVC_USER="$(id -un)"
TMP_SVC="$(mktemp)"
sed -e "s#^User=.*#User=${SVC_USER}#" \
    -e "s#/home/ubuntu/tsx-watcher#${REPO_DIR}#g" \
    deploy/tsx-watcher.service > "$TMP_SVC"
sudo cp "$TMP_SVC" /etc/systemd/system/tsx-watcher.service
sudo cp deploy/tsx-watcher.timer /etc/systemd/system/tsx-watcher.timer
rm -f "$TMP_SVC"

sudo systemctl daemon-reload
sudo systemctl enable --now tsx-watcher.timer

echo
echo "==> Done. Status:"
systemctl status tsx-watcher.timer --no-pager || true
echo
echo "Next trigger:"
systemctl list-timers tsx-watcher.timer --no-pager || true
echo
echo "Make sure deploy/tsx-watcher.env has your real secrets before the next trigger."
echo "Test a run right now with:   sudo systemctl start tsx-watcher.service"
echo "Watch logs with:             journalctl -u tsx-watcher.service -f"
