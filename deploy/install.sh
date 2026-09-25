#!/bin/bash
# Sets up the monitor as a user service, its Caddy site and the Grafana dashboard.
# Run from anywhere; safe to run again after a git pull.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DEPLOY="$REPO_DIR/deploy"

if ! grep -qs '^GITHUB_TOKEN=.' "$REPO_DIR/.env"; then
    echo "Missing GITHUB_TOKEN: copy .env.example to .env and fill it in." >&2
    exit 1
fi
if ! python3 -c 'import sys; sys.exit(sys.version_info < (3, 10))'; then
    echo "Python 3.10 or newer is needed." >&2
    exit 1
fi
chmod 600 "$REPO_DIR/.env"

# Grafana reads the dashboard straight from this repo
path="$REPO_DIR"
while [[ "$path" != "/" ]]; do
    sudo chmod o+x "$path"
    path="$(dirname "$path")"
done
chmod -R o+rX "$DEPLOY/grafana"

echo "Setting up the monitor service..."
sed -e "s|__REPO_DIR__|$REPO_DIR|g" "$DEPLOY/mixxx-ci-queue.service.template" > "$DEPLOY/mixxx-ci-queue.service"
mkdir -p ~/.config/systemd/user
ln -sf "$DEPLOY/mixxx-ci-queue.service" ~/.config/systemd/user/mixxx-ci-queue.service
export XDG_RUNTIME_DIR="/run/user/$(id -u)"
systemctl --user daemon-reload
systemctl --user enable mixxx-ci-queue.service
systemctl --user restart mixxx-ci-queue.service

echo "Setting up the Caddy site..."
sudo ln -sf "$DEPLOY/mixxx-ci.caddyfile" /etc/caddy/mixxx-ci.caddyfile
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl reload caddy

echo "Setting up the Grafana dashboard..."
sed -e "s|__REPO_DIR__|$REPO_DIR|g" "$DEPLOY/grafana/dashboard-provider.yaml.template" > "$DEPLOY/grafana/dashboard-provider.yaml"
sudo mkdir -p /etc/grafana/provisioning/dashboards
sudo ln -sf "$DEPLOY/grafana/dashboard-provider.yaml" /etc/grafana/provisioning/dashboards/mixxx-ci-queue.yaml
sudo systemctl restart grafana-server

echo "Done. Check the monitor with: curl -s localhost:8765/metrics | head"
