#!/bin/bash
# Sets up the monitor as a user service and its Caddy site.
# Runs on every deploy; safe to run again.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DEPLOY="$REPO_DIR/deploy"

if ! python3 -c 'import sys; sys.exit(sys.version_info < (3, 10))'; then
    echo "Python 3.10 or newer is needed." >&2
    exit 1
fi

if [[ ! -f "$REPO_DIR/.env" ]]; then
    cp "$REPO_DIR/.env.example" "$REPO_DIR/.env"
    echo "Created $REPO_DIR/.env, fill in GITHUB_TOKEN."
fi
chmod 600 "$REPO_DIR/.env"

echo "Setting up the monitor service..."
sed -e "s|__REPO_DIR__|$REPO_DIR|g" "$DEPLOY/mixxx-ci-queue.service.template" > "$DEPLOY/mixxx-ci-queue.service"
mkdir -p ~/.config/systemd/user
ln -sf "$DEPLOY/mixxx-ci-queue.service" ~/.config/systemd/user/mixxx-ci-queue.service
export XDG_RUNTIME_DIR="/run/user/$(id -u)"
systemctl --user daemon-reload
systemctl --user enable mixxx-ci-queue.service
if grep -qs '^GITHUB_TOKEN=.' "$REPO_DIR/.env"; then
    systemctl --user restart mixxx-ci-queue.service
else
    systemctl --user stop mixxx-ci-queue.service
    echo "GITHUB_TOKEN is empty in $REPO_DIR/.env: the monitor is not started." >&2
    echo "Fill it in, then run: systemctl --user restart mixxx-ci-queue" >&2
fi

echo "Setting up the Caddy site..."
sudo ln -sf "$DEPLOY/mixxx-ci.caddyfile" /etc/caddy/mixxx-ci.caddyfile
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl reload caddy

echo "Done."
