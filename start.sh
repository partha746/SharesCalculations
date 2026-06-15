#!/usr/bin/env bash
# Start the dashboard via Docker Compose (the canonical runner; see docker-compose.yml).
#   backend  -> Flask API on port 8080  (Dockerfile.backend)
#   frontend -> built Angular app + /api proxy on port 4201 (dashboard/Dockerfile)
#
# The SQLite DB / config persist on the host via the ./configs bind mount.
#
# Common commands:
#   sudo docker compose up -d --build     # build + run (after code changes)
#   sudo docker compose ps                # status
#   sudo docker compose logs -f           # logs
#   sudo docker compose down              # stop
#
# (Drop the sudo by adding your user to the docker group: sudo usermod -aG docker $USER)

set -e
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

DOCKER="docker"
if ! docker info >/dev/null 2>&1; then
  DOCKER="sudo docker"
fi

echo "Building and starting containers..."
$DOCKER compose up -d --build

$DOCKER compose ps

DEVICE_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
[ -z "$DEVICE_IP" ] && DEVICE_IP="<this-machine-ip>"

echo ""
echo "----------------------------------------"
echo "Backend:   http://127.0.0.1:8080"
echo "Dashboard (this machine):  http://127.0.0.1:4201"
echo "Dashboard (other devices): http://${DEVICE_IP}:4201"
echo "----------------------------------------"
echo "Logs:  $DOCKER compose logs -f"
echo "Stop:  $DOCKER compose down"
