#!/usr/bin/env bash
# Start/refresh the dashboard via pm2 (the canonical runner; see ecosystem.config.js).
#   shares-backend   -> dashboard/backend/server.py        (Flask API, port 8080)
#   shares-dashboard -> dashboard/serve-prod.js            (serves built Angular app, port 4201, proxies /api)
#
# Deploy frontend changes:  cd dashboard && npm run deploy        (build + restart dashboard)
# Deploy frontend+backend:  cd dashboard && npm run deploy:all

set -e
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

BACKEND_PORT=8080
FRONTEND_PORT=4201

if ! command -v pm2 >/dev/null 2>&1; then
  echo "pm2 not found. Install with: npm install -g pm2"
  exit 1
fi

# Build the Angular app if no dist yet (serve-prod.js serves the prebuilt output).
if [ ! -f dashboard/dist/dashboard/index.html ]; then
  echo "No build found; building Angular app..."
  (cd dashboard && [ -d node_modules ] || npm install)
  (cd dashboard && npm run build)
fi

echo "Starting/refreshing pm2 apps from ecosystem.config.js..."
pm2 start ecosystem.config.js
pm2 save

DEVICE_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
[ -z "$DEVICE_IP" ] && DEVICE_IP="<this-machine-ip>"

echo ""
echo "----------------------------------------"
echo "Backend:   http://127.0.0.1:${BACKEND_PORT}"
echo "Dashboard (this machine):  http://127.0.0.1:${FRONTEND_PORT}"
echo "Dashboard (other devices): http://${DEVICE_IP}:${FRONTEND_PORT}"
echo "----------------------------------------"
echo "Logs:   pm2 logs shares-backend | pm2 logs shares-dashboard"
echo "Status: pm2 status"
echo "Stop:   pm2 stop shares-backend shares-dashboard"
