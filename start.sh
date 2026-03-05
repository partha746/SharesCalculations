#!/usr/bin/env bash
# Start backend (Flask in dashboard/backend) and frontend (Angular). Stops any existing processes first.

set -e
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

BACKEND_PORT=8080
FRONTEND_PORT=4201

echo "Stopping existing processes..."
rm -f web_app.pid dashboard.pid
fuser -k "$BACKEND_PORT/tcp" 2>/dev/null || true
fuser -k "$FRONTEND_PORT/tcp" 2>/dev/null || true
sleep 1

echo "Starting backend (dashboard/backend/server.py) on port $BACKEND_PORT..."
nohup python3 dashboard/backend/server.py >> web_app.log 2>&1 &
echo "$!" > web_app.pid
sleep 2
if ! curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$BACKEND_PORT/api/tables" 2>/dev/null | grep -q 200; then
  echo "Backend failed to start. Check web_app.log"
  exit 1
fi
echo "Backend OK."

echo "Starting frontend (Angular) on port $FRONTEND_PORT..."
if [ ! -d dashboard/node_modules ]; then
  echo "Running npm install in dashboard..."
  (cd dashboard && npm install)
fi
(cd dashboard && nohup npm start > ../dashboard.log 2>&1 &)
echo "$!" > dashboard.pid

# Wait for frontend to be up (Angular can take 20–40s)
echo -n "Waiting for frontend"
for i in $(seq 1 60); do
  if curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$FRONTEND_PORT" 2>/dev/null | grep -q 200; then
    echo ""
    echo "Frontend OK."
    break
  fi
  echo -n "."
  sleep 1
  if [ "$i" -eq 60 ]; then
    echo ""
    echo "Frontend may still be compiling. Check dashboard.log and try http://127.0.0.1:$FRONTEND_PORT in a minute."
  fi
done

# Show device IP for access from other devices (frontend is bound to 0.0.0.0)
DEVICE_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
[ -z "$DEVICE_IP" ] && DEVICE_IP="<this-machine-ip>"

echo ""
echo "----------------------------------------"
echo "Backend:   http://127.0.0.1:$BACKEND_PORT"
echo "Dashboard (this machine): http://127.0.0.1:$FRONTEND_PORT"
echo "Dashboard (other devices): http://${DEVICE_IP}:$FRONTEND_PORT"
echo "----------------------------------------"
echo "Logs: web_app.log (backend), dashboard.log (frontend)"
echo "To stop: kill processes on ports $BACKEND_PORT and $FRONTEND_PORT"
