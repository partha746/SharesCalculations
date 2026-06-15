# NVDA Shares Dashboard (Angular)

## Prerequisites: Node.js and npm

If `npm` is not found, install Node.js and npm first.

**Ubuntu/Debian (apt):**
```bash
sudo apt-get update
sudo apt-get install -y nodejs npm
```
Check: `node -v` and `npm -v`.

**Alternative (newer Node version via NodeSource):**
```bash
# Example for Node 20.x on Ubuntu
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs
```

**Or use nvm (Node Version Manager):**
```bash
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
# Restart shell or: source ~/.nvm/nvm.sh
nvm install 20
nvm use 20
```

## Development (backend + frontend inside dashboard)

Run everything from the **dashboard** folder (parent repo must contain `configs/nvShares.db` and `helpers/`).

1. Install dependencies: `npm install`
2. **Option A – Backend and frontend together:**  
   `npm run start:all`  
   Starts the API on port 8080 and the Angular dev server on port 4201 (proxy forwards `/api` to the backend).
3. **Option B – Separate terminals:**  
   - Terminal 1: `npm run backend` (API on port 8080)  
   - Terminal 2: `npm start` (Angular on port 4201)
4. Open http://localhost:4201

The backend lives in `backend/server.py` and uses the repo root (parent of `dashboard`) for `configs/` and `helpers/`.

## Production (pm2)

The live system runs under pm2 (see [`../ecosystem.config.js`](../ecosystem.config.js)):

- `shares-backend` -> `backend/server.py` (Flask API, port 8080)
- `shares-dashboard` -> `serve-prod.js` (serves the built app on port 4201 + proxies `/api`)

Deploy after changes (from this `dashboard/` folder):

- `npm run deploy` - build + restart the dashboard
- `npm run deploy:all` - build + restart dashboard and backend

First-time / full start: `../start.sh` (builds if needed, then `pm2 start`).

See [`../ARCHITECTURE.md`](../ARCHITECTURE.md) for the full backend/frontend module layout.
