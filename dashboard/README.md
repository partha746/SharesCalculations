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

## Production build (served by Flask at project root)

From the **project root** (parent of dashboard):

1. Build: `cd dashboard && npm run build -- --base-href /dashboard/`
2. Run: `python web_app.py` (serves API and dashboard at http://localhost:8080/dashboard/)
