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

## Production (Docker — primary)

The live system runs as two containers via [`../docker-compose.yml`](../docker-compose.yml):

- `backend`  -> `backend/server.py` (Flask API, port 8080)
- `frontend` -> built Angular app + `/api` proxy via `serve-prod.js` (port 4201)

The host `../configs` is bind-mounted so the SQLite DB / config persist. From the repo root:

```bash
sudo docker compose up -d --build     # build + run (also use after any code change)
sudo docker compose ps                # status
sudo docker compose logs -f           # logs
sudo docker compose down              # stop
```

Or `../start.sh` (auto-detects whether `sudo` is needed). Convenience npm scripts (run from this
folder): `npm run docker:deploy` (rebuild frontend), `npm run docker:deploy:all` (rebuild both).

Tip: drop the `sudo` by adding your user to the docker group once: `sudo usermod -aG docker $USER`
(then log out/in).

> Alternative runner: pm2 ([`../ecosystem.config.js`](../ecosystem.config.js)) still works, but don't
> run pm2 and Docker at the same time (port/DB conflict).

See [`../ARCHITECTURE.md`](../ARCHITECTURE.md) for the full backend/frontend module layout.
