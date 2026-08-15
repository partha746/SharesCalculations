/**
 * Zero-dependency static file server for the prebuilt Angular app (dist/dashboard),
 * with SPA fallback to index.html and a reverse proxy for /api -> backend.
 *
 * Usage: node serve-prod.js
 * Env:
 *   PORT        (default 4201)   port to listen on
 *   API_TARGET  (default http://127.0.0.1:8080)  backend base URL for /api
 *   DIST_DIR    (default ./dist/dashboard)  built app directory
 *
 * Deploy new frontend changes with `npm run build` (ng build); this server picks them up on next request.
 */
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');

const PORT = parseInt(process.env.PORT || '4201', 10);
const API_TARGET = new URL(process.env.API_TARGET || 'http://127.0.0.1:8080');
const DIST_DIR = path.resolve(__dirname, process.env.DIST_DIR || 'dist/dashboard');
const INDEX_HTML = path.join(DIST_DIR, 'index.html');

// Optional TLS: if TLS_CERT_FILE + TLS_KEY_FILE point to readable files, serve HTTPS.
const TLS_CERT_FILE = process.env.TLS_CERT_FILE || '';
const TLS_KEY_FILE = process.env.TLS_KEY_FILE || '';

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.ico': 'image/x-icon',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.webp': 'image/webp',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
  '.eot': 'application/vnd.ms-fontobject',
  '.map': 'application/json; charset=utf-8',
  '.txt': 'text/plain; charset=utf-8',
};

function proxyApi(req, res) {
  // Keep the browser's Host and add the standard X-Forwarded-* headers. The backend builds
  // absolute URLs (e.g. the ICICI OAuth callback) from these; overwriting Host with the internal
  // backend name made it advertise http://backend:8080/... instead of the origin the user is on.
  const forwardedProto = req.headers['x-forwarded-proto'] || (req.socket.encrypted ? 'https' : 'http');
  const forwardedHost = req.headers['x-forwarded-host'] || req.headers.host || '';
  const options = {
    protocol: API_TARGET.protocol,
    hostname: API_TARGET.hostname,
    port: API_TARGET.port,
    method: req.method,
    path: req.url,
    headers: {
      ...req.headers,
      'x-forwarded-proto': forwardedProto,
      'x-forwarded-host': forwardedHost,
      'x-forwarded-for': req.socket.remoteAddress || '',
    },
  };
  const upstream = http.request(options, (up) => {
    res.writeHead(up.statusCode || 502, up.headers);
    up.pipe(res);
  });
  upstream.on('error', (err) => {
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Bad gateway to backend: ' + err.message }));
  });
  req.pipe(upstream);
}

function safeJoin(base, target) {
  const p = path.normalize(path.join(base, target));
  if (!p.startsWith(base)) return null; // path traversal guard
  return p;
}

function sendFile(res, filePath, statusCode = 200) {
  const ext = path.extname(filePath).toLowerCase();
  const type = MIME[ext] || 'application/octet-stream';
  // index.html must never be cached so new builds are picked up immediately; hashed assets cache forever.
  const isHtml = ext === '.html';
  const headers = {
    'Content-Type': type,
    'Cache-Control': isHtml ? 'no-cache, no-store, must-revalidate' : 'public, max-age=31536000, immutable',
  };
  res.writeHead(statusCode, headers);
  fs.createReadStream(filePath).pipe(res);
}

const requestHandler = (req, res) => {
  if (req.url === '/api' || req.url.startsWith('/api/')) {
    return proxyApi(req, res);
  }

  let pathname;
  try {
    pathname = decodeURIComponent(new URL(req.url, 'http://localhost').pathname);
  } catch {
    pathname = req.url.split('?')[0];
  }
  if (pathname === '/') pathname = '/index.html';

  const filePath = safeJoin(DIST_DIR, pathname);
  if (!filePath) {
    res.writeHead(400);
    return res.end('Bad request');
  }

  fs.stat(filePath, (err, stat) => {
    if (!err && stat.isFile()) return sendFile(res, filePath);
    // SPA fallback: serve index.html for client-side routes (GET/HEAD only).
    if (req.method === 'GET' || req.method === 'HEAD') {
      return fs.stat(INDEX_HTML, (e2, s2) => {
        if (!e2 && s2.isFile()) return sendFile(res, INDEX_HTML);
        res.writeHead(404);
        res.end('Not found (build the app with `ng build`)');
      });
    }
    res.writeHead(404);
    res.end('Not found');
  });
};

let server;
let scheme = 'http';
if (TLS_CERT_FILE && TLS_KEY_FILE && fs.existsSync(TLS_CERT_FILE) && fs.existsSync(TLS_KEY_FILE)) {
  server = https.createServer(
    { cert: fs.readFileSync(TLS_CERT_FILE), key: fs.readFileSync(TLS_KEY_FILE) },
    requestHandler,
  );
  scheme = 'https';
} else {
  server = http.createServer(requestHandler);
}

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Static dashboard server listening on ${scheme}://0.0.0.0:${PORT}`);
  console.log(`Serving: ${DIST_DIR}`);
  console.log(`Proxying /api -> ${API_TARGET.origin}`);
  if (!fs.existsSync(INDEX_HTML)) {
    console.warn(`WARNING: ${INDEX_HTML} not found. Run \`npm run build\` (ng build) first.`);
  }
});
