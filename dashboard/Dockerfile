# Frontend: build the Angular app, then serve it with the zero-dependency static
# server (serve-prod.js), which also reverse-proxies /api to the backend.
# Build context = dashboard/

# --- Build stage ---
FROM node:20-alpine AS build
# Build tools for any npm packages with native (node-gyp) components.
RUN apk add --no-cache python3 make g++
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

# --- Runtime stage ---
FROM node:20-alpine
WORKDIR /app
ENV PORT=4201 \
    API_TARGET=http://backend:8080
COPY serve-prod.js ./
COPY --from=build /app/dist ./dist
EXPOSE 4201

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://127.0.0.1:4201/ >/dev/null 2>&1 || exit 1

CMD ["node", "serve-prod.js"]
