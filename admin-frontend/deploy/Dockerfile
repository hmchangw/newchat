FROM node:22-alpine AS builder
WORKDIR /app
COPY admin-frontend/package.json admin-frontend/package-lock.json ./
RUN npm ci
COPY admin-frontend/ .
RUN npm run build

FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY admin-frontend/deploy/nginx.conf /etc/nginx/conf.d/default.conf
COPY admin-frontend/deploy/config.js.template /etc/config.js.template
COPY --chmod=755 admin-frontend/deploy/30-render-config.sh /docker-entrypoint.d/30-render-config.sh

EXPOSE 80
