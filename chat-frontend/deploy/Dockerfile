FROM node:22-alpine AS builder
WORKDIR /app
COPY chat-frontend/package.json chat-frontend/package-lock.json ./
RUN npm ci
COPY chat-frontend/ .
RUN npm run build

FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY chat-frontend/deploy/nginx.conf /etc/nginx/conf.d/default.conf
COPY chat-frontend/deploy/config.js.template /etc/config.js.template
COPY --chmod=755 chat-frontend/deploy/30-render-config.sh /docker-entrypoint.d/30-render-config.sh
RUN sed -i 's/\r$//' /docker-entrypoint.d/30-render-config.sh

EXPOSE 80
