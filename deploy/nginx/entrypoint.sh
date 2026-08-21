#!/bin/sh
set -e

SSL_DIR="/etc/nginx/ssl"
CRT_PATH="$SSL_DIR/sentinel.crt"
KEY_PATH="$SSL_DIR/sentinel.key"

mkdir -p "$SSL_DIR"

if [ ! -f "$CRT_PATH" ] || [ ! -f "$KEY_PATH" ]; then
    echo "[Sentinel Ingress] No TLS certificate found at $CRT_PATH. Generating self-signed certificate for dev/bootstrap..."
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout "$KEY_PATH" \
        -out "$CRT_PATH" \
        -subj "/C=US/ST=NewYork/L=NewYork/O=Sentinel/OU=Intelligence/CN=localhost" \
        -addext "subjectAltName=DNS:localhost,DNS:sentinel.local,IP:127.0.0.1"
    chmod 600 "$KEY_PATH"
    chmod 644 "$CRT_PATH"
    echo "[Sentinel Ingress] Self-signed certificate generated successfully."
else
    echo "[Sentinel Ingress] Using existing TLS certificate at $CRT_PATH."
fi

echo "[Sentinel Ingress] Starting Nginx reverse proxy..."
exec nginx -g "daemon off;"
