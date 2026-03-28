#!/usr/bin/env bash
set -euo pipefail

# Sets up Cloudflare Tunnel for form4.app
# Prerequisites: brew install cloudflared, domain added to Cloudflare

TUNNEL_NAME="form4"
DOMAIN="form4.app"
REPO_DIR="/Users/openclaw/trading-framework"

echo "=== Cloudflare Tunnel Setup for $DOMAIN ==="
echo ""

# Step 1: Login to Cloudflare (opens browser)
if [ ! -f "$HOME/.cloudflared/cert.pem" ]; then
    echo "[1/6] Logging in to Cloudflare..."
    cloudflared tunnel login
else
    echo "[1/6] Already logged in (cert.pem exists)"
fi

# Step 2: Create tunnel
EXISTING=$(cloudflared tunnel list -o json 2>/dev/null | python3 -c "
import json, sys
tunnels = json.load(sys.stdin)
for t in tunnels:
    if t['name'] == '$TUNNEL_NAME':
        print(t['id'])
        break
" 2>/dev/null || echo "")

if [ -n "$EXISTING" ]; then
    TUNNEL_UUID="$EXISTING"
    echo "[2/6] Tunnel '$TUNNEL_NAME' already exists: $TUNNEL_UUID"
else
    echo "[2/6] Creating tunnel '$TUNNEL_NAME'..."
    cloudflared tunnel create "$TUNNEL_NAME"
    TUNNEL_UUID=$(cloudflared tunnel list -o json | python3 -c "
import json, sys
tunnels = json.load(sys.stdin)
for t in tunnels:
    if t['name'] == '$TUNNEL_NAME':
        print(t['id'])
        break
")
    echo "  Tunnel UUID: $TUNNEL_UUID"
fi

# Step 3: Route DNS
echo "[3/6] Setting DNS routes..."
cloudflared tunnel route dns "$TUNNEL_NAME" "$DOMAIN" 2>&1 || echo "  (may already exist)"
cloudflared tunnel route dns "$TUNNEL_NAME" "www.$DOMAIN" 2>&1 || echo "  (may already exist)"

# Step 4: Write config
CONFIG_DIR="$HOME/.cloudflared"
CONFIG_FILE="$CONFIG_DIR/config.yml"
echo "[4/6] Writing config to $CONFIG_FILE..."

cat > "$CONFIG_FILE" <<EOF
tunnel: $TUNNEL_UUID
credentials-file: $CONFIG_DIR/$TUNNEL_UUID.json

ingress:
  - hostname: $DOMAIN
    service: http://localhost:80
  - hostname: www.$DOMAIN
    service: http://localhost:80
  - service: http_status:404
EOF

echo "  Config written."

# Step 5: Install as system service
echo "[5/6] Installing cloudflared as system service..."
if [ -f "/Library/LaunchDaemons/com.cloudflare.cloudflared.plist" ]; then
    echo "  Service already installed. Restarting..."
    sudo cloudflared service uninstall 2>/dev/null || true
fi
sudo cloudflared service install
echo "  Service installed and running."

# Step 6: Verify
echo "[6/6] Verifying tunnel..."
sleep 3
cloudflared tunnel info "$TUNNEL_NAME"

echo ""
echo "=== Setup Complete ==="
echo "Tunnel: $TUNNEL_NAME ($TUNNEL_UUID)"
echo "Config: $CONFIG_FILE"
echo ""
echo "Next steps:"
echo "  1. Ensure Docker is running: docker info"
echo "  2. Fill in .env with Clerk/Stripe/Resend keys"
echo "  3. Update .env: NEXT_PUBLIC_API_URL=https://$DOMAIN/api/v1"
echo "  4. Update .env: CORS_ORIGINS=https://$DOMAIN"
echo "  5. Deploy: bash deploy/deploy.sh"
echo ""
echo "The tunnel forwards https://$DOMAIN → localhost:80 → Caddy → API/Frontend"
