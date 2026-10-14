#!/bin/bash
# Start gs_tun_client (arm64). Edit GS_URL/TOKEN/SV below, then run.
# Usage: ./gs_tun_client.sh
set -e
cd "$(dirname "$0")"

GS_URL='https://script.google.com/macros/s/PASTE_YOUR_DEPLOYMENT_ID/exec'
TOKEN='PASTE_YOUR_TOKEN'
SV='PASTE_SERVER_ID'   # from gs_tun_server's startup log: "server id: xxxxxxxx"

if [ ! -x ./gs_tun_client_arm64 ]; then
  echo "gs_tun_client_arm64 not found, downloading..."
  wget -q https://github.com/woand/gs-tun/raw/refs/heads/master/bin/gs_tun_client_arm64
  chmod +x gs_tun_client_arm64
fi

exec ./gs_tun_client_arm64 --gs "$GS_URL" --token "$TOKEN" --sv "$SV" -l 127.0.0.1:18080
