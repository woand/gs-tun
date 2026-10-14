#!/bin/bash
# Build the shipped binaries in bin/:
#   gs_tun_server, gs_tun_client          (linux/amd64)
#   gs_tun_client_arm64                   (linux/arm64)
set -e
cd "$(dirname "$0")"
export PATH="$HOME/workspace/tools/go/bin:$PATH"

GOOS=linux GOARCH=amd64 go build -o bin/gs_tun_server ./gs_tun_server
GOOS=linux GOARCH=amd64 go build -o bin/gs_tun_client  ./gs_tun_client
GOOS=linux GOARCH=arm64 go build -o bin/gs_tun_client_arm64 ./gs_tun_client

ls -la bin/gs_tun_server bin/gs_tun_client bin/gs_tun_client_arm64
