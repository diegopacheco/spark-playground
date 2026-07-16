#!/bin/bash
cd "$(dirname "$0")"
podman-compose down --remove-orphans 2>/dev/null || true
echo "stopped"
