#!/bin/bash
set -e
cd "$(dirname "$0")"

HOST_PORT=8174
UI_CONTAINER=spark42-python3-ui

mkdir -p out
./fetch-deps.sh
podman-compose build
podman-compose run --rm spark-job
podman-compose up -d ui

for i in $(seq 1 60); do
  state=$(podman inspect -f '{{.State.Status}}' "$UI_CONTAINER" 2>/dev/null || echo missing)
  if [ "$state" = "exited" ] || [ "$state" = "missing" ]; then
    echo "UI container is not running (state: $state). Recent logs:"
    podman logs "$UI_CONTAINER" 2>&1 | tail -20 || true
    echo "If you see a port bind error, host port $HOST_PORT is already in use."
    exit 1
  fi
  if podman exec "$UI_CONTAINER" sh -c "wget -q -O /dev/null http://localhost:5173" 2>/dev/null; then
    echo "UI ready: http://localhost:$HOST_PORT"
    exit 0
  fi
  sleep 1
done

echo "UI did not become ready in 60s"
podman logs "$UI_CONTAINER" 2>&1 | tail -20 || true
exit 1
