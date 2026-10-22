#!/usr/bin/env bash
# Start or update the public demo on this server (run from the repo folder).
# Used by setup.sh the first time, and by the deploy workflow on every push.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build --wait --remove-orphans
docker image prune -f > /dev/null  # old builds would slowly fill the disk
for _ in $(seq 30); do  # ready = a model is scoring live requests
    if docker compose exec -T api python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/ready')" 2> /dev/null; then
        echo "Ready: https://$(grep -oP '^DOMAIN=\K.*' .env)"
        exit 0
    fi
    sleep 5
done
echo "Started, but not ready after 150 s: docker compose logs --tail 50" >&2
exit 1
