#!/usr/bin/env bash
# End-to-end smoke test against the local docker compose stack.
set -euo pipefail
BASE=${BASE:-http://localhost:28080}

# Mint a dev JWT (HS256) matching JWT_SECRET=dev-secret-change-me.
TOKEN=$(go run ./pkg/cmd/minttoken -secret dev-secret-change-me -sub smoke-user)

echo "==> health"
curl -fsS "$BASE/healthz" && echo

echo "==> post 3 events"
for i in 1 2 3; do
  curl -fsS -X POST "$BASE/v1/events" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"event_id\":\"smoke-$i\",\"user_id\":\"smoke-user\",\"item_id\":\"item-$i\",\"event_type\":\"view\"}" && echo
done

echo "==> wait for ingestor batch flush + similarity rebuild"
sleep 12

echo "==> get recommendations"
curl -fsS "$BASE/v1/users/smoke-user/recommendations?limit=5" \
  -H "Authorization: Bearer $TOKEN" && echo

echo "SMOKE OK"
