#!/usr/bin/env bash
# Generates synthetic interaction traffic through the public API so the
# pipeline (gateway -> Kafka -> ingestor -> Mongo -> recommender) has data.
set -euo pipefail
BASE=${BASE:-http://localhost:28080}
USERS=${USERS:-50}
ITEMS=${ITEMS:-100}
EVENTS=${EVENTS:-2000}

TOKEN=$(go run ./pkg/cmd/minttoken -secret dev-secret-change-me -sub seed)
events=0
while [ "$events" -lt "$EVENTS" ]; do
  u=$((RANDOM % USERS)); i=$((RANDOM % ITEMS))
  case $((RANDOM % 4)) in
    0) type=view ;; 1) type=like ;; 2) type=purchase ;; 3) type=rating ;;
  esac
  curl -fsS -o /dev/null -X POST "$BASE/v1/events" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"event_id\":\"seed-$events\",\"user_id\":\"user-$u\",\"item_id\":\"item-$i\",\"event_type\":\"$type\"}"
  events=$((events + 1))
done
echo "seeded $EVENTS events"
