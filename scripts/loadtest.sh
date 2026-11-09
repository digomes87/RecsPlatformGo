#!/usr/bin/env bash
# Load test with vegeta. Install: go install github.com/tsenart/vegeta@latest
set -euo pipefail
BASE=${BASE:-http://localhost:28080}
RATE=${RATE:-500}
DURATION=${DURATION:-30s}

TOKEN=$(go run ./pkg/cmd/minttoken -secret dev-secret-change-me -sub loadtest)

echo "GET $BASE/v1/users/user-1/recommendations?limit=10
Authorization: Bearer $TOKEN" | \
  vegeta attack -rate="$RATE"/1s -duration="$DURATION" | \
  vegeta report

# Example: also hit the write path.
# echo "POST $BASE/v1/events
# Authorization: Bearer $TOKEN
# Content-Type: application/json
#
# {\"event_id\":\"lt-$RANDOM\",\"user_id\":\"user-1\",\"item_id\":\"item-2\",\"event_type\":\"view\"}" | \
#   vegeta attack -rate="$RATE"/1s -duration="$DURATION" | vegeta report
