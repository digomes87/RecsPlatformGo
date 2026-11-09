# RecsPlatform — Recommendation Platform in Go

Study case of a high-scale recommendation/personalization platform,
covering: Go microservices, distributed systems, MongoDB, AWS, Kubernetes
and observability.

## Architecture

```
            ┌─────────┐   POST /v1/events    ┌───────┐
 Client ───▶│ gateway │──────────────────────▶│ Kafka │
            │  :8080  │                       └───┬───┘
            └────┬────┘                           │ consumer group
                 │ GET /v1/users/{id}/recs        ▼
                 │ (singleflight + Redis   ┌──────────┐  bulk writes  ┌─────────┐
                 │  cache + circuit        │ ingestor │──────────────▶│ MongoDB │
                 │  breaker)               └────┬─────┘               └────┬────┘
                 ▼                             │ similarity rebuild      │
            ┌─────────────┐                   └──────────────────────────┘
            │ recommender │◀── reads precomputed views (item_similarity,
            │    :8081    │                     trending, user_interactions)
            └─────────────┘
```

**Write path** (asynchronous): `client → gateway → Kafka → ingestor →
MongoDB` — the API keeps predictable latency; the ingestor performs
idempotent bulk writes (dedup by `event_id` with TTL) and periodically
rebuilds the item-item similarity matrix (cosine over co-interactions).

**Read path** (latency-critical): `client → gateway → Redis (cache with
singleflight stampede protection) → recommender (CF + trending + popular
blend) → MongoDB`. A circuit breaker in the gateway protects against a
degraded recommender; in degraded mode it serves stale cache.

## Services

| Service | Port | Responsibility |
|---|---|---|
| `gateway` | 8080 | Public API, JWT, rate limiting, Redis cache, Kafka producer |
| `recommender` | 8081 | Engine: collaborative filtering + trending + cold-start fallback |
| `ingestor` | 8082 | Kafka consumer group, Mongo bulk writes, similarity rebuild |

## Running locally (Docker via colima)

```bash
make up          # starts Mongo, Kafka (KRaft), Redis, the 3 services, Prometheus and Grafana
make seed        # generates synthetic traffic through the public API
make smoke       # end-to-end test: events → Kafka → Mongo → recommendations
make load-test   # vegeta against /v1/users/{id}/recommendations
make down
```

- Grafana: http://localhost:3000 (dashboard "RecsPlatform — Service Overview")
- Prometheus: http://localhost:9090
- Gateway: http://localhost:28080 (8080/18080 are usually taken locally)

Development token:

```bash
go run ./pkg/cmd/minttoken -secret dev-secret-change-me -sub user-1
```

## Development

```bash
make build   # builds the 4 modules (go.work)
make test    # unit tests with -race
make vet     # go vet
make lint    # golangci-lint
make bench   # engine benchmarks (~4µs/recommendation)
```

## Kubernetes

```bash
kubectl apply -f deploy/k8s/base.yml -f deploy/k8s/autoscaling.yml
```

Includes: Deployments with zero-downtime rolling updates
(`maxUnavailable: 0`), readiness/liveness probes, `preStop` for load
balancer draining, PodDisruptionBudgets, HPAs (CPU + Kafka consumer lag
for the ingestor) and an nginx Ingress with edge rate limiting.

## AWS (Terraform)

`deploy/terraform/aws/main.tf`: EKS (Graviton, managed node groups) + MSK
(replication.factor=3, min.insync.replicas=2) + DocumentDB (3 replicas,
encryption at rest) + ElastiCache Redis (multi-AZ) + VPC with per-AZ NAT.

```bash
cd deploy/terraform/aws
terraform init && terraform apply   # requires AWS credentials
```

## MongoDB data modeling

| Collection | Model | Indexes |
|---|---|---|
| `events` | append-only event log, bucketed by day | 30-day TTL |
| `processed_events` | dedup set (unique `event_id`) | unique + 24h TTL |
| `user_interactions` | denormalized, accumulated weight per (user,item) | `{user_id, at:-1}` |
| `trending` | per-item counters (atomic `$inc` upsert) | `{score:-1}` |
| `item_similarity` | precomputed neighbors (top-50) | `_id` |

Patterns applied: **day bucketing** for cheap TTL, a **single unique
index** for idempotency, **atomic writes** (`$inc`/`$max`) with no
read-modify-write, and **precomputed views** for cache-latency reads.

## Architecture decisions (condensed ADRs)

1. **Events via Kafka, not synchronous** — decouples the API from
   processing; traffic bursts are absorbed by the log instead of MongoDB.
2. **Precomputed similarity** — recomputing cosine per request does not
   scale; the scheduled rebuild keeps reads at O(neighbors).
3. **Idempotency by `event_id`** — Kafka's at-least-once + TTL dedup =
   no duplicates and no unbounded table.
4. **Cache with singleflight** — prevents stampedes when a hot user's
   cache entry expires.
5. **Circuit breaker + stale fallback** — availability prefers old
   recommendations over errors.
6. **Monorepo with go.work** — shared code without publishing modules.
7. **Distroless + nonroot + readOnlyRootFilesystem** — minimal container
   attack surface.

## Operations (runbook)

| Symptom | Likely cause | Action |
|---|---|---|
| `circuit_breaker_state = 2` sustained | recommender degraded | Check recommender pods; gateway is already serving stale cache |
| `kafka_consumer_lag_seconds` growing | ingestor overwhelmed | HPA scales; if at ceiling, increase topic partitions |
| Cache ratio ~0 | Redis unavailable | Gateway runs cache-less (noop); restore ElastiCache |
| `ingestor_batch_duration_seconds` high | Mongo under pressure | Check indexes; scale DocumentDB |
| Mass 429s | rate limit too tight | Adjust `RATE_LIMIT_RPS`/`RATE_LIMIT_BURST` |

## CI

GitHub Actions (`.github/workflows/ci.yml`): build, vet, race-detector
tests, benchmark smoke, golangci-lint and build/push of the 3 images to
ghcr.io on merges to `main`.
