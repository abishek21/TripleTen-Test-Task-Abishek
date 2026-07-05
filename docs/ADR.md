# ADR-001: Making the Order-Payment Pipeline Production-Grade

## Status
Submitted

## Context

The prototype is a naive order → payment pipeline: a FastAPI producer pushes orders to a
Redis Stream, a worker reads them and charges a flaky payments service, then records the
total in a Redis ledger. It "works" on the happy path but breaks under real conditions:

- **No retry logic** — when the payments service returns 500 (30% of the time) or hangs
  (10%), the worker crashes and the order is lost forever.
- **No idempotency** — duplicate orders (same `order_id` sent twice) are charged twice,
  overcharging the customer.
- **No crash recovery** — the worker uses `XREAD` with `last_id = "$"`, storing position
  in memory. On restart, all unprocessed messages are skipped permanently.
- **No timeout** — a hanging payments call blocks the worker indefinitely.

## Decision

### Delivery & consistency semantics

**Effectively-once** (at-least-once delivery + idempotent processing).

True exactly-once is impossible across two systems (Redis and an external payments API)
because you can't atomically commit an HTTP call and a database write. Instead:

- **At-least-once delivery**: Redis consumer groups (`XREADGROUP`) guarantee every message
  is delivered at least once. Unacknowledged messages are re-delivered on restart.
- **Idempotent processing**: a per-order state key in Redis (`order:{order_id}`) ensures
  that re-processing the same order is a no-op once it reaches the `"done"` state.

The consumer must guarantee: never ACK a message until all side effects (charge + ledger
update) are durably recorded.

### Idempotency

Each order gets a Redis key `order:{order_id}` that tracks a state machine:

```
(not exists) → "charging" → "charged" → "done"
```

- **Key**: the `order_id` from the payload (e.g., `order:order-7`).
- **State lives in**: Redis string keys, same instance as the stream and ledger.
- **Race handled**: the state transitions (`None` → `"charging"` → `"charged"` → `"done"`)
  are checked on every delivery. Even if two messages with the same `order_id` arrive
  concurrently, only the first one to reach `"done"` will update the ledger — the second
  will see `"done"` and skip. In our single-threaded worker, concurrent access doesn't
  occur, but the state machine is safe by design if we scale to multiple worker instances.
- **Crash recovery**: if the state is `"charged"` on restart, we know the payment went
  through but the ledger wasn't updated — we skip directly to the ledger update instead
  of re-charging.

### Failure handling

- **Retries**: up to 10 attempts per order with exponential backoff (1s, 2s, 4s, ...
  capped at 30s). Catches `HTTPError` (500s), `Timeout`, and `ConnectionError`.
- **Timeouts**: 10-second timeout on every `requests.post` call. The payments service
  hangs for 5s on ~10% of calls; 10s is generous enough to let slow-but-valid responses
  through while bounding truly stuck calls.
- **Transient vs permanent**: all payment failures are treated as transient (retry). With
  a 30% failure rate, the probability of 10 consecutive failures is 0.3^10 ≈ 0.0006% —
  essentially zero. A real system would distinguish 4xx (permanent, don't retry) from
  5xx (transient, retry).
- **Poison messages**: a message that fails all 10 retries is NOT acknowledged — it stays
  in the Pending Entries List (PEL) for re-delivery on the next worker restart. However,
  a permanently failing message must not block the entire pipeline. On startup, the worker
  checks each pending message's `times_delivered` count (tracked by Redis). If a message
  has been re-delivered more than `MAX_DELIVERIES` (3) times, it is treated as a poison
  message: ACK'd to clear it from the PEL, and logged as a dead-letter. This ensures one
  bad order cannot block processing of all subsequent new messages.
- **Isolation**: one failing order doesn't block others. Each message is processed
  independently; a failure just skips that message and moves to the next.

## Tradeoffs & alternatives

### Build vs adopt: Redis Streams vs Kafka / SQS / managed broker

Redis Streams is the right choice at this scale (50 orders, single consumer). I'd keep it
until one of these thresholds is crossed:

| Trigger | Switch to |
|---------|-----------|
| Need durable retention beyond Redis memory | Kafka (disk-backed, configurable retention) |
| Multi-region / multi-datacenter | SQS or Kafka with MirrorMaker |
| Multiple independent consumer groups with different read rates | Kafka (designed for this) |
| Team doesn't want to operate Redis for messaging | SQS (fully managed, zero ops) |
| >10K messages/sec sustained | Kafka (partitioned, horizontally scalable) |

Redis Streams is a pragmatic middle ground: lighter than Kafka, more capable than a simple
queue, but it trades off disk durability (data is in memory by default) and lacks Kafka's
partition-level parallelism.

### From CI to CD

The current CI pipeline (lint + integration test) is the foundation. To reach continuous
delivery:

1. **Image build & push**: on merge to `main`, build Docker images and push to GHCR with
   the git SHA as the tag. Tag `latest` only after CI passes.
2. **Environments**: three stages — `dev` (auto-deploy on every push), `staging` (mirrors
   prod config, gated), `prod` (manual approval or canary).
3. **Rollout strategy**: canary for the worker (shift 10% of consumer group traffic, watch
   error rates, then promote). Blue-green for the producer (swap the load balancer target).
4. **GitOps**: ArgoCD watching a `deploy/` directory with Kubernetes manifests or Helm
   charts. Merge a PR that bumps the image tag → ArgoCD syncs → rollout happens. Provides
   audit trail and easy rollback via `git revert`.
5. **Observability gate**: the CD pipeline should block promotion if error rate or latency
   exceeds thresholds (Prometheus + Alertmanager or Datadog monitors).

### Scaling to 100×

At 100× (5,000 orders), the **first bottleneck is the single-threaded worker**:

- Each order takes 1+ seconds (retries, backoff), processing is serial.
- Fix: scale to N worker instances (`docker compose up --scale worker=5`). Redis consumer
  groups natively distribute messages across consumers — no code changes needed (each
  worker just needs a unique `CONSUMER_NAME`).

**Next bottleneck**: Redis becomes a single point of failure and a throughput ceiling.
- Fix: move to Kafka with partitioned topics. Partition by `customer_id` to maintain
  per-customer ordering while parallelizing across partitions.

**After that**: the payments service itself becomes the bottleneck (rate limits, latency).
- Fix: client-side rate limiting, circuit breakers (e.g., `pybreaker`), and async/batch
  charging if the provider supports it.

## Consequences

**Better now:**
- Pipeline survives flaky payments, duplicate orders, and worker restarts.
- Ledger is always correct — `check.py` passes consistently.
- Clear state machine makes debugging easy (inspect `order:*` keys in Redis).
- Poison messages are detected via delivery count and removed from the PEL, preventing
  one bad order from halting the entire pipeline.
- Smoke-test CI job verifies freshly built images start and respond to `/health` before
  running the full integration suite, providing fast feedback on broken builds.

**Still weak / next steps with more time:**
- **Dead-letter persistence**: poison messages are currently logged and ACK'd, but not
  stored durably. In production, dead-lettered orders should be written to a dedicated
  Redis stream (`dead-letters`) or persisted to a database (e.g., PostgreSQL) with the
  failure reason, timestamps, and full order payload. This creates an audit trail and
  enables an operator or automated job to investigate, fix the root cause, and replay
  failed orders back into the main stream.
- **Alerting on dead-letters**: dead-lettered orders represent lost revenue. An alert
  (PagerDuty, Slack, email) should fire when a message is dead-lettered so the team can
  investigate promptly rather than discovering it hours or days later.
- Per-order state keys accumulate forever — add a TTL (e.g., 24h) after marking `"done"`.
- No observability — add structured logging, Prometheus metrics (orders processed,
  retries, failures), and health checks that verify stream lag.
- The pipeline of `INCRBY + SET "done"` is not truly atomic — a Lua script would close
  that gap completely.
- No graceful shutdown — `SIGTERM` handling would let the worker finish in-flight orders
  before stopping.
- **Sync-to-async with webhooks**: the current design blocks the worker on every payment
  call — each order ties up the worker for the full request duration (up to 10s on
  timeouts, longer with retries). If the payments provider supports webhooks, we could
  switch from synchronous polling to an async callback model: the worker submits the
  charge request and immediately moves on to the next order; the payments service calls
  back to a webhook endpoint when the charge completes (or fails). The order state would
  transition from `"charging"` to `"charged"` only when the webhook arrives, decoupling
  the worker's throughput from the provider's latency. This turns the payments bottleneck
  from a blocking call into an event-driven flow — significantly improving throughput
  without scaling worker instances.
