# AI Systems Engineering — SME Take-Home
### Track B: Cloud & Distributed Systems

Thanks for taking the time. This exercise is built to respect it: **target effort is 3–4 hours.**
If you find yourself going much past that, stop and write down what you *would* do next — we
care more about your judgment and tradeoff reasoning than about a 100%-polished result.

You are applying to **author** a graduate-level program for experienced engineers (5+ yrs)
moving into system-architecture roles. So we're not just checking whether you can build this —
we're checking whether you understand it deeply enough to *teach it* and to *explain why*.

---

## The scenario

A teammate hacked together a prototype of an **order → payment pipeline** and pushed it before
going on leave. It "works on my machine" for the happy path. Product now wants to put it in
front of real traffic.

```
  POST /orders          ┌──────────────┐   Redis Stream    ┌──────────┐   POST /charge   ┌────────────┐
  ───────────────────►  │  producer    │ ────"orders"────► │  worker  │ ───────────────► │  payments  │
                        │  (FastAPI)   │                   │          │                  │  (flaky)   │
                        └──────────────┘                   └──────────┘                  └────────────┘
                                                                 │
                                                                 ▼
                                                         Redis "ledger" (amount charged per customer)
```

- **producer** — accepts orders over HTTP and publishes them to a Redis Stream.
- **worker** — consumes orders and charges the customer via the payments service.
- **payments** — a downstream dependency we don't control. It is **flaky on purpose**: it returns
  `500`s a fraction of the time and occasionally hangs. Treat it like a real third-party provider.
- **ledger** — running total charged per customer, in Redis. This is how correctness is observed:
  each customer should be charged *exactly what they ordered* — no more, no less.

Everything runs locally with Docker Compose. No cloud account or spend required.

---

## Your job

**Make this pipeline production-grade.** The prototype is naive. Find what's wrong with it under
real conditions (duplicate deliveries, the worker restarting, the payments service failing) and
fix it so the system is **correct and resilient**.

We are deliberately **not** giving you a checklist of bugs. Deciding what matters here *is* the
exercise. A senior systems engineer should be able to look at this and know where it will hurt.

To see it misbehave before you change anything:

```bash
docker compose up --build -d
python scripts/load.py        # fires a batch of orders, including some duplicates
python scripts/check.py       # compares the ledger against what was actually ordered
```

Then try killing the worker mid-run (`docker compose restart worker`) while load is flowing, and
watch what happens to the numbers.

---

## What "good" looks like (without telling you how)

When you're done, we should be able to run `scripts/load.py` — including its duplicate sends, and
even with the worker restarted partway through and the payments service failing — and
`scripts/check.py` should report **every customer charged exactly the correct amount**, with no
lost orders and no double charges.

How you get there is up to you. We care about the reasoning behind the choices.

## The CI pipeline IS the definition of done

There's a GitHub Actions workflow at `.github/workflows/ci.yml`. It boots the whole stack and runs
the acceptance check as a gate. **On the prototype as shipped, CI is red.** Your job is to make it
**green** — without weakening what the check considers correct.

Work on a branch and open a pull request into `main`; we read the PR and its CI result. (If you send
a zip instead, that's fine — we'll run CI on our side. Don't disable or gut the workflow to force a
pass; that's the most obvious red flag there is.)

---

## Deliverables

Put everything in this repo and send it back (zip or a repo link). Three things:

1. **Working code.** `docker compose up --build` brings up a pipeline that passes `scripts/check.py`
   under the conditions above. Keep your changes focused and readable — you're writing for an
   audience of engineers who will read it.

2. **`ADR.md`** — a short Architecture Decision Record (a template is in `docs/`). Cover:
   - The **delivery/consistency semantics** you chose (at-most-once / at-least-once / effectively-once)
     and *why* — this is the heart of it.
   - Your **idempotency** strategy.
   - Your **failure-handling** policy: retries, backoff, timeouts, dead-letter / poison messages.
   - One **build-vs-adopt** call: we used **Redis Streams** to keep setup light. Would you reach for
     **Kafka / SQS / a managed broker** here, and at what point? What changes that decision?
   - **From CI to CD:** this repo stops at CI. How would you take it to real **continuous delivery** —
     image promotion, environments (dev/stage/prod), rollout strategy (blue-green / canary), and would
     you run **GitOps** (ArgoCD / Flux)? Reason about it; don't build it.
   - What you'd do differently to take this to **100× throughput**, and where the first bottleneck appears.
   Keep it tight — one to two pages. Decisions and tradeoffs, not an essay.

3. **`AI_NOTES.md`** — you'll almost certainly use AI tools; we want you to. This role is partly about
   *correcting* AI output. So: paste **one prompt** you used, and **one thing the AI got wrong or
   oversimplified** that you caught and corrected (a subtly wrong consumer-group pattern, a retry
   that isn't safe, a "just add a try/except" that drops data, etc.). One short paragraph is plenty.
   If you genuinely used no AI, say so and tell us how you'd have used it.

---

## Bonus (only if you have time — not required)

Strong CI/CD instincts are central to this track. If (and only if) the core is done and you have
time left, pick **one** small thing and do it; otherwise just describe it in the ADR:
- Extend the pipeline with an image **build-and-push** to GHCR on `main`, or
- Add a Docker image **vulnerability scan** (e.g. Trivy) or a **Dockerfile lint** (hadolint) job, or
- Add a tiny **smoke-test** job that hits `/health` on a freshly built image.

We'd rather see the core pipeline correct and a sharp ADR than a sprawl of half-wired bonus jobs.

## Constraints & notes

- **Language:** the starter is Python (the program's default teaching language). You may rewrite the
  worker in Go/Node/etc. if it's faster for you — but keep `docker compose up` as the single entry
  point and keep `scripts/check.py` working as the acceptance check.
- **Scope discipline is part of the test.** Don't add Kubernetes, Terraform, or a real cloud deploy —
  *discuss* those in the ADR instead. We want to see you spend the 3–4 hours where it counts.
- You can change anything in `services/` and the compose file. Try not to change `scripts/check.py`'s
  definition of "correct" — if you think it's wrong, note that in your ADR.
- Ask questions if anything is ambiguous. Knowing what to ask is also signal.

---

## Running it

```bash
# bring everything up
docker compose up --build -d

# generate load (includes intentional duplicate orders)
python scripts/load.py

# verify correctness (polls until correct or times out; exit 0 = correct ledger)
# this is the same check the CI pipeline runs
python scripts/check.py

# resilience: restart the worker while load is running, then re-check
docker compose restart worker

# tear down (clears Redis state)
docker compose down -v
```

`scripts/load.py` and `scripts/check.py` use only the Python standard library, so you don't need to
install anything on the host — just Docker.

Good luck. We're looking forward to seeing how you think.
