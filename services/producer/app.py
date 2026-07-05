"""Producer service: accepts orders over HTTP and publishes them to a Redis Stream.

This service is fine as-is. The interesting work is downstream, in the worker.
"""
import json
import os

import redis
from fastapi import FastAPI
from pydantic import BaseModel

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
ORDERS_STREAM = "orders"

r = redis.from_url(REDIS_URL, decode_responses=True)
app = FastAPI(title="orders-producer")


class Order(BaseModel):
    order_id: str
    customer_id: str
    amount_cents: int


@app.post("/orders", status_code=202)
def create_order(order: Order):
    # At-least-once upstreams (load balancers, client retries, redrives) mean the
    # same order_id can legitimately arrive here more than once. That's expected.
    r.xadd(ORDERS_STREAM, {"data": json.dumps(order.model_dump())})
    return {"status": "queued", "order_id": order.order_id}


@app.get("/ledger")
def ledger():
    # Read-only view of the ledger, used by scripts/check.py to verify correctness.
    # The ledger contract is fixed: one key per customer, `ledger:{customer_id}`,
    # whose integer value is the total cents charged to that customer.
    totals = {}
    for key in r.scan_iter(match="ledger:*"):
        customer_id = key.split("ledger:", 1)[1]
        totals[customer_id] = int(r.get(key) or 0)
    return {
        "ledger": totals,
        "processed_count": int(r.get("processed_count") or 0),
    }


@app.get("/health")
def health():
    return {"ok": True}
