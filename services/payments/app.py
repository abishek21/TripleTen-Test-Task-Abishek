"""Payments service: a stand-in for a third-party provider you do NOT control.

It is flaky on purpose:
  - returns 500 on a configurable fraction of calls (transient provider errors)
  - hangs for SLOW_SECONDS on a configurable fraction of calls (latency / timeouts)

Do not "fix" this service. The point of the exercise is to build a worker that stays
correct and resilient in spite of it. You may tune the env vars in docker-compose.yml
while developing, but it will be graded at the defaults.
"""
import os
import random
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

FAILURE_RATE = float(os.environ.get("FAILURE_RATE", "0.3"))
SLOW_RATE = float(os.environ.get("SLOW_RATE", "0.1"))
SLOW_SECONDS = float(os.environ.get("SLOW_SECONDS", "5"))

app = FastAPI(title="payments")


class Charge(BaseModel):
    order_id: str
    amount_cents: int


@app.post("/charge")
def charge(c: Charge):
    if random.random() < SLOW_RATE:
        time.sleep(SLOW_SECONDS)
    if random.random() < FAILURE_RATE:
        raise HTTPException(status_code=500, detail="payment provider error")
    return {"status": "charged", "order_id": c.order_id, "amount_cents": c.amount_cents}


@app.get("/health")
def health():
    return {"ok": True}
