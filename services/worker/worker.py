"""Order worker (PROTOTYPE).

This is the quick version a teammate threw together to demo the happy path. It reads
orders off the stream and charges the customer. It has not been run against real
conditions — duplicate deliveries, restarts, or the payments service misbehaving.

Your job: make this production-grade. See the README.
"""
import json
import os
import time

import redis
import requests

REDIS_URL = os.environ["REDIS_URL"]
PAYMENTS_URL = os.environ["PAYMENTS_URL"]
ORDERS_STREAM = "orders"
CONSUMER_GROUP = "order-workers"    # consumer group name
CONSUMER_NAME = os.environ.get("CONSUMER_NAME", "worker-1")  # unique per worker instance

MAX_RETRIES = 10          # max attempts before giving up on an order
BACKOFF_BASE = 1          # initial backoff in seconds
BACKOFF_MAX = 30          # cap the backoff so we don't wait forever
HTTP_TIMEOUT = 10         # seconds before we consider a payment call hung
MAX_DELIVERIES = 3        # max times a message can be re-delivered before dead-lettering

r = redis.from_url(REDIS_URL, decode_responses=True)


def charge_with_retry(order):
    """Call the payments service with retries and exponential backoff.

    Returns True if the charge succeeded, False if all retries exhausted.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                f"{PAYMENTS_URL}/charge",
                json={"order_id": order["order_id"], "amount_cents": order["amount_cents"]},
                timeout=HTTP_TIMEOUT,
            )
            resp.raise_for_status()
            return True  # success
        except (requests.exceptions.HTTPError, requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            wait = min(BACKOFF_BASE * (2 ** (attempt - 1)), BACKOFF_MAX)
            print(f"attempt {attempt}/{MAX_RETRIES} failed for {order['order_id']}: {e}. "
                  f"retrying in {wait}s...", flush=True)
            time.sleep(wait)

    print(f"all {MAX_RETRIES} attempts exhausted for {order['order_id']}, giving up", flush=True)
    return False


def process(order):
    """Process an order. Returns True if done (safe to ACK), False to retry later."""
    order_id = order["order_id"]
    state_key = f"order:{order_id}"

    status = r.get(state_key)

    # State: "done" → fully processed, skip this duplicate
    if status == "done":
        print(f"skipping duplicate {order_id}", flush=True)
        return True

    # State: "charged" → payment went through but worker crashed before
    # updating the ledger. Skip charging, go straight to ledger update.
    if status == "charged":
        print(f"recovering {order_id} — charge OK, updating ledger", flush=True)
    else:
        # State: None or "charging" → need to (re)charge.
        # "charging" means a previous worker crashed mid-charge — we re-charge
        # because we can't know if the payment actually went through.
        r.set(state_key, "charging")

        # Charge the customer with retries
        if not charge_with_retry(order):
            r.delete(state_key)  # release claim so it can be retried later
            return False  # DON'T ACK — message stays pending for re-delivery

        # Payment succeeded — record this so we can recover if we crash next
        r.set(state_key, "charged")

    # Update ledger and mark as done (pipeline sends all together)
    pipe = r.pipeline()
    pipe.incrby(f"ledger:{order['customer_id']}", order["amount_cents"])
    pipe.incr("processed_count")
    pipe.set(state_key, "done")
    pipe.execute()

    print(f"processed {order_id} for {order['customer_id']}", flush=True)
    return True


def ensure_consumer_group():
    """Create the consumer group if it doesn't exist.

    Uses MKSTREAM so the stream is also created if the producer hasn't started yet.
    Starts reading from "0" = the beginning of the stream, so no messages are missed.
    """
    try:
        r.xgroup_create(ORDERS_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
        print(f"created consumer group '{CONSUMER_GROUP}'", flush=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            # Group already exists — that's fine (another worker or previous run created it)
            print(f"consumer group '{CONSUMER_GROUP}' already exists", flush=True)
        else:
            raise


def process_messages(messages, check_delivery_count=False):
    """Process a batch of messages. Only ACK after successful processing."""
    for msg_id, fields in messages:
        order = json.loads(fields["data"])

        # For pending messages (Phase 1): check how many times this message
        # has been delivered. If it keeps failing, ACK it to unblock the PEL
        # (dead-letter). Otherwise one poison message blocks ALL new messages.
        if check_delivery_count:
            info = r.xpending_range(ORDERS_STREAM, CONSUMER_GROUP, msg_id, msg_id, 1)
            if info and info[0]["times_delivered"] > MAX_DELIVERIES:
                print(f"DEAD-LETTER: {order['order_id']} failed after "
                      f"{info[0]['times_delivered']} deliveries, giving up", flush=True)
                r.xack(ORDERS_STREAM, CONSUMER_GROUP, msg_id)
                continue

        if process(order):
            # Success or duplicate — safe to ACK
            r.xack(ORDERS_STREAM, CONSUMER_GROUP, msg_id)
        else:
            # Processing failed — do NOT ACK. Message stays pending
            # and will be re-delivered on next startup (Phase 1).
            print(f"not ACKing {order['order_id']} — will retry later", flush=True)


def main():
    print("worker started", flush=True)
    ensure_consumer_group()

    # Phase 1: re-process any messages that were delivered but never ACK'd
    # (happens if the worker crashed mid-processing). id="0" means
    # "give me all pending messages for this consumer".
    print("checking for unacknowledged messages from previous run...", flush=True)
    while True:
        resp = r.xreadgroup(
            CONSUMER_GROUP, CONSUMER_NAME,
            {ORDERS_STREAM: "0"},  # "0" = pending (unACK'd) messages
            count=10,
        )
        # When there are no more pending messages, Redis returns an empty list
        if not resp or not resp[0][1]:
            break
        for _stream, messages in resp:
            process_messages(messages, check_delivery_count=True)
    print("pending messages drained, reading new messages...", flush=True)

    # Phase 2: read new messages forever
    while True:
        resp = r.xreadgroup(
            CONSUMER_GROUP, CONSUMER_NAME,
            {ORDERS_STREAM: ">"},  # ">" = new, never-delivered messages
            count=10,
            block=5000,            # wait up to 5s for new messages
        )
        if not resp:
            continue
        for _stream, messages in resp:
            process_messages(messages)


if __name__ == "__main__":
    main()
