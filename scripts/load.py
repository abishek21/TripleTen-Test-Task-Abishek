"""Generate load against the producer.

Sends a batch of unique orders, then deliberately re-sends some of them to simulate the
duplicate deliveries you get from at-least-once upstreams (client retries, LB redrives).

Stdlib only -- no host install needed. Just `python scripts/load.py`.

  NUM_CUSTOMERS customers each place a fixed number of $1.00 orders, so the CORRECT
  ledger total per customer is deterministic and checkable (see scripts/check.py).
"""
import json
import urllib.request

PRODUCER = "http://localhost:8000"

NUM_ORDERS = 50          # unique orders
NUM_CUSTOMERS = 5        # orders are spread evenly across customers
AMOUNT_CENTS = 100       # $1.00 per order
NUM_DUPLICATES = 10      # how many of the orders get sent a second time


def post(path, payload):
    req = urllib.request.Request(
        PRODUCER + path,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status


def main():
    orders = [
        {
            "order_id": f"order-{i}",
            "customer_id": f"cust-{i % NUM_CUSTOMERS}",
            "amount_cents": AMOUNT_CENTS,
        }
        for i in range(NUM_ORDERS)
    ]

    # Re-send the first NUM_DUPLICATES orders. Same order_id => must NOT be charged twice.
    duplicates = orders[:NUM_DUPLICATES]

    sent = 0
    for order in orders + duplicates:
        post("/orders", order)
        sent += 1

    per_customer = (NUM_ORDERS // NUM_CUSTOMERS) * AMOUNT_CENTS
    print(f"sent {sent} messages: {NUM_ORDERS} unique + {NUM_DUPLICATES} duplicates")
    print(f"correct ledger total per customer = {per_customer} cents")
    print("run scripts/check.py to verify (give the worker a few seconds to drain)")


if __name__ == "__main__":
    main()
