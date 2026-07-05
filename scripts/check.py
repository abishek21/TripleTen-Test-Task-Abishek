"""Acceptance check: is every customer charged exactly the correct amount?

Reads the ledger via the producer's read-only /ledger endpoint and compares it against
what scripts/load.py is defined to send. Stdlib only.

Because the payments service is flaky, a correct solution may need a few seconds (retries,
redelivery) to converge. So this polls until the ledger is correct or a timeout elapses:

  - total == expected AND every customer correct  -> PASS (exit 0)
  - total > expected                              -> FAIL fast (overcharge is irrecoverable)
  - still short when the timeout hits             -> FAIL (lost orders / not retried)

Tunable via env: CHECK_TIMEOUT_SECONDS (default 120), CHECK_INTERVAL_SECONDS (default 3).

Treat this as the definition of "correct" for the exercise. If you think it's wrong,
don't quietly edit it -- say so in your ADR.
"""
import json
import os
import sys
import time
import urllib.request

PRODUCER = "http://localhost:8000"

# Must stay in sync with scripts/load.py
NUM_ORDERS = 50
NUM_CUSTOMERS = 5
AMOUNT_CENTS = 100

EXPECTED_PER_CUSTOMER = (NUM_ORDERS // NUM_CUSTOMERS) * AMOUNT_CENTS
EXPECTED_TOTAL = NUM_ORDERS * AMOUNT_CENTS

TIMEOUT = float(os.environ.get("CHECK_TIMEOUT_SECONDS", "120"))
INTERVAL = float(os.environ.get("CHECK_INTERVAL_SECONDS", "3"))


def get_ledger():
    with urllib.request.urlopen(PRODUCER + "/ledger", timeout=10) as resp:
        return json.loads(resp.read())


def snapshot():
    ledger = get_ledger()["ledger"]
    per = {f"cust-{i}": ledger.get(f"cust-{i}", 0) for i in range(NUM_CUSTOMERS)}
    return per, sum(per.values())


def print_table(per, total):
    print("customer    charged   expected   status")
    print("-" * 44)
    for cust, charged in per.items():
        status = "OK" if charged == EXPECTED_PER_CUSTOMER else "WRONG"
        print(f"{cust:<10}{charged:>9}{EXPECTED_PER_CUSTOMER:>11}   {status}")
    print("-" * 44)
    print(f"total charged: {total}   expected: {EXPECTED_TOTAL}")


def main():
    deadline = time.monotonic() + TIMEOUT
    per, total = snapshot()
    while True:
        correct = total == EXPECTED_TOTAL and all(
            v == EXPECTED_PER_CUSTOMER for v in per.values()
        )
        if correct:
            print_table(per, total)
            print("\nPASS: every customer charged exactly the correct amount.")
            sys.exit(0)
        if total > EXPECTED_TOTAL:
            print_table(per, total)
            print("\nFAIL: overcharged -> duplicates were processed more than once "
                  "(idempotency). This cannot self-correct.")
            sys.exit(1)
        if time.monotonic() >= deadline:
            print_table(per, total)
            print(f"\nFAIL: still incorrect after {TIMEOUT:.0f}s -> orders were lost "
                  "(delivery / ack semantics) or never retried after a downstream failure.")
            sys.exit(1)
        time.sleep(INTERVAL)
        per, total = snapshot()


if __name__ == "__main__":
    main()
