# AI Notes

## A prompt I used

"For crash recovery, use XREADGROUP with consumer groups. On startup, first read all
pending unACK'd messages with id='0' in a loop until the PEL is drained, then switch to
reading new messages with id='>'. This way no messages are lost on restart."

## Something the AI got wrong or oversimplified — and how I caught it

The AI suggested a two-phase startup for crash recovery: Phase 1 reads unACK'd (pending)
messages from the PEL using `XREADGROUP id="0"`, and Phase 2 reads new messages using
`XREADGROUP id=">"`. Phase 1 loops until the PEL is empty before moving to Phase 2. The
AI presented this as a complete solution for crash recovery and moved on.

While reviewing this design, I noticed a critical flaw: if a single message in the PEL
keeps failing (a poison message — e.g., malformed data, a permanently rejected charge),
it would never get ACK'd. Phase 1 would re-read it on every loop iteration, retry 10
times, fail, leave it unACK'd, read it again, retry 10 more times — forever. Phase 2
would **never be reached**, meaning all new incoming orders would be blocked indefinitely
by one bad message.

The AI's implicit assumption was that all failures are transient and will eventually
succeed with enough retries. That's not true in production — permanent failures exist, and
one should not block an entire pipeline. I fixed this by adding a delivery count check
using `XPENDING`: if a message has been re-delivered more than `MAX_DELIVERIES` times, ACK
it to clear it from the PEL and log it as a dead-letter. This ensures Phase 1 always
drains and Phase 2 is always reachable, regardless of individual message failures.

## How I used AI

I used AI (GitHub Copilot) as a coding assistant throughout this project. The design
decisions — delivery semantics, the state machine approach for idempotency, the two-phase
consumer group pattern, and the poison message handling — were mine. I directed the
implementation strategy and told the AI what to build at each step. The AI helped me write
the code efficiently, articulate my reasoning clearly in the ADR, and catch syntax issues
I'd have spent time debugging manually. Every piece of AI output was reviewed and tested
by me — the poison message fix above is one example where that review process caught a
real gap.
