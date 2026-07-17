# The Transactional Outbox Pattern

> Companion code for the *Backend at Scale* article on the Outbox Pattern.
> Reproduce a real dual-write bug on your own machine in about two minutes, then
> watch the outbox fix it.

You save a row to your database and publish a message to a queue. Two systems,
two writes, no shared transaction. **What happens when your process dies between
the two?** This repo lets you *see* the answer — and the fix.

The scenario is a webhook delivery service: a request comes in, you persist a
`delivery` and return `202 Accepted`, and a
worker publishes the webhook to a queue for delivery. If the publish is a
separate step from the DB write, there's a window where you tell the customer
"accepted" and the webhook **never fires**. The 202 lied.

---

## Stack (all current-LTS, zero cloud credentials)

- **Node.js 24** — runs the TypeScript directly, no build step (native type stripping).
- **PostgreSQL 17** — the source of truth.
- **ElasticMQ** — a tiny SQS-compatible queue in Docker. The code uses the real
  `@aws-sdk/client-sqs`; only the endpoint URL differs from real AWS. (Point it
  at real SQS by setting `SQS_ENDPOINT=` to an empty string and supplying AWS
  credentials the usual way.)

## Prerequisites

- **Docker** with Compose v2 (`docker compose version` should work)
- **Node.js 24+** — any install works; there's a `.nvmrc` if you use fnm/nvm

## Setup

```bash
git clone https://github.com/jmatom/backend-at-scale.git
cd backend-at-scale/outbox-pattern   # everything below runs from this folder

fnm use                   # or: nvm use — skip if node -v already says 24+
docker compose up -d      # Postgres (port 5544) + ElasticMQ (ports 9324/9325)
npm install
```

Give Postgres a few seconds to boot before the first scenario — if a script
greets you with `ECONNREFUSED`, it's just not up yet. The ports are deliberately
unusual to avoid collisions; if one is taken anyway, change the host side of the
mapping in `docker-compose.yml` and export the matching env var (`DATABASE_URL`
or `SQS_ENDPOINT`).

No `tsc`, no `tsx` — `node src/foo.ts` just runs the TypeScript. There's no build
step, but there's still a type check: `npm run typecheck` (what you'd run in CI).

---

## Run the demo

You'll want two terminals for the last part. Every command below is an npm script.

### Scenario 1 — the naive dual write, happy path

```bash
npm run reset
npm run ingest:naive     # commits a delivery, then publishes to SQS
npm run peek             # SQS queue: 1 visible  ✅
```

Looks perfect. Ship it. This is how most systems start.

### Scenario 2 — the naive dual write, with a crash 💥

`CRASH_AFTER_COMMIT=1` kills the process *after* the DB commit and *before* the
publish — simulating a deploy, an OOM kill, a `kill -9`, a hardware fault.

```bash
npm run reset
npm run ingest:naive:crash    # commits the delivery, then dies
npm run inspect               # deliveries: 1 row, status 'accepted'
npm run peek                  # SQS queue: 0 visible  ❌  the webhook is LOST
```

The database says `accepted`. The queue is empty. Nothing in the system knows
this delivery needs publishing — there's no undispatched marker, no way to find
it. This is silent data loss, and it's invisible until a customer complains.

### Scenario 3 — the outbox, with the same crash ✅

The outbox version writes the delivery **and** an `outbox` row in **one
transaction**, and does *not* talk to the queue at all. A separate **relay**
publishes outbox rows and marks them dispatched.

```bash
npm run reset
npm run ingest:outbox:crash   # commits delivery + outbox row ATOMICALLY, then dies
npm run inspect               # deliveries: 1 | outbox: 1 row, dispatched=false
npm run peek                  # SQS queue: 0 visible  (not published YET)
```

The crash landed *after* a durable, atomic commit. The intent-to-publish
survives as an undispatched outbox row. Now start the relay:

```bash
# terminal 1
npm run consumer     # prints whatever lands on the queue

# terminal 2
npm run relay        # finds the undispatched row, publishes it, marks it dispatched
```

The consumer prints the recovered message. `npm run inspect` now shows the
outbox row flipped to `dispatched=true`. **No message was lost — the process
crash simply didn't matter.**

---

## Why this works

The whole trick is a single line of leverage: **the delivery and the
intent-to-publish are written in the same transaction**, so they are atomic
together. Postgres guarantees both-or-neither. Publishing is decoupled into a
process (the relay) that can crash, restart, or run in parallel — and the
un-published work is always sitting durably in a table, waiting.

```
  NAIVE (dual write)                    OUTBOX
  ─────────────────                     ──────
  BEGIN                                 BEGIN
    INSERT delivery                       INSERT delivery
  COMMIT            ← durable here         INSERT outbox_event
  ─ ─ crash window ─ ─               COMMIT   ← both durable, atomically
  publish(queue)   ← may never run
                                        (separately, a relay:)
                                        publish(queue) → mark dispatched
```

## The tradeoffs (read before you reach for it)

- **At-least-once delivery.** The relay publishes, *then* marks the row
  dispatched. Crash in between and the row is re-published next round — so a
  message can arrive **more than once**. This is unavoidable, and it's why your
  **consumer must be idempotent**. (That's the next article.)
- **Latency.** A message isn't published the instant you commit; it waits for
  the relay's next poll. Tune the poll interval, or graduate to Change Data
  Capture (e.g. Debezium tailing the WAL) to push latency toward zero.
- **The relay is infrastructure you now own.** It needs monitoring (outbox lag,
  oldest-undispatched age) and it must be idempotent and restart-safe.
- **`FOR UPDATE SKIP LOCKED`** lets you run N relays concurrently with zero
  coordination — each claims a different batch. Free horizontal scale.

## When you *don't* need it

If both "writes" go to the same database (no external queue), just use one
transaction. The outbox earns its keep specifically when you must update your DB
**and** hand work to another system (queue, broker, third-party API) atomically.

---

## Files

| File | What it shows |
|------|---------------|
| `src/naive/ingest.ts`  | The dual write. The bug lives here. |
| `src/outbox/ingest.ts` | Delivery + outbox row in one transaction. |
| `src/outbox/relay.ts`  | The dispatcher: poll → batch by topic → publish → mark dispatched. |
| `src/sqs.ts`           | Topic → queue routing and idempotent queue creation. |
| `src/consumer.ts`      | Stand-in downstream worker; prints what it receives. |
| `src/inspect.ts` / `src/peek.ts` | Look at DB / queue state — the "proof". |
| `src/reset.ts`         | Clean slate between scenarios: truncate tables, purge queue. |

Teardown: `docker compose down -v`.
