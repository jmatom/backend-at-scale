// THE OUTBOX VERSION — relay (a.k.a. the dispatcher / message relay).
//
// A long-running process that does one thing: find undispatched outbox rows,
// publish them to SQS, and mark them dispatched. Run it in a loop, run several
// copies, restart it any time — the pattern tolerates all of that.
//
//   npm run relay
//
// Two details that matter at scale:
//
//  1) FOR UPDATE SKIP LOCKED — lets you run N relays concurrently. Each grabs a
//     different batch instead of fighting over the same rows. Free horizontal
//     scaling with zero coordination.
//
//  2) We publish to SQS FIRST, then mark dispatched. If we crash in between, the
//     row stays undispatched and gets re-published next round. That means
//     AT-LEAST-ONCE delivery: a message can arrive more than once. That is the
//     unavoidable tradeoff of the outbox — and exactly why your CONSUMER must be
//     idempotent (the subject of the next article).
//
// Sharp-eyed readers will notice we hold the transaction open across the SQS
// call — the exact thing the ingest path must never do. It's deliberate here,
// and the situation is different on every axis that made it dangerous there:
// nobody is waiting on this transaction (background process, not a request);
// the locked rows live in a dedicated table nothing else queries, so there's
// zero contention with business traffic; concurrency is bounded (one tx per
// relay, not one per request); and the lock actually BUYS correctness — it's
// the mutual exclusion that stops two relays double-sending the same rows.
// Worst case is a duplicate, which at-least-once already embraces. At very
// high volume you'd graduate to a lease column (claim in a short tx, publish
// with no tx open, then mark dispatched) to keep transactions short for
// vacuum's sake — a scale refinement, not a correctness fix.

import { SendMessageBatchCommand } from "@aws-sdk/client-sqs";
import { pool, bootstrap, shutdown } from "../db.ts";
import { sqs, ensureQueue } from "../sqs.ts";

const BATCH_SIZE = 10;
const POLL_INTERVAL_MS = 1000;

interface OutboxRow {
  id: string;
  topic: string;
  payload: unknown;
}

async function dispatchBatch(queueUrl: string): Promise<number> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Claim a batch. SKIP LOCKED means a second relay instance silently steps
    // over rows this one already holds, instead of blocking.
    const { rows } = await client.query<OutboxRow>(
      `SELECT id, topic, payload
         FROM outbox
        WHERE dispatched = false
        ORDER BY id
        FOR UPDATE SKIP LOCKED
        LIMIT $1`,
      [BATCH_SIZE],
    );

    if (rows.length > 0) {
      // Publish the whole claimed batch in ONE SQS call, then flip all the
      // dispatched flags in ONE UPDATE. A per-row send + per-row update works,
      // but in production it turns 10 rows into 20 round-trips; batching makes
      // it 2. SendMessageBatch caps at 10 entries, which is why BATCH_SIZE=10.
      //
      // Publish BEFORE marking dispatched — see the at-least-once note above.
      const result = await sqs.send(
        new SendMessageBatchCommand({
          QueueUrl: queueUrl,
          Entries: rows.map((row) => ({
            Id: row.id, // batch-entry id; lets us match results to rows
            MessageBody: JSON.stringify(row.payload),
          })),
        }),
      );

      // A batch send can PARTIALLY fail (some entries accepted, some rejected).
      // Only mark the successful ones dispatched — the failed ones simply stay
      // dispatched=false and get retried on the next poll. No special handling.
      const okIds = (result.Successful ?? []).map((s) => s.Id);
      for (const f of result.Failed ?? []) {
        console.warn(`[relay] outbox row ${f.Id} rejected by SQS (${f.Code}); will retry`);
      }

      if (okIds.length > 0) {
        await client.query(
          "UPDATE outbox SET dispatched = true, dispatched_at = now() WHERE id = ANY($1)",
          [okIds],
        );
        console.log(`[relay] dispatched ${okIds.length} outbox row(s) -> SQS`);
      }
    }

    await client.query("COMMIT");
    return rows.length;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function main(): Promise<void> {
  await bootstrap();
  const queueUrl = await ensureQueue();
  console.log("[relay] polling for undispatched outbox rows… (Ctrl-C to stop)");

  let running = true;
  process.on("SIGINT", () => {
    running = false;
    console.log("\n[relay] shutting down…");
  });

  while (running) {
    const n = await dispatchBatch(queueUrl);
    // Only sleep when there was nothing to do — otherwise drain as fast as we can.
    if (n === 0) await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
  }

  await shutdown();
}

await main();
