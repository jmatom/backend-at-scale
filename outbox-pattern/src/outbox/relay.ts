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
import { sqs, ensureQueue, queueNameForTopic } from "../sqs.ts";

const BATCH_SIZE = 10;
const POLL_INTERVAL_MS = 1000;

interface OutboxRow {
  id: string;
  topic: string;
  payload: unknown;
}

// topic -> queue URL, resolved once per topic instead of on every batch.
const queueUrlCache = new Map<string, string>();

async function queueUrlFor(topic: string): Promise<string> {
  let url = queueUrlCache.get(topic);
  if (!url) {
    url = await ensureQueue(queueNameForTopic(topic));
    queueUrlCache.set(topic, url);
  }
  return url;
}

// Returns the number of rows actually dispatched (not merely claimed): if SQS
// permanently rejects a row, counting it as progress would make the poll loop
// spin at full speed retrying it forever. Returning only successes lets main()
// back off between attempts.
async function dispatchBatch(): Promise<number> {
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

    const okIds: string[] = [];
    if (rows.length > 0) {
      // One outbox table carries EVERY event type in the system; each row's
      // topic says where it goes. SendMessageBatch is per-queue, so group the
      // claimed rows by topic first, then publish one batch per topic and flip
      // all the dispatched flags in ONE UPDATE at the end. A per-row send +
      // per-row update works, but it turns 10 rows into 20 round-trips;
      // batching makes it (topics + 1). SendMessageBatch caps at 10 entries,
      // which is why BATCH_SIZE=10.
      //
      // Publish BEFORE marking dispatched — see the at-least-once note above.
      const byTopic = new Map<string, OutboxRow[]>();
      for (const row of rows) {
        const group = byTopic.get(row.topic) ?? [];
        group.push(row);
        byTopic.set(row.topic, group);
      }

      for (const [topic, group] of byTopic) {
        const result = await sqs.send(
          new SendMessageBatchCommand({
            QueueUrl: await queueUrlFor(topic),
            Entries: group.map((row) => ({
              Id: row.id, // batch-entry id; lets us match results to rows
              MessageBody: JSON.stringify(row.payload),
            })),
          }),
        );

        // A batch send can PARTIALLY fail (some entries accepted, some
        // rejected). Only mark the successful ones dispatched — the failed
        // ones simply stay dispatched=false and get retried on the next poll.
        okIds.push(...(result.Successful ?? []).map((s) => s.Id!));
        for (const f of result.Failed ?? []) {
          console.warn(`[relay] outbox row ${f.Id} rejected by SQS (${f.Code}); will retry`);
        }
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
    return okIds.length;
  } catch (err) {
    // Swallow ROLLBACK failures (the connection may already be dead); letting
    // one throw here would mask the original error.
    try {
      await client.query("ROLLBACK");
    } catch {
      /* ignore */
    }
    throw err;
  } finally {
    client.release();
  }
}

async function main(): Promise<void> {
  await bootstrap();
  console.log("[relay] polling for undispatched outbox rows… (Ctrl-C to stop)");

  let running = true;
  process.on("SIGINT", () => {
    running = false;
    console.log("\n[relay] shutting down…");
  });

  // No try/catch around the loop: an unexpected error crashes the relay on
  // purpose. Restart-to-recover is the supervision strategy for a demo (your
  // process manager restarts it; the outbox rows wait patiently). A production
  // relay would catch, log, and back off instead of dying.
  while (running) {
    const n = await dispatchBatch();
    // Only sleep when no progress was made — otherwise drain as fast as we can.
    if (n === 0) await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
  }

  await shutdown();
}

await main();
