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

import { SendMessageCommand } from "@aws-sdk/client-sqs";
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

    for (const row of rows) {
      // Publish BEFORE marking dispatched — see the at-least-once note above.
      await sqs.send(
        new SendMessageCommand({
          QueueUrl: queueUrl,
          MessageBody: JSON.stringify(row.payload),
        }),
      );
      await client.query(
        "UPDATE outbox SET dispatched = true, dispatched_at = now() WHERE id = $1",
        [row.id],
      );
      console.log(`[relay] dispatched outbox row ${row.id} -> SQS`);
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
