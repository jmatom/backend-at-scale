// THE NAIVE VERSION — a dual write.
//
// Step 1 commits a row to Postgres. Step 2 publishes to SQS. They are two
// independent systems and there is NO transaction spanning them (there can't
// be — SQS cannot enroll in a Postgres transaction).
//
// Everything looks fine until the process dies between the two steps. Then the
// customer has a committed `accepted` delivery that will NEVER be published.
// That's the "202 that lied": your API said accepted, the webhook never fires.
//
// Reproduce it:
//   npm run ingest:naive         -> row committed AND published (happy path)
//   npm run ingest:naive:crash   -> row committed, process dies, NOTHING published
//
// This is the classic shape of the bug: insert a row, then publish to a broker
// (SQS, Kafka, RabbitMQ…) as two separate, unlinked steps.

import { SendMessageCommand } from "@aws-sdk/client-sqs";
import { pool, bootstrap, shutdown } from "../db.ts";
import { sqs, ensureQueue } from "../sqs.ts";
import { deliveryId } from "../id.ts";
import { config } from "../config.ts";

async function ingest(payload: unknown): Promise<string> {
  const queueUrl = await ensureQueue();
  const id = deliveryId();

  // --- Step 1: write the business row. Autocommit — it is durable NOW. ---
  await pool.query(
    "INSERT INTO deliveries (public_id, payload) VALUES ($1, $2)",
    [id, JSON.stringify(payload)],
  );
  console.log(`[naive] committed delivery ${id} to Postgres`);

  // --- The crash window. In production this is a deploy, an OOM kill, a
  //     hardware fault, a `kill -9`, a GC-pause-triggered health-check restart.
  if (config.crashAfterCommit) {
    console.error(
      `[naive] 💥 crashing before publish — delivery ${id} is now orphaned`,
    );
    process.exit(1);
  }

  // --- Step 2: publish. If we never get here, the row is stranded forever. ---
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify({ deliveryId: id, payload }),
    }),
  );
  console.log(`[naive] published delivery ${id} to SQS`);

  return id;
}

await bootstrap();
await ingest({ event: "order.created", orderId: "ord_123", amount: 4999 });
await shutdown();
