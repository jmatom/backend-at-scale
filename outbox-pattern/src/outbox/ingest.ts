// THE OUTBOX VERSION — ingest side.
//
// The fix is almost boring: instead of writing the row and then publishing, we
// write the row AND an `outbox` row in ONE Postgres transaction. We do NOT talk
// to SQS here at all. Publishing is somebody else's job (relay.ts).
//
// Because both inserts share a transaction, they commit together or not at all.
// There is no window where the delivery exists but the intent-to-publish
// doesn't. Crash whenever you like:
//
//   npm run ingest:outbox        -> both rows committed
//   npm run ingest:outbox:crash  -> both rows committed, THEN crash. The outbox
//                                   row survives and the relay publishes it later.
//
// The message reaching SQS is no longer coupled to this process staying alive.

import { pool, bootstrap, shutdown } from "../db.ts";
import { deliveryId } from "../id.ts";
import { config } from "../config.ts";

async function ingest(payload: unknown): Promise<string> {
  const id = deliveryId();

  // Best practice with node-postgres: a transaction must run on a SINGLE checked-out
  // client. Never `pool.query('BEGIN')` — the pool hands each query a different
  // connection, so your BEGIN and COMMIT can land on different sockets. Check one
  // client out, do all the work on it, and always release() it in `finally`.
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Both writes in the same transaction ↓↓↓
    await client.query(
      "INSERT INTO deliveries (public_id, payload) VALUES ($1, $2)",
      [id, JSON.stringify(payload)],
    );
    await client.query(
      "INSERT INTO outbox (topic, payload) VALUES ($1, $2)",
      ["webhooks.deliver", JSON.stringify({ deliveryId: id, payload })],
    );

    await client.query("COMMIT");
    console.log(`[outbox] committed delivery ${id} + outbox row atomically`);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }

  // The crash now lands AFTER a durable, atomic commit. The delivery and its
  // outbox row are both safe; the relay will publish on its next poll. No
  // orphan is possible.
  if (config.crashAfterCommit) {
    console.error(
      `[outbox] 💥 crashing after commit — but the outbox row is safe; the relay will publish it`,
    );
    process.exit(1);
  }

  return id;
}

await bootstrap();
await ingest({ event: "order.created", orderId: "ord_123", amount: 4999 });
await shutdown();
