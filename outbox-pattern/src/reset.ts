// Wipe both tables AND the queue so you can re-run the demo from a clean slate.
// Purging the queue matters: without it, a message left over from a previous
// scenario would still show up in `npm run peek` and muddy the proof.
//   npm run reset

import { PurgeQueueCommand } from "@aws-sdk/client-sqs";
import { pool, bootstrap, shutdown } from "./db.ts";
import { sqs, ensureQueue } from "./sqs.ts";

await bootstrap();
await pool.query("TRUNCATE deliveries, outbox RESTART IDENTITY");

const queueUrl = await ensureQueue();
await sqs.send(new PurgeQueueCommand({ QueueUrl: queueUrl }));

console.log("[reset] deliveries + outbox truncated, queue purged");
await shutdown();
