// Wipe both tables so you can re-run the demo from a clean slate.
//   npm run reset

import { pool, bootstrap, shutdown } from "./db.ts";

await bootstrap();
await pool.query("TRUNCATE deliveries, outbox RESTART IDENTITY");
console.log("[reset] deliveries + outbox truncated");
await shutdown();
