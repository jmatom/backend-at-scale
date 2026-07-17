// Show the current state of both tables — the "proof" you look at after each
// scenario. In the naive-crash case you'll see a delivery with NO way to tell
// it was never published. In the outbox case you'll see the outbox row flip
// from dispatched=false to dispatched=true once the relay runs.
//
//   npm run inspect

import { pool, bootstrap, shutdown } from "./db.ts";

await bootstrap();

const deliveries = await pool.query(
  "SELECT public_id, status, created_at FROM deliveries ORDER BY id",
);
const outbox = await pool.query(
  "SELECT id, topic, dispatched, dispatched_at FROM outbox ORDER BY id",
);

console.log(`\n=== deliveries (${deliveries.rowCount}) ===`);
console.table(deliveries.rows);

console.log(`\n=== outbox (${outbox.rowCount}) ===`);
console.table(outbox.rows);

const pending = outbox.rows.filter((r) => !r.dispatched).length;
console.log(
  `\n${pending} outbox row(s) still undispatched (the relay will publish these).\n`,
);

await shutdown();
