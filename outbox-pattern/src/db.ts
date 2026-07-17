import { Pool } from "pg";
import { config } from "./config.ts";

export const pool = new Pool({ connectionString: config.databaseUrl });

// Two tables tell the whole story:
//
//   deliveries    — the business row. In the naive version we write this, then
//                   separately publish to SQS (two systems, no shared transaction).
//
//   outbox        — the fix. In the outbox version we write the delivery AND an
//                   outbox row in ONE transaction. A separate relay reads this
//                   table and publishes to SQS, then marks the row dispatched.
//
// `dispatched` + `dispatched_at` let the relay find un-published events and be
// safely re-run (at-least-once). Nothing here is webhook-specific — swap
// "delivery" for "order", "payment", "email" and the shape is the same.
export async function bootstrap(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS deliveries (
      id          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      public_id   TEXT        NOT NULL UNIQUE,
      payload     JSONB       NOT NULL,
      status      TEXT        NOT NULL DEFAULT 'accepted',
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS outbox (
      id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      topic         TEXT        NOT NULL,
      payload       JSONB       NOT NULL,
      dispatched    BOOLEAN     NOT NULL DEFAULT false,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
      dispatched_at TIMESTAMPTZ
    );
  `);

  // The relay scans for undispatched rows constantly; a partial index keeps that
  // scan cheap even when the table has millions of already-dispatched rows.
  await pool.query(`
    CREATE INDEX IF NOT EXISTS outbox_undispatched_idx
      ON outbox (id) WHERE dispatched = false;
  `);
}

export async function shutdown(): Promise<void> {
  await pool.end();
}
