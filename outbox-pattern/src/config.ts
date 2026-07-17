// All config has a local default so `git clone && docker compose up && npm start`
// works with no setup. Override any value with an env var to point at real infra.

export const config = {
  // Postgres — matches docker-compose.yml (host port 5544).
  databaseUrl:
    process.env.DATABASE_URL ??
    "postgres://outbox:outbox@localhost:5544/outbox",

  // SQS. Locally this is ElasticMQ; the SDK code is identical to real AWS.
  // To use real SQS: unset SQS_ENDPOINT and provide real AWS credentials + region.
  sqs: {
    endpoint: process.env.SQS_ENDPOINT ?? "http://localhost:9324",
    region: process.env.AWS_REGION ?? "us-east-1",
    queueName: process.env.QUEUE_NAME ?? "webhooks-deliver",
    // ElasticMQ ignores credentials, but the AWS SDK insists on *some* value.
    accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? "local",
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? "local",
  },

  // When set, ingest processes exit(1) right after the DB commit and before the
  // message reaches SQS — simulating a crash, GC pause, OOM kill, or deploy.
  crashAfterCommit: process.env.CRASH_AFTER_COMMIT === "1",
};
