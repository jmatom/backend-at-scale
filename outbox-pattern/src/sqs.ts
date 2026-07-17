import {
  SQSClient,
  CreateQueueCommand,
  GetQueueUrlCommand,
  QueueDoesNotExist,
} from "@aws-sdk/client-sqs";
import { config } from "./config.ts";

// One SQS client for the whole process. Against ElasticMQ locally; point it at
// real AWS by dropping `endpoint` and supplying real credentials + region.
export const sqs = new SQSClient({
  endpoint: config.sqs.endpoint,
  region: config.sqs.region,
  credentials: {
    accessKeyId: config.sqs.accessKeyId,
    secretAccessKey: config.sqs.secretAccessKey,
  },
});

// Map an outbox topic to an SQS queue name. One outbox table serves every
// event type in the system; the topic tells the relay where each row goes.
// SQS queue names can't contain dots, so "webhooks.deliver" -> "webhooks-deliver".
export function queueNameForTopic(topic: string): string {
  return topic.replace(/\./g, "-");
}

// Resolve a queue URL, creating the queue if it doesn't exist yet. CreateQueue
// is idempotent, so it's safe to call on every startup.
export async function ensureQueue(
  queueName: string = config.sqs.queueName,
): Promise<string> {
  try {
    const { QueueUrl } = await sqs.send(
      new GetQueueUrlCommand({ QueueName: queueName }),
    );
    if (QueueUrl) return QueueUrl;
  } catch (err) {
    if (!(err instanceof QueueDoesNotExist)) throw err;
  }

  const { QueueUrl } = await sqs.send(
    new CreateQueueCommand({ QueueName: queueName }),
  );
  if (!QueueUrl) throw new Error(`Failed to create SQS queue ${queueName}`);
  return QueueUrl;
}
