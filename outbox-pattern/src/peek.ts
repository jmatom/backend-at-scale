// Show how many messages are sitting in the SQS queue right now. This is the
// other half of the proof: after a naive crash it stays 0 (the message was
// lost); after the relay runs it shows the recovered message.
//
//   npm run peek

import { GetQueueAttributesCommand } from "@aws-sdk/client-sqs";
import { sqs, ensureQueue } from "./sqs.ts";

const queueUrl = await ensureQueue();
const { Attributes } = await sqs.send(
  new GetQueueAttributesCommand({
    QueueUrl: queueUrl,
    AttributeNames: [
      "ApproximateNumberOfMessages",
      "ApproximateNumberOfMessagesNotVisible",
    ],
  }),
);

const visible = Number(Attributes?.ApproximateNumberOfMessages ?? 0);
const inFlight = Number(Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0);
console.log(`[peek] SQS queue: ${visible} visible, ${inFlight} in-flight`);
