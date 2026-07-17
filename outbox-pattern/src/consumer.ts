// The downstream consumer — stands in for whatever actually delivers the
// webhook (an HTTP call to the customer's URL). Here it just prints what it
// receives, so you can SEE whether a message made it onto the queue.
//
//   npm run consumer
//
// Leave this running in its own terminal during the demo. In the naive-crash
// scenario it prints nothing (the message was lost). In the outbox scenario it
// prints the message after the relay publishes it.

import {
  ReceiveMessageCommand,
  DeleteMessageCommand,
} from "@aws-sdk/client-sqs";
import { sqs, ensureQueue } from "./sqs.ts";

async function main(): Promise<void> {
  const queueUrl = await ensureQueue();
  console.log("[consumer] waiting for messages… (Ctrl-C to stop)");

  let running = true;
  process.on("SIGINT", () => {
    running = false;
    console.log("\n[consumer] shutting down…");
  });

  while (running) {
    const { Messages } = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 5, // long poll
      }),
    );

    for (const msg of Messages ?? []) {
      console.log(`[consumer] 📬 received: ${msg.Body}`);
      await sqs.send(
        new DeleteMessageCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: msg.ReceiptHandle,
        }),
      );
    }
  }
}

await main();
