import { asc, eq, isNull } from "drizzle-orm";
import { kafka } from "./kafka.ts";
import { closeOrderDb, orderDb } from "./order/db.ts";
import { outbox } from "./order/schema.ts";
import { tracePrefix } from "./trace.ts";

const POLL_INTERVAL_MS = 1000;

type OrderCreatedPayload = {
  id: string;
  traceId?: string;
  orderId: number;
  eventId: string;
  seats: number;
};

const producer = kafka.producer();
let shuttingDown = false;

function wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function publishPendingEvents() {
  const events = await orderDb
    .select()
    .from(outbox)
    .where(isNull(outbox.publishedAt))
    .orderBy(asc(outbox.createdAt))
    .limit(10);

  for (const event of events) {
    const payload = event.payload as OrderCreatedPayload;
    const trace = tracePrefix(payload.traceId ?? "missing-trace-id");

    await producer.send({
      topic: event.topic,
      messages: [
        {
          key: String(payload.orderId),
          value: JSON.stringify(payload)
        }
      ]
    });

    await orderDb
      .update(outbox)
      .set({ publishedAt: new Date() })
      .where(eq(outbox.id, event.id));

    console.log(`${trace} Published ${event.eventType} outbox event ${event.id} for order ${payload.orderId}`);
  }
}

async function shutdown() {
  shuttingDown = true;
  await producer.disconnect();
  await closeOrderDb();
}

process.on("SIGINT", () => {
  void shutdown().then(() => process.exit(0));
});

process.on("SIGTERM", () => {
  void shutdown().then(() => process.exit(0));
});

await producer.connect();
console.log("Order outbox worker polling for unpublished events...");

while (!shuttingDown) {
  try {
    await publishPendingEvents();
  } catch (error) {
    console.error("Order outbox worker failed to publish events:", error);
  }

  await wait(POLL_INTERVAL_MS);
}
