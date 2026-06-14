import { context, propagation, SpanKind, SpanStatusCode, trace as otelTrace } from "@opentelemetry/api";
import { asc, eq, isNull } from "drizzle-orm";
import { kafka } from "./kafka.ts";
import { closeOrderDb, orderDb } from "./order/db.ts";
import { outbox } from "./order/schema.ts";

const POLL_INTERVAL_MS = 1000;

type OrderCreatedPayload = {
  id: string;
  traceContext?: Record<string, string>;
  orderId: number;
  eventId: string;
  seats: number;
};

const producer = kafka.producer();
const tracer = otelTrace.getTracer("order-outbox-worker");
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
    const parentContext = propagation.extract(context.active(), payload.traceContext ?? {});

    await context.with(parentContext, async () => {
      await tracer.startActiveSpan(
        `kafka publish ${event.topic}`,
        {
          kind: SpanKind.PRODUCER,
          attributes: {
            "messaging.system": "kafka",
            "messaging.destination.name": event.topic,
            "messaging.operation.name": "publish"
          }
        },
        async (span) => {
          try {
            const headers: Record<string, string> = {};
            propagation.inject(context.active(), headers);

            await producer.send({
              topic: event.topic,
              messages: [
                {
                  key: String(payload.orderId),
                  value: JSON.stringify(payload),
                  headers
                }
              ]
            });
          } catch (error) {
            span.recordException(error as Error);
            span.setStatus({ code: SpanStatusCode.ERROR });
            throw error;
          } finally {
            span.end();
          }
        }
      );
    });

    await orderDb
      .update(outbox)
      .set({ publishedAt: new Date() })
      .where(eq(outbox.id, event.id));

    console.log(`Published ${event.eventType} outbox event ${event.id} for order ${payload.orderId}`);
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
