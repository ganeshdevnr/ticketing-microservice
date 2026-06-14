import { context, propagation, SpanKind, SpanStatusCode, trace, type TextMapGetter } from "@opentelemetry/api";
import type { IHeaders } from "kafkajs";
import { kafka, ORDER_CREATED_TOPIC } from "./kafka.ts";
import { notificationDb } from "./notification/db.ts";
import { processedEvents } from "./notification/schema.ts";

type OrderCreatedEvent = {
  id: string;
  traceContext?: Record<string, string>;
  orderId: number;
  eventId: string;
  seats: number;
};

const consumer = kafka.consumer({ groupId: "notification-service" });
const tracer = trace.getTracer("notification-service");

const kafkaHeaderGetter: TextMapGetter<IHeaders> = {
  keys(carrier) {
    return Object.keys(carrier);
  },
  get(carrier, key) {
    const value = carrier[key];

    if (Array.isArray(value)) {
      return value.map((item) => item.toString());
    }

    return value?.toString();
  }
};

await consumer.connect();
await consumer.subscribe({ topic: ORDER_CREATED_TOPIC, fromBeginning: false });

console.log("Notification service listening for OrderCreated events...");

await consumer.run({
  eachMessage: async ({ message }) => {
    if (!message.value) {
      return;
    }

    const messageValue = message.value;
    const parentContext = propagation.extract(context.active(), message.headers ?? {}, kafkaHeaderGetter);

    await context.with(parentContext, async () => {
      await tracer.startActiveSpan(
        `kafka consume ${ORDER_CREATED_TOPIC}`,
        {
          kind: SpanKind.CONSUMER,
          attributes: {
            "messaging.system": "kafka",
            "messaging.destination.name": ORDER_CREATED_TOPIC,
            "messaging.operation.name": "consume"
          }
        },
        async (span) => {
          try {
            const event = JSON.parse(messageValue.toString()) as OrderCreatedEvent;

            await notificationDb.transaction(async (tx) => {
              const [processedEvent] = await tx
                .insert(processedEvents)
                .values({ eventId: event.id })
                .onConflictDoNothing()
                .returning({ eventId: processedEvents.eventId });

              if (!processedEvent) {
                console.log(`duplicate OrderCreated event ${event.id}, ignoring`);
                return;
              }

              console.log(`sending confirmation for order ${event.orderId}`);
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
  }
});
