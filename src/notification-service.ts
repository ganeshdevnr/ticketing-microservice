import { kafka, ORDER_CREATED_TOPIC } from "./kafka.ts";
import { notificationDb } from "./notification/db.ts";
import { processedEvents } from "./notification/schema.ts";
import { tracePrefix } from "./trace.ts";

type OrderCreatedEvent = {
  id: string;
  traceId?: string;
  orderId: number;
  eventId: string;
  seats: number;
};

const consumer = kafka.consumer({ groupId: "notification-service" });

await consumer.connect();
await consumer.subscribe({ topic: ORDER_CREATED_TOPIC, fromBeginning: false });

console.log("Notification service listening for OrderCreated events...");

await consumer.run({
  eachMessage: async ({ message }) => {
    if (!message.value) {
      return;
    }

    const event = JSON.parse(message.value.toString()) as OrderCreatedEvent;
    const trace = tracePrefix(event.traceId ?? "missing-trace-id");

    await notificationDb.transaction(async (tx) => {
      const [processedEvent] = await tx
        .insert(processedEvents)
        .values({ eventId: event.id })
        .onConflictDoNothing()
        .returning({ eventId: processedEvents.eventId });

      if (!processedEvent) {
        console.log(`${trace} duplicate OrderCreated event ${event.id}, ignoring`);
        return;
      }

      console.log(`${trace} sending confirmation for order ${event.orderId}`);
    });
  }
});
