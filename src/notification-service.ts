import { kafka, ORDER_CREATED_TOPIC } from "./kafka.ts";

type OrderCreatedEvent = {
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

    // Notification service has no database in this step; it only reacts to the event.
    console.log(`sending confirmation for order ${event.orderId}`);
  }
});
