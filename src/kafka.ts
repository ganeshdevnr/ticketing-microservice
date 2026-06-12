import "dotenv/config";
import { Kafka } from "kafkajs";

const brokers = process.env.KAFKA_BROKERS?.split(",").map((broker) => broker.trim()).filter(Boolean);

if (!brokers?.length) {
  throw new Error("KAFKA_BROKERS is required, for example: localhost:9092");
}

export const ORDER_CREATED_TOPIC = "order-created";

export const kafka = new Kafka({
  clientId: "ticketing-microservice-learning",
  brokers
});
