import { randomUUID } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { closeOrderDb, orderDb } from "./order/db.ts";
import { orders, outbox } from "./order/schema.ts";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROTO_PATH = path.join(__dirname, "../proto/listing.proto");
const LISTING_SERVICE_ADDRESS = "localhost:50051";
const EVENT_ID_TO_CHECK = "event-1";
const ORDER_CREATED_TOPIC = "order-created";

type CheckAvailabilityRequest = {
  eventId: string;
};

type CheckAvailabilityResponse = {
  available: boolean;
  availableSeats: number;
};

type ListingServiceClient = grpc.Client & {
  CheckAvailability: (
    request: CheckAvailabilityRequest,
    callback: grpc.requestCallback<CheckAvailabilityResponse>
  ) => void;
};

type ListingProto = {
  listing: {
    ListingService: new (address: string, credentials: grpc.ChannelCredentials) => ListingServiceClient;
  };
};

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const listingProto = grpc.loadPackageDefinition(packageDefinition) as unknown as ListingProto;

// Order service owns the client side of the ListingService contract.
const listingClient = new listingProto.listing.ListingService(
  LISTING_SERVICE_ADDRESS,
  grpc.credentials.createInsecure()
);

// Client-side call to the CheckAvailability RPC implemented by Listing service.
listingClient.CheckAvailability({ eventId: EVENT_ID_TO_CHECK }, async (error, response) => {
  try {
    if (error) {
      console.error("Order service failed to check availability:", error.message);
      return;
    }

    if (!response) {
      console.error("Order service received no availability response.");
      return;
    }

    console.log("Availability response:");
    console.log(`eventId: ${EVENT_ID_TO_CHECK}`);
    console.log(`available: ${response.available}`);
    console.log(`availableSeats: ${response.availableSeats}`);

    const status = response.available ? "created" : "rejected_no_seats";

    const order = await orderDb.transaction(async (tx) => {
      // Order service writes the order and event atomically to its own database.
      const [storedOrder] = await tx
        .insert(orders)
        .values({
          eventId: EVENT_ID_TO_CHECK,
          status
        })
        .returning({ id: orders.id, status: orders.status });

      if (!storedOrder) {
        throw new Error("Order service did not receive a stored order response.");
      }

      await tx.insert(outbox).values({
        topic: ORDER_CREATED_TOPIC,
        eventType: "OrderCreated",
        payload: {
          id: randomUUID(),
          orderId: storedOrder.id,
          eventId: EVENT_ID_TO_CHECK,
          seats: response.availableSeats
        }
      });

      return storedOrder;
    });

    console.log(`Order stored in Order service database with id: ${order.id}`);
    console.log(`Order status: ${order.status}`);
    console.log(`Stored OrderCreated outbox event for order ${order.id}`);
  } catch (dbError) {
    console.error("Order service failed:", dbError);
  } finally {
    await closeOrderDb();
    listingClient.close();
  }
});
