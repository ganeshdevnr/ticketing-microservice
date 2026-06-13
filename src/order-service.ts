import "dotenv/config";
import { randomUUID } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { eq } from "drizzle-orm";
import { closeOrderDb, orderDb } from "./order/db.ts";
import { orders, outbox } from "./order/schema.ts";
import { getGrpcTraceId, grpcTraceMetadata, tracePrefix } from "./trace.ts";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ORDER_PROTO_PATH = path.join(__dirname, "../proto/order.proto");
const LISTING_PROTO_PATH = path.join(__dirname, "../proto/listing.proto");
const PAYMENT_PROTO_PATH = path.join(__dirname, "../proto/payment.proto");
const ORDER_SERVER_ADDRESS = process.env.ORDER_GRPC_BIND_ADDRESS ?? "0.0.0.0:50053";
const LISTING_SERVICE_ADDRESS = process.env.LISTING_SERVICE_ADDRESS ?? "localhost:50051";
const PAYMENT_SERVICE_ADDRESS = process.env.PAYMENT_SERVICE_ADDRESS ?? "localhost:50052";
const EVENT_ID_TO_CHECK = "event-3";
const SEATS_TO_RESERVE = 1;
const ORDER_AMOUNT = Number(process.env.ORDER_AMOUNT ?? 100);
const ORDER_CREATED_TOPIC = "order-created";

type ReserveSeatsRequest = {
  eventId: string;
  seats: number;
};

type ReserveSeatsResponse = {
  reserved: boolean;
  availableSeats: number;
};

type ReleaseSeatsRequest = {
  eventId: string;
  seats: number;
};

type ReleaseSeatsResponse = {
  released: boolean;
  availableSeats: number;
};

type ChargeRequest = {
  orderId: number;
  amount: number;
};

type ChargeResponse = {
  charged: boolean;
  reason: string;
};

type PlaceOrderRequest = {
  order_id: number;
};

type PlaceOrderResponse = {
  orderId: number;
  status: string;
};

type ListingServiceClient = grpc.Client & {
  ReserveSeats: (
    request: ReserveSeatsRequest,
    metadata: grpc.Metadata,
    callback: grpc.requestCallback<ReserveSeatsResponse>
  ) => void;
  ReleaseSeats: (
    request: ReleaseSeatsRequest,
    metadata: grpc.Metadata,
    callback: grpc.requestCallback<ReleaseSeatsResponse>
  ) => void;
};

type PaymentServiceClient = grpc.Client & {
  Charge: (request: ChargeRequest, metadata: grpc.Metadata, callback: grpc.requestCallback<ChargeResponse>) => void;
};

type OrderProto = {
  order: {
    OrderService: grpc.ServiceClientConstructor;
  };
};

type ListingProto = {
  listing: {
    ListingService: new (address: string, credentials: grpc.ChannelCredentials) => ListingServiceClient;
  };
};

type PaymentProto = {
  payment: {
    PaymentService: new (address: string, credentials: grpc.ChannelCredentials) => PaymentServiceClient;
  };
};

const protoLoaderOptions = {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
};

const orderProto = grpc.loadPackageDefinition(
  protoLoader.loadSync(ORDER_PROTO_PATH, protoLoaderOptions)
) as unknown as OrderProto;

const listingProto = grpc.loadPackageDefinition(
  protoLoader.loadSync(LISTING_PROTO_PATH, protoLoaderOptions)
) as unknown as ListingProto;
const paymentProto = grpc.loadPackageDefinition(
  protoLoader.loadSync(PAYMENT_PROTO_PATH, protoLoaderOptions)
) as unknown as PaymentProto;

const listingClient = new listingProto.listing.ListingService(
  LISTING_SERVICE_ADDRESS,
  grpc.credentials.createInsecure()
);
const paymentClient = new paymentProto.payment.PaymentService(
  PAYMENT_SERVICE_ADDRESS,
  grpc.credentials.createInsecure()
);

function reserveSeats(request: ReserveSeatsRequest, traceId: string) {
  return new Promise<ReserveSeatsResponse>((resolve, reject) => {
    listingClient.ReserveSeats(request, grpcTraceMetadata(traceId), (error, response) => {
      if (error) {
        reject(error);
        return;
      }

      if (!response) {
        reject(new Error("Listing service returned no reserve response."));
        return;
      }

      resolve(response);
    });
  });
}

function releaseSeats(request: ReleaseSeatsRequest, traceId: string) {
  return new Promise<ReleaseSeatsResponse>((resolve, reject) => {
    listingClient.ReleaseSeats(request, grpcTraceMetadata(traceId), (error, response) => {
      if (error) {
        reject(error);
        return;
      }

      if (!response) {
        reject(new Error("Listing service returned no release response."));
        return;
      }

      resolve(response);
    });
  });
}

function charge(request: ChargeRequest, traceId: string) {
  return new Promise<ChargeResponse>((resolve, reject) => {
    paymentClient.Charge(request, grpcTraceMetadata(traceId), (error, response) => {
      if (error) {
        reject(error);
        return;
      }

      if (!response) {
        reject(new Error("Payment service returned no charge response."));
        return;
      }

      resolve(response);
    });
  });
}

async function createOrder(userId: string, traceId: string) {
  const trace = tracePrefix(traceId);

  const [order] = await orderDb
    .insert(orders)
    .values({
      eventId: EVENT_ID_TO_CHECK,
      status: "pending"
    })
    .returning({ id: orders.id });

  if (!order) {
    throw new Error("Order service did not receive a stored order response.");
  }

  console.log(`${trace} Order request accepted for user ${userId}`);
  console.log(`${trace} Order stored in Order service database with id: ${order.id}`);
  console.log(`${trace} Order status: pending`);

  const reserve = await reserveSeats({ eventId: EVENT_ID_TO_CHECK, seats: SEATS_TO_RESERVE }, traceId);

  if (!reserve.reserved) {
    await orderDb.update(orders).set({ status: "failed_no_seats" }).where(eq(orders.id, order.id));
    console.log(`${trace} failed to reserve seats for event ${EVENT_ID_TO_CHECK}`);
    console.log(`${trace} Order status: failed_no_seats`);
    return { orderId: order.id, status: "failed_no_seats" };
  } else {
    console.log(`${trace} reserved seats for event ${EVENT_ID_TO_CHECK}`);

    const payment = await charge({ orderId: order.id, amount: ORDER_AMOUNT }, traceId);

    if (payment.charged) {
      await orderDb.transaction(async (tx) => {
        await tx.update(orders).set({ status: "confirmed" }).where(eq(orders.id, order.id));
        await tx.insert(outbox).values({
          topic: ORDER_CREATED_TOPIC,
          eventType: "OrderCreated",
          payload: {
            id: randomUUID(),
            traceId,
            orderId: order.id,
            eventId: EVENT_ID_TO_CHECK,
            seats: SEATS_TO_RESERVE
          }
        });
      });

      console.log(`${trace} payment charged`);
      console.log(`${trace} Order status: confirmed`);
      console.log(`${trace} Stored OrderCreated outbox event for order ${order.id}`);
      return { orderId: order.id, status: "confirmed" };
    } else {
      console.log(`${trace} payment failed`);

      try {
        const release = await releaseSeats({ eventId: EVENT_ID_TO_CHECK, seats: SEATS_TO_RESERVE }, traceId);

        if (release.released) {
          console.log(`${trace} released seats for event ${EVENT_ID_TO_CHECK}`);
        } else {
          console.error(`${trace} failed to release seats for event ${EVENT_ID_TO_CHECK}`);
        }
      } catch (releaseError) {
        console.error(`${trace} failed to release seats for event ${EVENT_ID_TO_CHECK}:`, releaseError);
      }

      await orderDb.update(orders).set({ status: "failed_payment" }).where(eq(orders.id, order.id));
      console.log(`${trace} Order status: failed_payment`);
      return { orderId: order.id, status: "failed_payment" };
    }
  }
}

async function placeOrder(
  call: grpc.ServerUnaryCall<PlaceOrderRequest, PlaceOrderResponse>,
  callback: grpc.sendUnaryData<PlaceOrderResponse>
) {
  const userId = call.metadata.get("user_id")[0];
  const traceId = getGrpcTraceId(call);
  const trace = tracePrefix(traceId);

  if (typeof userId !== "string" || userId.length === 0) {
    console.error(`${trace} Rejected order request: missing X-User-ID header`);
    callback({ code: grpc.status.UNAUTHENTICATED, message: "missing X-User-ID" });
    return;
  }

  try {
    const result = await createOrder(userId, traceId);

    callback(null, result);
  } catch (error) {
    console.error(`${trace} Order saga failed:`, error);
    callback({ code: grpc.status.INTERNAL, message: "order failed" });
  }
}

const server = new grpc.Server();

server.addService(orderProto.order.OrderService.service, {
  PlaceOrder: placeOrder
});

server.bindAsync(ORDER_SERVER_ADDRESS, grpc.ServerCredentials.createInsecure(), (error, port) => {
  if (error) {
    console.error("Failed to start Order service:", error);
    return;
  }

  console.log(`Order service listening on ${ORDER_SERVER_ADDRESS}`);
  console.log(`gRPC server bound to port ${port}`);
});

async function shutdown() {
  await new Promise<void>((resolve) => {
    server.tryShutdown(() => resolve());
  });
  listingClient.close();
  paymentClient.close();
  await closeOrderDb();
}

process.on("SIGINT", () => {
  void shutdown().finally(() => process.exit(0));
});

process.on("SIGTERM", () => {
  void shutdown().finally(() => process.exit(0));
});
