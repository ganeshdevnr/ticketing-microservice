import http from "node:http";
import { createHmac, timingSafeEqual } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { newTraceId, TRACE_HEADER, tracePrefix } from "./trace.ts";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ORDER_PROTO_PATH = path.join(__dirname, "../proto/order.proto");
const SERVER_PORT = Number(process.env.PORT ?? 3000);
const ORDER_SERVICE_ADDRESS = process.env.ORDER_SERVICE_ADDRESS ?? "localhost:50053";
const JWT_SECRET = "dev-gateway-secret";

type PlaceOrderRequest = {
  order_id: number;
};

type PlaceOrderResponse = {
  orderId: number;
  status: string;
};

type OrderServiceClient = grpc.Client & {
  PlaceOrder: (
    request: PlaceOrderRequest,
    metadata: grpc.Metadata,
    callback: grpc.requestCallback<PlaceOrderResponse>
  ) => void;
};

type OrderProto = {
  order: {
    OrderService: new (address: string, credentials: grpc.ChannelCredentials) => OrderServiceClient;
  };
};

const orderProto = grpc.loadPackageDefinition(
  protoLoader.loadSync(ORDER_PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
  })
) as unknown as OrderProto;

const orderClient = new orderProto.order.OrderService(ORDER_SERVICE_ADDRESS, grpc.credentials.createInsecure());

type TokenPayload = {
  userId: string;
};

function base64UrlEncode(value: string | Buffer) {
  return Buffer.from(value).toString("base64url");
}

function base64UrlDecode(value: string) {
  return Buffer.from(value, "base64url").toString("utf8");
}

function sign(value: string) {
  return createHmac("sha256", JWT_SECRET).update(value).digest("base64url");
}

function issueToken(userId: string) {
  const header = base64UrlEncode(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const payload = base64UrlEncode(JSON.stringify({ userId } satisfies TokenPayload));
  const unsignedToken = `${header}.${payload}`;

  return `${unsignedToken}.${sign(unsignedToken)}`;
}

function verifyToken(token: string) {
  const [header, payload, signature] = token.split(".");

  if (!header || !payload || !signature) {
    return null;
  }

  const expectedSignature = sign(`${header}.${payload}`);
  const signatureBuffer = Buffer.from(signature);
  const expectedSignatureBuffer = Buffer.from(expectedSignature);

  if (
    signatureBuffer.length !== expectedSignatureBuffer.length ||
    !timingSafeEqual(signatureBuffer, expectedSignatureBuffer)
  ) {
    return null;
  }

  try {
    const tokenPayload = JSON.parse(base64UrlDecode(payload)) as Partial<TokenPayload>;

    if (typeof tokenPayload.userId !== "string" || tokenPayload.userId.length === 0) {
      return null;
    }

    return tokenPayload.userId;
  } catch {
    return null;
  }
}

function readBody(request: http.IncomingMessage) {
  return new Promise<string>((resolve, reject) => {
    let body = "";

    request.setEncoding("utf8");
    request.on("data", (chunk) => {
      body += chunk;
    });
    request.on("end", () => resolve(body));
    request.on("error", reject);
  });
}

function orderMetadata(userId: string, traceId: string) {
  const metadata = new grpc.Metadata();
  metadata.set("user_id", userId);
  metadata.set(TRACE_HEADER, traceId); // [trace=${traceId}] will be included in logs of the Order service for observability across services

  return metadata;
}

async function forwardToOrder(userId: string, orderId: number, traceId: string) {
  return new Promise<PlaceOrderResponse>((resolve, reject) => {
    orderClient.PlaceOrder({ order_id: orderId }, orderMetadata(userId, traceId), (error, orderResponse) => {
      if (error) {
        reject(error);
        return;
      }

      if (!orderResponse) {
        reject(new Error("Order service returned no place order response."));
        return;
      }

      resolve(orderResponse);
    });
  });
}

const server = http.createServer(async (request, response) => {
  if (request.method === "POST" && request.url === "/login") {
    const body = await readBody(request);

    let login: Partial<TokenPayload> = {};

    try {
      login = body ? (JSON.parse(body) as Partial<TokenPayload>) : {};
    } catch {
      response.writeHead(400, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ error: "invalid JSON" }));
      return;
    }

    const token = issueToken(login.userId ?? "user-123");

    response.writeHead(200, { "Content-Type": "application/json" });
    response.end(JSON.stringify({ token }));
    return;
  }

  if (request.method === "POST" && request.url === "/orders") {
    // Tracing for observability
    const traceId = newTraceId();
    const trace = tracePrefix(traceId);

    const authorization = request.headers.authorization;
    const token = authorization?.startsWith("Bearer ") ? authorization.slice("Bearer ".length) : "";
    const userId = verifyToken(token);

    console.log(`${trace} Gateway received order request`);

    if (!userId) {
      console.error(`${trace} Gateway rejected order request: unauthorized`);
      response.writeHead(401, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ error: "unauthorized" }));
      return;
    }

    const body = await readBody(request);
    let orderRequest: Partial<PlaceOrderRequest> = {};

    try {
      orderRequest = body ? (JSON.parse(body) as Partial<PlaceOrderRequest>) : {};
    } catch {
      response.writeHead(400, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ error: "invalid JSON" }));
      return;
    }

    const orderId = typeof orderRequest.order_id === "number" ? orderRequest.order_id : 0;
    let orderResponse: PlaceOrderResponse;

    try {
      // forward the incoming request to the Order service, including the trace ID for observability across services
      console.log(`${trace} Gateway forwarding order request to Order service`);
      orderResponse = await forwardToOrder(userId, orderId, traceId);
    } catch (error) {
      console.error(`${trace} Failed to forward request to Order service:`, error);

      const status = error instanceof Error && "code" in error && error.code === grpc.status.UNAUTHENTICATED ? 401 : 502;
      const message = status === 401 ? "unauthorized" : "order service unavailable";

      response.writeHead(status, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ error: message }));
      return;
    }

    console.log(`${trace} Gateway received Order service response with status 201`);

    response.writeHead(201, { "Content-Type": "application/json" });
    response.end(JSON.stringify(orderResponse));
    return;
  }

  response.writeHead(404, { "Content-Type": "application/json" });
  response.end(JSON.stringify({ error: "not found" }));
});

server.listen(SERVER_PORT, () => {
  console.log(`Gateway listening on port ${SERVER_PORT}`);
});

process.on("SIGINT", () => {
  orderClient.close();
  process.exit(0);
});

process.on("SIGTERM", () => {
  orderClient.close();
  process.exit(0);
});
