import http from "node:http";
import { createHmac, timingSafeEqual } from "node:crypto";
import { newTraceId, TRACE_HEADER, tracePrefix } from "./trace.ts";

const SERVER_PORT = Number(process.env.PORT ?? 3000);
const ORDER_SERVICE_URL = process.env.ORDER_SERVICE_URL ?? "http://localhost:3001/orders";
const JWT_SECRET = "dev-gateway-secret";

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

async function forwardToOrder(userId: string, body: string, traceId: string) {
  return fetch(ORDER_SERVICE_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-User-ID": userId,
      [TRACE_HEADER]: traceId // [trace=${traceId}] will be included in logs of the Order service for observability across services
    },
    body
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

    let orderResponse: Response;

    try {

      // forward the incoming request to the Order service, including the trace ID for observability across services
      console.log(`${trace} Gateway forwarding order request to Order service`);
      orderResponse = await forwardToOrder(userId, body, traceId);
    } catch (error) {
      console.error(`${trace} Failed to forward request to Order service:`, error);
      response.writeHead(502, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ error: "order service unavailable" }));
      return;
    }

    const orderBody = await orderResponse.text();

    console.log(`${trace} Gateway received Order service response with status ${orderResponse.status}`);

    response.writeHead(orderResponse.status, { "Content-Type": "application/json" });
    response.end(orderBody);
    return;
  }

  response.writeHead(404, { "Content-Type": "application/json" });
  response.end(JSON.stringify({ error: "not found" }));
});

server.listen(SERVER_PORT, () => {
  console.log(`Gateway listening on port ${SERVER_PORT}`);
});
