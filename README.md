# Ticketing Microservice

A learning-oriented Node.js microservices project for exploring service ownership, gRPC, Kafka messaging, transactional outbox, idempotent consumers, API gateway routing, OpenID Connect, and distributed tracing.

The project models a small ticket ordering flow:

1. A client calls `POST /orders` through APISIX.
2. APISIX validates a Keycloak access token and forwards the authenticated user id to the Order service.
3. Order creates a pending order in its own database.
4. Order reserves seats through the Listing gRPC service.
5. Order charges payment through the Payment gRPC service.
6. On success, Order stores an `OrderCreated` event in its outbox table.
7. The outbox worker publishes the event to Kafka-compatible Redpanda.
8. Notification consumes the event and records it as processed for idempotency.
9. OpenTelemetry sends connected traces to Jaeger across APISIX, gRPC calls, and Kafka boundaries.

## What This Repo Demonstrates

- Service-owned databases with separate Listing, Order, and Notification Postgres databases.
- Synchronous service-to-service calls with gRPC and `.proto` contracts.
- API gateway routing with Apache APISIX and `grpc-transcode`.
- Bearer-token authentication with APISIX OpenID Connect and Keycloak.
- Saga-style order flow with compensating seat release on payment failure.
- Transactional outbox for reliable event publishing after database commits.
- Idempotent Kafka consumer using a processed-events table.
- Distributed tracing with OpenTelemetry, Jaeger, APISIX, gRPC instrumentation, and manual Kafka context propagation.

## Architecture

```text
Client
  |
  | POST /orders + Bearer token
  v
APISIX gateway
  | validates token with Keycloak
  | grpc-transcode HTTP -> gRPC
  v
Order service ----gRPC----> Listing service ----> listing-db
  |
  | gRPC
  v
Payment service
  |
  | writes order + outbox event
  v
order-db
  |
  | polls unpublished outbox rows
  v
Order outbox worker ----Kafka/Redpanda----> Notification service ----> notification-db
```

## Services

| Service | Responsibility | Storage |
| --- | --- | --- |
| `apisix` | Public gateway, OIDC validation, HTTP-to-gRPC transcoding, tracing | none |
| `keycloak` | Local identity provider for access tokens | `keycloak-db` |
| `listing` | Owns event inventory and seat reservation/release | `listing-db` |
| `order` | Owns order creation, saga orchestration, and outbox writes | `order-db` |
| `order-outbox` | Publishes unpublished Order outbox rows to Kafka | `order-db`, Redpanda |
| `payment` | Simulated payment gRPC service | none |
| `notification` | Consumes `OrderCreated` events and deduplicates them | `notification-db` |
| `redpanda` | Kafka-compatible broker | container storage |
| `jaeger` | Trace collector and UI | in memory |

## Prerequisites

- Docker and Docker Compose.
- Node.js 24 if you want to run services directly with `npm`.
- npm.
- `curl` and `jq` for the example token and order requests.

The Docker path is the easiest way to run the full system.

## Run The Full Stack

```bash
docker compose up --build
```

This starts Postgres databases, Keycloak, APISIX, Jaeger, Redpanda, and all Node services. The `db-setup` container runs Drizzle schema push commands and seeds Listing with:

| Event | Available Seats |
| --- | ---: |
| `event-1` | 12 |
| `event-2` | 0 |
| `event-3` | 5 |

The Order service currently reserves one seat for `event-3` and charges `ORDER_AMOUNT`, which defaults to `100`.

Useful local URLs:

- APISIX gateway: `http://localhost:9080`
- Keycloak: `http://localhost:8080`
- Jaeger UI: `http://localhost:16686`

Keycloak admin credentials in the local compose file are `admin` / `admin`.

## Configure Keycloak For Local Calls

APISIX is configured to validate tokens from:

```text
http://keycloak:8080/realms/ticketing/.well-known/openid-configuration
```

The compose stack starts Keycloak, but it does not import a realm. Before calling `POST /orders`, create:

- Realm: `ticketing`
- Client: `ticketing-app`
- Client authentication: enabled
- Client secret: either use the default in `docker-compose.yml` or set `KEYCLOAK_CLIENT_SECRET`
- User: any test user with a password

For local browser setup, open `http://localhost:8080` and use the admin console.

## Place An Order

After the stack is running and Keycloak has a `ticketing` realm/client/user, request a token from Keycloak:

```bash
TOKEN=$(curl -s \
  -X POST http://localhost:8080/realms/ticketing/protocol/openid-connect/token \
  -H 'content-type: application/x-www-form-urlencoded' \
  -d 'grant_type=password' \
  -d 'client_id=ticketing-app' \
  -d 'client_secret=<client-secret>' \
  -d 'username=<username>' \
  -d 'password=<password>' | jq -r .access_token)
```

Then call APISIX:

```bash
curl -i \
  -X POST http://localhost:9080/orders \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"order_id": 1}'
```

Expected successful response shape:

```json
{
  "orderId": 1,
  "status": "confirmed"
}
```

The stored database order id is generated by the Order service. The request `order_id` field exists because the current `order.proto` contract includes it, but the implementation creates its own order record.

## Observability

Open Jaeger at `http://localhost:16686`.

Useful services to search for:

- `apisix-gateway`
- `order-service`
- `listing-service`
- `payment-service`
- `order-outbox-worker`
- `notification-service`

The Node services initialize tracing through `src/otel.ts`. Runtime commands preload it with:

```bash
node --experimental-loader=@opentelemetry/instrumentation/hook.mjs --import ./dist/src/otel.js ...
```

That loader/import combination is important because this repo uses ESM with TypeScript `NodeNext`. HTTP and gRPC spans are auto-instrumented. Kafka spans are created manually where context crosses the async message boundary:

- `src/order-service.ts` captures the current trace context into the outbox payload.
- `src/order-outbox-worker.ts` extracts that context, creates a producer span, and injects Kafka headers.
- `src/notification-service.ts` extracts Kafka headers and creates a consumer span.

Only traces are exported. Logs and metrics exporters are disabled in `src/otel.ts`.

## Development Commands

Install dependencies:

```bash
npm ci
```

Typecheck:

```bash
npm run typecheck
```

Build:

```bash
npm run build
```

Validate Docker Compose config:

```bash
docker compose config
```

Run database schema push for all service databases:

```bash
npm run db:push
```

Seed Listing data:

```bash
npm run db:seed:listing
```

Generate Drizzle migrations:

```bash
npm run db:generate
```

## Running Services Directly

Direct `npm` service scripts are useful for local debugging, but they require supporting infrastructure and environment variables.

Example environment variables are in `.env.example`:

```bash
cp .env.example .env
```

For tracing, each service also needs:

```bash
OTEL_SERVICE_NAME=<service-name>
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
```

Example:

```bash
OTEL_SERVICE_NAME=listing-service \
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces \
npm run listing
```

The Docker Compose setup already provides these values inside containers.

## gRPC Contracts

The `.proto` files are in `proto/`:

- `proto/order.proto`: `OrderService.PlaceOrder`
- `proto/listing.proto`: `ListingService.CheckAvailability`, `ReserveSeats`, `ReleaseSeats`
- `proto/payment.proto`: `PaymentService.Charge`

The public HTTP route is defined in `apisix/apisix.yaml`:

- `POST /orders` maps to `order.OrderService.PlaceOrder`
- APISIX uses `grpc-transcode` to translate JSON HTTP requests into gRPC calls
- APISIX uses `openid-connect` to require a bearer token
- APISIX uses `serverless-pre-function` to derive `X-User-ID` from Keycloak user info

## Data Patterns

### Service-Owned Databases

Each stateful service owns its own schema and connection config:

- Listing: `src/listing/schema.ts`, `drizzle.listing.config.ts`
- Order: `src/order/schema.ts`, `drizzle.order.config.ts`
- Notification: `src/notification/schema.ts`, `drizzle.notification.config.ts`

Services do not read each other's databases.

### Saga And Compensation

`src/order-service.ts` orchestrates the order flow. If payment fails after seats are reserved, it calls Listing to release those seats and marks the order as `failed_payment`.

### Transactional Outbox

When payment succeeds, Order updates the order status and inserts an outbox row in one database transaction. `src/order-outbox-worker.ts` polls unpublished rows, publishes them to `order-created`, then marks them as published.

### Idempotent Consumer

`src/notification-service.ts` inserts the event id into a `processed_events` table with `onConflictDoNothing()`. Duplicate Kafka deliveries are ignored.

## Branch Learning Path

This repo also has branches that represent incremental learning steps:

- `01-outbox`
- `02-inbox-pattern`
- `03-the-saga`
- `04-gateway`
- `05-packaging`
- `06-gateway-to-service-grpc`
- `07-keyclock`
- `07-routing-table`
- `08-apisix`
- `09-opentelemetry`
- `kafka-messaging`

Use them to compare how the project evolved from simpler service boundaries toward gateway, authentication, messaging, and tracing.

## Reset Local State

To stop containers:

```bash
docker compose down
```

To remove service databases and start from a clean seed:

```bash
docker compose down -v
docker compose up --build
```

## Notes And Limitations

- This is a learning project, not a production-ready ticketing platform.
- Payment is simulated and accepts positive amounts.
- Keycloak realm/client/user setup is manual unless you add a realm import file.
- Jaeger uses in-memory storage in this compose setup.
- Redpanda data is not persisted with a named volume in the current compose file.
