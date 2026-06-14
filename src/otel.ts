import "dotenv/config";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { GrpcInstrumentation } from "@opentelemetry/instrumentation-grpc";
import { HttpInstrumentation } from "@opentelemetry/instrumentation-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";

const serviceName = process.env.OTEL_SERVICE_NAME;
const tracesEndpoint = process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT;

if (!serviceName) {
  throw new Error("OTEL_SERVICE_NAME is required.");
}

if (!tracesEndpoint) {
  throw new Error("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT is required.");
}

const sdk = new NodeSDK({
  resource: resourceFromAttributes({
    [ATTR_SERVICE_NAME]: serviceName
  }),
  traceExporter: new OTLPTraceExporter({
    url: tracesEndpoint
  }),
  instrumentations: [new HttpInstrumentation(), new GrpcInstrumentation()]
});

sdk.start();

async function shutdownTracing() {
  await sdk.shutdown();
}

process.once("SIGINT", () => {
  void shutdownTracing().catch((error) => {
    console.error("Failed to shut down OpenTelemetry SDK:", error);
  });
});

process.once("SIGTERM", () => {
  void shutdownTracing().catch((error) => {
    console.error("Failed to shut down OpenTelemetry SDK:", error);
  });
});
