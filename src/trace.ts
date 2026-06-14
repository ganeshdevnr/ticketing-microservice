import { randomUUID } from "node:crypto";
import * as grpc from "@grpc/grpc-js";
import {
  context,
  propagation,
  SpanKind,
  SpanStatusCode,
  trace as otelTrace,
  type Context,
  type TextMapGetter,
  type TextMapSetter
} from "@opentelemetry/api";

export const TRACE_HEADER = "x-trace-id";

export function newTraceId() {
  return randomUUID();
}

export function tracePrefix(traceId: string) {
  return `[trace=${traceId}]`;
}

const grpcMetadataGetter: TextMapGetter<grpc.Metadata> = {
  keys(metadata) {
    return Object.keys(metadata.getMap());
  },
  get(metadata, key) {
    const values = metadata.get(key);

    return values.map((value) => value.toString());
  }
};

const grpcMetadataSetter: TextMapSetter<grpc.Metadata> = {
  set(metadata, key, value) {
    metadata.set(key, value);
  }
};

const tracer = otelTrace.getTracer("ticketing-microservice");

export async function runInSpan<T>(name: string, kind: SpanKind, callback: () => Promise<T>, parentContext: Context = context.active()) {
  return context.with(parentContext, async () => {
    return tracer.startActiveSpan(name, { kind }, async (span) => {
      try {
        return await callback();
      } catch (error) {
        span.recordException(error as Error);
        span.setStatus({ code: SpanStatusCode.ERROR });
        throw error;
      } finally {
        span.end();
      }
    });
  });
}

export function getGrpcPropagationContext(metadata: grpc.Metadata) {
  return propagation.extract(context.active(), metadata, grpcMetadataGetter);
}

export function grpcTraceMetadata(traceId: string) {
  const metadata = new grpc.Metadata();
  metadata.set(TRACE_HEADER, traceId);
  propagation.inject(context.active(), metadata, grpcMetadataSetter);
  return metadata;
}

export function getGrpcTraceId(call: grpc.ServerUnaryCall<unknown, unknown>) {
  const traceId = call.metadata.get(TRACE_HEADER)[0];

  return typeof traceId === "string" ? traceId : "missing-trace-id";
}
