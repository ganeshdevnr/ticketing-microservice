import { randomUUID } from "node:crypto";
import * as grpc from "@grpc/grpc-js";

export const TRACE_HEADER = "x-trace-id";

export function newTraceId() {
  return randomUUID();
}

export function tracePrefix(traceId: string) {
  return `[trace=${traceId}]`;
}

export function grpcTraceMetadata(traceId: string) {
  const metadata = new grpc.Metadata();
  metadata.set(TRACE_HEADER, traceId);
  return metadata;
}

export function getGrpcTraceId(call: grpc.ServerUnaryCall<unknown, unknown>) {
  const traceId = call.metadata.get(TRACE_HEADER)[0];

  return typeof traceId === "string" ? traceId : "missing-trace-id";
}
