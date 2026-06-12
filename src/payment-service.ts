import path from "node:path";
import { fileURLToPath } from "node:url";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROTO_PATH = path.join(__dirname, "../proto/payment.proto");
const SERVER_ADDRESS = "localhost:50052";

type ChargeRequest = {
  orderId: number;
  amount: number;
};

type ChargeResponse = {
  charged: boolean;
  reason: string;
};

type PaymentProto = {
  payment: {
    PaymentService: grpc.ServiceClientConstructor;
  };
};

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const paymentProto = grpc.loadPackageDefinition(packageDefinition) as unknown as PaymentProto;

async function charge(call: grpc.ServerUnaryCall<ChargeRequest, ChargeResponse>, callback: grpc.sendUnaryData<ChargeResponse>) {
  if (call.request.amount <= 0) {
    console.log(`payment failed for order ${call.request.orderId}`);
    callback(null, { charged: false, reason: "amount must be positive" });
    return;
  }

  console.log(`payment charged for order ${call.request.orderId}`);
  callback(null, { charged: true, reason: "" });
}

const server = new grpc.Server();

server.addService(paymentProto.payment.PaymentService.service, {
  Charge: charge
});

server.bindAsync(SERVER_ADDRESS, grpc.ServerCredentials.createInsecure(), (error, port) => {
  if (error) {
    console.error("Failed to start Payment service:", error);
    return;
  }

  console.log(`Payment service listening on ${SERVER_ADDRESS}`);
  console.log(`gRPC server bound to port ${port}`);
});
