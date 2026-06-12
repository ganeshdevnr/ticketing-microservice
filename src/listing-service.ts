import path from "node:path";
import { fileURLToPath } from "node:url";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { eq } from "drizzle-orm";
import { listingDb } from "./listing/db.ts";
import { events } from "./listing/schema.ts";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROTO_PATH = path.join(__dirname, "../proto/listing.proto");
const SERVER_ADDRESS = "localhost:50051";


// Defining the types for the request and response of the CheckAvailability RPC method.
type CheckAvailabilityRequest = {
  eventId: string;
};

type CheckAvailabilityResponse = {
  available: boolean;
  availableSeats: number;
};

// Defining the type for the loaded gRPC package, which includes the ListingService.
type ListingProto = {
  listing: {
    ListingService: grpc.ServiceClientConstructor;
  };
};

// Construct the package definition from the .proto file.
// The options provided to loadSync ensure that the generated JavaScript code will have properties that match the case of the .proto definitions, and that long integers and enums are represented as strings for easier handling in JavaScript.
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const listingProto = grpc.loadPackageDefinition(packageDefinition) as unknown as ListingProto;

// Server-side implementation of the CheckAvailability RPC from listing.proto.
async function checkAvailability(
  call: grpc.ServerUnaryCall<CheckAvailabilityRequest, CheckAvailabilityResponse>,
  callback: grpc.sendUnaryData<CheckAvailabilityResponse>
) {
  try {
    // Listing service reads only from its own database.
    const event = await listingDb.query.events.findFirst({
      where: eq(events.eventId, call.request.eventId)
    });
    const availableSeats = event?.availableSeats ?? 0;

    callback(null, {
      available: availableSeats > 0,
      availableSeats
    });
  } catch (error) {
    callback(error as Error);
  }
}

// Create a new gRPC server
const server = new grpc.Server();


// Register the ListingService with the server, providing the implementation of CheckAvailability.
// Listing service owns the server side of this contract.
server.addService(listingProto.listing.ListingService.service, {
  CheckAvailability: checkAvailability
});

server.bindAsync(SERVER_ADDRESS, grpc.ServerCredentials.createInsecure(), (error, port) => {
  if (error) {
    console.error("Failed to start Listing service:", error);
    return;
  }

  console.log(`Listing service listening on ${SERVER_ADDRESS}`);
  console.log(`gRPC server bound to port ${port}`);
});
