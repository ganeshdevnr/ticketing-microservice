import "dotenv/config";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import * as schema from "./schema.ts";

const connectionString = process.env.LISTING_DATABASE_URL;

if (!connectionString) {
  throw new Error("LISTING_DATABASE_URL is required for the Listing service.");
}

// Listing service connects only to the Listing database.
const client = postgres(connectionString);

export const listingDb = drizzle(client, { schema });

export function closeListingDb() {
  return client.end();
}
