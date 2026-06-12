import "dotenv/config";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import * as schema from "./schema.ts";

const connectionString = process.env.ORDER_DATABASE_URL;

if (!connectionString) {
  throw new Error("ORDER_DATABASE_URL is required for the Order service.");
}

// Order service connects only to the Order database.
const client = postgres(connectionString);

export const orderDb = drizzle(client, { schema });

export function closeOrderDb() {
  return client.end();
}
