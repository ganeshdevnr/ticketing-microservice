import "dotenv/config";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import * as schema from "./schema.ts";

const connectionString = process.env.NOTIFICATION_DATABASE_URL;

if (!connectionString) {
  throw new Error("NOTIFICATION_DATABASE_URL is required for the Notification service.");
}

// Notification service connects only to the Notification database.
const client = postgres(connectionString);

export const notificationDb = drizzle(client, { schema });

export function closeNotificationDb() {
  return client.end();
}
