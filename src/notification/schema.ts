import { pgTable, text, timestamp } from "drizzle-orm/pg-core";

// Notification service owns this table in its own Postgres database.
export const processedEvents = pgTable("processed_events", {
  eventId: text("event_id").primaryKey(),
  processedAt: timestamp("processed_at").notNull().defaultNow()
});
