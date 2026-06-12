import { pgTable, serial, text, timestamp } from "drizzle-orm/pg-core";

// Order service owns this table in its own Postgres database.
export const orders = pgTable("orders", {
  id: serial("id").primaryKey(),
  eventId: text("event_id").notNull(),
  status: text("status").notNull(),
  createdAt: timestamp("created_at").notNull().defaultNow()
});
