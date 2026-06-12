import { jsonb, pgTable, serial, text, timestamp } from "drizzle-orm/pg-core";

// Order service owns this table in its own Postgres database.
export const orders = pgTable("orders", {
  id: serial("id").primaryKey(),
  eventId: text("event_id").notNull(),
  status: text("status").notNull(),
  createdAt: timestamp("created_at").notNull().defaultNow()
});

export const outbox = pgTable("outbox", {
  id: serial("id").primaryKey(),
  topic: text("topic").notNull(),
  eventType: text("event_type").notNull(),
  payload: jsonb("payload").notNull(),
  createdAt: timestamp("created_at").notNull().defaultNow(),
  publishedAt: timestamp("published_at")
});
