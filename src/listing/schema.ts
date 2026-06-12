import { integer, pgTable, text } from "drizzle-orm/pg-core";

// Listing service owns this table in its own Postgres database.
export const events = pgTable("events", {
  eventId: text("event_id").primaryKey(),
  availableSeats: integer("available_seats").notNull(),
  reservedSeats: integer("reserved_seats").notNull().default(0)
});
