import { sql } from "drizzle-orm";
import { closeListingDb, listingDb } from "./db.ts";
import { events } from "./schema.ts";

// Seed data belongs to the Listing service database only.
try {
  await listingDb
    .insert(events)
    .values([
      { eventId: "event-1", availableSeats: 12 },
      { eventId: "event-2", availableSeats: 0 },
      { eventId: "event-3", availableSeats: 5 }
    ])
    .onConflictDoUpdate({
      target: events.eventId,
      set: {
        availableSeats: sql`excluded.available_seats`,
        reservedSeats: 0
      }
    });

  console.log("Seeded Listing service events.");
} finally {
  await closeListingDb();
}
