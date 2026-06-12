import "dotenv/config";
import { defineConfig } from "drizzle-kit";

export default defineConfig({
  schema: "./src/listing/schema.ts",
  out: "./drizzle/listing",
  dialect: "postgresql",
  dbCredentials: {
    url: process.env.LISTING_DATABASE_URL ?? ""
  }
});
