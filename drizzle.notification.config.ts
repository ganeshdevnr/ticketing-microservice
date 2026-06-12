import "dotenv/config";
import { defineConfig } from "drizzle-kit";

export default defineConfig({
  schema: "./src/notification/schema.ts",
  out: "./drizzle/notification",
  dialect: "postgresql",
  dbCredentials: {
    url: process.env.NOTIFICATION_DATABASE_URL ?? ""
  }
});
