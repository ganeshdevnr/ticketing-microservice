import "dotenv/config";
import { defineConfig } from "drizzle-kit";

export default defineConfig({
  schema: "./src/order/schema.ts",
  out: "./drizzle/order",
  dialect: "postgresql",
  dbCredentials: {
    url: process.env.ORDER_DATABASE_URL ?? ""
  }
});
