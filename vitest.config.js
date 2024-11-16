import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    hookTimeout: 30000, // Increase hook timeout to 30 seconds
  },
});