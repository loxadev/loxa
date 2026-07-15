import { fileURLToPath } from "node:url";

import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  optimizeDeps: {
    include: ["class-variance-authority", "clsx", "lucide-react", "tailwind-merge"],
  },
  resolve: {
    alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) },
  },
  test: {
    env: {
      VITE_LOXA_CSP_PROBE: "1",
      VITE_LOXA_CSP_PROBE_CASE: "early-blocked-image",
    },
    include: ["src/**/*.browser.test.{ts,tsx}"],
    setupFiles: ["./src/test/browser.setup.tsx"],
    browser: {
      enabled: true,
      headless: true,
      provider: playwright({ contextOptions: { locale: "en-US", timezoneId: "UTC" } }),
      instances: [{ browser: "chromium" }],
    },
  },
});
