import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react(), tailwindcss()],
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
      expect: {
        toMatchScreenshot: {
          resolveScreenshotPath: ({ arg, browserName, ext, platform, root, testFileDirectory }) => {
            if (!new Set(["darwin", "linux", "win32"]).has(platform)) {
              throw new Error(`Unsupported screenshot platform: ${platform}`);
            }
            const baselineDirectory = arg.startsWith("baseline-shell-") ? resolve(root, "src/test") : testFileDirectory;
            return resolve(root, baselineDirectory, "__screenshots__", "shared", browserName, `${arg}${ext}`);
          },
        },
      },
    },
  },
});
