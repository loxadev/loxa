import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) },
  },
  test: {
    include: ["src/**/*.browser.test.{ts,tsx}"],
    setupFiles: ["./src/test/browser.setup.tsx"],
    browser: {
      enabled: true,
      headless: true,
      provider: playwright(),
      instances: [{ browser: "chromium" }],
      expect: {
        toMatchScreenshot: {
          resolveScreenshotPath: ({ arg, browserName, ext, platform, root, testFileDirectory }) => {
            if (!new Set(["darwin", "linux", "win32"]).has(platform)) {
              throw new Error(`Unsupported screenshot platform: ${platform}`);
            }
            return resolve(root, testFileDirectory, "__screenshots__", "shared", browserName, `${arg}${ext}`);
          },
        },
      },
    },
  },
});
