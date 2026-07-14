import { afterEach } from "vitest";

import "@/index.css";
import { cleanupBrowser } from "./browser";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

afterEach(cleanupBrowser);
