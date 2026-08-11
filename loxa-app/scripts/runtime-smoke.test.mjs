import assert from "node:assert/strict";
import { resolve } from "node:path";
import test from "node:test";

import { smokeFinalRuntime } from "./runtime-smoke.mjs";

const app = process.env.LOXA_BUILT_APP;
const model = process.env.LOXA_SMALL_MODEL;

test(
  "the final app runtime serves the exact small fixture and leaves no process or listener",
  { skip: !app || !model },
  async () => {
    const result = await smokeFinalRuntime(resolve(app), resolve(model));

    assert.equal(result.runtimeBuild, "b10344");
    assert.equal(result.modelSha256, "741ad12b64088fedc17c33aacb22e48be1972ef36a39f03666dd68bd15614fb9");
    assert.equal(result.modelSize, 88202080);
    assert.deepEqual(result.modelIds, ["loxa-runtime-smoke"]);
    assert.equal(result.ready, true);
    assert.equal(result.forcedKill, false);
    assert.equal(result.processGone, true);
    assert.equal(result.listenerClosed, true);
    assert.deepEqual(result.homeEntries, []);
  },
);
