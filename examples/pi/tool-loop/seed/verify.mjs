import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const source = await readFile(new URL("./source.txt", import.meta.url), "utf8");
assert.equal(source, "alpha=7\nbeta=11\n");

const result = await readFile(new URL("./result.txt", import.meta.url), "utf8");
if (process.argv.includes("--precheck")) {
  assert.equal(result, "sum=pending\n");
  console.log("precheck passed");
} else {
  assert.equal(result, "sum=18\n");
  console.log("verification passed");
}
