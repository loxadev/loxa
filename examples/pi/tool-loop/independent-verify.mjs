import assert from "node:assert/strict";
import { writeSync } from "node:fs";
import { pathToFileURL } from "node:url";
import path from "node:path";

const deepEqual = assert.deepEqual.bind(assert);
const throws = assert.throws.bind(assert);
const writeSuccess = writeSync.bind(undefined, 1);
const sourceUrl = pathToFileURL(
  path.resolve("src/merge-ranges.mjs"),
).href;

let challenge = "";
process.stdin.setEncoding("utf8");
for await (const chunk of process.stdin) {
  challenge += chunk;
}
if (!/^[0-9a-f]{64}\n$/.test(challenge)) {
  throw new Error("independent verification challenge is invalid");
}
challenge = challenge.slice(0, -1);

const { mergeRanges } = await import(sourceUrl);

deepEqual(
  mergeRanges([
    [8, 10],
    [1, 3],
    [2, 6],
    [11, 12],
    [20, 21],
  ]),
  [
    [1, 6],
    [8, 12],
    [20, 21],
  ],
);

const input = [
  [5, 5],
  [-2, 0],
  [3, 4],
];
deepEqual(mergeRanges(input), [
  [-2, 0],
  [3, 5],
]);
deepEqual(input, [
  [5, 5],
  [-2, 0],
  [3, 4],
]);
deepEqual(mergeRanges([]), []);
throws(() => mergeRanges([[3, 2]]), /range/i);
throws(() => mergeRanges([[1, 2.5]]), /integer/i);
throws(() => mergeRanges("not an array"), /array/i);

writeSuccess(`LOXA_PI_ACCEPTANCE_PASS ${challenge}\n`);
