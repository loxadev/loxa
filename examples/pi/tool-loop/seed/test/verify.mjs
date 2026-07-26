import assert from "node:assert/strict";

import { mergeRanges } from "../src/merge-ranges.mjs";

const failures = [];

function check(name, body) {
  try {
    body();
  } catch {
    failures.push(name);
  }
}

check("merge", () => {
  assert.deepEqual(
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
});

check("immutability", () => {
  const input = [
    [5, 5],
    [-2, 0],
    [3, 4],
  ];
  const snapshot = structuredClone(input);

  assert.deepEqual(mergeRanges(input), [
    [-2, 0],
    [3, 5],
  ]);
  assert.deepEqual(input, snapshot);
});

check("empty", () => {
  assert.deepEqual(mergeRanges([]), []);
});

check("validation", () => {
  assert.throws(() => mergeRanges([[3, 2]]), /range/i);
  assert.throws(() => mergeRanges([[1, 2.5]]), /integer/i);
  assert.throws(() => mergeRanges("not an array"), /array/i);
});

if (failures.length > 0) {
  console.error(`FAIL ${failures.join(",")}`);
  process.exitCode = 1;
} else {
  console.log("PASS 4 checks");
}
