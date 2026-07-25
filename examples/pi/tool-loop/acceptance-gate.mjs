const BLOCKED = {
  block: true,
  reason: "Pi acceptance tool call is not the next exact step.",
};

const STEPS = [
  { toolName: "read", input: { path: "source.txt" } },
  { toolName: "bash", input: { command: "node verify.mjs --precheck" } },
  {
    toolName: "write",
    input: { path: "result.txt", content: "sum=18\n" },
  },
  { toolName: "bash", input: { command: "node verify.mjs" } },
];

function matchesExactly(actual, expected) {
  if (actual === null || typeof actual !== "object" || Array.isArray(actual)) {
    return false;
  }
  const actualKeys = Object.keys(actual);
  const expectedKeys = Object.keys(expected);
  return (
    actualKeys.length === expectedKeys.length &&
    expectedKeys.every(
      (key) =>
        Object.hasOwn(actual, key) &&
        actual[key] === expected[key],
    )
  );
}

export function createAcceptanceGate() {
  let step = 0;
  return async ({ toolName, input } = {}) => {
    const expected = STEPS[step];
    if (
      expected === undefined ||
      toolName !== expected.toolName ||
      !matchesExactly(input, expected.input)
    ) {
      return BLOCKED;
    }
    step += 1;
    return undefined;
  };
}

export default function acceptanceGate(pi) {
  const gate = createAcceptanceGate();
  pi.on("tool_call", gate);
}
