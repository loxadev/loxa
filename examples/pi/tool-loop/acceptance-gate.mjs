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
  let pending;
  let failed = false;
  const gate = async ({ toolCallId, toolName, input } = {}) => {
    const expected = STEPS[step];
    if (
      failed ||
      pending !== undefined ||
      expected === undefined ||
      typeof toolCallId !== "string" ||
      toolCallId.length === 0 ||
      toolName !== expected.toolName ||
      !matchesExactly(input, expected.input)
    ) {
      return BLOCKED;
    }
    pending = { toolCallId, toolName, input: { ...input } };
    return undefined;
  };
  gate.toolResult = async ({ toolCallId, toolName, input, isError } = {}) => {
    if (
      pending === undefined ||
      isError !== false ||
      toolCallId !== pending.toolCallId ||
      toolName !== pending.toolName ||
      !matchesExactly(input, pending.input)
    ) {
      failed = true;
      pending = undefined;
      return { isError: true };
    }
    pending = undefined;
    step += 1;
    return undefined;
  };
  return gate;
}

export default function acceptanceGate(pi) {
  const gate = createAcceptanceGate();
  pi.on("tool_call", gate);
  pi.on("tool_result", gate.toolResult);
}
