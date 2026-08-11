import assert from "node:assert/strict";
import test from "node:test";

import { assertMtpPromotion } from "./runtime-qualification.mjs";

function passingEvidence() {
  return {
    mode: "mtp",
    effective_profile: "gemma4_mtp",
    launch_count: 1,
    primary_only_retry_count: 0,
    log_mentions_target: true,
    log_mentions_exact_draft: true,
    log_mentions_speculative: true,
    exact_command: [
      "/app/Contents/MacOS/llama-server",
      "--model", "/models/target.gguf",
      "--reasoning", "off",
      "--spec-draft-model", "/models/draft.gguf",
      "--spec-type", "draft-mtp",
      "--spec-draft-n-max", "4",
    ],
    forced_kill: false,
    process_returncode: 0,
    pid_gone: true,
    port_closed: true,
    nonstream_reasoning_off: true,
    cancellation: { observed_content: true, reasoning_bytes: 0 },
    stream: {
      http_status: 200,
      completion_valid_exact_sequence: true,
      completion_sha256: "42aa8f2ad7c42dc95093d20913842e4b96850c6a115b305e09aa06dbe22273c1",
      reasoning_bytes: 0,
      done: true,
      finish_reason: "stop",
      timings: { draft_n: 148, draft_n_accepted: 147 },
    },
  };
}

test("only one exact MTP launch with positive attempted and accepted draft counters promotes", () => {
  assert.deepEqual(assertMtpPromotion(passingEvidence()), {
    attempted: 148,
    accepted: 147,
  });
});

test("the relocated counter gate does not require the separate persistent-cycle cancellation row", () => {
  const evidence = passingEvidence();
  delete evidence.cancellation;

  assert.deepEqual(assertMtpPromotion(evidence), { attempted: 148, accepted: 147 });
});

test("fallback, primary-only retry, zero attempts, and zero acceptances each block promotion", () => {
  const fallback = passingEvidence();
  fallback.launch_count = 2;
  const primaryRetry = passingEvidence();
  primaryRetry.primary_only_retry_count = 1;
  const zeroAttempts = passingEvidence();
  zeroAttempts.stream.timings.draft_n = 0;
  const zeroAccepted = passingEvidence();
  zeroAccepted.stream.timings.draft_n_accepted = 0;

  for (const [name, evidence] of [
    ["fallback", fallback],
    ["primary-only retry", primaryRetry],
    ["zero attempts", zeroAttempts],
    ["zero acceptances", zeroAccepted],
  ]) {
    assert.throws(() => assertMtpPromotion(evidence), /MTP evidence/, name);
  }
});
