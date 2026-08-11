import { lstat, readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const COMPLETION_SHA256 = "42aa8f2ad7c42dc95093d20913842e4b96850c6a115b305e09aa06dbe22273c1";
const MAX_EVIDENCE_BYTES = 256 * 1024;

function invariant(condition, detail) {
  if (!condition) throw new Error(`MTP evidence ${detail}`);
}

function exactOption(command, option, value) {
  const positions = [];
  for (let index = 0; index < command.length; index += 1) {
    if (command[index] === option) positions.push(index);
  }
  return positions.length === 1 && command[positions[0] + 1] === value;
}

export function assertMtpPromotion(evidence) {
  invariant(evidence && typeof evidence === "object" && !Array.isArray(evidence), "must be an object");
  invariant(evidence.mode === "mtp", "must report MTP mode");
  invariant(evidence.effective_profile === "gemma4_mtp", "must report the exact effective profile");
  invariant(evidence.launch_count === 1, "must contain exactly one launch and no fallback");
  invariant(evidence.primary_only_retry_count === 0, "must contain no primary-only retry");
  invariant(evidence.log_mentions_target === true, "must identify the exact target");
  invariant(evidence.log_mentions_exact_draft === true, "must identify the exact draft");
  invariant(evidence.log_mentions_speculative === true, "must identify speculative decoding");
  invariant(Array.isArray(evidence.exact_command), "must contain the exact command");
  invariant(exactOption(evidence.exact_command, "--reasoning", "off"), "must disable reasoning exactly once");
  invariant(exactOption(evidence.exact_command, "--spec-type", "draft-mtp"), "must select draft-mtp exactly once");
  invariant(exactOption(evidence.exact_command, "--spec-draft-n-max", "4"), "must freeze the draft width exactly once");
  invariant(
    evidence.exact_command.filter((value) => value === "--spec-draft-model").length === 1,
    "must load one exact draft model",
  );
  invariant(evidence.forced_kill === false, "must not require SIGKILL");
  invariant(evidence.process_returncode === 0, "must exit cleanly");
  invariant(evidence.pid_gone === true, "must leave no process");
  invariant(evidence.port_closed === true, "must leave no listener");
  invariant(evidence.nonstream_reasoning_off === true, "must preserve non-stream reasoning-off framing");
  if (Object.hasOwn(evidence, "cancellation")) {
    invariant(evidence.cancellation?.observed_content === true, "must observe cancellable content");
    invariant(evidence.cancellation?.reasoning_bytes === 0, "must preserve cancellation reasoning-off framing");
  }

  const stream = evidence.stream;
  invariant(stream && typeof stream === "object", "must contain streaming evidence");
  invariant(stream.http_status === 200, "must contain a successful streamed response");
  invariant(stream.completion_valid_exact_sequence === true, "must contain the deterministic completion");
  invariant(stream.completion_sha256 === COMPLETION_SHA256, "must contain the deterministic completion digest");
  invariant(stream.reasoning_bytes === 0, "must preserve stream reasoning-off framing");
  invariant(stream.done === true && stream.finish_reason === "stop", "must finish normally with DONE");

  const attempted = stream.timings?.draft_n;
  const accepted = stream.timings?.draft_n_accepted;
  invariant(Number.isSafeInteger(attempted) && attempted > 0, "must report positive attempted draft tokens");
  invariant(Number.isSafeInteger(accepted) && accepted > 0, "must report positive accepted draft tokens");
  invariant(accepted <= attempted, "cannot accept more draft tokens than were attempted");
  return { attempted, accepted };
}

async function readEvidence(path) {
  const metadata = await lstat(path).catch(() => null);
  invariant(metadata?.isFile() && metadata.nlink === 1, "file must be a single-link regular file");
  invariant(metadata.size > 0 && metadata.size <= MAX_EVIDENCE_BYTES, "file size is invalid");
  try {
    return JSON.parse(await readFile(path, "utf8"));
  } catch {
    throw new Error("MTP evidence is invalid JSON");
  }
}

async function main() {
  const args = process.argv.slice(2);
  invariant(args.length === 2 && args[0] === "--evidence", "usage: runtime-qualification.mjs --evidence /path/to/result.json");
  const counters = assertMtpPromotion(await readEvidence(resolve(args[1])));
  process.stdout.write(`${JSON.stringify({ promoted: true, ...counters })}\n`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(`Runtime qualification failed: ${error.message}\n`);
    process.exitCode = 1;
  });
}
