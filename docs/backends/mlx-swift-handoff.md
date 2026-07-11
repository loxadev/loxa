# Swift MLX replacement handoff

Phase 2 replaces the externally installed Python engine with a prebuilt, signed
`loxa-mlx` sidecar based on Apple's official
[`mlx-swift`](https://github.com/ml-explore/mlx-swift) and
[`mlx-swift-lm`](https://github.com/ml-explore/mlx-swift-lm) libraries. It does
not add another public API server or another supervisor.

The Rust Loxa gateway remains the only stable client endpoint. The Swift
process should expose the smallest private loopback protocol needed for model
load, generation, streaming, cancellation, unload, readiness, and metrics.
Rust continues to translate that internal protocol to the public OpenAI-style
HTTP/SSE contract.

## Exact five-piece replacement seam

Only these backend-owned pieces change:

1. **Executable discovery** — replace external `mlx_lm.server` and
   `mlx_lm --version` discovery with discovery of the bundled, signed
   `loxa-mlx` executable and its build metadata.
2. **Launch arguments** — replace Python's `--model`, `--host`, and `--port`
   argument construction with the Swift sidecar's narrow, OS-native launch
   contract.
3. **Upstream model identifier** — replace Python's `default_model` identifier
   with the Swift protocol's single loaded-model identifier.
4. **Readiness probe** — replace the Python health-plus-Chat-Completions probe
   with truthful Swift load/warm readiness, while still requiring a live child
   and a bounded generation proof before publication.
5. **Engine metadata** — replace `mlx-lm`/`0.31.3` metadata with signed sidecar
   version and build provenance.

These pieces remain unchanged:

- the `loxa run` and `loxa serve` CLI contract;
- Rust child ownership, process groups, TERM-to-KILL escalation, and reaping;
- runtime state, `loxa ps`, `loxa stop`, and no-orphan semantics;
- bounded stdout/stderr logs and one-restart crash policy;
- the stable gateway, public `model: "loxa"`, and normalized error surface;
- incremental HTTP/SSE delivery and disconnect cancellation;
- lifecycle, gateway, readiness, and ignored real-integration acceptance tests.

This is the acceptance test for the seam: replacing the five backend-owned
items must not require a second gateway, copied lifecycle loop, persisted-state
schema migration, or client API change.

## Initial sidecar contract

Start with one model per process and one active generation at a time. Bind only
to `127.0.0.1` on the supervisor-reserved port, require a random engine token
passed by Rust, keep the loaded model warm, and use process termination as the
final memory-reclamation guarantee.

A narrow private protocol can provide:

```text
GET  /health       process liveness
GET  /ready        model, tokenizer, and warm-up complete
POST /load         load one local model
POST /generate     stream generated token pieces
POST /cancel       cancel one generation task
POST /unload       release model resources
GET  /metrics      load, prefill, decode, and memory observations
```

Chunked NDJSON is sufficient for the private token stream. Swift should apply
the chat template and tokenize; Rust should own public request normalization,
the `loxa` model alias, OpenAI SSE framing, authentication/CORS, usage policy,
and stable errors. Do not implement full OpenAI compatibility twice.

`GET /ready` may become true only after model and tokenizer load plus warm-up.
Before Loxa publishes the gateway target, retain a bounded generation proof so
that protocol reachability cannot masquerade as usable inference.

## Packaging and platform policy

End users receive a signed, notarized native sidecar and do not need Xcode or a
Swift toolchain. Pin immutable `mlx-swift` and `mlx-swift-lm` releases in
`Package.resolved`; never track `main` in a release build. Package and test all
required Metal resources, including `mlx.metallib` when the selected dependency
layout requires it.

The initial policy is:

```text
Apple Silicon + supported macOS + supported MLX model -> loxa-mlx
Apple Silicon + unsupported model or OS              -> llama-server
Intel macOS, Linux, Windows                           -> llama-server
```

Swift is selected for packaging, lifecycle control, truthful readiness,
cancellation, observability, native integration, and product ownership—not on
an assumption that the language alone improves tokens per second. Benchmark
Python and Swift with the same model revision, quantization, prompt, context,
sampling, cache state, and cold/warm condition.

## First implementation slice

Support text generation for a small explicit model-family matrix before
expanding scope. The first vertical slice needs:

- local model loading and chat-template application;
- streaming generation;
- temperature, top-p, top-k, stop sequences, and usage counts;
- task cancellation, unload, and warm-up/readiness;
- cold-load, time-to-first-token, decode-rate, and memory observations;
- repeated load/generate/unload and forced-process-exit tests.

Defer parallel batching, multiple resident models, LoRA, VLMs, embeddings,
speculative decoding, complex tool-call parsing, and public OpenAI response
formatting until the sidecar contract is stable.

## Source and community findings

The read-only research snapshot used these immutable official revisions:

- [`ml-explore/mlx-swift` tag `0.31.6` at
  `0bb916c67f4b9e5c682cbe02a42c701c93ab5021`](https://github.com/ml-explore/mlx-swift/tree/0bb916c67f4b9e5c682cbe02a42c701c93ab5021),
  [MIT](https://github.com/ml-explore/mlx-swift/blob/0bb916c67f4b9e5c682cbe02a42c701c93ab5021/LICENSE):
  native MLX arrays, computation, memory, concurrency, and Metal integration.
- [`ml-explore/mlx-swift-lm` tag `3.31.4` at
  `bd4b7434e6bdb588c7ef55706ff8904cb7fd4c57`](https://github.com/ml-explore/mlx-swift-lm/tree/bd4b7434e6bdb588c7ef55706ff8904cb7fd4c57),
  [MIT](https://github.com/ml-explore/mlx-swift-lm/blob/bd4b7434e6bdb588c7ef55706ff8904cb7fd4c57/LICENSE):
  reusable model loading, tokenization, generation, cache, LLM/VLM, and
  embedding libraries. It is a library dependency, not a drop-in production
  HTTP server.

Those SHAs are research evidence, not dependencies added by Phase 1. The two
tags are the recommended starting candidates for the Phase 2 spike. Phase 2
must verify their toolchain, SDK, model, cancellation, memory, and packaging
behavior together before selecting production dependency pins and committing
the resulting `Package.resolved`. It must not silently follow either `main`
branch.

Community repositories were inspected as design evidence only. None is a
production dependency and no code was copied:

- [`magicnight/Mac-MLX` tag `v0.5.0` at
  `f4fba0e0537d144e84eed577c77080ceaf505507`](https://github.com/magicnight/Mac-MLX/tree/f4fba0e0537d144e84eed577c77080ceaf505507),
  [Apache-2.0](https://github.com/magicnight/Mac-MLX/blob/f4fba0e0537d144e84eed577c77080ceaf505507/LICENSE):
  **adopt** a small engine protocol boundary; **adapt** its readiness,
  cancellation, dynamic-port, shutdown, signing, and Metal-resource lessons to
  a headless sidecar; **avoid** SwiftUI, chat, downloads, model-pool, and
  product-specific scope.
- [`Trans-N-ai/swama` at
  `2bf4ed270b4a553e88b6aaeefae251132f094439`](https://github.com/Trans-N-ai/swama/tree/2bf4ed270b4a553e88b6aaeefae251132f094439),
  [MIT](https://github.com/Trans-N-ai/swama/blob/2bf4ed270b4a553e88b6aaeefae251132f094439/LICENSE):
  **adopt** clean Swift package boundaries around official MLX dependencies;
  **adapt** SwiftNIO streaming and model management behind Loxa's private
  protocol; **avoid** its GUI, downloader, broad modality, and public API
  surface.
- [`SharpAI/SwiftLM` at
  `d5a9d118910142ce092fc4357777884a61bb8137`](https://github.com/SharpAI/SwiftLM/tree/d5a9d118910142ce092fc4357777884a61bb8137),
  [MIT](https://github.com/SharpAI/SwiftLM/blob/d5a9d118910142ce092fc4357777884a61bb8137/LICENSE):
  **adopt** release checks that keep the executable and `mlx.metallib`
  together; **adapt** its large-model benchmark dimensions only after
  reproducible Loxa baselines; **avoid** its forked MLX submodules, custom C++
  primitives, custom Metal kernels, TurboQuant, SSD expert streaming, and
  unverified performance claims.
- [`jundot/omlx` at
  `d5fcb22a87c3b46ab6dd91016fbbbdb1e624f374`](https://github.com/jundot/omlx/tree/d5fcb22a87c3b46ab6dd91016fbbbdb1e624f374),
  [Apache-2.0](https://github.com/jundot/omlx/blob/d5fcb22a87c3b46ab6dd91016fbbbdb1e624f374/LICENSE):
  **adopt** keepalive-aware long-prefill serving; **adapt** memory guards and
  cache residency policies only after profiling; **avoid** importing the
  Python service, menu-bar application, multi-model pool, or cache stack.

Before using any community idea, re-pin its immutable commit, confirm the
license and notices at that commit, and reproduce the relevant behavior in
Loxa-owned tests. Community benchmarks are hypotheses, not acceptance evidence.

## Promotion gates

Make `loxa-mlx` the default supported Mac engine only after all of these are
demonstrated on a clean supported Mac:

- every declared model family loads and applies the correct chat template;
- non-streaming and streaming output pass through `loxa serve` as `model=loxa`;
- cancellation releases request resources;
- killing the process releases model memory;
- readiness reflects load and generation, not merely an open socket;
- repeated load/generate/unload and crash/restart cycles leave no orphan;
- failures include actionable diagnostics and bounded logs;
- signed/notarized artifacts find every packaged Metal resource;
- controlled Python/Swift benchmarks and compatibility results are recorded.

Until those gates pass, `llama-server` remains the broad fallback and the
external Python adapter remains a development/reference backend.
