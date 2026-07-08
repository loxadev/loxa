# loxa

[![CI](https://github.com/loxadev/loxa/actions/workflows/ci.yml/badge.svg)](https://github.com/loxadev/loxa/actions/workflows/ci.yml)

Running AI models on your own hardware is still a guessing game. You pick a
model, wait through a 20GB download, and only then find out it crawls, blows
past your memory, or breaks the agent you wired it into. Fit labels in other
tools are estimates. Nobody actually measures.

Loxa measures. It benchmarks speed, memory, and tool-calling reliability on
your machine, picks the best configuration from those results, and serves it
through OpenAI and Anthropic compatible APIs. When your cloud provider goes
down, it fails over to the local setup it already verified.

## What it does

```
loxa doctor    # what this machine has, and what it can actually run
loxa pull      # download models, checksum verified, resumable
loxa run       # serve a model locally with a supervised llama-server
loxa bench     # real numbers: time to first token, tokens/sec, peak memory
```

Every download is SHA-256 verified against a pinned registry. Every server
is supervised: clean shutdown, no orphan processes. Every recommendation
comes from a benchmark that ran on your hardware, not a lookup table.

## Status

Early development. The CLI works on macOS (Apple silicon) and builds on
Linux. Expect sharp edges before 0.1.0.

## License

Apache-2.0
