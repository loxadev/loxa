# loxa

[![CI](https://github.com/loxadev/loxa/actions/workflows/ci.yml/badge.svg)](https://github.com/loxadev/loxa/actions/workflows/ci.yml)

Running a local AI model is easy now. Knowing which setup will actually
handle your agent's workload on your machine is not. A model can load
fine and still return broken tool calls, run out of memory at the
context you need, or quietly underperform the other runtime you already
have installed. Today you find that out during real work.

Loxa is being built to answer that question before it costs you an
evening: qualify local AI configurations against a real tool-using
workload on your own hardware, reject the ones that fail, and keep the
verified choice running behind one stable endpoint.

## What works today

```
loxa doctor    # hardware report and detected local AI tools
loxa pull      # download pinned models, SHA-256 verified, resumable
loxa list      # registry and download status
loxa run       # serve a model with a supervised llama-server
loxa serve     # expose the managed model through the stable `loxa` API alias
loxa ps        # show the managed server
loxa stop      # stop it cleanly, no orphan processes
```

Downloads are verified against pinned checksums. The managed server has
a race-tested lifecycle: clean shutdown, crash recovery, one bounded
restart, no orphans.

## Experimental Python MLX backend

On Apple Silicon macOS, Loxa can supervise an externally installed
`mlx_lm.server` as an experimental backend. Python and `mlx-lm` remain
user-managed: Loxa does not bundle Python, create an environment, or install
packages.

```bash
uv tool install mlx-lm==0.31.3

loxa run /absolute/path/to/mlx-model --engine py-mlx-lm

loxa serve \
  --engine py-mlx-lm \
  --model /absolute/path/to/mlx-model \
  --port 11435
```

Clients of `loxa serve` continue to use `model: "loxa"` at the Loxa endpoint.
The model path must already be a local directory, and `--ctx` is intentionally
rejected for this backend. Run `loxa doctor` to inspect the external executable,
exact version, platform compatibility, and default-endpoint reachability.

## What is being built now

Workload qualification: run the same tool-use workload against two
candidate setups, a Loxa-managed llama-server and an existing Ollama
install, on your machine. Reject candidates that fail correctness,
context, stability, or memory gates. Select the winner with recorded
evidence, or say honestly that no verified plan exists. Then serve that
choice at one stable endpoint.

## Status

Early development. macOS (Apple silicon) first; builds on Linux. The
qualification loop described above is in active development and not
released yet. Expect sharp edges before 0.1.0.

## License

Apache-2.0
