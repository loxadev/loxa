# Loxa

[![CI](https://github.com/loxadev/loxa/actions/workflows/ci.yml/badge.svg)](https://github.com/loxadev/loxa/actions/workflows/ci.yml)

I was curious to test local models.
My Mac was barely capable enough to run local AI (36 GB RAM M4 MAX), but using it for more than a quick experiment meant piecing everything together myself: finding the right model, choosing a quantization, configuring a runtime, managing ports and processes, and reconnecting each app whenever something changed.

Getting the first response was easy. Trusting the setup to stay available—and
knowing what to do when it failed—was the hard part.

Loxa is the tool I wanted for that job. It is being built to turn hardware you
already own into a private AI node that manages its models and runtimes, keeps
them healthy, and gives your trusted apps and devices one stable place to
connect.

## The problem

Local inference tools have made it much easier to run a model, but users still
have to assemble the surrounding system themselves:

- find an artifact that is compatible with their hardware and runtime;
- keep model revisions, formats, quantizations, and checksums straight;
- start, monitor, restart, and clean up model-server processes;
- manage ports, API endpoints, credentials, and client configuration;
- understand whether a model is downloaded, loaded, ready, unhealthy, or
  recovering;
- repeat that work for every application, device, and machine.

The result can work in a demo and still fail during real work. Loxa's goal is
to make the node—not merely the model—reliable.

## What exists today

Loxa is in early development. The current repository provides a local,
single-node foundation with one active model at a time:

- a Rust daemon and CLI with ordered startup, shutdown, and recovery;
- resumable, checksum-verified downloads for known recipes and compatible
  Hugging Face GGUF artifacts;
- a supervised llama.cpp backend with exact child-process ownership;
- an experimental adapter for an externally managed Python MLX server;
- an OpenAI-compatible local chat endpoint with streaming and cancellation;
- a stable `loxa` model alias that does not expose the selected backend to
  clients;
- authenticated local control, operation progress, and structured runtime
  diagnostics;
- a Tauri desktop client for node status, model inventory, model lifecycle,
  chat, and recovery flows.

The strongest parts of the current implementation are model verification,
process supervision, stable local serving, and lifecycle safety. The backend
is being refactored incrementally so these guarantees remain intact as the
product grows.

```text
loxa doctor    # inspect hardware and detected local AI tools
loxa pull      # resolve and download a compatible model artifact
loxa list      # show model inventory and download state
loxa run       # run a model with a supervised engine
loxa serve     # keep the managed node available at a stable endpoint
loxa ps        # inspect the managed server
loxa stop      # stop it with bounded, exact-process cleanup
```

## Product direction

The near-term product is a dependable personal AI node, initially focused on
Apple Silicon developers and power users. Loxa should resolve compatible
models, run them through managed backends, expose one stable API, recover from
failures, and let trusted desktop, coding, and mobile clients use the same
node without each becoming a separate runtime owner.

The backend is evolving toward autonomous, fleet-aware nodes with stable
identity, independently owned model slots, durable operations, runtime
profiles, resource-aware scheduling, and secure connectivity. Each node must
continue to work locally on its own.

Longer term, an optional control plane can help teams manage their own Macs
and GPU machines: inventory, runtime profiles, rollout and rollback, health
alerts, access policy, audit history, and remote diagnostics. This does **not**
mean distributed inference; inference remains local to the selected node and
outside the fleet-control hot path.

Loxa is not trying to become another generic chat application, inference
engine, or model benchmark. Its focus is the reliable management layer around
user-owned AI hardware.

## Current limitations

The following direction is not yet fully implemented:

- multiple concurrent model slots;
- node collections and fleet connectivity;
- secure device pairing and revocation;
- durable operation history and resumable event cursors across restarts;
- runtime installation, updates, activation, and rollback;
- the bundled native Swift MLX sidecar;
- the complete searchable model-discovery and compatibility experience.

The current production path remains the scalar local node and managed
llama.cpp baseline. Experimental capabilities are identified as such rather
than presented as finished product behavior.

## Experimental Python MLX backend

On Apple Silicon macOS, Loxa can supervise an externally installed
`mlx_lm.server` as a development and compatibility backend. Python and
`mlx-lm` remain user-managed: Loxa does not bundle Python, create an
environment, or install packages.

```bash
uv tool install mlx-lm==0.31.3

loxa run /absolute/path/to/mlx-model --engine py-mlx-lm

loxa serve \
  --engine py-mlx-lm \
  --model /absolute/path/to/mlx-model \
  --port 11435
```

Clients continue to use `model: "loxa"` at the Loxa endpoint. The model path
must already be a local directory, and `--ctx` is intentionally rejected for
this backend. Run `loxa doctor` to inspect the external executable, version,
platform compatibility, and default-endpoint reachability.

## Platform status

Loxa is Apple Silicon-first and also builds on Linux. It is not yet a stable
release; expect sharp edges before version 0.1.0.

## License

Apache-2.0
