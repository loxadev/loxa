# Experimental Python MLX backend

`py-mlx-lm` is a Phase 1 prototype for Apple Silicon macOS. Loxa directly
supervises the official `mlx_lm.server` process while retaining its own stable
OpenAI-compatible gateway. The dependency is external and user-managed: Loxa
does **not** bundle Python, create a virtual environment, run `pip` or `uv`, or
update packages during startup.

The prototype is pinned to [`mlx-lm` v0.31.3 at
`ed1fca4cef15a824c5f1702c80f70b4cffc8e4dd`](https://github.com/ml-explore/mlx-lm/tree/ed1fca4cef15a824c5f1702c80f70b4cffc8e4dd),
released under the [MIT
license](https://github.com/ml-explore/mlx-lm/blob/ed1fca4cef15a824c5f1702c80f70b4cffc8e4dd/LICENSE).
The upstream server describes its API as similar to OpenAI Chat Completions and
warns that it is not recommended as a production server because it implements
only basic security checks ([server
documentation](https://github.com/ml-explore/mlx-lm/blob/ed1fca4cef15a824c5f1702c80f70b4cffc8e4dd/mlx_lm/SERVER.md)).

## Requirements

- Apple Silicon Mac (`macos/aarch64`). Intel Macs, Linux, and Windows fail
  before the server is spawned.
- A user-managed Python installation compatible with `mlx-lm==0.31.3`.
- `uv` for the documented tool installation flow.
- An existing **local MLX model directory**. This prototype does not download,
  resolve, register, or verify MLX models and does not accept a Hugging Face
  repository ID in place of the directory.

The required version is exact, not a minimum. Loxa runs `mlx_lm --version`
directly with a five-second bound and accepts only `0.31.3`.

## Install, repair/update, and uninstall

Install the pinned external tool:

```bash
uv tool install mlx-lm==0.31.3
```

Reinstall or return an existing tool environment to the supported pin:

```bash
uv tool install --force mlx-lm==0.31.3
```

Remove the external tool:

```bash
uv tool uninstall mlx-lm
```

These commands are operator actions, never Loxa startup actions. See the
official [`uv tool install`](https://docs.astral.sh/uv/reference/cli/#uv-tool-install)
and [`uv tool uninstall`](https://docs.astral.sh/uv/reference/cli/#uv-tool-uninstall)
references for tool-environment behavior.

Loxa discovers the executable in this order:

1. `LOXA_MLX_LM_SERVER`, if its value names a file;
2. `mlx_lm.server` on `PATH`.

It discovers the version command as a sibling named `mlx_lm` first, then on
`PATH`. An override therefore normally needs both commands in the same tool
directory:

```bash
export LOXA_MLX_LM_SERVER=/absolute/path/to/bin/mlx_lm.server
```

`loxa doctor` reports platform compatibility, installation state, server path,
detected or required version, and whether the upstream default endpoint
`127.0.0.1:8080` is reachable. That reachability evidence is informational; a
managed Loxa run normally uses a reserved dynamic port.

## Run and serve

Start the backend under Loxa supervision:

```bash
loxa run /absolute/path/to/mlx-model \
  --engine py-mlx-lm
```

Expose it through Loxa's stable gateway:

```bash
loxa serve \
  --engine py-mlx-lm \
  --model /absolute/path/to/mlx-model \
  --port 11435
```

The first command is the direct managed-engine workflow. `loxa serve` is the
client-facing workflow and listens on `http://127.0.0.1:11435` in this example.
The supplied model path must exist as a directory and is canonicalized before
spawn, so paths containing spaces remain one native process argument.

`--ctx` has no verified upstream equivalent in this prototype and is rejected:

```bash
loxa run /absolute/path/to/mlx-model \
  --engine py-mlx-lm \
  --ctx 8192
```

The default remains `--engine llama-cpp`; existing registry-ID commands are
unchanged.

### Client request

Clients use the public model alias `loxa`:

```bash
curl http://127.0.0.1:11435/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "loxa",
    "messages": [{"role": "user", "content": "Say hello."}],
    "stream": false
  }'
```

For streaming, set `"stream": true`. Loxa relays SSE incrementally, preserves
keepalive/comment frames and one terminal `[DONE]`, and normalizes upstream
model identifiers back to `loxa`.

## Control and data flow

```text
client
  | HTTP/SSE, model=loxa
  v
Loxa Rust gateway (127.0.0.1:11435)
  | private HTTP/SSE, model=default_model
  v
mlx_lm.server (127.0.0.1:reserved dynamic port)

Loxa Rust supervisor
  | direct spawn, PID/PGID, signals, exit, bounded stdout/stderr logs
  +---------------------------------------------------------------> mlx_lm.server
```

Loxa launches the executable directly, equivalent to:

```text
mlx_lm.server --model <canonical-model-dir> --host 127.0.0.1 --port <port>
```

There is no shell layer, generated script, stdin/stdout inference protocol, or
FFI. stdout and stderr are logs only. The private Python listener is always
loopback-only and is not the stable client endpoint.

The upstream server maps the special request identifier `default_model` to the
model supplied by `--model` ([pinned server
source](https://github.com/ml-explore/mlx-lm/blob/ed1fca4cef15a824c5f1702c80f70b4cffc8e4dd/mlx_lm/server.py)).
Loxa therefore rewrites `loxa` to `default_model` upstream and rewrites response
identifiers back to `loxa`.

## Liveness is not readiness

Upstream `GET /health` proves only that the HTTP process responds. Its
`GET /v1/models` route lists cached inventory; it does not prove that the model
selected at launch can generate. Loxa does not treat either result alone as
model readiness.

Within one fixed 60-second startup deadline, Loxa first requires health and then
sends a bounded, non-streaming one-token request to
`POST /v1/chat/completions` with `model: "default_model"`. Readiness requires:

- a live child process;
- an HTTP 2xx response;
- valid JSON;
- a non-empty first choice with generated message content or completion text.

Timeouts, redirects, transport failures, non-2xx responses, malformed JSON,
empty choices, empty output, or an early child exit cannot mark the backend
ready. A failed startup tears down the owned process group before returning.

## Status, logs, stop, and recovery

Inspect managed state:

```bash
loxa ps
```

Stop all managed sidecars from another terminal:

```bash
loxa stop all
```

Foreground `Ctrl-C` uses the same owned-process cleanup boundary. Loxa sends a
graceful termination, escalates when necessary, reaps the child, and removes
runtime state only after cleanup is confirmed.

Server stdout and stderr are drained into a bounded file under:

```text
~/.loxa/run/logs/py-mlx-lm-<port>-<unix-timestamp>.log
```

Each file is capped at 1 MiB. Startup failures print the log path; crash errors
include a short tail. There is no `loxa logs` command in this prototype, so
inspect the printed path with normal local file tools.

An unexpected server exit is restarted at most once. Loxa re-runs platform,
model, executable, and version validation before the replacement generation.
A second crash ends the run instead of creating an infinite restart loop. If
cleanup cannot be confirmed, Loxa retains recovery evidence rather than
pretending the run is gone.

## Limitations

- Experimental developer backend, not a production security boundary.
- External Python and `mlx-lm==0.31.3` are mandatory and user-managed.
- Apple Silicon macOS only.
- Existing local MLX directories only; no MLX-aware `loxa pull`, registry,
  resolver, checksum, or model-compatibility validation.
- One supervised model process; no model pool, continuous batching, TTL/LRU,
  LoRA, VLM, embedding, or custom tool-parser expansion in Phase 1.
- No verified `--ctx` mapping.
- Loxa exposes only its existing Chat Completions gateway surface; upstream
  support does not imply full OpenAI API compatibility.
- A reachable `/health` endpoint is not proof of generation readiness.

## Research record

The required upstream inspection used the immutable `mlx-lm` commit above. The
prototype depends only on the official package.

Community inspiration was inspected read-only and was **not** added as a
dependency:

- [`jundot/omlx` at
  `d5fcb22a87c3b46ab6dd91016fbbbdb1e624f374`](https://github.com/jundot/omlx/tree/d5fcb22a87c3b46ab6dd91016fbbbdb1e624f374),
  [Apache-2.0](https://github.com/jundot/omlx/blob/d5fcb22a87c3b46ab6dd91016fbbbdb1e624f374/LICENSE):
  **adopt** explicit SSE keepalive behavior for long prefill;
  **adapt** memory guards plus TTL/LRU/pinning into Loxa-owned policy only when
  multi-model residency is in scope; **avoid** importing its menu-bar,
  multi-model, batching, or tiered-cache product complexity into this narrow
  adapter.

No community code was copied. Performance claims remain unverified until Loxa
runs controlled, same-model benchmarks.

## Phase 2

The Python process is deliberately behind a small backend boundary. See
[Swift MLX handoff](mlx-swift-handoff.md) for the exact replacement seam and
the invariants that must remain unchanged.
