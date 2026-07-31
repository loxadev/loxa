# Loxa

[![CI](https://github.com/loxadev/loxa/actions/workflows/ci.yml/badge.svg)](https://github.com/loxadev/loxa/actions/workflows/ci.yml)

Loxa is a small Rust CLI for downloading verified GGUF models from Hugging
Face and running them locally with `llama-server`.

`run` and `chat` use Loxa's managed `llama-server` when present. Otherwise, if
`llama-server` is not already on `PATH` on macOS:

```sh
brew install llama.cpp
```

Build and try the MVP:

```sh
cargo build --release --locked
./target/release/loxa pull bartowski/SmolLM2-135M-Instruct-GGUF --quant Q4_K_M --name smollm2-135m
./target/release/loxa list
./target/release/loxa run smollm2-135m
```

When `run` is ready, it prints:

```text
ready: http://127.0.0.1:<automatic-port> (model smollm2-135m)
```

The printed loopback URL with `/v1/models` is the readiness check. Stop `run`
with Ctrl-C, then chat in the terminal:

```sh
./target/release/loxa chat smollm2-135m
```

`pull` resolves the requested revision to an immutable Hugging Face commit,
resumes interrupted transfers, verifies the expected size and SHA-256, and
publishes the local manifest only after verification. Split GGUF models are not
supported yet.

Both commands choose a local port automatically and use a 4096-token context.
To override a default, create `~/.loxa/config.json`, for example:

```json
{"version":1,"ctx":8192}
```

In chat, `/clear` resets the in-memory conversation and `/exit` quits.
Ctrl-C also shuts down the owned server process.

Models are stored under `~/.loxa/models`. Set `LOXA_HOME` to use another
location.

## License

Apache-2.0
