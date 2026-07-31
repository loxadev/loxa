# Loxa

[![CI](https://github.com/loxadev/loxa/actions/workflows/ci.yml/badge.svg)](https://github.com/loxadev/loxa/actions/workflows/ci.yml)

Loxa is a small Rust CLI for downloading verified GGUF models from Hugging
Face and running them locally with `llama-server`.

This is an early MVP with four commands:

```sh
loxa pull owner/repository --quant Q4_K_M --name my-model
loxa list
loxa run my-model
loxa chat my-model
```

`pull` resolves the requested revision to an immutable Hugging Face commit,
resumes interrupted transfers, verifies the expected size and SHA-256, and
publishes the local manifest only after verification. Split GGUF models are not
supported yet.

`run` and `chat` automatically use Loxa's managed `llama-server`, then fall
back to one on `PATH`. If neither is installed on macOS:

```sh
brew install llama.cpp
```

Both commands choose a local port automatically and use a 4096-token context.
Optional defaults can be placed in `~/.loxa/config.json`:

```json
{"version":1,"ctx":4096,"port":0}
```

In chat, `/clear` resets the in-memory conversation and `/exit` quits.
Ctrl-C also shuts down the owned server process.

Models are stored under `~/.loxa/models`. Set `LOXA_HOME` to use another
location.

Build locally with:

```sh
cargo build --release --locked
```

## License

Apache-2.0
