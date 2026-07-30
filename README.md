# Loxa

[![CI](https://github.com/loxadev/loxa/actions/workflows/ci.yml/badge.svg)](https://github.com/loxadev/loxa/actions/workflows/ci.yml)

Loxa is a small Rust CLI for downloading verified GGUF models from Hugging
Face and running them locally with `llama-server`.

This is an early MVP with three commands:

```sh
loxa pull owner/repository --quant Q4_K_M --name my-model
loxa list
loxa run my-model
```

`pull` resolves the requested revision to an immutable Hugging Face commit,
resumes interrupted transfers, verifies the expected size and SHA-256, and
publishes the local manifest only after verification. Split GGUF models are not
supported yet.

`run` requires `llama-server` on `PATH`, or an explicit path through
`LOXA_LLAMA_SERVER` or `--server`. It starts a foreground OpenAI-compatible
endpoint on `127.0.0.1`; on macOS, Ctrl-C shuts down its owned
process group.

Models are stored under `~/.loxa/models`. Set `LOXA_HOME` to use another
location.

Build locally with:

```sh
cargo build --release --locked
```

## License

Apache-2.0
