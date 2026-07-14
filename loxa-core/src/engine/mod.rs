pub mod py_mlx_lm;
pub mod swift_mlx;

use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeBackendKind {
    LlamaCpp,
    PyMlxLm,
}

impl fmt::Display for RuntimeBackendKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::LlamaCpp => "llama-cpp",
            Self::PyMlxLm => "py-mlx-lm",
        })
    }
}

impl FromStr for RuntimeBackendKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "llama-cpp" => Ok(Self::LlamaCpp),
            "py-mlx-lm" => Ok(Self::PyMlxLm),
            _ => Err(format!(
                "unsupported engine '{value}'; expected llama-cpp or py-mlx-lm"
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineLaunchSpec {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub port: u16,
    pub engine_name: String,
    pub engine_version: String,
    pub runtime_model: String,
    pub upstream_model: String,
    pub readiness: ReadinessStrategy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadinessStrategy {
    LlamaModelAlias { expected_alias: String },
    ChatCompletionProbe { request_model: String },
}

#[cfg(test)]
mod tests {
    use super::{EngineLaunchSpec, ReadinessStrategy, RuntimeBackendKind};
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::str::FromStr;

    #[test]
    fn backend_kind_parses_and_displays_cli_values() {
        assert_eq!(
            RuntimeBackendKind::from_str("llama-cpp"),
            Ok(RuntimeBackendKind::LlamaCpp)
        );
        assert_eq!(
            RuntimeBackendKind::from_str("py-mlx-lm"),
            Ok(RuntimeBackendKind::PyMlxLm)
        );
        assert_eq!(RuntimeBackendKind::LlamaCpp.to_string(), "llama-cpp");
        assert_eq!(RuntimeBackendKind::PyMlxLm.to_string(), "py-mlx-lm");
        assert!(RuntimeBackendKind::from_str("mlx").is_err());
    }

    #[test]
    fn launch_spec_retains_os_native_arguments_and_readiness() {
        let spec = EngineLaunchSpec {
            program: PathBuf::from("/tmp/mlx_lm.server"),
            args: vec![OsString::from("--port"), OsString::from("8123")],
            port: 8123,
            engine_name: "mlx-lm".into(),
            engine_version: "0.31.3".into(),
            runtime_model: "/tmp/model".into(),
            upstream_model: "default_model".into(),
            readiness: ReadinessStrategy::ChatCompletionProbe {
                request_model: "default_model".into(),
            },
        };

        assert_eq!(spec.args[0], OsString::from("--port"));
        assert_eq!(spec.port, 8123);
        assert_eq!(spec.upstream_model, "default_model");
    }
}
