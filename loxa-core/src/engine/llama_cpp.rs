use super::{EngineLaunchSpec, ReadinessStrategy};
use std::ffi::OsString;
use std::fmt;
use std::path::Path;

pub const QUALIFIED_LLAMA_CPP_BUILD: &str = "10107";
pub const QUALIFIED_LLAMA_CPP_COMMIT: &str = "c0bc8591e";
pub const QUALIFIED_LLAMA_CPP_VERSION_FIRST_LINE: &str = "version: 10107 (c0bc8591e)";

#[derive(Clone, Copy, Debug)]
pub enum LlamaCppLaunchMode<'a> {
    Unpaired {
        ctx_size: u32,
    },
    QualifiedGemma4Mtp {
        drafter: &'a Path,
        ctx_size: u32,
        jinja: bool,
        spec_type: &'a str,
        draft_n_max: u32,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct LlamaCppLaunchInput<'a> {
    pub program: &'a Path,
    pub target: &'a Path,
    pub alias: &'a str,
    pub port: u16,
    pub engine_version: &'a str,
    pub mode: LlamaCppLaunchMode<'a>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LlamaCppLaunchError {
    InvalidQualifiedProfile { field: &'static str },
    UnqualifiedRuntimeVersion,
}

impl fmt::Display for LlamaCppLaunchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidQualifiedProfile { field } => {
                write!(
                    formatter,
                    "invalid qualified llama.cpp profile field: {field}"
                )
            }
            Self::UnqualifiedRuntimeVersion => write!(
                formatter,
                "qualified Gemma 4 MTP requires llama.cpp version: {QUALIFIED_LLAMA_CPP_BUILD} ({QUALIFIED_LLAMA_CPP_COMMIT})"
            ),
        }
    }
}

impl std::error::Error for LlamaCppLaunchError {}

pub fn build_launch_spec(
    input: LlamaCppLaunchInput<'_>,
) -> Result<EngineLaunchSpec, LlamaCppLaunchError> {
    let mut args = vec![
        OsString::from("--model"),
        input.target.as_os_str().to_owned(),
        OsString::from("--alias"),
        OsString::from(input.alias),
        OsString::from("--host"),
        OsString::from("127.0.0.1"),
        OsString::from("--port"),
        OsString::from(input.port.to_string()),
    ];
    match input.mode {
        LlamaCppLaunchMode::Unpaired { ctx_size } => {
            args.extend([
                OsString::from("--ctx-size"),
                OsString::from(ctx_size.to_string()),
                OsString::from("--gpu-layers"),
                OsString::from("auto"),
                OsString::from("--flash-attn"),
                OsString::from("auto"),
                OsString::from("--metrics"),
            ]);
        }
        LlamaCppLaunchMode::QualifiedGemma4Mtp {
            drafter,
            ctx_size,
            jinja,
            spec_type,
            draft_n_max,
        } => {
            validate_qualified_runtime_version(input.engine_version)?;
            let profile = crate::runtime_profile::runtime_profile("loxa")
                .expect("qualified Gemma 4 MTP runtime profile");
            validate_qualified_profile(ctx_size, jinja, spec_type, draft_n_max, profile)?;
            args.extend([
                OsString::from("--ctx-size"),
                OsString::from(profile.ctx_size.to_string()),
                OsString::from("--jinja"),
                OsString::from("--reasoning"),
                OsString::from("off"),
                OsString::from("--metrics"),
                OsString::from("--n-gpu-layers"),
                OsString::from("all"),
                OsString::from("--fit"),
                OsString::from("off"),
                OsString::from("--spec-draft-model"),
                drafter.as_os_str().to_owned(),
                OsString::from("--spec-type"),
                OsString::from(profile.spec_type),
                OsString::from("--spec-draft-n-max"),
                OsString::from(profile.draft_n_max.to_string()),
                OsString::from("--n-gpu-layers-draft"),
                OsString::from("all"),
            ]);
        }
    }
    args.push(OsString::from("--log-disable"));

    Ok(EngineLaunchSpec {
        program: input.program.to_path_buf(),
        args,
        port: input.port,
        engine_name: "llama.cpp".into(),
        engine_version: input.engine_version.into(),
        runtime_model: input.target.display().to_string(),
        upstream_model: input.alias.into(),
        readiness: ReadinessStrategy::LlamaModelAlias {
            expected_alias: input.alias.into(),
        },
    })
}

fn validate_qualified_runtime_version(engine_version: &str) -> Result<(), LlamaCppLaunchError> {
    match engine_version.lines().next() {
        Some(QUALIFIED_LLAMA_CPP_VERSION_FIRST_LINE) => Ok(()),
        _ => Err(LlamaCppLaunchError::UnqualifiedRuntimeVersion),
    }
}

fn validate_qualified_profile(
    ctx_size: u32,
    jinja: bool,
    spec_type: &str,
    draft_n_max: u32,
    profile: &crate::runtime_profile::LlamaRuntimeProfile,
) -> Result<(), LlamaCppLaunchError> {
    let field = if ctx_size != profile.ctx_size {
        Some("ctx-size")
    } else if jinja != profile.jinja {
        Some("jinja")
    } else if spec_type != profile.spec_type {
        Some("spec-type")
    } else if draft_n_max != profile.draft_n_max {
        Some("draft-n-max")
    } else {
        None
    };
    match field {
        Some(field) => Err(LlamaCppLaunchError::InvalidQualifiedProfile { field }),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::{build_launch_spec, LlamaCppLaunchError, LlamaCppLaunchInput, LlamaCppLaunchMode};
    use crate::engine::ReadinessStrategy;
    use std::ffi::OsString;
    use std::path::Path;

    fn unpaired_input<'a>(program: &'a Path, target: &'a Path) -> LlamaCppLaunchInput<'a> {
        LlamaCppLaunchInput {
            program,
            target,
            alias: "loxa-run-g1",
            port: 11_435,
            engine_version: "b10107",
            mode: LlamaCppLaunchMode::Unpaired { ctx_size: 4_096 },
        }
    }

    fn qualified_input<'a>(
        program: &'a Path,
        target: &'a Path,
        drafter: &'a Path,
        ctx_size: u32,
        jinja: bool,
        spec_type: &'a str,
        draft_n_max: u32,
    ) -> LlamaCppLaunchInput<'a> {
        LlamaCppLaunchInput {
            program,
            target,
            alias: "loxa-run-g2",
            port: 11_436,
            engine_version: "version: 10107 (c0bc8591e)\nbuilt with AppleClang",
            mode: LlamaCppLaunchMode::QualifiedGemma4Mtp {
                drafter,
                ctx_size,
                jinja,
                spec_type,
                draft_n_max,
            },
        }
    }

    #[test]
    fn unpaired_mode_preserves_the_exact_ordered_legacy_argv() {
        let program = Path::new("/opt/llama/llama-server");
        let target = Path::new("/models/ordinary.gguf");

        let spec = build_launch_spec(unpaired_input(program, target)).unwrap();

        assert_eq!(spec.program, program);
        assert_eq!(
            spec.args,
            vec![
                OsString::from("--model"),
                target.as_os_str().to_owned(),
                OsString::from("--alias"),
                OsString::from("loxa-run-g1"),
                OsString::from("--host"),
                OsString::from("127.0.0.1"),
                OsString::from("--port"),
                OsString::from("11435"),
                OsString::from("--ctx-size"),
                OsString::from("4096"),
                OsString::from("--gpu-layers"),
                OsString::from("auto"),
                OsString::from("--flash-attn"),
                OsString::from("auto"),
                OsString::from("--metrics"),
                OsString::from("--log-disable"),
            ]
        );
        assert_eq!(spec.engine_name, "llama.cpp");
        assert_eq!(spec.engine_version, "b10107");
        assert_eq!(spec.runtime_model, target.display().to_string());
        assert_eq!(spec.upstream_model, "loxa-run-g1");
        assert_eq!(
            spec.readiness,
            ReadinessStrategy::LlamaModelAlias {
                expected_alias: "loxa-run-g1".into()
            }
        );
    }

    #[test]
    fn qualified_mode_emits_the_exact_ordered_b10107_mtp_argv() {
        let program = Path::new("/opt/llama/llama-server");
        let target = Path::new("/models/gemma 4 target.gguf");
        let drafter = Path::new("/models/gemma 4 drafter.gguf");

        let spec = build_launch_spec(qualified_input(
            program,
            target,
            drafter,
            8_192,
            true,
            "draft-mtp",
            4,
        ))
        .unwrap();

        assert_eq!(
            spec.args,
            vec![
                OsString::from("--model"),
                target.as_os_str().to_owned(),
                OsString::from("--alias"),
                OsString::from("loxa-run-g2"),
                OsString::from("--host"),
                OsString::from("127.0.0.1"),
                OsString::from("--port"),
                OsString::from("11436"),
                OsString::from("--ctx-size"),
                OsString::from("8192"),
                OsString::from("--jinja"),
                OsString::from("--reasoning"),
                OsString::from("off"),
                OsString::from("--metrics"),
                OsString::from("--n-gpu-layers"),
                OsString::from("all"),
                OsString::from("--fit"),
                OsString::from("off"),
                OsString::from("--spec-draft-model"),
                drafter.as_os_str().to_owned(),
                OsString::from("--spec-type"),
                OsString::from("draft-mtp"),
                OsString::from("--spec-draft-n-max"),
                OsString::from("4"),
                OsString::from("--n-gpu-layers-draft"),
                OsString::from("all"),
                OsString::from("--log-disable"),
            ]
        );
    }

    #[test]
    fn model_paths_with_spaces_remain_single_os_arguments() {
        let program = Path::new("/opt/llama/llama-server");
        let target = Path::new("/models/gemma 4 target.gguf");
        let drafter = Path::new("/models/gemma 4 drafter.gguf");

        let spec = build_launch_spec(qualified_input(
            program,
            target,
            drafter,
            8_192,
            true,
            "draft-mtp",
            4,
        ))
        .unwrap();

        assert_eq!(
            spec.args
                .iter()
                .filter(|arg| arg.as_os_str() == target.as_os_str())
                .count(),
            1
        );
        assert_eq!(
            spec.args
                .iter()
                .filter(|arg| arg.as_os_str() == drafter.as_os_str())
                .count(),
            1
        );
    }

    #[test]
    fn qualified_mode_rejects_ctx_jinja_spec_type_and_draft_max_drift() {
        let program = Path::new("/opt/llama/llama-server");
        let target = Path::new("/models/target.gguf");
        let drafter = Path::new("/models/drafter.gguf");

        assert_eq!(
            build_launch_spec(qualified_input(
                program,
                target,
                drafter,
                4_096,
                true,
                "draft-mtp",
                4,
            )),
            Err(LlamaCppLaunchError::InvalidQualifiedProfile { field: "ctx-size" })
        );
        assert_eq!(
            build_launch_spec(qualified_input(
                program,
                target,
                drafter,
                8_192,
                false,
                "draft-mtp",
                4,
            )),
            Err(LlamaCppLaunchError::InvalidQualifiedProfile { field: "jinja" })
        );
        assert_eq!(
            build_launch_spec(qualified_input(
                program, target, drafter, 8_192, true, "draft", 4,
            )),
            Err(LlamaCppLaunchError::InvalidQualifiedProfile { field: "spec-type" })
        );
        assert_eq!(
            build_launch_spec(qualified_input(
                program,
                target,
                drafter,
                8_192,
                true,
                "draft-mtp",
                8,
            )),
            Err(LlamaCppLaunchError::InvalidQualifiedProfile {
                field: "draft-n-max"
            })
        );
    }

    #[test]
    fn qualified_mode_accepts_the_exact_pinned_llama_version_first_line() {
        let program = Path::new("/opt/llama/llama-server");
        let target = Path::new("/models/target.gguf");
        let drafter = Path::new("/models/drafter.gguf");

        assert!(build_launch_spec(qualified_input(
            program,
            target,
            drafter,
            8_192,
            true,
            "draft-mtp",
            4,
        ))
        .is_ok());
    }

    #[test]
    fn qualified_mode_rejects_any_non_exact_llama_version_first_line() {
        let program = Path::new("/opt/llama/llama-server");
        let target = Path::new("/models/target.gguf");
        let drafter = Path::new("/models/drafter.gguf");

        for version in [
            "version: 10108 (c0bc8591e)",
            "version: 10107 (deadbeef0)",
            "version: 10107",
            "version: 10107 (c0bc8591e) extra",
            "untrusted version: 10107 (c0bc8591e)",
            "version: 10108 (deadbeef0)\nversion: 10107 (c0bc8591e)",
        ] {
            let mut input = qualified_input(program, target, drafter, 8_192, true, "draft-mtp", 4);
            input.engine_version = version;

            assert!(matches!(
                build_launch_spec(input),
                Err(LlamaCppLaunchError::UnqualifiedRuntimeVersion)
            ));
        }
    }

    #[test]
    fn modes_omit_each_others_forbidden_flags_and_wildcard_host() {
        let program = Path::new("/opt/llama/llama-server");
        let target = Path::new("/models/target.gguf");
        let drafter = Path::new("/models/drafter.gguf");
        let unpaired = build_launch_spec(unpaired_input(program, target)).unwrap();
        let qualified = build_launch_spec(qualified_input(
            program,
            target,
            drafter,
            8_192,
            true,
            "draft-mtp",
            4,
        ))
        .unwrap();

        for forbidden in [
            "--jinja",
            "--reasoning",
            "--n-gpu-layers",
            "--fit",
            "--spec-draft-model",
            "--spec-type",
            "--spec-draft-n-max",
            "--n-gpu-layers-draft",
        ] {
            assert!(!unpaired.args.iter().any(|arg| arg == forbidden));
        }
        for forbidden in ["--gpu-layers", "--flash-attn", "0.0.0.0"] {
            assert!(!qualified.args.iter().any(|arg| arg == forbidden));
        }
        assert!(!unpaired.args.iter().any(|arg| arg == "0.0.0.0"));
    }
}
