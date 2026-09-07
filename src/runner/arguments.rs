use super::launch::{Launch, LaunchPolicy, LaunchProfile};
use crate::runtime_fingerprint::{EffectiveProfile, RuntimeFingerprint, ServiceRuntimeProfile};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

pub(crate) fn build_args(launch: &Launch, port: u16) -> Vec<OsString> {
    build_args_for_endpoint(launch, port, None)
}

pub(super) fn build_args_for_endpoint(
    launch: &Launch,
    port: u16,
    unix_socket: Option<&Path>,
) -> Vec<OsString> {
    let mtp = matches!(&launch.profile, LaunchProfile::Gemma4Mtp { .. });
    let service_profile =
        (launch.policy == LaunchPolicy::Service).then(ServiceRuntimeProfile::qualified);
    let mut args = vec![
        "--model".into(),
        launch.model.as_os_str().to_owned(),
        "--alias".into(),
        launch.id.clone().into(),
        "--host".into(),
        unix_socket
            .map(|path| path.as_os_str().to_owned())
            .unwrap_or_else(|| "127.0.0.1".into()),
    ];
    if unix_socket.is_none() {
        args.extend(["--cors-origins".into(), "localhost".into()]);
    }
    args.extend([
        "--no-ui".into(),
        "--port".into(),
        port.to_string().into(),
        "--ctx-size".into(),
        launch.ctx.to_string().into(),
        "--n-gpu-layers".into(),
        service_profile
            .as_ref()
            .map(|profile| profile.gpu_layers.as_str())
            .unwrap_or(if mtp { "all" } else { "99" })
            .into(),
    ]);
    if let Some(profile) = &service_profile {
        args.extend([
            "--n-predict".into(),
            profile.max_output_tokens.to_string().into(),
            "--batch-size".into(),
            profile.batch_size.to_string().into(),
            "--ubatch-size".into(),
            profile.micro_batch_size.to_string().into(),
            "--threads".into(),
            profile.threads.to_string().into(),
            "--threads-batch".into(),
            profile.batch_threads.to_string().into(),
            "--cache-type-k".into(),
            profile.cache_type_k.as_str().into(),
            "--cache-type-v".into(),
            profile.cache_type_v.as_str().into(),
            "--parallel".into(),
            profile.parallel.to_string().into(),
            "--threads-http".into(),
            profile.http_threads.to_string().into(),
            "--poll".into(),
            profile.poll.to_string().into(),
            "--poll-batch".into(),
            profile.batch_poll.to_string().into(),
            "--cache-ram".into(),
            profile.extra_cache_mib.to_string().into(),
        ]);
        if profile.kv_offload {
            args.push("--kv-offload".into());
        } else {
            args.push("--no-kv-offload".into());
        }
        if profile.offline {
            args.push("--offline".into());
        }
    }
    if mtp || service_profile.as_ref().is_some_and(|profile| !profile.fit) {
        args.extend(["--fit".into(), "off".into()]);
    }
    args.extend(["--jinja".into(), "--reasoning".into(), "off".into()]);
    if let Some(draft) = launch.profile.draft() {
        args.extend([
            "--spec-draft-model".into(),
            draft.as_os_str().to_owned(),
            "--spec-type".into(),
            "draft-mtp".into(),
            "--spec-draft-n-max".into(),
            "4".into(),
            "--n-gpu-layers-draft".into(),
            "all".into(),
        ]);
    }
    if let Some(seconds) = launch.policy.sleep_idle_seconds() {
        args.extend(["--sleep-idle-seconds".into(), seconds.to_string().into()]);
    }
    args
}

pub(crate) fn build_persistent_args_for_fingerprint(
    models_root: &Path,
    fingerprint: &RuntimeFingerprint,
    port: u16,
) -> Result<Vec<OsString>, String> {
    fingerprint.validate_persistent_lease(fingerprint.model_id())?;
    if port == 0 {
        return Err("persistent runtime port must be nonzero".into());
    }
    let model_dir = models_root.join(fingerprint.model_id());
    let profile = match fingerprint.effective_profile() {
        EffectiveProfile::Generic => LaunchProfile::generic(),
        EffectiveProfile::Gemma4Mtp => LaunchProfile::gemma4_mtp(Some(
            model_dir.join(
                fingerprint
                    .draft_local_filename()
                    .ok_or_else(|| "MTP runtime fingerprint is missing its draft".to_string())?,
            ),
        )),
        EffectiveProfile::PrimaryOnly => LaunchProfile::gemma4_mtp(None),
    };
    Ok(build_args(
        &Launch {
            server: PathBuf::new(),
            managed_runtime: None,
            model: model_dir.join(fingerprint.primary_local_filename()),
            id: fingerprint.model_id().to_owned(),
            requested_port: port,
            ctx: fingerprint.effective_context(),
            profile,
            policy: LaunchPolicy::PersistentApp,
        },
        port,
    ))
}

pub(super) fn resolve_requested_port(requested: u16) -> Result<u16, String> {
    if requested != 0 {
        return Ok(requested);
    }
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| format!("failed to choose a local port: {error}"))?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|error| format!("failed to read the selected local port: {error}"))
}
