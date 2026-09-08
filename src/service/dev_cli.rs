use crate::cli::{ServiceDevArgs, ServiceDevCommand, ServiceTargetArgs};
use loxa_ipc::{ConnectMode, OperationTarget, ReplyOutcome, RuntimePhase, ServiceCommand};

pub(crate) fn run(args: ServiceDevArgs) -> Result<i32, String> {
    if let ServiceDevCommand::Init { origin } = &args.command {
        match origin {
            Some(origin) => super::initialize_development_root_from(&args.data_root, origin)?,
            None => super::initialize_development_root(&args.data_root)?,
        };
        anstream::println!("Initialized {}", args.data_root.display());
        return Ok(0);
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    if matches!(args.command, ServiceDevCommand::Api { .. }) {
        return runtime.block_on(run_api(args));
    }
    if let ServiceDevCommand::Chat {
        model_id,
        max_tokens,
        target,
    } = &args.command
    {
        let client = runtime.block_on(development_client(&args.data_root))?;
        let target = target_from_args(target)
            .ok_or_else(|| "service chat requires an exact copied service target".to_string())?;
        return crate::session::run_service(client, model_id.clone(), target, *max_tokens);
    }
    let outcome = runtime.block_on(execute(args))?;
    let output = serde_json::to_string_pretty(&outcome).map_err(|error| error.to_string())?;
    anstream::println!("{output}");
    Ok(0)
}

async fn run_api(args: ServiceDevArgs) -> Result<i32, String> {
    let ServiceDevCommand::Api {
        model_id,
        endpoint,
        data,
        target,
    } = args.command
    else {
        unreachable!()
    };
    let client = development_client(&args.data_root).await?;
    let expected = target_from_args(&target);
    let (method, path, body, model) = match endpoint.as_str() {
        "models" => (
            hyper::Method::GET,
            "/v1/models",
            Vec::new(),
            Some(model_id.clone()),
        ),
        "chat-completions" => {
            let body = data.expect("validated --data").into_bytes();
            let model = serde_json::from_slice::<serde_json::Value>(&body)
                .ok()
                .and_then(|value| {
                    value
                        .get("model")
                        .and_then(|value| value.as_str())
                        .map(str::to_owned)
                })
                .ok_or_else(|| "chat-completions --data must include a string model".to_string())?;
            if model != model_id {
                return Err("chat-completions model must match the selected service model".into());
            }
            (
                hyper::Method::POST,
                "/v1/chat/completions",
                body,
                Some(model),
            )
        }
        _ => unreachable!("validated endpoint"),
    };
    super::attachment::api(
        &client,
        model.as_deref(),
        expected.as_ref(),
        method,
        path,
        body,
    )
    .await?;
    Ok(0)
}

async fn execute(args: ServiceDevArgs) -> Result<ReplyOutcome, String> {
    match args.command {
        ServiceDevCommand::Init { .. } => unreachable!("init returned before runtime setup"),
        ServiceDevCommand::Start => {
            super::development_request(
                &args.data_root,
                ConnectMode::EnsureStarted,
                ServiceCommand::Status,
            )
            .await
        }
        ServiceDevCommand::Status => {
            super::development_request(
                &args.data_root,
                ConnectMode::ObserveExisting,
                ServiceCommand::Status,
            )
            .await
        }
        ServiceDevCommand::Load { model_id } => {
            super::development_request(
                &args.data_root,
                ConnectMode::EnsureStarted,
                ServiceCommand::Load { model_id },
            )
            .await
        }
        ServiceDevCommand::Unload => unload(&args.data_root).await,
        ServiceDevCommand::Stop => {
            super::development_request(
                &args.data_root,
                ConnectMode::ObserveExisting,
                ServiceCommand::StopService,
            )
            .await
        }
        ServiceDevCommand::Api { .. } | ServiceDevCommand::Chat { .. } => unreachable!(),
    }
}

async fn development_client(root: &std::path::Path) -> Result<loxa_ipc::ServiceClient, String> {
    let normal = crate::paths::AppPaths::from_env()?;
    loxa_ipc::ServiceClient::load_async(root, Some(&normal.root), super::BUILD_ID).await
}

fn target_from_args(args: &ServiceTargetArgs) -> Option<OperationTarget> {
    Some(OperationTarget {
        boot_epoch: args.boot_epoch.clone()?,
        task_id: args.task_id.clone()?,
        generation: args.generation.clone()?,
    })
}

async fn unload(data_root: &std::path::Path) -> Result<ReplyOutcome, String> {
    let observed = super::development_request(
        data_root,
        ConnectMode::ObserveExisting,
        ServiceCommand::Status,
    )
    .await?;
    let ReplyOutcome::Status(status) = observed else {
        return Err("service returned an invalid status outcome".into());
    };
    let (task_id, generation) = match status.runtime.phase {
        RuntimePhase::Starting {
            task_id,
            generation,
            ..
        }
        | RuntimePhase::Ready {
            task_id,
            generation,
            ..
        }
        | RuntimePhase::Stopping {
            task_id,
            generation,
            ..
        }
        | RuntimePhase::CleanupFailed {
            task_id,
            generation,
            ..
        } => (task_id, generation),
        _ => return Err("no unloadable service operation is active".into()),
    };
    super::development_request(
        data_root,
        ConnectMode::ObserveExisting,
        ServiceCommand::Unload {
            target: OperationTarget {
                boot_epoch: status.runtime.boot_epoch,
                task_id,
                generation,
            },
        },
    )
    .await
}
