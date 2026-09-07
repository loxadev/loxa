use crate::cli::{ServiceDevArgs, ServiceDevCommand};
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
    let outcome = runtime.block_on(execute(args))?;
    let output = serde_json::to_string_pretty(&outcome).map_err(|error| error.to_string())?;
    anstream::println!("{output}");
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
    }
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
