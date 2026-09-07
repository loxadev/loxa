use super::{InspectArgs, SearchArgs};
use crate::paths::AppPaths;
use crate::{app, cli, discovery};

fn discovery_error_message(kind: discovery::DiscoveryErrorKind) -> &'static str {
    use discovery::DiscoveryErrorKind;

    match kind {
        DiscoveryErrorKind::InvalidQuery => "invalid Hugging Face search query",
        DiscoveryErrorKind::InvalidRepository => {
            "invalid Hugging Face repository; expected owner/repo"
        }
        DiscoveryErrorKind::InvalidRevision => "invalid Hugging Face revision",
        DiscoveryErrorKind::AuthenticationRequired => "Hugging Face authentication is required",
        DiscoveryErrorKind::AccessDenied => "Hugging Face repository access was denied",
        DiscoveryErrorKind::RepositoryNotFound => "Hugging Face repository was not found",
        DiscoveryErrorKind::RevisionNotFound => "Hugging Face revision was not found",
        DiscoveryErrorKind::RateLimited => "Hugging Face rate limit exceeded; try again later",
        DiscoveryErrorKind::RemoteUnavailable => "Hugging Face is unavailable; try again later",
        DiscoveryErrorKind::DeadlineExceeded => "Hugging Face request timed out",
        DiscoveryErrorKind::RedirectRejected => "Hugging Face response was rejected: redirect",
        DiscoveryErrorKind::PaginationRejected => {
            "Hugging Face response was rejected: invalid pagination"
        }
        DiscoveryErrorKind::ResponseTooLarge => {
            "Hugging Face response was rejected: response too large"
        }
        DiscoveryErrorKind::MalformedResponse => {
            "Hugging Face response was rejected: malformed response"
        }
    }
}

fn execute_search<F>(args: cli::SearchArgs, operation: F) -> Result<String, String>
where
    F: FnOnce(
        discovery::SearchModels,
    ) -> Result<discovery::ModelSearchPage, discovery::DiscoveryError>,
{
    let page = operation(discovery::SearchModels::new(args.query))
        .map_err(|error| discovery_error_message(error.kind()).to_owned())?;
    Ok(format_search_results(&page))
}

fn format_search_results(page: &discovery::ModelSearchPage) -> String {
    use std::fmt::Write as _;

    let hits = page.hits();
    let mut output = format!("Repositories ({})\n", hits.len());
    if hits.is_empty() {
        output.push_str("No matching repositories.\n");
        return output;
    }

    for hit in hits {
        let access = match hit.gated() {
            discovery::GatedStatus::Public => "Public",
            discovery::GatedStatus::AutomaticApproval => "Automatic approval",
            discovery::GatedStatus::ManualApproval => "Manual approval",
            discovery::GatedStatus::Unknown => "Unknown",
        };
        let downloads = hit
            .downloads()
            .map_or_else(|| "Unknown".to_owned(), |downloads| downloads.to_string());
        writeln!(
            output,
            "\n{}\n  Access: {access}\n  Downloads: {downloads}\n  Inspect: loxa inspect {}",
            hit.repo(),
            hit.repo(),
        )
        .expect("writing to a String cannot fail");
    }
    output
}

fn execute_inspect<F>(args: cli::InspectArgs, operation: F) -> Result<String, String>
where
    F: FnOnce(
        discovery::InspectRepository,
    ) -> Result<discovery::RepositoryPlan, discovery::DiscoveryError>,
{
    let requested_revision = args.revision.clone();
    let plan = operation(discovery::InspectRepository::new(args.repo, args.revision))
        .map_err(|error| discovery_error_message(error.kind()).to_owned())?;
    Ok(format_repository_plan(&plan, requested_revision.as_deref()))
}

fn format_repository_plan(
    plan: &discovery::RepositoryPlan,
    requested_revision: Option<&str>,
) -> String {
    use std::fmt::Write as _;

    let candidates = plan.candidates();
    let eligible = candidates
        .iter()
        .filter(|candidate| {
            candidate.disposition()
                == discovery::CandidateDisposition::EligibleForDownloadAndLocalValidation
        })
        .count();
    let mut output = format!(
        "Repository: {}\nCommit: {}\nRuntime compatibility: Unknown (local validation not run)\nGGUF candidates ({}; {eligible} eligible)\n",
        plan.repo(),
        plan.commit(),
        candidates.len(),
    );

    for candidate in candidates {
        let size = candidate
            .size()
            .map_or_else(|| "Unknown".to_owned(), |size| format!("{size} bytes"));
        writeln!(
            output,
            "\n{}\n  Size: {size}\n  Packaging: {}",
            candidate.display_path(),
            packaging_label(candidate.disposition()),
        )
        .expect("writing to a String cannot fail");
        if let Some(identity) = candidate.identity() {
            writeln!(output, "  SHA-256: {}", identity.sha256())
                .expect("writing to a String cannot fail");
            if candidate.disposition()
                == discovery::CandidateDisposition::EligibleForDownloadAndLocalValidation
            {
                writeln!(
                    output,
                    "  Pull: {}",
                    inspection_pull_command(plan.repo(), identity.path(), requested_revision)
                )
                .expect("writing to a String cannot fail");
            }
        }
    }
    output
}

fn inspection_pull_command(repo: &str, filename: &str, requested_revision: Option<&str>) -> String {
    let revision = requested_revision
        .map(|revision| format!(" --revision={}", cli::shell_quote(revision)))
        .unwrap_or_default();
    if let Some(reference) = cli::compact_file_reference(repo, filename) {
        format!("loxa pull {}{revision}", cli::shell_quote(&reference))
    } else {
        format!(
            "loxa pull {repo} --file={}{revision}",
            cli::shell_quote(filename)
        )
    }
}

fn packaging_label(disposition: discovery::CandidateDisposition) -> &'static str {
    use discovery::{AuxiliaryRole, CandidateDisposition, UnsupportedPackagingReason};

    match disposition {
        CandidateDisposition::EligibleForDownloadAndLocalValidation => {
            "Eligible for download and local validation"
        }
        CandidateDisposition::UnsupportedPackaging(reason) => match reason {
            UnsupportedPackagingReason::UnsupportedEntryType => {
                "Unsupported (unsupported entry type)"
            }
            UnsupportedPackagingReason::UnsafePath => "Unsupported (unsafe path)",
            UnsupportedPackagingReason::NestedPath => "Unsupported (nested path)",
            UnsupportedPackagingReason::Sharded => "Unsupported (sharded)",
            UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Mtp) => {
                "Unsupported (MTP auxiliary)"
            }
            UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Draft) => {
                "Unsupported (draft auxiliary)"
            }
            UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Mmproj) => {
                "Unsupported (mmproj auxiliary)"
            }
            UnsupportedPackagingReason::MissingSize => "Unsupported (missing size)",
            UnsupportedPackagingReason::ZeroSize => "Unsupported (zero size)",
            UnsupportedPackagingReason::MissingLfsIdentity => "Unsupported (missing LFS identity)",
            UnsupportedPackagingReason::SizeMismatch => "Unsupported (size mismatch)",
            UnsupportedPackagingReason::InvalidLfsSha256 => "Unsupported (invalid LFS SHA-256)",
        },
    }
}

pub(super) fn run_search(args: SearchArgs, paths: AppPaths) -> Result<i32, String> {
    let service = app::AppService::from_paths(paths);
    let output = execute_search(args, |request| service.search_models(request))?;
    anstream::print!("{output}");
    Ok(0)
}

pub(super) fn run_inspect(args: InspectArgs, paths: AppPaths) -> Result<i32, String> {
    let service = app::AppService::from_paths(paths);
    let output = execute_inspect(args, |request| service.inspect_repository(request))?;
    anstream::print!("{output}");
    Ok(0)
}

#[cfg(test)]
#[path = "discovery/tests.rs"]
mod tests;
