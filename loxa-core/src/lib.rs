//! Loxa core library.

pub mod detect;
pub mod download;
pub mod hardware;
pub mod plan;
pub mod provider;
pub mod qualification;
pub mod registry;
pub mod supervisor;

#[cfg(test)]
mod tests {
    use crate::detect::{InstallState, LocalToolsReport, RunState, ToolDetection};
    use crate::hardware::HardwareReport;

    #[test]
    fn hardware_report_has_machine_values() {
        let report = HardwareReport::detect();

        assert!(!report.chip.trim().is_empty());
        assert!(report.physical_cores > 0);
        assert!(report.logical_cores > 0);
        assert!(report.ram_total_bytes > 0);
        assert!(report.ram_used_bytes > 0);
        assert!(report.root_disk_total_bytes.is_some());
        assert!(report.root_disk_available_bytes.is_some());
        assert!(report.os_name.trim() != "unknown");
    }

    #[test]
    fn tool_detection_evidence_is_human_readable() {
        let detection = ToolDetection {
            install_state: InstallState::Installed,
            run_state: RunState::NotRunning,
            evidence: vec![
                "binary found on PATH: /usr/local/bin/example".to_string(),
                "port 127.0.0.1:9999 not reachable".to_string(),
            ],
        };

        let rendered = detection.evidence.join("; ");

        assert!(rendered.contains("binary found on PATH"));
        assert!(rendered.contains("127.0.0.1:9999"));
    }

    #[test]
    fn status_enums_render_plain_english() {
        assert_eq!(InstallState::Installed.to_string(), "installed");
        assert_eq!(InstallState::NotInstalled.to_string(), "not installed");
        assert_eq!(RunState::Running.to_string(), "running");
        assert_eq!(RunState::NotRunning.to_string(), "not running");
    }

    #[test]
    fn local_tools_report_contains_default_tool_entries() {
        let report = LocalToolsReport::detect();
        let names = report
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>();

        assert!(names.contains(&"Ollama"));
        assert!(names.contains(&"LM Studio"));
    }
}
