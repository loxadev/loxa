use super::{
    DurableHealthMonitor, DurableHealthShutdownFailure, NodeOwnerGuard, NodeOwnerShutdownFailure,
    PublicationGate,
};
use crate::chat_history::{ChatHistoryShutdownFailure, ChatHistoryWorker};
use crate::chat_routes::{ChatRoutesShutdownFailure, ChatRoutesState};
use crate::control_state::{
    ControlStateHandle, ControlStateShutdownFailure, ControlStateStartupFailure,
};
#[cfg(test)]
use crate::download_control::ExecutionShutdownFailureClass;
use crate::download_control::RetainedExecutionShutdown;
use crate::RunTermination;
use loxa_core::gateway::{GatewayServer, GatewayShutdownFailure, GatewayState};
use loxa_core::supervisor::ManagedRun;
use std::io;
use std::mem::ManuallyDrop;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ShutdownDeadlines {
    pub(crate) admission: Instant,
    pub(crate) signal: Instant,
    pub(crate) verification: Instant,
    pub(crate) download: Instant,
    pub(crate) lifecycle: Instant,
    pub(crate) repository: Instant,
}

impl ShutdownDeadlines {
    pub(crate) fn from_started(started: Instant) -> Self {
        Self {
            admission: started + Duration::from_secs(2),
            signal: started + Duration::from_secs(3),
            verification: started + Duration::from_secs(6),
            download: started + Duration::from_secs(10),
            lifecycle: started + Duration::from_secs(18),
            repository: started + Duration::from_secs(20),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ShutdownFailureClass {
    ExactChild,
    Artifact,
    DurableRepository,
    Lifecycle,
    Download,
    Verification,
    Routes,
    OrdinaryCancellation,
}

pub(crate) const SHUTDOWN_FAILURE_PRECEDENCE: [ShutdownFailureClass; 8] = [
    ShutdownFailureClass::ExactChild,
    ShutdownFailureClass::Artifact,
    ShutdownFailureClass::DurableRepository,
    ShutdownFailureClass::Lifecycle,
    ShutdownFailureClass::Download,
    ShutdownFailureClass::Verification,
    ShutdownFailureClass::Routes,
    ShutdownFailureClass::OrdinaryCancellation,
];

pub(crate) fn shutdown_failure_rank(class: ShutdownFailureClass) -> usize {
    SHUTDOWN_FAILURE_PRECEDENCE
        .iter()
        .position(|candidate| *candidate == class)
        .expect("every shutdown failure class has fixed precedence")
}

pub(crate) struct FatalShutdownParts {
    pub(crate) diagnostic: String,
    pub(crate) gateway: Option<GatewayServer>,
    pub(crate) gateway_failure: Option<GatewayShutdownFailure>,
    pub(crate) history: Option<ChatHistoryWorker>,
    pub(crate) history_failure: Option<ChatHistoryShutdownFailure>,
    pub(crate) health: Option<DurableHealthMonitor>,
    pub(crate) health_failure: Option<DurableHealthShutdownFailure>,
    pub(crate) execution: Option<Box<RetainedExecutionShutdown>>,
    pub(crate) control_failure: Option<ControlStateShutdownFailure>,
    pub(crate) control_startup_failure: Option<ControlStateStartupFailure>,
    pub(crate) control: Option<ControlStateHandle>,
    pub(crate) unloaded_run: Option<ManagedRun>,
    pub(crate) publication: Option<PublicationGate>,
    pub(crate) owner: Option<NodeOwnerGuard>,
    pub(crate) owner_failure: Option<Box<NodeOwnerShutdownFailure>>,
    pub(crate) routes: Option<ChatRoutesState>,
    pub(crate) routes_failure: Option<ChatRoutesShutdownFailure>,
    pub(crate) gateway_state: Option<GatewayState>,
}

#[must_use = "fatal shutdown ownership must be retained until process exit"]
pub struct FatalShutdown {
    _diagnostic: ManuallyDrop<String>,
    _gateway: ManuallyDrop<Option<GatewayServer>>,
    _gateway_failure: ManuallyDrop<Option<GatewayShutdownFailure>>,
    _history: ManuallyDrop<Option<ChatHistoryWorker>>,
    _history_failure: ManuallyDrop<Option<ChatHistoryShutdownFailure>>,
    _health: ManuallyDrop<Option<DurableHealthMonitor>>,
    _health_failure: ManuallyDrop<Option<DurableHealthShutdownFailure>>,
    _execution: ManuallyDrop<Option<Box<RetainedExecutionShutdown>>>,
    _control_failure: ManuallyDrop<Option<ControlStateShutdownFailure>>,
    _control_startup_failure: ManuallyDrop<Option<ControlStateStartupFailure>>,
    _control: ManuallyDrop<Option<ControlStateHandle>>,
    _unloaded_run: ManuallyDrop<Option<ManagedRun>>,
    _publication: ManuallyDrop<Option<PublicationGate>>,
    _owner: ManuallyDrop<Option<NodeOwnerGuard>>,
    _owner_failure: ManuallyDrop<Option<Box<NodeOwnerShutdownFailure>>>,
    _routes: ManuallyDrop<Option<ChatRoutesState>>,
    _routes_failure: ManuallyDrop<Option<ChatRoutesShutdownFailure>>,
    _gateway_state: ManuallyDrop<Option<GatewayState>>,
}

impl FatalShutdown {
    pub(crate) fn new(parts: FatalShutdownParts) -> Self {
        Self {
            _diagnostic: ManuallyDrop::new(parts.diagnostic),
            _gateway: ManuallyDrop::new(parts.gateway),
            _gateway_failure: ManuallyDrop::new(parts.gateway_failure),
            _history: ManuallyDrop::new(parts.history),
            _history_failure: ManuallyDrop::new(parts.history_failure),
            _health: ManuallyDrop::new(parts.health),
            _health_failure: ManuallyDrop::new(parts.health_failure),
            _execution: ManuallyDrop::new(parts.execution),
            _control_failure: ManuallyDrop::new(parts.control_failure),
            _control_startup_failure: ManuallyDrop::new(parts.control_startup_failure),
            _control: ManuallyDrop::new(parts.control),
            _unloaded_run: ManuallyDrop::new(parts.unloaded_run),
            _publication: ManuallyDrop::new(parts.publication),
            _owner: ManuallyDrop::new(parts.owner),
            _owner_failure: ManuallyDrop::new(parts.owner_failure),
            _routes: ManuallyDrop::new(parts.routes),
            _routes_failure: ManuallyDrop::new(parts.routes_failure),
            _gateway_state: ManuallyDrop::new(parts.gateway_state),
        }
    }

    pub fn exit(self, code: i32) -> ! {
        let _retained = ManuallyDrop::new(self);
        std::process::exit(code)
    }

    #[cfg(test)]
    pub(crate) fn diagnostic_for_test(&self) -> &str {
        self._diagnostic.as_str()
    }

    #[cfg(test)]
    pub(crate) fn retained_classes_for_test(&self) -> Vec<ShutdownFailureClass> {
        let mut classes = Vec::new();
        if self._gateway_failure.is_some() || self._routes_failure.is_some() {
            classes.push(ShutdownFailureClass::Routes);
        }
        if self._history_failure.is_some() || self._control_failure.is_some() {
            classes.push(ShutdownFailureClass::DurableRepository);
        }
        if self._health_failure.is_some() {
            classes.push(ShutdownFailureClass::Routes);
        }
        if self._execution.is_some() {
            let execution = self._execution.as_ref().expect("execution retained");
            classes.push(match execution.diagnostics().primary() {
                ExecutionShutdownFailureClass::ExactChild => ShutdownFailureClass::ExactChild,
                ExecutionShutdownFailureClass::Artifact => ShutdownFailureClass::Artifact,
                ExecutionShutdownFailureClass::DurableRepository => {
                    ShutdownFailureClass::DurableRepository
                }
                ExecutionShutdownFailureClass::Lifecycle => ShutdownFailureClass::Lifecycle,
                ExecutionShutdownFailureClass::Download => ShutdownFailureClass::Download,
                ExecutionShutdownFailureClass::Verification => ShutdownFailureClass::Verification,
            });
        }
        if self._owner_failure.is_some() {
            classes.push(ShutdownFailureClass::ExactChild);
        }
        classes
    }
}

impl Drop for FatalShutdown {
    fn drop(&mut self) {
        std::process::abort();
    }
}

#[must_use = "shutdown outcomes must be handled at an executable boundary"]
pub enum ShutdownResult {
    Stopped(RunTermination),
    Failed(io::Error),
    RequiresProcessExit(Box<FatalShutdown>),
}

impl ShutdownResult {
    #[cfg(test)]
    pub(crate) fn unwrap(self) -> RunTermination {
        self.expect("shutdown failed")
    }

    #[cfg(test)]
    pub(crate) fn expect(self, message: &str) -> RunTermination {
        match self {
            Self::Stopped(termination) => termination,
            Self::Failed(error) => panic!("{message}: {error}"),
            Self::RequiresProcessExit(fatal) => {
                panic!(
                    "{message}: process exit required: {}",
                    fatal.diagnostic_for_test()
                )
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn expect_err(self, message: &str) -> io::Error {
        match self {
            Self::Failed(error) => error,
            Self::Stopped(_) => panic!("{message}: shutdown stopped successfully"),
            Self::RequiresProcessExit(fatal) => {
                panic!(
                    "{message}: process exit required: {}",
                    fatal.diagnostic_for_test()
                )
            }
        }
    }
}
