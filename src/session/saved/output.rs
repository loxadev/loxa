use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd, RawFd};

use tokio::io::unix::AsyncFd;
use tokio::io::Interest;

use super::signals::{Interrupt, SessionSignals};

const MAX_WRITE_STEPS: usize = 16;

#[derive(Debug, Eq, PartialEq)]
pub(super) enum OutputError {
    Interrupted(Interrupt),
    Failed,
}

struct FlaggedDescriptor {
    descriptor: OwnedFd,
    original_flags: libc::c_int,
    restored: bool,
}

impl FlaggedDescriptor {
    fn stdout() -> Result<Self, OutputError> {
        let descriptor = unsafe { BorrowedFd::borrow_raw(libc::STDOUT_FILENO) }
            .try_clone_to_owned()
            .map_err(|_| OutputError::Failed)?;
        Self::new(descriptor)
    }

    fn new(descriptor: OwnedFd) -> Result<Self, OutputError> {
        let original_flags = unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_GETFL) };
        if original_flags == -1
            || unsafe {
                libc::fcntl(
                    descriptor.as_raw_fd(),
                    libc::F_SETFL,
                    original_flags | libc::O_NONBLOCK,
                )
            } == -1
        {
            return Err(OutputError::Failed);
        }
        Ok(Self {
            descriptor,
            original_flags,
            restored: false,
        })
    }

    fn restore(&mut self) -> Result<(), OutputError> {
        if self.restored {
            return Ok(());
        }
        if unsafe {
            libc::fcntl(
                self.descriptor.as_raw_fd(),
                libc::F_SETFL,
                self.original_flags,
            )
        } == -1
        {
            return Err(OutputError::Failed);
        }
        self.restored = true;
        Ok(())
    }
}

impl AsRawFd for FlaggedDescriptor {
    fn as_raw_fd(&self) -> RawFd {
        self.descriptor.as_raw_fd()
    }
}

impl Drop for FlaggedDescriptor {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

pub(super) struct TerminalOutput {
    descriptor: Option<AsyncFd<FlaggedDescriptor>>,
}

impl TerminalOutput {
    pub(super) fn stdout() -> Result<Self, OutputError> {
        let descriptor = AsyncFd::with_interest(FlaggedDescriptor::stdout()?, Interest::WRITABLE)
            .map_err(|_| OutputError::Failed)?;
        Ok(Self {
            descriptor: Some(descriptor),
        })
    }

    #[cfg(test)]
    pub(super) fn from_fd(descriptor: OwnedFd) -> Result<Self, OutputError> {
        let descriptor =
            AsyncFd::with_interest(FlaggedDescriptor::new(descriptor)?, Interest::WRITABLE)
                .map_err(|_| OutputError::Failed)?;
        Ok(Self {
            descriptor: Some(descriptor),
        })
    }

    pub(super) async fn write_sanitized(
        &self,
        text: &str,
        signals: &SessionSignals,
        tag: u64,
    ) -> Result<(), OutputError> {
        let sanitized = crate::ui::sanitize_terminal(text);
        self.write_all(sanitized.as_bytes(), signals, tag).await
    }

    pub(super) async fn write_all(
        &self,
        bytes: &[u8],
        signals: &SessionSignals,
        tag: u64,
    ) -> Result<(), OutputError> {
        let descriptor = self.descriptor.as_ref().ok_or(OutputError::Failed)?;
        let mut offset = 0;
        let mut steps = 0;
        while offset < bytes.len() {
            let mut ready = tokio::select! {
                interrupt = signals.interrupted(tag) => {
                    return Err(OutputError::Interrupted(interrupt));
                }
                ready = descriptor.writable() => ready.map_err(|_| OutputError::Failed)?,
            };
            match ready.try_io(|inner| write_once(inner.get_ref().as_raw_fd(), &bytes[offset..])) {
                Ok(Ok(0)) => return Err(OutputError::Failed),
                Ok(Ok(written)) => {
                    offset = offset.checked_add(written).ok_or(OutputError::Failed)?;
                    steps += 1;
                    if steps == MAX_WRITE_STEPS && offset < bytes.len() {
                        steps = 0;
                        tokio::task::yield_now().await;
                    }
                }
                Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => continue,
                Ok(Err(_)) => return Err(OutputError::Failed),
                Err(_) => continue,
            }
        }
        Ok(())
    }

    pub(super) fn restore(mut self) -> Result<(), OutputError> {
        let descriptor = self.descriptor.take().ok_or(OutputError::Failed)?;
        let mut descriptor = descriptor.into_inner();
        descriptor.restore()
    }
}

fn write_once(descriptor: RawFd, bytes: &[u8]) -> io::Result<usize> {
    let written = unsafe { libc::write(descriptor, bytes.as_ptr().cast(), bytes.len()) };
    if written == -1 {
        Err(io::Error::last_os_error())
    } else {
        usize::try_from(written).map_err(|_| io::Error::other("invalid terminal write count"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::FromRawFd;

    #[test]
    fn shared_descriptor_flags_are_restored_before_readline_reentry() {
        let mut pipe = [-1; 2];
        assert_eq!(unsafe { libc::pipe(pipe.as_mut_ptr()) }, 0);
        let reader = unsafe { OwnedFd::from_raw_fd(pipe[0]) };
        let writer = unsafe { OwnedFd::from_raw_fd(pipe[1]) };
        let original = unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETFL) };
        assert_ne!(original, -1);
        let duplicate = unsafe { libc::dup(writer.as_raw_fd()) };
        assert_ne!(duplicate, -1);
        let duplicate = unsafe { OwnedFd::from_raw_fd(duplicate) };
        let mut flagged = FlaggedDescriptor::new(duplicate).unwrap();
        assert_ne!(
            unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );
        flagged.restore().unwrap();
        assert_eq!(
            unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETFL) },
            original
        );
        drop(flagged);
        drop(writer);
        drop(reader);
    }
}
