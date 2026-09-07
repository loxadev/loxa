#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerCredentials {
    pub uid: u32,
    pub pid: u32,
}

pub fn peer_credentials(stream: &tokio::net::UnixStream) -> Result<PeerCredentials, String> {
    let credentials = stream.peer_cred().map_err(|error| error.to_string())?;
    let pid = credentials
        .pid()
        .and_then(|pid| u32::try_from(pid).ok())
        .filter(|pid| *pid != 0)
        .ok_or_else(|| "Unix socket peer has no valid process identity".to_string())?;
    Ok(PeerCredentials {
        uid: credentials.uid(),
        pid,
    })
}

pub(crate) fn current_uid() -> u32 {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() }
}

#[cfg(target_os = "linux")]
pub(crate) fn peer_executable(pid: u32) -> Result<std::path::PathBuf, String> {
    std::fs::read_link(format!("/proc/{pid}/exe")).map_err(|error| error.to_string())
}

#[cfg(target_os = "macos")]
pub(crate) fn peer_executable(pid: u32) -> Result<std::path::PathBuf, String> {
    use std::os::unix::ffi::OsStringExt as _;

    let pid = i32::try_from(pid).map_err(|_| "invalid peer pid".to_string())?;
    let mut buffer = [0_u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    // SAFETY: `buffer` is writable for the exact maximum capacity supplied.
    let written = unsafe {
        libc::proc_pidpath(
            pid,
            buffer.as_mut_ptr().cast(),
            u32::try_from(buffer.len()).expect("pid path buffer length fits u32"),
        )
    };
    if written <= 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    let written = usize::try_from(written)
        .ok()
        .filter(|written| *written <= buffer.len())
        .ok_or_else(|| "invalid peer path".to_string())?;
    Ok(std::path::PathBuf::from(std::ffi::OsString::from_vec(
        buffer[..written].to_vec(),
    )))
}
