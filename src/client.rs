use std::io;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use signal_terminal::{Query as SignalTerminalRequest, Response as SignalTerminalEvent};

use crate::{
    SocketReplyReader, SocketRequestWriter, TerminalInputGateLease, TerminalInputGateRelease,
    TerminalSize,
};

/// Client to a terminal-cell daemon's two-socket production surface.
///
/// `terminal-cell` exposes a privileged length-prefixed control plane on
/// `control.sock` and a raw attached-viewer byte plane on `data.sock`.
/// Every control verb (capture, subscribe, programmatic/viewer input,
/// gate close/open, resize, wait, worker observation, Signal frames)
/// connects to the control socket. Attach — the latency-sensitive raw
/// byte path — connects to the data socket. The split is wire-level: a
/// client connected to one socket never carries the other plane's
/// traffic.
///
/// Two construction shapes match the two consumer kinds:
///
/// - `TerminalCellSocketClient::new(control, data)` for a full client
///   that can attach viewers and issue control verbs.
/// - `TerminalCellSocketClient::for_control_only(control)` for the
///   command-line tools that never attach — capture, send, resize,
///   wait, exit, gate, Signal. Calling `open_attach_stream` on a
///   control-only client is a logic error: no data socket is bound, so
///   the connect will fail and the error names the missing plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalCellSocketClient {
    control_socket: PathBuf,
    data_socket: Option<PathBuf>,
}

impl TerminalCellSocketClient {
    /// Construct a client that addresses both planes of a terminal-cell
    /// daemon. `control_socket` must point at the daemon's `control.sock`;
    /// `data_socket` must point at the daemon's `data.sock`. Reattach,
    /// attach, and viewer adapters use the data socket; everything else
    /// uses the control socket.
    pub fn new(control_socket: impl Into<PathBuf>, data_socket: impl Into<PathBuf>) -> Self {
        Self {
            control_socket: control_socket.into(),
            data_socket: Some(data_socket.into()),
        }
    }

    /// Construct a control-only client for tools that never attach.
    /// Calling `open_attach_stream` on this client returns an explicit
    /// error rather than silently using the control socket — the typed
    /// missing-data-socket failure is the discipline that keeps the two
    /// planes from being confused at the call site.
    pub fn for_control_only(control_socket: impl Into<PathBuf>) -> Self {
        Self {
            control_socket: control_socket.into(),
            data_socket: None,
        }
    }

    pub fn control_socket(&self) -> &Path {
        self.control_socket.as_path()
    }

    pub fn data_socket(&self) -> Option<&Path> {
        self.data_socket.as_deref()
    }

    pub fn capture(&self) -> io::Result<Vec<u8>> {
        let mut stream = self.connect_control()?;
        SocketRequestWriter::new(&mut stream).write_capture_request()?;
        SocketReplyReader::new(&mut stream).read_snapshot()
    }

    pub fn send_programmatic_input(&self, bytes: &[u8]) -> io::Result<()> {
        let mut stream = self.connect_control()?;
        SocketRequestWriter::new(&mut stream).write_programmatic_input(bytes)?;
        SocketReplyReader::new(&mut stream).read_acceptance()
    }

    pub fn send_viewer_input(&self, bytes: &[u8]) -> io::Result<()> {
        let mut stream = self.connect_control()?;
        SocketRequestWriter::new(&mut stream).write_viewer_input(bytes)?;
        SocketReplyReader::new(&mut stream).read_acceptance()
    }

    pub fn open_attach_stream(&self) -> io::Result<UnixStream> {
        let mut stream = self.connect_data()?;
        SocketRequestWriter::new(&mut stream).write_attach_request()?;
        SocketReplyReader::new(&mut stream).read_attach_acceptance()?;
        Ok(stream)
    }

    pub fn close_human_input(&self) -> io::Result<TerminalInputGateLease> {
        let mut stream = self.connect_control()?;
        SocketRequestWriter::new(&mut stream).write_close_human_input_request()?;
        SocketReplyReader::new(&mut stream).read_gate_lease()
    }

    pub fn open_human_input(
        &self,
        lease: TerminalInputGateLease,
    ) -> io::Result<TerminalInputGateRelease> {
        let mut stream = self.connect_control()?;
        SocketRequestWriter::new(&mut stream).write_open_human_input_request(lease)?;
        SocketReplyReader::new(&mut stream).read_gate_release()
    }

    pub fn resize(&self, size: TerminalSize) -> io::Result<()> {
        let mut stream = self.connect_control()?;
        SocketRequestWriter::new(&mut stream).write_resize_request(size)?;
        SocketReplyReader::new(&mut stream).read_acceptance()
    }

    pub fn wait_for_transcript(&self, bytes: &[u8]) -> io::Result<()> {
        let mut stream = self.connect_control()?;
        SocketRequestWriter::new(&mut stream).write_wait_request(bytes)?;
        SocketReplyReader::new(&mut stream).read_wait_satisfied()
    }

    pub fn wait_for_terminal_exit(&self) -> io::Result<String> {
        let mut stream = self.connect_control()?;
        SocketRequestWriter::new(&mut stream).write_wait_exit_request()?;
        SocketReplyReader::new(&mut stream).read_exit_status()
    }

    pub fn worker_observation_text(&self) -> io::Result<String> {
        let mut stream = self.connect_control()?;
        SocketRequestWriter::new(&mut stream).write_worker_observation_request()?;
        String::from_utf8(SocketReplyReader::new(&mut stream).read_snapshot()?).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("worker observation was not utf-8: {error}"),
            )
        })
    }

    pub fn send_signal_request(
        &self,
        request: SignalTerminalRequest,
    ) -> io::Result<SignalTerminalEvent> {
        let mut stream = self.connect_control()?;
        SocketRequestWriter::new(&mut stream).write_signal_request(request)?;
        SocketReplyReader::new(&mut stream).read_signal_event()
    }

    pub fn subscribe_from_beginning(&self) -> io::Result<UnixStream> {
        let mut stream = self.connect_control()?;
        SocketRequestWriter::new(&mut stream).write_subscription_request()?;
        Ok(stream)
    }

    fn connect_control(&self) -> io::Result<UnixStream> {
        UnixStream::connect(&self.control_socket)
    }

    fn connect_data(&self) -> io::Result<UnixStream> {
        let data_socket = self.data_socket.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "terminal-cell client has no data-socket path; \
                 construct with TerminalCellSocketClient::new for the attach plane",
            )
        })?;
        UnixStream::connect(data_socket)
    }
}
