use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use signal_terminal as terminal_signal;
use terminal_cell::{
    Configuration, SocketReplyReader, SocketRequestWriter, TerminalCellSocketClient, TerminalSize,
};

/// Widen a `u8` byte literal into the schema-emitted `Integer` (`u64`)
/// byte vector the signal-terminal contract carries on its byte-bearing
/// fields (`TerminalInputBytes`, `PromptPatternBytes`).
fn signal_bytes(bytes: &[u8]) -> Vec<i64> {
    bytes.iter().map(|byte| i64::from(*byte)).collect()
}

fn signal_terminal(name: &str) -> terminal_signal::Terminal {
    name.to_string()
}

fn signal_regex_pattern(bytes: &[u8]) -> terminal_signal::Pattern {
    terminal_signal::PromptPattern::RegexSuffix(signal_bytes(bytes))
}

fn signal_input_bytes(bytes: &[u8]) -> terminal_signal::InputBytes {
    signal_bytes(bytes)
}

fn signal_prompt_identifier_selection(
    pattern_identifier: terminal_signal::PatternIdentifier,
) -> terminal_signal::PromptPatternIdentifierSelection {
    Some(pattern_identifier)
}

fn signal_cached_human_byte_count(bytes: &terminal_signal::CachedHumanBytes) -> u64 {
    *bytes as u64
}

struct DaemonFixture {
    child: Child,
    root: PathBuf,
    control_socket: PathBuf,
    data_socket: PathBuf,
}

impl DaemonFixture {
    fn binary(name: &str) -> String {
        env::var(format!("CARGO_BIN_EXE_{name}")).expect("cargo exposes binary path to test")
    }

    fn spawn(name: &str) -> Self {
        Self::spawn_command(name, &Self::binary("agent-terminal-fixture"), &[])
    }

    fn spawn_shell(name: &str, script: &str) -> Self {
        let shell = env::var("TERMINAL_CELL_TEST_SHELL").unwrap_or_else(|_| "bash".to_string());
        Self::spawn_command(name, &shell, &["-lc", script])
    }

    fn spawn_command(name: &str, program: &str, arguments: &[&str]) -> Self {
        let root = env::temp_dir().join(format!("terminal-cell-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("daemon test root created");
        let control_socket = root.join("control.sock");
        let data_socket = root.join("data.sock");
        let configuration_path = root.join("daemon-configuration.rkyv");

        let configuration = Configuration::new(
            control_socket.to_string_lossy().into_owned(),
            data_socket.to_string_lossy().into_owned(),
            program.to_owned(),
            arguments
                .iter()
                .map(|argument| argument.to_string())
                .collect::<Vec<_>>(),
        );
        fs::write(
            &configuration_path,
            configuration
                .to_signal_bytes()
                .expect("daemon configuration encodes to rkyv"),
        )
        .expect("daemon configuration file written");

        let mut command = Command::new(Self::binary("terminal-cell-daemon"));
        command.arg(&configuration_path);
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("daemon spawned");

        let stdout = child.stdout.take().expect("daemon stdout is captured");
        let mut stderr = child.stderr.take().expect("daemon stderr is captured");
        let mut reader = BufReader::new(stdout);
        let mut ready = String::new();
        reader
            .read_line(&mut ready)
            .expect("daemon announces readiness");
        let mut error_output = String::new();
        if ready.is_empty() {
            let _ = stderr.read_to_string(&mut error_output);
        }
        assert!(
            ready.contains(control_socket.to_string_lossy().as_ref())
                && ready.contains(data_socket.to_string_lossy().as_ref()),
            "daemon ready line names both socket paths: {ready}; stderr: {error_output}"
        );

        Self {
            child,
            root,
            control_socket,
            data_socket,
        }
    }

    fn wait_for_text(&self, text: &str) {
        let status = Command::new(Self::binary("terminal-cell-wait"))
            .arg("--control-socket")
            .arg(&self.control_socket)
            .arg("--text")
            .arg(text)
            .status()
            .expect("wait command runs");
        assert!(status.success(), "wait command succeeded");
    }

    fn send_line(&self, line: &str) {
        let status = Command::new(Self::binary("terminal-cell-send"))
            .arg("--control-socket")
            .arg(&self.control_socket)
            .arg("--line")
            .arg(line)
            .status()
            .expect("send command runs");
        assert!(status.success(), "send command succeeded");
    }

    fn resize(&self, rows: u16, columns: u16) {
        TerminalCellSocketClient::for_control_only(&self.control_socket)
            .resize(TerminalSize::new(rows, columns))
            .expect("resize request accepted");
    }

    fn client(&self) -> TerminalCellSocketClient {
        TerminalCellSocketClient::new(self.control_socket.clone(), self.data_socket.clone())
    }

    fn open_attach_stream(&self) -> std::os::unix::net::UnixStream {
        self.client()
            .open_attach_stream()
            .expect("attach stream opened")
    }

    fn capture_text(&self) -> String {
        let output = Command::new(Self::binary("terminal-cell-capture"))
            .arg("--control-socket")
            .arg(&self.control_socket)
            .output()
            .expect("capture command runs");
        assert!(output.status.success(), "capture command succeeded");
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    fn view_once_text(&self) -> String {
        let output = Command::new(Self::binary("terminal-cell-view"))
            .arg("--control-socket")
            .arg(&self.control_socket)
            .arg("--data-socket")
            .arg(&self.data_socket)
            .arg("--once")
            .output()
            .expect("view command runs");
        assert!(output.status.success(), "view --once command succeeded");
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    fn wait_for_exit_status(&self) -> String {
        let output = Command::new(Self::binary("terminal-cell-exit"))
            .arg("--control-socket")
            .arg(&self.control_socket)
            .output()
            .expect("exit command runs");
        assert!(output.status.success(), "exit command succeeded");
        String::from_utf8_lossy(&output.stdout).into_owned()
    }
}

impl Drop for DaemonFixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn control_socket_mode_is_enforced_by_daemon() {
    let daemon = DaemonFixture::spawn("control-socket-mode");
    let control_mode = fs::metadata(&daemon.control_socket)
        .expect("control socket metadata readable")
        .permissions()
        .mode()
        & 0o777;
    let data_mode = fs::metadata(&daemon.data_socket)
        .expect("data socket metadata readable")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(control_mode, 0o600, "control socket is mode 0600");
    assert_eq!(data_mode, 0o600, "data socket is mode 0600");
}

/// Architectural-truth witness for the two-plane wire boundary mandated
/// by `terminal-cell/ARCHITECTURE.md` §1: the control socket must not
/// accept an Attach request, and the data socket must not accept any
/// control-plane request. Both rejections arrive as `ATTACH_REJECTED`
/// before any byte transport begins, so the client observes the typed
/// failure rather than a stuck-stream silent confusion.
#[test]
fn control_socket_rejects_attach_and_data_socket_rejects_non_attach() {
    use std::os::unix::net::UnixStream;
    use terminal_cell::SocketReplyReader;
    use terminal_cell::SocketRequestWriter;

    let daemon = DaemonFixture::spawn("plane-isolation");
    daemon.wait_for_text("agent-ready");

    // Attach on the control socket: a wire-level violation. The control
    // listener writes ATTACH_REJECTED naming the wrong plane before
    // closing.
    let mut control_attach_stream =
        UnixStream::connect(&daemon.control_socket).expect("control socket reachable");
    SocketRequestWriter::new(&mut control_attach_stream)
        .write_attach_request()
        .expect("attach request writes");
    let control_attach_error = SocketReplyReader::new(&mut control_attach_stream)
        .read_attach_acceptance()
        .expect_err("control socket rejects attach");
    assert_eq!(
        control_attach_error.kind(),
        std::io::ErrorKind::ConnectionRefused,
        "control socket emits a typed attach rejection"
    );
    assert!(
        control_attach_error.to_string().contains("control socket"),
        "rejection names the violated plane: {control_attach_error}"
    );

    // Programmatic input on the data socket: a wire-level violation. The
    // data listener writes ATTACH_REJECTED before closing.
    let mut data_input_stream =
        UnixStream::connect(&daemon.data_socket).expect("data socket reachable");
    SocketRequestWriter::new(&mut data_input_stream)
        .write_programmatic_input(b"this byte tag must not be honored on the data plane\r")
        .expect("input request writes");
    let data_input_error = SocketReplyReader::new(&mut data_input_stream)
        .read_attach_acceptance()
        .expect_err("data socket rejects non-attach");
    assert_eq!(
        data_input_error.kind(),
        std::io::ErrorKind::ConnectionRefused,
        "data socket emits a typed attach rejection"
    );
    assert!(
        data_input_error.to_string().contains("data socket"),
        "rejection names the violated plane: {data_input_error}"
    );

    // The agent transcript must not contain the bytes that violated the
    // data plane: the data socket never wrote them to the PTY.
    let transcript = daemon.capture_text();
    assert!(
        !transcript.contains("this byte tag must not be honored on the data plane"),
        "violation bytes never reach the child PTY: {transcript}"
    );
}

#[test]
fn daemon_accepts_programmatic_prompt_and_capture_reads_transcript() {
    let daemon = DaemonFixture::spawn("capture");

    daemon.wait_for_text("agent-ready");
    daemon.send_line("hello attached daemon");
    daemon.wait_for_text("agent-response: hello attached daemon");
    daemon.send_line("/usage");
    daemon.wait_for_text("usage-window: five-hour=73 weekly=41");

    let transcript = daemon.capture_text();
    assert!(transcript.contains("agent-response: hello attached daemon"));
    assert!(transcript.contains("usage-window: five-hour=73 weekly=41"));
}

#[test]
fn attach_view_replays_transcript_without_owning_the_child() {
    let daemon = DaemonFixture::spawn("view");

    daemon.wait_for_text("agent-ready");
    daemon.send_line("hello replayed viewer");
    daemon.wait_for_text("agent-response: hello replayed viewer");

    let replay = daemon.view_once_text();
    assert!(replay.contains("agent-ready"));
    assert!(replay.contains("agent-response: hello replayed viewer"));
}

#[test]
fn daemon_exposes_terminal_exit_status() {
    let daemon = DaemonFixture::spawn_shell("exit", "printf 'done-exiting\\n'; exit 7");

    daemon.wait_for_text("done-exiting");

    let status = daemon.wait_for_exit_status();
    assert!(
        !status.trim().is_empty(),
        "terminal-cell-exit prints the child status"
    );
}

#[test]
fn daemon_resizes_the_owned_pty() {
    let daemon = DaemonFixture::spawn_shell("resize", "stty size; IFS= read -r _; stty size");

    daemon.wait_for_text("24 80");
    daemon.resize(31, 97);
    daemon.send_line("");
    daemon.wait_for_text("31 97");

    let transcript = daemon.capture_text();
    assert!(transcript.contains("24 80"));
    assert!(transcript.contains("31 97"));
}

#[test]
fn attach_stream_is_raw_bidirectional_byte_path() {
    let daemon = DaemonFixture::spawn("attach-stream");

    daemon.wait_for_text("agent-ready");
    let mut stream = daemon.open_attach_stream();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("attach stream read timeout set");

    let mut output = Vec::new();
    let mut buffer = [0_u8; 4096];
    for _ in 0..8 {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => output.extend_from_slice(&buffer[..count]),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(error) => panic!("attach stream read failed: {error}"),
        }
        if String::from_utf8_lossy(&output).contains("agent-ready") {
            break;
        }
    }
    assert!(
        String::from_utf8_lossy(&output).contains("agent-ready"),
        "attached stream emits transcript replay before live bytes"
    );

    stream
        .write_all(b"hello raw attach stream\r")
        .expect("attach stream accepts input bytes");
    daemon.wait_for_text("agent-response: hello raw attach stream");

    let transcript = daemon.capture_text();
    assert!(transcript.contains("agent-response: hello raw attach stream"));
}

/// Architectural-truth witness for the input-gate's multi-client
/// serialization promise stated in `terminal-cell/ARCHITECTURE.md` §3:
/// when two independent harness clients try to acquire the input gate
/// simultaneously, the first lease is granted, the second observes the
/// already-closed state and is rejected with a typed
/// `InputGateAlreadyClosed`. Release of the first lease reopens the
/// gate for a follow-up acquire.
#[test]
fn input_gate_serializes_two_harness_lease_attempts() {
    let daemon = DaemonFixture::spawn("multi-harness-gate");
    daemon.wait_for_text("agent-ready");

    let client = daemon.client();
    let first_lease = client
        .close_human_input()
        .expect("first harness acquires the input gate");

    // The second harness opens its own connection. The control plane
    // serializes both: the second one returns the typed
    // `InputGateAlreadyClosed` shape (mapped to `ATTACH_REJECTED` on
    // wire? No — the byte-tag protocol returns `terminal_error` from
    // `close_human_input`. The error originates from the actor.)
    let second_attempt = client.close_human_input();
    assert!(
        second_attempt.is_err(),
        "second concurrent gate acquire is rejected while the first is open"
    );

    // Release the first lease. The gate reopens.
    let release = client
        .open_human_input(first_lease)
        .expect("first harness releases the input gate");
    assert_eq!(release.lease(), first_lease);

    // A fresh acquire after release succeeds.
    let third_lease = client
        .close_human_input()
        .expect("post-release acquire succeeds");
    let _ = client
        .open_human_input(third_lease)
        .expect("third lease released");
}

#[test]
fn input_gate_holds_human_bytes_during_programmatic_injection() {
    let daemon = DaemonFixture::spawn_shell(
        "input_gate",
        "IFS= read -r first; printf 'first:%s\\n' \"$first\"; \
         IFS= read -r second; printf 'second:%s\\n' \"$second\"",
    );
    let client = daemon.client();
    let mut viewer = daemon.open_attach_stream();

    let lease = client.close_human_input().expect("human input gate closes");
    viewer
        .write_all(b"human-held-behind-gate\r")
        .expect("viewer bytes are accepted while gate is closed");

    client
        .send_programmatic_input(b"persona-injection\r")
        .expect("programmatic bytes write while gate is closed");
    daemon.wait_for_text("first:persona-injection");

    let release = client
        .open_human_input(lease)
        .expect("human input gate reopens");
    assert_eq!(release.lease(), lease);
    assert_eq!(release.held_byte_count(), "human-held-behind-gate\r".len());
    daemon.wait_for_text("second:human-held-behind-gate");

    let transcript = daemon.capture_text();
    let programmatic = transcript
        .find("first:persona-injection")
        .expect("programmatic line appears");
    let human = transcript
        .find("second:human-held-behind-gate")
        .expect("held human line appears");
    assert!(
        programmatic < human,
        "programmatic input is written before held human input"
    );
}

#[test]
fn signal_control_plane_acquires_gate_injects_releases_and_replays_human_bytes() {
    let daemon = DaemonFixture::spawn("signal-control-plane");
    let client = daemon.client();

    daemon.wait_for_text("agent-ready");
    let registered = client
        .send_signal_request(terminal_signal::Query::RegisterPromptPattern(
            terminal_signal::RegisterPromptPatternRequest {
                terminal: signal_terminal("operator"),
                pattern: signal_regex_pattern(br"agent-ready\r+\n$"),
            },
        ))
        .expect("prompt pattern registration succeeds");
    let pattern_id = match registered {
        terminal_signal::Response::PromptPatternRegistered(registered) => {
            registered.pattern_identifier
        }
        other => panic!("expected prompt-pattern registration, got {other:?}"),
    };

    let acquired = client
        .send_signal_request(terminal_signal::Query::AcquireInputGate(
            terminal_signal::AcquireInputGateRequest {
                terminal: signal_terminal("operator"),
                input_gate_reason: "witness injection".to_string(),
                prompt_pattern_identifier_selection: signal_prompt_identifier_selection(pattern_id),
            },
        ))
        .expect("input gate acquisition succeeds");
    let lease = match acquired {
        terminal_signal::Response::GateAcquired(acquired) => {
            assert_eq!(acquired.prompt_state, terminal_signal::PromptState::Clean);
            acquired.lease
        }
        other => panic!("expected gate acquisition, got {other:?}"),
    };

    let mut viewer = daemon.open_attach_stream();
    viewer
        .write_all(b"human-held-behind-signal-gate\r")
        .expect("viewer bytes are accepted while Signal gate is closed");

    let ack = client
        .send_signal_request(terminal_signal::Query::WriteInjection(
            terminal_signal::WriteInjectionRequest {
                terminal: signal_terminal("operator"),
                lease: lease.clone(),
                input_bytes: signal_input_bytes(b"persona-signal\r"),
            },
        ))
        .expect("Signal write injection succeeds");
    assert!(
        matches!(ack, terminal_signal::Response::InjectionAck(_)),
        "Signal write injection is acknowledged: {ack:?}"
    );
    daemon.wait_for_text("agent-response: persona-signal");

    let released = client
        .send_signal_request(terminal_signal::Query::ReleaseInputGate(
            terminal_signal::ReleaseInputGateRequest {
                terminal: signal_terminal("operator"),
                lease,
            },
        ))
        .expect("Signal input gate release succeeds");
    match released {
        terminal_signal::Response::GateReleased(released) => {
            assert_eq!(
                signal_cached_human_byte_count(&released.cached_human_bytes),
                "human-held-behind-signal-gate\r".len() as u64
            );
        }
        other => panic!("expected gate release, got {other:?}"),
    }
    daemon.wait_for_text("agent-response: human-held-behind-signal-gate");
}

#[test]
fn signal_dirty_prompt_rejects_write_injection_by_default() {
    let daemon = DaemonFixture::spawn_shell(
        "signal-dirty-prompt",
        "printf 'ready> dirty'; IFS= read -r line; printf 'seen:%s\\n' \"$line\"",
    );
    let client = daemon.client();

    daemon.wait_for_text("ready> dirty");
    let registered = client
        .send_signal_request(terminal_signal::Query::RegisterPromptPattern(
            terminal_signal::RegisterPromptPatternRequest {
                terminal: signal_terminal("operator"),
                pattern: signal_regex_pattern(br"ready> "),
            },
        ))
        .expect("prompt pattern registration succeeds");
    let pattern_id = match registered {
        terminal_signal::Response::PromptPatternRegistered(registered) => {
            registered.pattern_identifier
        }
        other => panic!("expected prompt-pattern registration, got {other:?}"),
    };

    let acquired = client
        .send_signal_request(terminal_signal::Query::AcquireInputGate(
            terminal_signal::AcquireInputGateRequest {
                terminal: signal_terminal("operator"),
                input_gate_reason: "dirty prompt witness".to_string(),
                prompt_pattern_identifier_selection: signal_prompt_identifier_selection(pattern_id),
            },
        ))
        .expect("dirty prompt still returns a gate acquisition with state");
    let lease = match acquired {
        terminal_signal::Response::GateAcquired(acquired) => {
            assert_eq!(
                acquired.prompt_state,
                terminal_signal::PromptState::Dirty(5),
                "regex suffix prompt reports trailing dirty bytes"
            );
            acquired.lease
        }
        other => panic!("expected gate acquisition, got {other:?}"),
    };

    let rejected = client
        .send_signal_request(terminal_signal::Query::WriteInjection(
            terminal_signal::WriteInjectionRequest {
                terminal: signal_terminal("operator"),
                lease: lease.clone(),
                input_bytes: signal_input_bytes(b"must-not-inject\r"),
            },
        ))
        .expect("dirty injection produces a typed rejection");
    assert!(
        matches!(
            rejected,
            terminal_signal::Response::InjectionRejected(terminal_signal::InjectionRejectedReply {
                injection_rejection_reason: terminal_signal::InjectionRejectionReason::DirtyPrompt,
                ..
            })
        ),
        "dirty prompt rejects write injection: {rejected:?}"
    );

    let _ = client
        .send_signal_request(terminal_signal::Query::ReleaseInputGate(
            terminal_signal::ReleaseInputGateRequest {
                terminal: signal_terminal("operator"),
                lease,
            },
        ))
        .expect("dirty prompt gate release succeeds");
}

#[test]
fn signal_worker_lifecycle_subscription_delivers_snapshot_then_deltas() {
    let daemon = DaemonFixture::spawn("signal-worker-lifecycle");
    daemon.wait_for_text("agent-ready");

    let mut subscription = std::os::unix::net::UnixStream::connect(&daemon.control_socket)
        .expect("subscription socket opens");
    subscription
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("subscription read timeout set");
    SocketRequestWriter::new(&mut subscription)
        .write_signal_request(terminal_signal::Query::SubscribeTerminalWorkerLifecycle(
            terminal_signal::SubscribeTerminalWorkerLifecycleRequest {
                terminal: signal_terminal("operator"),
            },
        ))
        .expect("worker lifecycle subscription request writes");

    let snapshot = SocketReplyReader::new(&mut subscription)
        .read_signal_event()
        .expect("initial worker lifecycle snapshot arrives");
    match snapshot {
        terminal_signal::Response::TerminalWorkerLifecycleSnapshot(snapshot) => {
            assert!(
                snapshot.observations.iter().any(|observation| matches!(
                    observation,
                    terminal_signal::TerminalWorkerLifecycle::Started(
                        terminal_signal::TerminalWorkerKind::InputWriter
                    )
                )),
                "worker lifecycle snapshot includes already-started workers"
            );
        }
        other => panic!("expected worker lifecycle snapshot, got {other:?}"),
    }

    let mut viewer = daemon.open_attach_stream();
    let event = SocketReplyReader::new(&mut subscription)
        .read_signal_event()
        .expect("worker lifecycle delta arrives");
    assert!(
        matches!(
            event,
            terminal_signal::Response::Event(
                terminal_signal::TerminalEvent::TerminalWorkerLifecycleEvent(
                    terminal_signal::TerminalWorkerLifecycleEventPayload {
                        ref observation,
                        ..
                    }
                )
            ) if matches!(
                observation,
                terminal_signal::TerminalWorkerLifecycle::Started(
                    terminal_signal::TerminalWorkerKind::AttachConnectionPump
                )
            )
        ),
        "attach pump start is delivered as a Signal lifecycle event: {event:?}"
    );
    viewer
        .write_all(b"signal lifecycle stream remains separate from raw input\r")
        .expect("raw viewer stream is still writable after Signal subscription");
}
