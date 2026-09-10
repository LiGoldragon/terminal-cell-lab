//! terminal-cell's daemon hooks — the only daemon code terminal-cell
//! hand-writes.
//!
//! The uniform daemon skeleton (argv parsing, async task-backed multi-listener
//! binding, the decode -> serve spine, lifecycle, and the `ExitReport` entry)
//! is emitted into `src/schema/daemon.rs` by schema-rust's daemon emitter.
//! terminal-cell fills only the `ComponentDaemon` escape hatches.
//!
//! terminal-cell's working tier is *component-decoded*: it speaks its own
//! `SocketRequest` wire on the control plane (not a schema-derived contract),
//! and the data plane is raw bidirectional bytes. So the engine is shared by
//! `&self` and terminal-cell owns the per-connection wire dialect:
//!
//! - the **working** (control) listener -> [`TerminalCellProcessDaemon::handle_working_connection`]
//! - the **meta** (data) listener -> [`TerminalCellProcessDaemon::handle_meta_connection`]
//!
//! Each accepted connection arrives as a Tokio `AcceptedConnection`. The
//! existing connection handlers are blocking (`std` `UnixStream`,
//! `runtime.block_on` for actor asks, raw byte pumps), so the hook converts the
//! Tokio stream to a blocking `std` stream and runs the existing connection
//! struct on `tokio::task::spawn_blocking`, holding a stored runtime `Handle`
//! for the actor asks.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use kameo::actor::ActorRef;
use regex::bytes::Regex;
use signal_terminal as terminal_signal;
use thiserror::Error;
use tokio::runtime::Handle;
use triad_runtime::AcceptedConnection;

use crate::schema::daemon::ComponentDaemon;
use crate::{
    Configuration, ConfigurationError, InputSource, SignalSocketRequest, SocketReplyWriter,
    SocketRequest, SocketRequestReader, TerminalCell, TerminalCellError, TerminalCommand,
    TerminalInput, TerminalInputGateLease, TerminalInputGateSequence, TerminalInputPort,
    TerminalLaunch, TerminalOutputPort, TerminalSize, TerminalViewerLease, TerminalWorkerKind,
    TerminalWorkerLifecycle, TerminalWorkerLifecycleSubscriptionRequest,
    TerminalWorkerObservationRequest, TerminalWorkerStop, TranscriptSnapshotRequest,
    TranscriptSubscriptionRequest, WaitForTerminalExit, WaitForTranscriptText,
};

const DEFAULT_TERMINAL_ROWS: u16 = 24;
const DEFAULT_TERMINAL_COLUMNS: u16 = 80;

/// The type-level selector for terminal-cell's emitted daemon. It carries no
/// runtime data — it is the marker the emitted `DaemonCommand` and generated
/// runtime dispatch on, selecting terminal-cell's `Configuration` / `Engine` /
/// `Error` types through the `ComponentDaemon` associated types.
#[derive(Debug)]
pub struct TerminalCellProcessDaemon;

/// terminal-cell's daemon error: the engine-facing IO variant plus the typed
/// terminal-cell domain error. The emitted `DaemonError` wraps this under its
/// `Component` arm.
#[derive(Debug, Error)]
pub enum TerminalCellDaemonError {
    #[error("terminal-cell daemon IO error: {0}")]
    Io(#[from] io::Error),

    #[error("terminal-cell daemon join error: {0}")]
    Join(String),

    #[error("terminal-cell engine error: {0}")]
    Terminal(#[from] TerminalCellError),

    #[error("terminal-cell session not started")]
    SessionNotStarted,

    #[error("terminal-cell session startup failed: {0}")]
    Startup(String),
}

/// The live handles a serving connection needs: the root `TerminalCell` actor,
/// the input/output ports, a Tokio runtime `Handle` for the blocking handlers'
/// actor asks, and the transitional Signal control state.
#[derive(Clone)]
pub struct TerminalSession {
    actor: ActorRef<TerminalCell>,
    input_port: TerminalInputPort,
    output_port: TerminalOutputPort,
    runtime: Handle,
    signal_state: Arc<Mutex<TerminalSignalControlState>>,
}

impl TerminalSession {
    fn new(
        actor: ActorRef<TerminalCell>,
        input_port: TerminalInputPort,
        output_port: TerminalOutputPort,
        runtime: Handle,
    ) -> Self {
        Self {
            actor,
            input_port,
            output_port,
            runtime,
            signal_state: Arc::new(Mutex::new(TerminalSignalControlState::new())),
        }
    }
}

/// terminal-cell's engine: the launch parameters and, once `start` has run, the
/// live [`TerminalSession`]. The component-decoded tier shares `&engine`, so the
/// session lives behind a `OnceLock`, set exactly once at startup and read by
/// every connection handler.
pub struct TerminalCellEngine {
    launch: TerminalLaunch,
    control_socket_path: String,
    data_socket_path: String,
    session: OnceLock<TerminalSession>,
}

impl TerminalCellEngine {
    fn from_configuration(configuration: &Configuration) -> Self {
        let command = TerminalCommand::with_working_directory_and_environment(
            configuration.program().to_owned(),
            configuration.arguments().to_vec(),
            configuration
                .working_directory()
                .map(std::string::ToString::to_string),
            configuration
                .environment()
                .iter()
                .map(|variable| (variable.name().to_owned(), variable.value().to_owned()))
                .collect(),
        );
        let launch = TerminalLaunch::new(
            command,
            TerminalSize::new(DEFAULT_TERMINAL_ROWS, DEFAULT_TERMINAL_COLUMNS),
        )
        .with_child_process_identifier_path(
            configuration
                .child_process_identifier_path()
                .map(std::string::ToString::to_string),
        );
        Self {
            launch,
            control_socket_path: configuration.control_socket_path().to_owned(),
            data_socket_path: configuration.data_socket_path().to_owned(),
            session: OnceLock::new(),
        }
    }

    fn session(&self) -> Result<&TerminalSession, TerminalCellDaemonError> {
        self.session
            .get()
            .ok_or(TerminalCellDaemonError::SessionNotStarted)
    }

    /// Spawn the `TerminalCell` session, wait for the actor to come up, store
    /// the live handles, and announce readiness. Called once by the emitted
    /// lifecycle `start` hook, after both listener sockets are bound but before
    /// any connection is served.
    ///
    /// `start` runs on a Tokio worker inside the async runtime, so the
    /// startup-await uses `block_in_place` + the current `Handle` rather than a
    /// nested `block_on` (which would panic). The per-connection handlers run on
    /// `spawn_blocking` threads, where a plain `block_on` for actor asks is
    /// sound.
    fn start(&self) -> Result<(), TerminalCellDaemonError> {
        let runtime = Handle::current();
        let cell_session = TerminalCell::spawn_session(self.launch.clone());
        let actor = cell_session.actor();
        let input_port = cell_session.input_port();
        let output_port = cell_session.output_port();
        tokio::task::block_in_place(|| {
            runtime.block_on(async { actor.wait_for_startup_result().await })
        })
        .map_err(|error| TerminalCellDaemonError::Startup(error.to_string()))?;

        let session = TerminalSession::new(actor.clone(), input_port, output_port, runtime);
        self.session
            .set(session)
            .map_err(|_| TerminalCellDaemonError::SessionNotStarted)?;

        // The emitted shell owns the socket-accept loops; record that worker as
        // started now that both listeners are bound and serving, preserving the
        // worker-lifecycle observation the retired hand-rolled accept loop made.
        let _ = actor
            .tell(TerminalWorkerLifecycle::Started(
                TerminalWorkerKind::SocketAcceptLoop,
            ))
            .try_send();

        // The witness fixtures parse this readiness line off stdout to learn the
        // daemon is serving both planes. Both sockets are already bound when
        // `start` runs.
        println!(
            "terminal-cell-daemon control-socket={} data-socket={}",
            self.control_socket_path, self.data_socket_path
        );
        io::stdout().flush()?;
        Ok(())
    }

    async fn handle_working_connection(
        &self,
        connection: AcceptedConnection,
    ) -> Result<(), TerminalCellDaemonError> {
        let session = self.session()?.clone();
        let stream = Self::into_blocking_stream(connection)?;
        Self::serve_blocking(move || TerminalControlConnection::new(stream, session).run()).await
    }

    async fn handle_meta_connection(
        &self,
        connection: AcceptedConnection,
    ) -> Result<(), TerminalCellDaemonError> {
        let session = self.session()?.clone();
        let stream = Self::into_blocking_stream(connection)?;
        Self::serve_blocking(move || TerminalDataConnection::new(stream, session).run()).await
    }

    /// Drain the accepted Tokio connection into a blocking `std::UnixStream`.
    /// `into_std` yields a stream in non-blocking mode; the existing connection
    /// handlers are written against blocking IO, so restore blocking mode.
    fn into_blocking_stream(
        connection: AcceptedConnection,
    ) -> Result<UnixStream, TerminalCellDaemonError> {
        let (tokio_stream, _context) = connection.into_parts();
        let stream = tokio_stream.into_std()?;
        stream.set_nonblocking(false)?;
        Ok(stream)
    }

    /// Run a blocking connection handler on the blocking pool and translate its
    /// outcome into the daemon error. A connection failure is logged and
    /// swallowed (the listener serves the next connection); only a join failure
    /// surfaces, matching the per-connection isolation of the retired spine.
    async fn serve_blocking<Handler>(handler: Handler) -> Result<(), TerminalCellDaemonError>
    where
        Handler: FnOnce() -> io::Result<()> + Send + 'static,
    {
        match tokio::task::spawn_blocking(handler).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                eprintln!("terminal cell connection failed: {error}");
                Ok(())
            }
            Err(error) => Err(TerminalCellDaemonError::Join(error.to_string())),
        }
    }
}

impl ComponentDaemon for TerminalCellProcessDaemon {
    type Configuration = Configuration;
    type ConfigurationError = ConfigurationError;
    type Engine = TerminalCellEngine;
    type Error = TerminalCellDaemonError;

    const PROCESS_NAME: &'static str = "terminal-cell-daemon";

    fn load_configuration(
        path: &std::path::Path,
    ) -> Result<Self::Configuration, Self::ConfigurationError> {
        Configuration::from_signal_file(path)
    }

    fn build_runtime(configuration: &Self::Configuration) -> Result<Self::Engine, Self::Error> {
        Ok(TerminalCellEngine::from_configuration(configuration))
    }

    fn start(engine: &Self::Engine) -> Result<(), Self::Error> {
        engine.start()
    }

    async fn handle_working_connection(
        engine: &Self::Engine,
        connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        engine.handle_working_connection(connection).await
    }

    async fn handle_meta_connection(
        engine: &Self::Engine,
        connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        engine.handle_meta_connection(connection).await
    }
}

/// TRANSITIONAL: direct `signal-terminal` handling in this daemon is a witness
/// path while `terminal` becomes the production Signal endpoint. Keep this state
/// local to the terminal-cell daemon and do not grow it into a Persona-facing
/// registry or policy owner.
struct TerminalSignalControlState {
    next_prompt_pattern: u64,
    prompt_patterns: HashMap<String, terminal_signal::PromptPattern>,
    signal_leases: HashMap<u64, terminal_signal::PromptState>,
}

impl TerminalSignalControlState {
    fn new() -> Self {
        Self {
            next_prompt_pattern: 1,
            prompt_patterns: HashMap::new(),
            signal_leases: HashMap::new(),
        }
    }

    fn register_prompt_pattern(
        &mut self,
        pattern: terminal_signal::PromptPattern,
    ) -> terminal_signal::PromptPatternIdentifier {
        let id = format!("prompt-pattern-{}", self.next_prompt_pattern);
        self.next_prompt_pattern = self.next_prompt_pattern.saturating_add(1);
        self.prompt_patterns
            .insert(id.as_str().to_string(), pattern);
        id
    }

    fn unregister_prompt_pattern(&mut self, id: &terminal_signal::PromptPatternIdentifier) {
        self.prompt_patterns.remove(id.as_str());
    }

    fn prompt_pattern_entries(&self) -> Vec<terminal_signal::PromptPatternEntry> {
        self.prompt_patterns
            .iter()
            .map(
                |(pattern_id, pattern)| terminal_signal::PromptPatternEntry {
                    pattern_identifier: pattern_id.clone(),
                    pattern: pattern.clone(),
                },
            )
            .collect()
    }

    fn prompt_pattern(
        &self,
        id: &terminal_signal::PromptPatternIdentifier,
    ) -> Option<terminal_signal::PromptPattern> {
        self.prompt_patterns.get(id.as_str()).cloned()
    }

    fn record_signal_lease(
        &mut self,
        lease: terminal_signal::Lease,
        prompt_state: terminal_signal::PromptState,
    ) {
        self.signal_leases
            .insert(Self::signal_lease_key(&lease), prompt_state);
    }

    fn signal_lease_prompt_state(
        &self,
        lease: &terminal_signal::Lease,
    ) -> Option<&terminal_signal::PromptState> {
        self.signal_leases.get(&Self::signal_lease_key(lease))
    }

    fn release_signal_lease(&mut self, lease: &terminal_signal::Lease) {
        self.signal_leases.remove(&Self::signal_lease_key(lease));
    }

    fn release_terminal_lease(&mut self, lease: &TerminalInputGateLease) {
        self.signal_leases.remove(&lease.sequence().into_u64());
    }

    fn signal_lease_key(lease: &terminal_signal::Lease) -> u64 {
        lease.input_gate_lease_identifier as u64
    }
}

/// A control-plane connection. Carries every kind of request *except* `Attach`:
/// an attach request on this socket is an architectural-truth violation and is
/// explicitly rejected with the `ATTACH_REJECTED` reply so the wire boundary
/// stays clean.
struct TerminalControlConnection {
    stream: UnixStream,
    session: TerminalSession,
}

impl TerminalControlConnection {
    fn new(stream: UnixStream, session: TerminalSession) -> Self {
        Self { stream, session }
    }

    fn terminal(&self) -> &ActorRef<TerminalCell> {
        &self.session.actor
    }

    fn input_port(&self) -> &TerminalInputPort {
        &self.session.input_port
    }

    fn runtime(&self) -> &Handle {
        &self.session.runtime
    }

    fn run(mut self) -> io::Result<()> {
        let request = SocketRequestReader::new(&mut self.stream).read_request()?;
        match request {
            SocketRequest::Capture => self.write_snapshot(),
            SocketRequest::SubscribeFromBeginning => self.stream_subscription(),
            SocketRequest::Attach => self.reject_attach_on_control_plane(),
            SocketRequest::Input(input) => self.write_input(input),
            SocketRequest::CloseHumanInput => self.close_human_input(),
            SocketRequest::OpenHumanInput(lease) => self.open_human_input(lease),
            SocketRequest::Resize(size) => self.write_resize(size),
            SocketRequest::Wait(wait) => self.wait_for_text(wait),
            SocketRequest::WaitExit => self.wait_for_exit(),
            SocketRequest::WorkerObservation => self.write_worker_observation(),
            SocketRequest::Signal(request) => self.handle_signal_request(request),
        }
    }

    fn reject_attach_on_control_plane(&mut self) -> io::Result<()> {
        SocketReplyWriter::new(&mut self.stream).write_attach_rejected(
            "control socket does not accept viewer attach; use the data socket",
        )?;
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "attach request arrived on terminal-cell control socket",
        ))
    }

    fn write_snapshot(&mut self) -> io::Result<()> {
        let snapshot = self.snapshot()?;
        SocketReplyWriter::new(&mut self.stream).write_snapshot(snapshot.bytes())
    }

    fn stream_subscription(&mut self) -> io::Result<()> {
        let mut subscription = self.subscription()?;
        self.stream.write_all(&subscription.replay_bytes())?;
        self.stream.flush()?;
        while let Some(delta) = subscription.blocking_next_live_delta() {
            if self.stream.write_all(delta.bytes()).is_err() {
                break;
            }
            if self.stream.flush().is_err() {
                break;
            }
        }
        Ok(())
    }

    fn write_input(&mut self, input: TerminalInput) -> io::Result<()> {
        let acceptance = self
            .input_port()
            .accept(input)
            .map_err(Self::terminal_error)?;
        let _accepted_source = acceptance.source();
        SocketReplyWriter::new(&mut self.stream).write_acceptance()
    }

    fn close_human_input(&mut self) -> io::Result<()> {
        let lease = self
            .input_port()
            .close_human_input()
            .map_err(Self::terminal_error)?;
        SocketReplyWriter::new(&mut self.stream).write_gate_lease(lease)
    }

    fn open_human_input(&mut self, lease: TerminalInputGateLease) -> io::Result<()> {
        let release = self
            .input_port()
            .open_human_input(lease)
            .map_err(Self::terminal_error)?;
        self.signal_state()?
            .release_terminal_lease(&release.lease());
        SocketReplyWriter::new(&mut self.stream).write_gate_release(release)
    }

    fn write_resize(&mut self, size: TerminalSize) -> io::Result<()> {
        let terminal = self.terminal().clone();
        self.runtime()
            .block_on(async { terminal.ask(size).await })
            .map_err(Self::actor_error)?;
        SocketReplyWriter::new(&mut self.stream).write_acceptance()
    }

    fn wait_for_text(&mut self, wait: WaitForTranscriptText) -> io::Result<()> {
        let terminal = self.terminal().clone();
        let matched = self
            .runtime()
            .block_on(async { terminal.ask(wait).await })
            .map_err(Self::actor_error)?;
        if matched {
            SocketReplyWriter::new(&mut self.stream).write_wait_satisfied()
        } else {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "terminal transcript waiter ended without a match",
            ))
        }
    }

    fn snapshot(&self) -> io::Result<crate::TranscriptSnapshot> {
        let terminal = self.terminal().clone();
        let reply = self
            .runtime()
            .block_on(async { terminal.ask(TranscriptSnapshotRequest).await })
            .map_err(Self::actor_error)?;
        Ok(reply)
    }

    fn wait_for_exit(&mut self) -> io::Result<()> {
        let terminal = self.terminal().clone();
        let exit = self
            .runtime()
            .block_on(async { terminal.ask(WaitForTerminalExit).await })
            .map_err(Self::actor_error)?;
        SocketReplyWriter::new(&mut self.stream).write_exit_status(exit.status())
    }

    fn write_worker_observation(&mut self) -> io::Result<()> {
        let terminal = self.terminal().clone();
        let observation = self
            .runtime()
            .block_on(async { terminal.ask(TerminalWorkerObservationRequest).await })
            .map_err(Self::actor_error)?;
        SocketReplyWriter::new(&mut self.stream).write_snapshot(observation.to_text().as_bytes())
    }

    fn handle_signal_request(&mut self, request: SignalSocketRequest) -> io::Result<()> {
        match request.into_payload() {
            terminal_signal::Query::SubscribeTerminalWorkerLifecycle(subscription) => {
                self.stream_signal_worker_lifecycle(subscription)
            }
            terminal_signal::Query::TerminalWorkerLifecycleRetraction(token) => {
                let ack = terminal_signal::Response::SubscriptionRetracted(
                    terminal_signal::SubscriptionRetractedReply { token },
                );
                SocketReplyWriter::new(&mut self.stream).write_signal_event(ack)
            }
            payload => {
                let event = self.signal_event(payload)?;
                SocketReplyWriter::new(&mut self.stream).write_signal_event(event)
            }
        }
    }

    fn signal_event(
        &mut self,
        request: terminal_signal::Query,
    ) -> io::Result<terminal_signal::Response> {
        match request {
            terminal_signal::Query::TerminalConnection(connection) => Ok(
                terminal_signal::Response::TerminalReady(terminal_signal::TerminalReadyReply {
                    terminal: connection.terminal,
                    generation: Self::signal_generation(1),
                }),
            ),
            terminal_signal::Query::TerminalInput(input) => {
                self.input_port()
                    .accept(TerminalInput::new(
                        Self::input_bytes_to_bytes(&input.input_bytes),
                        InputSource::Programmatic,
                    ))
                    .map_err(Self::terminal_error)?;
                Ok(terminal_signal::Response::TerminalInputAccepted(
                    terminal_signal::TerminalInputAcceptedReply {
                        terminal: input.terminal,
                        generation: Self::signal_generation(1),
                    },
                ))
            }
            terminal_signal::Query::TerminalResize(resize) => {
                let size = TerminalSize::new(
                    Self::rows_to_u16(&resize.rows),
                    Self::columns_to_u16(&resize.columns),
                );
                let terminal = self.terminal().clone();
                self.runtime()
                    .block_on(async { terminal.ask(size).await })
                    .map_err(Self::actor_error)?;
                Ok(terminal_signal::Response::TerminalResized(
                    terminal_signal::TerminalResizedReply {
                        terminal: resize.terminal,
                        rows: resize.rows,
                        columns: resize.columns,
                        generation: Self::signal_generation(1),
                    },
                ))
            }
            terminal_signal::Query::TerminalDetachment(detachment) => {
                Ok(terminal_signal::Response::TerminalDetached(
                    terminal_signal::TerminalDetachedReply {
                        terminal: detachment.terminal,
                        generation: Self::signal_generation(1),
                        terminal_detachment_reason: detachment.terminal_detachment_reason,
                    },
                ))
            }
            terminal_signal::Query::TerminalCapture(capture) => {
                let snapshot = self.snapshot()?;
                Ok(terminal_signal::Response::TerminalCaptured(
                    terminal_signal::TerminalCapturedReply {
                        terminal: capture.terminal,
                        generation: Self::signal_generation(1),
                        transcript_bytes: Self::signal_transcript_bytes(snapshot.bytes()),
                    },
                ))
            }
            terminal_signal::Query::RegisterPromptPattern(registration) => {
                let pattern_id = self
                    .signal_state()?
                    .register_prompt_pattern(registration.pattern);
                Ok(terminal_signal::Response::PromptPatternRegistered(
                    terminal_signal::PromptPatternRegisteredReply {
                        terminal: registration.terminal,
                        pattern_identifier: pattern_id,
                    },
                ))
            }
            terminal_signal::Query::UnregisterPromptPattern(unregistration) => {
                self.signal_state()?
                    .unregister_prompt_pattern(&unregistration.pattern_identifier);
                Ok(terminal_signal::Response::PromptPatternUnregistered(
                    terminal_signal::PromptPatternUnregisteredReply {
                        terminal: unregistration.terminal,
                        pattern_identifier: unregistration.pattern_identifier,
                    },
                ))
            }
            terminal_signal::Query::ListPromptPatterns(list) => {
                let entries = self.signal_state()?.prompt_pattern_entries();
                Ok(terminal_signal::Response::PromptPatternList(
                    terminal_signal::PromptPatternListReply {
                        terminal: list.terminal,
                        entries,
                    },
                ))
            }
            terminal_signal::Query::AcquireInputGate(acquire) => {
                self.acquire_signal_input_gate(acquire)
            }
            terminal_signal::Query::ReleaseInputGate(release) => {
                self.release_signal_input_gate(release)
            }
            terminal_signal::Query::WriteInjection(injection) => {
                self.write_signal_injection(injection)
            }
            terminal_signal::Query::SubscribeTerminalWorkerLifecycle(subscription) => {
                Ok(terminal_signal::Response::TerminalRejected(
                    terminal_signal::TerminalRejectedReply {
                        terminal: subscription.terminal,
                        terminal_rejection_reason:
                            terminal_signal::TerminalRejectionReason::TransportFailed,
                    },
                ))
            }
            terminal_signal::Query::TerminalWorkerLifecycleRetraction(token) => {
                Ok(terminal_signal::Response::TerminalRejected(
                    terminal_signal::TerminalRejectedReply {
                        terminal: token.terminal,
                        terminal_rejection_reason:
                            terminal_signal::TerminalRejectionReason::TransportFailed,
                    },
                ))
            }
            terminal_signal::Query::ListSessions(_) => Ok(terminal_signal::Response::SessionList(
                terminal_signal::SessionListReply {
                    session_entries: Vec::new(),
                },
            )),
            terminal_signal::Query::ResolveSession(resolve) => {
                Ok(terminal_signal::Response::TerminalRejected(
                    terminal_signal::TerminalRejectedReply {
                        terminal: resolve.name,
                        terminal_rejection_reason:
                            terminal_signal::TerminalRejectionReason::TransportFailed,
                    },
                ))
            }
        }
    }

    fn acquire_signal_input_gate(
        &mut self,
        acquire: terminal_signal::AcquireInputGateRequest,
    ) -> io::Result<terminal_signal::Response> {
        let prompt_state =
            self.signal_prompt_state(acquire.prompt_pattern_identifier_selection.as_ref())?;
        match self.input_port().close_human_input() {
            Ok(lease) => {
                let signal_lease = Self::signal_lease(lease);
                self.signal_state()?
                    .record_signal_lease(signal_lease.clone(), prompt_state.clone());
                Ok(terminal_signal::Response::GateAcquired(
                    terminal_signal::GateAcquiredReply {
                        terminal: acquire.terminal,
                        lease: signal_lease,
                        prompt_state,
                    },
                ))
            }
            Err(TerminalCellError::InputGateAlreadyClosed(lease)) => Ok(
                terminal_signal::Response::GateBusy(terminal_signal::GateBusyReply {
                    terminal: acquire.terminal,
                    current_holder: lease.sequence().into_u64() as i64,
                }),
            ),
            Err(error) => Err(Self::terminal_error(error)),
        }
    }

    fn release_signal_input_gate(
        &mut self,
        release: terminal_signal::ReleaseInputGateRequest,
    ) -> io::Result<terminal_signal::Response> {
        if self
            .signal_state()?
            .signal_lease_prompt_state(&release.lease)
            .is_none()
        {
            return Ok(terminal_signal::Response::InjectionRejected(
                terminal_signal::InjectionRejectedReply {
                    terminal: release.terminal,
                    injection_rejection_reason:
                        terminal_signal::InjectionRejectionReason::UnknownLease,
                },
            ));
        }

        let lease = Self::terminal_lease(&release.lease);
        match self.input_port().open_human_input(lease) {
            Ok(gate_release) => {
                self.signal_state()?.release_signal_lease(&release.lease);
                Ok(terminal_signal::Response::GateReleased(
                    terminal_signal::GateReleasedReply {
                        terminal: release.terminal,
                        lease: release.lease,
                        cached_human_bytes: gate_release.held_byte_count() as i64,
                    },
                ))
            }
            Err(TerminalCellError::StaleInputGateLease) => {
                self.signal_state()?.release_signal_lease(&release.lease);
                Ok(terminal_signal::Response::InjectionRejected(
                    terminal_signal::InjectionRejectedReply {
                        terminal: release.terminal,
                        injection_rejection_reason:
                            terminal_signal::InjectionRejectionReason::UnknownLease,
                    },
                ))
            }
            Err(error) => Err(Self::terminal_error(error)),
        }
    }

    fn write_signal_injection(
        &mut self,
        injection: terminal_signal::WriteInjectionRequest,
    ) -> io::Result<terminal_signal::Response> {
        let prompt_state = self
            .signal_state()?
            .signal_lease_prompt_state(&injection.lease)
            .cloned();
        let Some(prompt_state) = prompt_state else {
            return Ok(terminal_signal::Response::InjectionRejected(
                terminal_signal::InjectionRejectedReply {
                    terminal: injection.terminal,
                    injection_rejection_reason:
                        terminal_signal::InjectionRejectionReason::UnknownLease,
                },
            ));
        };

        if matches!(prompt_state, terminal_signal::PromptState::Dirty(_)) {
            return Ok(terminal_signal::Response::InjectionRejected(
                terminal_signal::InjectionRejectedReply {
                    terminal: injection.terminal,
                    injection_rejection_reason:
                        terminal_signal::InjectionRejectionReason::DirtyPrompt,
                },
            ));
        }

        self.input_port()
            .accept(TerminalInput::new(
                Self::input_bytes_to_bytes(&injection.input_bytes),
                InputSource::Programmatic,
            ))
            .map_err(Self::terminal_error)?;
        let snapshot = self.snapshot()?;
        Ok(terminal_signal::Response::InjectionAck(
            terminal_signal::InjectionAckReply {
                terminal: injection.terminal,
                generation: Self::signal_generation(1),
                sequence: snapshot.last_sequence().into_u64() as i64,
            },
        ))
    }

    fn stream_signal_worker_lifecycle(
        &mut self,
        subscription: terminal_signal::SubscribeTerminalWorkerLifecycleRequest,
    ) -> io::Result<()> {
        let terminal_name = subscription;
        let terminal = self.terminal().clone();
        let mut lifecycle = self
            .runtime()
            .block_on(async {
                terminal
                    .ask(TerminalWorkerLifecycleSubscriptionRequest)
                    .await
            })
            .map_err(Self::actor_error)?;
        SocketReplyWriter::new(&mut self.stream).write_signal_event(
            terminal_signal::Response::TerminalWorkerLifecycleSnapshot(
                terminal_signal::TerminalWorkerLifecycleSnapshotReply {
                    terminal: terminal_name.terminal.clone(),
                    observations: lifecycle
                        .replay()
                        .iter()
                        .cloned()
                        .map(Self::signal_worker_lifecycle)
                        .collect::<Vec<_>>(),
                },
            ),
        )?;

        while let Some(event) = lifecycle.blocking_next_live_event() {
            let event = terminal_signal::TerminalEvent::TerminalWorkerLifecycleEvent(
                terminal_signal::TerminalWorkerLifecycleEventPayload {
                    terminal: terminal_name.terminal.clone(),
                    observation: Self::signal_worker_lifecycle(event),
                },
            );
            SocketReplyWriter::new(&mut self.stream)
                .write_signal_event(terminal_signal::Response::Event(event))?;
        }
        Ok(())
    }

    fn signal_prompt_state(
        &self,
        pattern_id: Option<&terminal_signal::PromptPatternIdentifier>,
    ) -> io::Result<terminal_signal::PromptState> {
        let Some(pattern_id) = pattern_id else {
            return Ok(terminal_signal::PromptState::NotChecked);
        };
        let pattern = self.signal_state()?.prompt_pattern(pattern_id);
        let Some(pattern) = pattern else {
            return Ok(terminal_signal::PromptState::Dirty(
                self.snapshot()?.bytes().len() as i64,
            ));
        };
        let snapshot = self.snapshot()?;
        let trailing_count = Self::prompt_suffix_trailing_count(&pattern, snapshot.bytes())?;
        if trailing_count == 0 {
            Ok(terminal_signal::PromptState::Clean)
        } else {
            Ok(terminal_signal::PromptState::Dirty(trailing_count as i64))
        }
    }

    fn prompt_suffix_trailing_count(
        pattern: &terminal_signal::PromptPattern,
        transcript: &[u8],
    ) -> io::Result<usize> {
        match pattern {
            terminal_signal::PromptPattern::LiteralSuffix(suffix) => Ok(Self::literal_suffix_gap(
                transcript,
                &Self::signal_bytes_to_bytes(suffix.as_slice()),
            )),
            terminal_signal::PromptPattern::RegexSuffix(pattern) => {
                let pattern = Self::signal_bytes_to_bytes(pattern.as_slice());
                let pattern = std::str::from_utf8(&pattern).map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("prompt regex pattern is not utf-8: {error}"),
                    )
                })?;
                Regex::new(pattern)
                    .map(|regex| {
                        regex
                            .find_iter(transcript)
                            .last()
                            .map_or(transcript.len(), |matched| transcript.len() - matched.end())
                    })
                    .map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("prompt regex pattern is invalid: {error}"),
                        )
                    })
            }
        }
    }

    fn literal_suffix_gap(transcript: &[u8], suffix: &[u8]) -> usize {
        if suffix.is_empty() || transcript.ends_with(suffix) {
            return 0;
        }

        transcript
            .windows(suffix.len())
            .rposition(|window| window == suffix)
            .map_or(transcript.len(), |position| {
                transcript.len() - position - suffix.len()
            })
    }

    fn signal_lease(lease: TerminalInputGateLease) -> terminal_signal::Lease {
        terminal_signal::InputGateLease {
            input_gate_lease_identifier: lease.sequence().into_u64() as i64,
        }
    }

    fn terminal_lease(lease: &terminal_signal::Lease) -> TerminalInputGateLease {
        TerminalInputGateLease::new(TerminalInputGateSequence::new(
            lease.input_gate_lease_identifier as u64,
        ))
    }

    fn signal_generation(value: u64) -> terminal_signal::Generation {
        value as i64
    }

    fn rows_to_u16(rows: &terminal_signal::Rows) -> u16 {
        *rows as u16
    }

    fn columns_to_u16(columns: &terminal_signal::Columns) -> u16 {
        *columns as u16
    }

    fn input_bytes_to_bytes(input_bytes: &terminal_signal::InputBytes) -> Vec<u8> {
        Self::signal_bytes_to_bytes(input_bytes.as_slice())
    }

    fn signal_transcript_bytes(bytes: &[u8]) -> terminal_signal::TranscriptBytes {
        Self::bytes_to_signal_bytes(bytes)
    }

    /// Lower terminal-cell's `u8` byte buffer into the schema-emitted
    /// `Integer` (`u64`) byte vector the signal-terminal contract carries.
    fn bytes_to_signal_bytes(bytes: &[u8]) -> Vec<i64> {
        bytes.iter().map(|byte| i64::from(*byte)).collect()
    }

    /// Narrow the schema-emitted `Integer` (`u64`) byte vector back into a
    /// terminal-cell `u8` buffer, truncating each element to its low byte.
    fn signal_bytes_to_bytes(bytes: &[i64]) -> Vec<u8> {
        bytes.iter().map(|byte| *byte as u8).collect()
    }

    fn signal_worker_lifecycle(
        lifecycle: TerminalWorkerLifecycle,
    ) -> terminal_signal::TerminalWorkerLifecycle {
        match lifecycle {
            TerminalWorkerLifecycle::Started(worker) => {
                terminal_signal::TerminalWorkerLifecycle::Started(Self::signal_worker_kind(worker))
            }
            TerminalWorkerLifecycle::Stopped { worker, reason } => {
                terminal_signal::TerminalWorkerLifecycle::Stopped(
                    terminal_signal::TerminalWorkerStop {
                        terminal_worker_kind: Self::signal_worker_kind(worker),
                        terminal_worker_stop_reason: Self::signal_worker_stop(reason),
                    },
                )
            }
        }
    }

    fn signal_worker_kind(worker: TerminalWorkerKind) -> terminal_signal::TerminalWorkerKind {
        match worker {
            TerminalWorkerKind::InputWriter => terminal_signal::TerminalWorkerKind::InputWriter,
            TerminalWorkerKind::ViewerFanout => terminal_signal::TerminalWorkerKind::ViewerFanout,
            TerminalWorkerKind::TranscriptScriber => {
                terminal_signal::TerminalWorkerKind::TranscriptScriber
            }
            TerminalWorkerKind::OutputReader => terminal_signal::TerminalWorkerKind::OutputReader,
            TerminalWorkerKind::ChildExitWatcher => {
                terminal_signal::TerminalWorkerKind::ChildExitWatcher
            }
            TerminalWorkerKind::SocketAcceptLoop => {
                terminal_signal::TerminalWorkerKind::SocketAcceptLoop
            }
            TerminalWorkerKind::AttachConnectionPump => {
                terminal_signal::TerminalWorkerKind::AttachConnectionPump
            }
        }
    }

    fn signal_worker_stop(reason: TerminalWorkerStop) -> terminal_signal::TerminalWorkerStopReason {
        match reason {
            TerminalWorkerStop::InputCommandChannelClosed => {
                terminal_signal::TerminalWorkerStopReason::InputCommandChannelClosed
            }
            TerminalWorkerStop::InputWriteFailed(error) => {
                terminal_signal::TerminalWorkerStopReason::InputWriteFailed(error)
            }
            TerminalWorkerStop::OutputCommandChannelClosed => {
                terminal_signal::TerminalWorkerStopReason::OutputCommandChannelClosed
            }
            TerminalWorkerStop::TranscriptNoticeChannelClosed => {
                terminal_signal::TerminalWorkerStopReason::TranscriptNoticeChannelClosed
            }
            TerminalWorkerStop::OutputReaderFinished => {
                terminal_signal::TerminalWorkerStopReason::OutputReaderFinished
            }
            TerminalWorkerStop::OutputReadFailed(error) => {
                terminal_signal::TerminalWorkerStopReason::OutputReadFailed(error)
            }
            TerminalWorkerStop::OutputPortClosed => {
                terminal_signal::TerminalWorkerStopReason::OutputPortClosed
            }
            TerminalWorkerStop::ChildExited(status) => {
                terminal_signal::TerminalWorkerStopReason::ChildExited(status)
            }
            TerminalWorkerStop::ChildWaitFailed(error) => {
                terminal_signal::TerminalWorkerStopReason::ChildWaitFailed(error)
            }
            TerminalWorkerStop::SocketAcceptFailed(error) => {
                terminal_signal::TerminalWorkerStopReason::SocketAcceptFailed(error)
            }
            TerminalWorkerStop::AttachConnectionClosed => {
                terminal_signal::TerminalWorkerStopReason::AttachConnectionClosed
            }
            TerminalWorkerStop::AttachConnectionFailed(error) => {
                terminal_signal::TerminalWorkerStopReason::AttachConnectionFailed(error)
            }
        }
    }

    fn signal_state(&self) -> io::Result<MutexGuard<'_, TerminalSignalControlState>> {
        self.session
            .signal_state
            .lock()
            .map_err(|_| io::Error::other("terminal signal control state lock poisoned"))
    }

    fn subscription(&self) -> io::Result<crate::TranscriptSubscription> {
        let terminal = self.terminal().clone();
        let reply = self
            .runtime()
            .block_on(async {
                terminal
                    .ask(TranscriptSubscriptionRequest::from_beginning())
                    .await
            })
            .map_err(Self::actor_error)?;
        Ok(reply)
    }

    fn actor_error(error: impl std::fmt::Display) -> io::Error {
        io::Error::new(io::ErrorKind::BrokenPipe, error.to_string())
    }

    fn terminal_error(error: TerminalCellError) -> io::Error {
        io::Error::new(io::ErrorKind::BrokenPipe, error.to_string())
    }
}

/// A data-plane connection. Carries only an attach handshake followed by raw
/// bidirectional bytes between the viewer and the child PTY. The connection
/// rejects every kind of request other than `Attach` with an explicit
/// attach-rejection reply so the wire boundary stays clean.
struct TerminalDataConnection {
    stream: UnixStream,
    session: TerminalSession,
}

impl TerminalDataConnection {
    fn new(stream: UnixStream, session: TerminalSession) -> Self {
        Self { stream, session }
    }

    fn terminal(&self) -> &ActorRef<TerminalCell> {
        &self.session.actor
    }

    fn input_port(&self) -> &TerminalInputPort {
        &self.session.input_port
    }

    fn output_port(&self) -> &TerminalOutputPort {
        &self.session.output_port
    }

    fn runtime(&self) -> &Handle {
        &self.session.runtime
    }

    fn run(mut self) -> io::Result<()> {
        let request = SocketRequestReader::new(&mut self.stream).read_request()?;
        match request {
            SocketRequest::Attach => self.attach_viewer(),
            other => self.reject_non_attach_on_data_plane(other),
        }
    }

    fn reject_non_attach_on_data_plane(&mut self, _request: SocketRequest) -> io::Result<()> {
        SocketReplyWriter::new(&mut self.stream).write_attach_rejected(
            "data socket only accepts viewer attach; use the control socket",
        )?;
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "non-attach request arrived on terminal-cell data socket",
        ))
    }

    fn attach_viewer(&mut self) -> io::Result<()> {
        let lease = match self.output_port().reserve_viewer() {
            Ok(lease) => lease,
            Err(TerminalCellError::ViewerAlreadyAttached) => {
                SocketReplyWriter::new(&mut self.stream)
                    .write_attach_rejected("terminal cell already has an attached viewer")?;
                return Ok(());
            }
            Err(error) => return Err(Self::terminal_error(error)),
        };

        let result = self.complete_viewer_attach(lease);
        if result.is_err() {
            let _ = self.output_port().detach(lease);
        }
        result
    }

    fn complete_viewer_attach(&mut self, lease: TerminalViewerLease) -> io::Result<()> {
        SocketReplyWriter::new(&mut self.stream).write_attach_accepted()?;

        let snapshot = self.snapshot()?;
        if !snapshot.bytes().is_empty() {
            self.stream.write_all(snapshot.bytes())?;
            self.stream.flush()?;
        }

        self.output_port()
            .activate_viewer(lease, self.stream.try_clone()?)
            .map_err(Self::terminal_error)?;

        self.record_worker_started(TerminalWorkerKind::AttachConnectionPump);
        let result = self.pump_viewer_input();
        let reason = match &result {
            Ok(()) => TerminalWorkerStop::AttachConnectionClosed,
            Err(error) => TerminalWorkerStop::AttachConnectionFailed(error.to_string()),
        };
        self.record_worker_stopped(TerminalWorkerKind::AttachConnectionPump, reason);
        let _ = self.output_port().detach(lease);
        result
    }

    fn pump_viewer_input(&mut self) -> io::Result<()> {
        let mut buffer = [0_u8; 8192];
        loop {
            let count = self.stream.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            self.input_port()
                .accept(TerminalInput::new(
                    buffer[..count].to_vec(),
                    InputSource::Viewer,
                ))
                .map_err(Self::terminal_error)?;
        }
        Ok(())
    }

    fn snapshot(&self) -> io::Result<crate::TranscriptSnapshot> {
        // The data plane reads the transcript snapshot through the actor only to
        // replay history bytes once at attach time. Bytes after replay flow
        // directly from the PTY-output fanout to the data stream, never through
        // the actor mailbox.
        let terminal = self.terminal().clone();
        let reply = self
            .runtime()
            .block_on(async { terminal.ask(TranscriptSnapshotRequest).await })
            .map_err(Self::actor_error)?;
        Ok(reply)
    }

    fn record_worker_started(&self, worker: TerminalWorkerKind) {
        let _ = self
            .terminal()
            .tell(TerminalWorkerLifecycle::Started(worker))
            .try_send();
    }

    fn record_worker_stopped(&self, worker: TerminalWorkerKind, reason: TerminalWorkerStop) {
        let _ = self
            .terminal()
            .tell(TerminalWorkerLifecycle::Stopped { worker, reason })
            .try_send();
    }

    fn actor_error(error: impl std::fmt::Display) -> io::Error {
        io::Error::new(io::ErrorKind::BrokenPipe, error.to_string())
    }

    fn terminal_error(error: TerminalCellError) -> io::Error {
        io::Error::new(io::ErrorKind::BrokenPipe, error.to_string())
    }
}
