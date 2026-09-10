use std::io::{self, Read, Write};

use signal_terminal::{
    ByteViewable, Query as SignalTerminalRequest, Response as SignalTerminalEvent, Restorable,
    Signal, Signalizable,
};

use crate::{
    InputSource, TerminalInput, TerminalInputGateLease, TerminalInputGateRelease,
    TerminalInputGateSequence, TerminalSize, WaitForTranscriptText,
};

const CAPTURE_REQUEST: u8 = b'C';
const SUBSCRIBE_REQUEST: u8 = b'S';
const ATTACH_REQUEST: u8 = b'T';
const PROGRAMMATIC_INPUT_REQUEST: u8 = b'P';
const VIEWER_INPUT_REQUEST: u8 = b'V';
const CLOSE_HUMAN_INPUT_REQUEST: u8 = b'G';
const OPEN_HUMAN_INPUT_REQUEST: u8 = b'O';
const RESIZE_REQUEST: u8 = b'R';
const WAIT_REQUEST: u8 = b'W';
const WAIT_EXIT_REQUEST: u8 = b'X';
const WORKER_OBSERVATION_REQUEST: u8 = b'H';
const ACCEPTANCE_REPLY: u8 = b'A';
const ATTACH_ACCEPTED_REPLY: u8 = b'I';
const ATTACH_REJECTED_REPLY: u8 = b'J';
const GATE_LEASE_REPLY: u8 = b'L';
const GATE_RELEASE_REPLY: u8 = b'U';
const WAIT_SATISFIED_REPLY: u8 = b'Y';
const MAXIMUM_FRAME_LENGTH: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub enum SocketRequest {
    Capture,
    SubscribeFromBeginning,
    Attach,
    Input(TerminalInput),
    CloseHumanInput,
    OpenHumanInput(TerminalInputGateLease),
    Resize(TerminalSize),
    Wait(WaitForTranscriptText),
    WaitExit,
    WorkerObservation,
    Signal(SignalSocketRequest),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SignalSocketRequest {
    payload: SignalTerminalRequest,
}

impl SignalSocketRequest {
    fn new(payload: SignalTerminalRequest) -> Self {
        Self { payload }
    }

    pub fn payload(&self) -> &SignalTerminalRequest {
        &self.payload
    }

    pub fn into_payload(self) -> SignalTerminalRequest {
        self.payload
    }
}

pub struct SocketRequestReader<Reader> {
    reader: Reader,
}

impl<Reader> SocketRequestReader<Reader>
where
    Reader: Read,
{
    pub fn new(reader: Reader) -> Self {
        Self { reader }
    }

    pub fn read_request(&mut self) -> io::Result<SocketRequest> {
        let mut tag = [0_u8; 1];
        self.reader.read_exact(&mut tag)?;
        match tag[0] {
            CAPTURE_REQUEST => Ok(SocketRequest::Capture),
            SUBSCRIBE_REQUEST => Ok(SocketRequest::SubscribeFromBeginning),
            ATTACH_REQUEST => Ok(SocketRequest::Attach),
            PROGRAMMATIC_INPUT_REQUEST => {
                let bytes = self.read_frame()?;
                Ok(SocketRequest::Input(TerminalInput::new(
                    bytes,
                    InputSource::Programmatic,
                )))
            }
            VIEWER_INPUT_REQUEST => {
                let bytes = self.read_frame()?;
                Ok(SocketRequest::Input(TerminalInput::new(
                    bytes,
                    InputSource::Viewer,
                )))
            }
            CLOSE_HUMAN_INPUT_REQUEST => Ok(SocketRequest::CloseHumanInput),
            OPEN_HUMAN_INPUT_REQUEST => {
                let sequence = TerminalInputGateSequence::new(self.read_u64()?);
                Ok(SocketRequest::OpenHumanInput(TerminalInputGateLease::new(
                    sequence,
                )))
            }
            RESIZE_REQUEST => {
                let rows = self.read_u16()?;
                let columns = self.read_u16()?;
                Ok(SocketRequest::Resize(TerminalSize::new(rows, columns)))
            }
            WAIT_REQUEST => {
                let bytes = self.read_frame()?;
                Ok(SocketRequest::Wait(WaitForTranscriptText::new(bytes)))
            }
            WAIT_EXIT_REQUEST => Ok(SocketRequest::WaitExit),
            WORKER_OBSERVATION_REQUEST => Ok(SocketRequest::WorkerObservation),
            other => self.read_signal_request(other),
        }
    }

    fn read_signal_request(&mut self, first_length_byte: u8) -> io::Result<SocketRequest> {
        let mut tail = [0_u8; 3];
        self.reader.read_exact(&mut tail)?;
        let length = u32::from_be_bytes([first_length_byte, tail[0], tail[1], tail[2]]) as u64;
        if length > MAXIMUM_FRAME_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "signal length exceeds maximum",
            ));
        }
        let mut bytes = vec![0; length as usize];
        self.reader.read_exact(&mut bytes)?;
        let payload = Signal::<SignalTerminalRequest>::from(bytes)
            .restore()
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("signal query restore failed: {error}"),
                )
            })?;
        Ok(SocketRequest::Signal(SignalSocketRequest::new(payload)))
    }

    fn read_frame(&mut self) -> io::Result<Vec<u8>> {
        let length = self.read_u64()?;
        if length > MAXIMUM_FRAME_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("socket frame length {length} exceeds maximum"),
            ));
        }
        let mut bytes = vec![0_u8; length as usize];
        self.reader.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        let mut bytes = [0_u8; 8];
        self.reader.read_exact(&mut bytes)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn read_u16(&mut self) -> io::Result<u16> {
        let mut bytes = [0_u8; 2];
        self.reader.read_exact(&mut bytes)?;
        Ok(u16::from_be_bytes(bytes))
    }
}

pub struct SocketRequestWriter<Writer> {
    writer: Writer,
}

impl<Writer> SocketRequestWriter<Writer>
where
    Writer: Write,
{
    pub fn new(writer: Writer) -> Self {
        Self { writer }
    }

    pub fn write_capture_request(&mut self) -> io::Result<()> {
        self.writer.write_all(&[CAPTURE_REQUEST])?;
        self.writer.flush()
    }

    pub fn write_subscription_request(&mut self) -> io::Result<()> {
        self.writer.write_all(&[SUBSCRIBE_REQUEST])?;
        self.writer.flush()
    }

    pub fn write_attach_request(&mut self) -> io::Result<()> {
        self.writer.write_all(&[ATTACH_REQUEST])?;
        self.writer.flush()
    }

    pub fn write_programmatic_input(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(&[PROGRAMMATIC_INPUT_REQUEST])?;
        self.write_frame(bytes)?;
        self.writer.flush()
    }

    pub fn write_viewer_input(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(&[VIEWER_INPUT_REQUEST])?;
        self.write_frame(bytes)?;
        self.writer.flush()
    }

    pub fn write_close_human_input_request(&mut self) -> io::Result<()> {
        self.writer.write_all(&[CLOSE_HUMAN_INPUT_REQUEST])?;
        self.writer.flush()
    }

    pub fn write_open_human_input_request(
        &mut self,
        lease: TerminalInputGateLease,
    ) -> io::Result<()> {
        self.writer.write_all(&[OPEN_HUMAN_INPUT_REQUEST])?;
        self.write_u64(lease.sequence().into_u64())?;
        self.writer.flush()
    }

    pub fn write_resize_request(&mut self, size: TerminalSize) -> io::Result<()> {
        self.writer.write_all(&[RESIZE_REQUEST])?;
        self.write_u16(size.rows())?;
        self.write_u16(size.columns())?;
        self.writer.flush()
    }

    pub fn write_wait_request(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.writer.write_all(&[WAIT_REQUEST])?;
        self.write_frame(bytes)?;
        self.writer.flush()
    }

    pub fn write_wait_exit_request(&mut self) -> io::Result<()> {
        self.writer.write_all(&[WAIT_EXIT_REQUEST])?;
        self.writer.flush()
    }

    pub fn write_worker_observation_request(&mut self) -> io::Result<()> {
        self.writer.write_all(&[WORKER_OBSERVATION_REQUEST])?;
        self.writer.flush()
    }

    pub fn write_signal_request(&mut self, request: SignalTerminalRequest) -> io::Result<()> {
        let bytes = request
            .signalize()
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("signal query archive failed: {error}"),
                )
            })?
            .bytes()
            .to_vec();
        self.writer.write_all(&(bytes.len() as u32).to_be_bytes())?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()
    }

    fn write_frame(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.write_u64(bytes.len() as u64)?;
        self.writer.write_all(bytes)
    }

    fn write_u64(&mut self, value: u64) -> io::Result<()> {
        self.writer.write_all(&value.to_be_bytes())
    }

    fn write_u16(&mut self, value: u16) -> io::Result<()> {
        self.writer.write_all(&value.to_be_bytes())
    }
}

pub struct SocketReplyReader<Reader> {
    reader: Reader,
}

impl<Reader> SocketReplyReader<Reader>
where
    Reader: Read,
{
    pub fn new(reader: Reader) -> Self {
        Self { reader }
    }

    pub fn read_snapshot(&mut self) -> io::Result<Vec<u8>> {
        self.read_frame()
    }

    pub fn read_acceptance(&mut self) -> io::Result<()> {
        self.read_expected_tag(ACCEPTANCE_REPLY)
    }

    pub fn read_attach_acceptance(&mut self) -> io::Result<()> {
        let mut tag = [0_u8; 1];
        self.reader.read_exact(&mut tag)?;
        match tag[0] {
            ATTACH_ACCEPTED_REPLY => Ok(()),
            ATTACH_REJECTED_REPLY => {
                let message = self.read_string()?;
                Err(io::Error::new(io::ErrorKind::ConnectionRefused, message))
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected attach reply tag: {other}"),
            )),
        }
    }

    pub fn read_gate_lease(&mut self) -> io::Result<TerminalInputGateLease> {
        self.read_expected_tag(GATE_LEASE_REPLY)?;
        Ok(TerminalInputGateLease::new(TerminalInputGateSequence::new(
            self.read_u64()?,
        )))
    }

    pub fn read_gate_release(&mut self) -> io::Result<TerminalInputGateRelease> {
        self.read_expected_tag(GATE_RELEASE_REPLY)?;
        let lease = TerminalInputGateLease::new(TerminalInputGateSequence::new(self.read_u64()?));
        let held_byte_count = self.read_u64()?;
        let held_byte_count = usize::try_from(held_byte_count).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("gate release held byte count overflowed usize: {error}"),
            )
        })?;
        Ok(TerminalInputGateRelease::new(lease, held_byte_count))
    }

    pub fn read_wait_satisfied(&mut self) -> io::Result<()> {
        self.read_expected_tag(WAIT_SATISFIED_REPLY)
    }

    pub fn read_exit_status(&mut self) -> io::Result<String> {
        self.read_string()
    }

    pub fn read_signal_event(&mut self) -> io::Result<SignalTerminalEvent> {
        let mut length = [0; 4];
        self.reader.read_exact(&mut length)?;
        let length = u32::from_be_bytes(length) as u64;
        if length > MAXIMUM_FRAME_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "signal length exceeds maximum",
            ));
        }
        let mut bytes = vec![0; length as usize];
        self.reader.read_exact(&mut bytes)?;
        Signal::<SignalTerminalEvent>::from(bytes)
            .restore()
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("signal response restore failed: {error}"),
                )
            })
    }

    fn read_expected_tag(&mut self, expected: u8) -> io::Result<()> {
        let mut tag = [0_u8; 1];
        self.reader.read_exact(&mut tag)?;
        if tag[0] == expected {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected socket reply tag: {}", tag[0]),
            ))
        }
    }

    fn read_frame(&mut self) -> io::Result<Vec<u8>> {
        let length = self.read_u64()?;
        if length > MAXIMUM_FRAME_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("socket reply length {length} exceeds maximum"),
            ));
        }
        let mut bytes = vec![0_u8; length as usize];
        self.reader.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        let mut bytes = [0_u8; 8];
        self.reader.read_exact(&mut bytes)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn read_string(&mut self) -> io::Result<String> {
        String::from_utf8(self.read_frame()?).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("socket reply string was not utf-8: {error}"),
            )
        })
    }
}

pub struct SocketReplyWriter<Writer> {
    writer: Writer,
}

impl<Writer> SocketReplyWriter<Writer>
where
    Writer: Write,
{
    pub fn new(writer: Writer) -> Self {
        Self { writer }
    }

    pub fn write_snapshot(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.write_frame(bytes)?;
        self.writer.flush()
    }

    pub fn write_acceptance(&mut self) -> io::Result<()> {
        self.writer.write_all(&[ACCEPTANCE_REPLY])?;
        self.writer.flush()
    }

    pub fn write_attach_accepted(&mut self) -> io::Result<()> {
        self.writer.write_all(&[ATTACH_ACCEPTED_REPLY])?;
        self.writer.flush()
    }

    pub fn write_attach_rejected(&mut self, reason: &str) -> io::Result<()> {
        self.writer.write_all(&[ATTACH_REJECTED_REPLY])?;
        self.write_frame(reason.as_bytes())?;
        self.writer.flush()
    }

    pub fn write_gate_lease(&mut self, lease: TerminalInputGateLease) -> io::Result<()> {
        self.writer.write_all(&[GATE_LEASE_REPLY])?;
        self.write_u64(lease.sequence().into_u64())?;
        self.writer.flush()
    }

    pub fn write_gate_release(&mut self, release: TerminalInputGateRelease) -> io::Result<()> {
        self.writer.write_all(&[GATE_RELEASE_REPLY])?;
        self.write_u64(release.lease().sequence().into_u64())?;
        self.write_u64(release.held_byte_count() as u64)?;
        self.writer.flush()
    }

    pub fn write_wait_satisfied(&mut self) -> io::Result<()> {
        self.writer.write_all(&[WAIT_SATISFIED_REPLY])?;
        self.writer.flush()
    }

    pub fn write_exit_status(&mut self, status: &str) -> io::Result<()> {
        self.write_frame(status.as_bytes())?;
        self.writer.flush()
    }

    pub fn write_signal_event(&mut self, event: SignalTerminalEvent) -> io::Result<()> {
        let bytes = event
            .signalize()
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("signal response archive failed: {error}"),
                )
            })?
            .bytes()
            .to_vec();
        self.writer.write_all(&(bytes.len() as u32).to_be_bytes())?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()
    }

    fn write_frame(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.write_u64(bytes.len() as u64)?;
        self.writer.write_all(bytes)
    }

    fn write_u64(&mut self, value: u64) -> io::Result<()> {
        self.writer.write_all(&value.to_be_bytes())
    }
}
