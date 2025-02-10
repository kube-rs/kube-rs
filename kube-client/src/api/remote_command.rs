use std::future::Future;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Status;

use futures::{
    channel::{mpsc, oneshot},
    FutureExt, SinkExt, StreamExt,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream},
    select,
};
use tokio_tungstenite::tungstenite as ws;

use crate::client::Connection;

use super::AttachParams;

type StatusReceiver = oneshot::Receiver<Status>;
type StatusSender = oneshot::Sender<Status>;

type TerminalSizeReceiver = mpsc::Receiver<TerminalSize>;
type TerminalSizeSender = mpsc::Sender<TerminalSize>;

/// TerminalSize define the size of a terminal
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
#[serde(rename_all = "PascalCase")]
pub struct TerminalSize {
    /// width of the terminal
    pub width: u16,
    /// height of the terminal
    pub height: u16,
}

/// Errors from attaching to a pod.
#[derive(Debug, Error)]
pub enum Error {
    /// Failed to read from stdin
    #[error("failed to read from stdin: {0}")]
    ReadStdin(#[source] std::io::Error),

    /// Failed to send stdin data to the pod
    #[error("failed to send a stdin data: {0}")]
    SendStdin(#[source] ws::Error),

    /// Failed to write to stdout
    #[error("failed to write to stdout: {0}")]
    WriteStdout(#[source] std::io::Error),

    /// Failed to write to stderr
    #[error("failed to write to stderr: {0}")]
    WriteStderr(#[source] std::io::Error),

    /// Failed to receive a WebSocket message from the server.
    #[error("failed to receive a WebSocket message: {0}")]
    ReceiveWebSocketMessage(#[source] ws::Error),

    // Failed to complete the background task
    #[error("failed to complete the background task: {0}")]
    Spawn(#[source] tokio::task::JoinError),

    /// Failed to send close message.
    #[error("failed to send a WebSocket close message: {0}")]
    SendClose(#[source] ws::Error),

    /// Failed to deserialize status object
    #[error("failed to deserialize status object: {0}")]
    DeserializeStatus(#[source] serde_json::Error),

    /// Failed to send status object
    #[error("failed to send status object")]
    SendStatus,

    /// Fail to serialize Terminalsize object
    #[error("failed to serialize TerminalSize object: {0}")]
    SerializeTerminalSize(#[source] serde_json::Error),

    /// Fail to send terminal size message
    #[error("failed to send terminal size message")]
    SendTerminalSize(#[source] ws::Error),

    /// Failed to set terminal size, tty need to be true to resize the terminal
    #[error("failed to set terminal size, tty need to be true to resize the terminal")]
    TtyNeedToBeTrue,
}

const MAX_BUF_SIZE: usize = 1024;

/// Represents an attached process in a container for [`attach`] and [`exec`].
///
/// Provides access to `stdin`, `stdout`, and `stderr` if attached.
///
/// Use [`AttachedProcess::join`] to wait for the process to terminate.
///
/// [`attach`]: crate::Api::attach
/// [`exec`]: crate::Api::exec
#[cfg_attr(docsrs, doc(cfg(feature = "ws")))]
pub struct AttachedProcess {
    has_stdin: bool,
    has_stdout: bool,
    has_stderr: bool,
    stdin_writer: Option<DuplexStream>,
    stdout_reader: Option<DuplexStream>,
    stderr_reader: Option<DuplexStream>,
    status_rx: Option<StatusReceiver>,
    terminal_resize_tx: Option<TerminalSizeSender>,
    task: tokio::task::JoinHandle<Result<(), Error>>,
}

impl AttachedProcess {
    pub(crate) fn new(connection: Connection, ap: &AttachParams) -> Self
    {
        // To simplify the implementation, always create a pipe for stdin.
        // The caller does not have access to it unless they had requested.
        let (stdin_writer, stdin_reader) = tokio::io::duplex(ap.max_stdin_buf_size.unwrap_or(MAX_BUF_SIZE));
        let (stdout_writer, stdout_reader) = if ap.stdout {
            let (w, r) = tokio::io::duplex(ap.max_stdout_buf_size.unwrap_or(MAX_BUF_SIZE));
            (Some(w), Some(r))
        } else {
            (None, None)
        };
        let (stderr_writer, stderr_reader) = if ap.stderr {
            let (w, r) = tokio::io::duplex(ap.max_stderr_buf_size.unwrap_or(MAX_BUF_SIZE));
            (Some(w), Some(r))
        } else {
            (None, None)
        };
        let (status_tx, status_rx) = oneshot::channel();
        let (terminal_resize_tx, terminal_resize_rx) = if ap.tty {
            let (w, r) = mpsc::channel(10);
            (Some(w), Some(r))
        } else {
            (None, None)
        };

        let task = tokio::spawn(start_message_loop(
            connection,
            stdin_reader,
            stdout_writer,
            stderr_writer,
            status_tx,
            terminal_resize_rx,
        ));

        AttachedProcess {
            has_stdin: ap.stdin,
            has_stdout: ap.stdout,
            has_stderr: ap.stderr,
            task,
            stdin_writer: Some(stdin_writer),
            stdout_reader,
            stderr_reader,
            terminal_resize_tx,
            status_rx: Some(status_rx),
        }
    }

    /// Async writer to stdin.
    /// ```no_run
    /// # use kube_client::api::AttachedProcess;
    /// # use tokio::io::{AsyncReadExt, AsyncWriteExt};
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let attached: AttachedProcess = todo!();
    /// let mut stdin_writer = attached.stdin().unwrap();
    /// stdin_writer.write(b"foo\n").await?;
    /// # Ok(())
    /// # }
    /// ```
    /// Only available if [`AttachParams`](super::AttachParams) had `stdin`.
    pub fn stdin(&mut self) -> Option<impl AsyncWrite + Unpin> {
        if !self.has_stdin {
            return None;
        }
        self.stdin_writer.take()
    }

    /// Async reader for stdout outputs.
    /// ```no_run
    /// # use kube_client::api::AttachedProcess;
    /// # use tokio::io::{AsyncReadExt, AsyncWriteExt};
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let attached: AttachedProcess = todo!();
    /// let mut stdout_reader = attached.stdout().unwrap();
    /// let mut buf = [0u8; 4];
    /// stdout_reader.read_exact(&mut buf).await?;
    /// # Ok(())
    /// # }
    /// ```
    /// Only available if [`AttachParams`](super::AttachParams) had `stdout`.
    pub fn stdout(&mut self) -> Option<impl AsyncRead + Unpin> {
        if !self.has_stdout {
            return None;
        }
        self.stdout_reader.take()
    }

    /// Async reader for stderr outputs.
    /// ```no_run
    /// # use kube_client::api::AttachedProcess;
    /// # use tokio::io::{AsyncReadExt, AsyncWriteExt};
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let attached: AttachedProcess = todo!();
    /// let mut stderr_reader = attached.stderr().unwrap();
    /// let mut buf = [0u8; 4];
    /// stderr_reader.read_exact(&mut buf).await?;
    /// # Ok(())
    /// # }
    /// ```
    /// Only available if [`AttachParams`](super::AttachParams) had `stderr`.
    pub fn stderr(&mut self) -> Option<impl AsyncRead + Unpin> {
        if !self.has_stderr {
            return None;
        }
        self.stderr_reader.take()
    }

    /// Abort the background task, causing remote command to fail.
    #[inline]
    pub fn abort(&self) {
        self.task.abort();
    }

    /// Waits for the remote command task to complete.
    pub async fn join(self) -> Result<(), Error> {
        self.task.await.unwrap_or_else(|e| Err(Error::Spawn(e)))
    }

    /// Take a future that resolves with any status object or when the sender is dropped.
    ///
    /// Returns `None` if called more than once.
    pub fn take_status(&mut self) -> Option<impl Future<Output = Option<Status>>> {
        self.status_rx.take().map(|recv| recv.map(|res| res.ok()))
    }

    /// Async writer to change the terminal size
    /// ```no_run
    /// # use kube_client::api::{AttachedProcess, TerminalSize};
    /// # use tokio::io::{AsyncReadExt, AsyncWriteExt};
    /// # use futures::SinkExt;
    /// # async fn wrapper() -> Result<(), Box<dyn std::error::Error>> {
    /// # let attached: AttachedProcess = todo!();
    /// let mut terminal_size_writer = attached.terminal_size().unwrap();
    /// terminal_size_writer.send(TerminalSize{
    ///     height: 100,
    ///     width: 200,
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    /// Only available if [`AttachParams`](super::AttachParams) had `tty`.
    pub fn terminal_size(&mut self) -> Option<TerminalSizeSender> {
        self.terminal_resize_tx.take()
    }
}

// theses values come from here: https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apimachinery/pkg/util/remotecommand/constants.go#L57
const STDIN_CHANNEL: u8 = 0;
const STDOUT_CHANNEL: u8 = 1;
const STDERR_CHANNEL: u8 = 2;
// status channel receives `Status` object on exit.
const STATUS_CHANNEL: u8 = 3;
// resize channel is use to send TerminalSize object to change the size of the terminal
const RESIZE_CHANNEL: u8 = 4;
/// Used to signal that a channel has reached EOF. Only works on V5 of the protocol.
const CLOSE_CHANNEL: u8 = 255;

async fn start_message_loop(
    connection: Connection,
    stdin: impl AsyncRead + Unpin,
    mut stdout: Option<impl AsyncWrite + Unpin>,
    mut stderr: Option<impl AsyncWrite + Unpin>,
    status_tx: StatusSender,
    mut terminal_size_rx: Option<TerminalSizeReceiver>,
) -> Result<(), Error>
{
    let supports_stream_close = connection.supports_stream_close();
    let stream = connection.into_stream();
    let mut stdin_stream = tokio_util::io::ReaderStream::new(stdin);
    let (mut server_send, raw_server_recv) = stream.split();
    // Work with filtered messages to reduce noise.
    let mut server_recv = raw_server_recv.filter_map(filter_message).boxed();
    let mut have_terminal_size_rx = terminal_size_rx.is_some();

    // True until we reach EOF for stdin.
    let mut stdin_is_open = true;

    loop {
        let terminal_size_next = async {
            match terminal_size_rx.as_mut() {
                Some(tmp) => Some(tmp.next().await),
                None => None,
            }
        };
        select! {
            server_message = server_recv.next() => {
                match server_message {
                    Some(Ok(Message::Stdout(bin))) => {
                        if let Some(stdout) = stdout.as_mut() {
                            stdout.write_all(&bin[1..]).await.map_err(Error::WriteStdout)?;
                        }
                    },
                    Some(Ok(Message::Stderr(bin))) => {
                        if let Some(stderr) = stderr.as_mut() {
                            stderr.write_all(&bin[1..]).await.map_err(Error::WriteStderr)?;
                        }
                    },
                    Some(Ok(Message::Status(bin))) => {
                        let status = serde_json::from_slice::<Status>(&bin[1..]).map_err(Error::DeserializeStatus)?;
                        status_tx.send(status).map_err(|_| Error::SendStatus)?;
                        break
                    },
                    Some(Err(err)) => {
                        return Err(Error::ReceiveWebSocketMessage(err));
                    },
                    None => {
                        // Connection closed properly
                        break
                    },
                }
            },
            stdin_message = stdin_stream.next(), if stdin_is_open => {
                match stdin_message {
                    Some(Ok(bytes)) => {
                        if !bytes.is_empty() {
                            let mut vec = Vec::with_capacity(bytes.len() + 1);
                            vec.push(STDIN_CHANNEL);
                            vec.extend_from_slice(&bytes[..]);
                            server_send
                                .send(ws::Message::binary(vec))
                                .await
                                .map_err(Error::SendStdin)?;
                        }
                    },
                    Some(Err(err)) => {
                        return Err(Error::ReadStdin(err));
                    }
                    None => {
                        // Stdin closed (writer half dropped).
                        // Let the server know we reached the end of stdin.
                        if supports_stream_close {
                            // Signal stdin has reached EOF.
                            // See: https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apimachinery/pkg/util/httpstream/wsstream/conn.go#L346
                            let vec = vec![CLOSE_CHANNEL, STDIN_CHANNEL];
                            server_send
                                .send(ws::Message::binary(vec))
                                .await
                                .map_err(Error::SendStdin)?;
                        } else {
                            // Best we can do is trigger the whole websocket to close.
                            // We may miss out on any remaining stdout data that has not
                            // been sent yet.
                            server_send.close().await.map_err(Error::SendClose)?;
                        }

                        // Do not check stdin_stream for data in future loops.
                        stdin_is_open = false;
                    }
                }
            },
            Some(terminal_size_message) = terminal_size_next, if have_terminal_size_rx => {
                match terminal_size_message {
                    Some(new_size) => {
                        let new_size = serde_json::to_vec(&new_size).map_err(Error::SerializeTerminalSize)?;
                        let mut vec = Vec::with_capacity(new_size.len() + 1);
                        vec.push(RESIZE_CHANNEL);
                        vec.extend_from_slice(&new_size[..]);
                        server_send.send(ws::Message::Binary(vec.into())).await.map_err(Error::SendTerminalSize)?;
                    },
                    None => {
                        have_terminal_size_rx = false;
                    }
                }
            },
        }
    }

    Ok(())
}

/// Channeled messages from the server.
enum Message {
    /// To Stdout channel (1)
    Stdout(Vec<u8>),
    /// To stderr channel (2)
    Stderr(Vec<u8>),
    /// To error/status channel (3)
    Status(Vec<u8>),
}

// Filter to reduce all the possible WebSocket messages into a few we expect to receive.
async fn filter_message(wsm: Result<ws::Message, ws::Error>) -> Option<Result<Message, ws::Error>> {
    match wsm {
        // The protocol only sends binary frames.
        // Message of size 1 (only channel number) is sent on connection.
        Ok(ws::Message::Binary(bin)) if bin.len() > 1 => match bin[0] {
            STDOUT_CHANNEL => Some(Ok(Message::Stdout(bin.into()))),
            STDERR_CHANNEL => Some(Ok(Message::Stderr(bin.into()))),
            STATUS_CHANNEL => Some(Ok(Message::Status(bin.into()))),
            // We don't receive messages to stdin and resize channels.
            _ => None,
        },
        // Ignore any other message types.
        // We can ignore close message because the server never sends anything special.
        // The connection terminates on `None`.
        Ok(_) => None,
        // Fatal errors. `WebSocketStream` turns `ConnectionClosed` and `AlreadyClosed` into `None`.
        // So these are unrecoverables.
        Err(err) => Some(Err(err)),
    }
}
