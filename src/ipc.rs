//! Cross-process state sharing for multi-monitor panels.
//!
//! COSMIC spawns one applet process per output, so each monitor would
//! otherwise hold its own disconnected `Caffeine`/`Timer` state. Here, the
//! first process to bind a Unix socket becomes the "master": it performs the
//! real `org.freedesktop.ScreenSaver` inhibit calls and owns the timer. Every
//! other process connects to that socket as a "client": it forwards user
//! actions to the master and mirrors the state the master broadcasts back,
//! instead of touching D-Bus itself. If the master process exits, every
//! client races to take over the now-free socket, so exactly one of them is
//! promoted to master and re-asserts the last known state for real.

use std::path::PathBuf;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use cosmic::iced::Subscription;
use cosmic::iced_futures::futures::channel::mpsc as iced_mpsc;
use cosmic::iced_futures::futures::SinkExt;
use cosmic::iced_futures::stream;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc as tokio_mpsc};

const SOCKET_NAME: &str = "net.tropicbliss.CosmicExtAppletCaffeine.sock";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Master,
    Client,
}

/// The state shared across every instance of the applet.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SharedState {
    pub caffeinated: bool,
    pub timer_end_millis: Option<i64>,
    pub remember_state: bool,
}

/// A user action that changes shared state, sent from a client to the master.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessage {
    SetCaffeinated(bool),
    SetTimerMinutes(u64),
    StartCustomDurationSeconds(u64),
    SetRememberState(bool),
}

/// A handle the app uses to talk back to the running IPC engine.
#[derive(Debug, Clone)]
pub enum ToEngine {
    /// Master only: a new state to persist and fan out to every client.
    Broadcast(SharedState),
    /// Client only: forward this action to the master.
    SendCommand(ClientMessage),
}

#[derive(Debug, Clone)]
pub enum IpcEvent {
    /// This instance's role is now settled; use the sender to talk back.
    Elected(Role, tokio_mpsc::UnboundedSender<ToEngine>),
    /// Master only: a client asked for a state change.
    RemoteCommand(ClientMessage),
    /// Client only: the master's state changed.
    StateFromMaster(SharedState),
    /// Client only: the master went away; a re-election is in progress.
    Disconnected,
}

pub fn epoch_millis_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn socket_path() -> PathBuf {
    let dir = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    dir.join(SOCKET_NAME)
}

pub fn subscription() -> Subscription<IpcEvent> {
    Subscription::run_with_id(
        "caffeine-ipc",
        stream::channel(16, move |mut output| async move {
            loop {
                let path = socket_path();
                match try_become_master(&path).await {
                    Ok(listener) => run_master(listener, &mut output).await,
                    Err(()) => match UnixStream::connect(&path).await {
                        Ok(stream) => run_client(stream, &mut output).await,
                        Err(_) => {
                            // Stale socket with nobody listening; clear it so
                            // the next loop iteration can bind it.
                            let _ = std::fs::remove_file(&path);
                        }
                    },
                }
                tokio::time::sleep(StdDuration::from_millis(300)).await;
            }
        }),
    )
}

/// Tries to claim the master role. Cleans up and retries once if the socket
/// file is stale (left behind by a master that didn't exit cleanly).
async fn try_become_master(path: &std::path::Path) -> Result<UnixListener, ()> {
    match UnixListener::bind(path) {
        Ok(listener) => Ok(listener),
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            if UnixStream::connect(path).await.is_ok() {
                // A master is genuinely alive; stay a client.
                return Err(());
            }
            let _ = std::fs::remove_file(path);
            UnixListener::bind(path).map_err(|_| ())
        }
        Err(_) => Err(()),
    }
}

async fn run_master(listener: UnixListener, output: &mut iced_mpsc::Sender<IpcEvent>) {
    let (to_engine_tx, mut to_engine_rx) = tokio_mpsc::unbounded_channel::<ToEngine>();
    let (state_tx, _) = broadcast::channel::<SharedState>(16);
    let mut last_state = SharedState::default();

    if output
        .send(IpcEvent::Elected(Role::Master, to_engine_tx))
        .await
        .is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };
                spawn_master_connection(stream, state_tx.subscribe(), last_state.clone(), output.clone());
            }
            event = to_engine_rx.recv() => {
                match event {
                    Some(ToEngine::Broadcast(state)) => {
                        last_state = state.clone();
                        let _ = state_tx.send(state);
                    }
                    Some(ToEngine::SendCommand(_)) => { /* master never commands itself */ }
                    None => return,
                }
            }
        }
    }
}

fn spawn_master_connection(
    stream: UnixStream,
    mut state_rx: broadcast::Receiver<SharedState>,
    seed: SharedState,
    mut output: iced_mpsc::Sender<IpcEvent>,
) {
    tokio::spawn(async move {
        let (read_half, mut write_half) = stream.into_split();
        if write_line(&mut write_half, &seed).await.is_err() {
            return;
        }
        let mut lines = BufReader::new(read_half).lines();
        loop {
            tokio::select! {
                broadcast_msg = state_rx.recv() => {
                    match broadcast_msg {
                        Ok(state) => {
                            if write_line(&mut write_half, &state).await.is_err() {
                                return;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    }
                }
                line = lines.next_line() => {
                    match line {
                        Ok(Some(text)) => {
                            if let Ok(cmd) = serde_json::from_str::<ClientMessage>(&text)
                                && output.send(IpcEvent::RemoteCommand(cmd)).await.is_err()
                            {
                                return;
                            }
                        }
                        _ => return,
                    }
                }
            }
        }
    });
}

async fn run_client(stream: UnixStream, output: &mut iced_mpsc::Sender<IpcEvent>) {
    let (to_engine_tx, mut to_engine_rx) = tokio_mpsc::unbounded_channel::<ToEngine>();
    if output
        .send(IpcEvent::Elected(Role::Client, to_engine_tx))
        .await
        .is_err()
    {
        return;
    }

    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();
    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(text)) => {
                        if let Ok(state) = serde_json::from_str::<SharedState>(&text)
                            && output.send(IpcEvent::StateFromMaster(state)).await.is_err()
                        {
                            return;
                        }
                    }
                    _ => {
                        let _ = output.send(IpcEvent::Disconnected).await;
                        return;
                    }
                }
            }
            event = to_engine_rx.recv() => {
                match event {
                    Some(ToEngine::SendCommand(cmd)) => {
                        if write_line(&mut write_half, &cmd).await.is_err() {
                            let _ = output.send(IpcEvent::Disconnected).await;
                            return;
                        }
                    }
                    Some(ToEngine::Broadcast(_)) => { /* clients never broadcast */ }
                    None => return,
                }
            }
        }
    }
}

async fn write_line<T: Serialize>(
    half: &mut tokio::net::unix::OwnedWriteHalf,
    value: &T,
) -> std::io::Result<()> {
    let mut line = serde_json::to_string(value).map_err(std::io::Error::other)?;
    line.push('\n');
    half.write_all(line.as_bytes()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use cosmic::iced_futures::futures::StreamExt;

    fn test_socket_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cosmic-ext-applet-caffeine-test-{name}-{}.sock",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn master_seeds_state_broadcasts_changes_and_forwards_commands() {
        let path = test_socket_path("basic");
        let _ = std::fs::remove_file(&path);

        let listener = UnixListener::bind(&path).unwrap();
        let (mut master_out, mut master_in) = iced_mpsc::channel::<IpcEvent>(16);
        tokio::spawn(async move {
            run_master(listener, &mut master_out).await;
        });

        let Some(IpcEvent::Elected(Role::Master, engine_tx)) = master_in.next().await else {
            panic!("master did not announce its election");
        };

        let stream = UnixStream::connect(&path).await.unwrap();
        let (mut client_out, mut client_in) = iced_mpsc::channel::<IpcEvent>(16);
        tokio::spawn(async move {
            run_client(stream, &mut client_out).await;
        });

        let Some(IpcEvent::Elected(Role::Client, client_tx)) = client_in.next().await else {
            panic!("client did not announce its election");
        };

        // A newly connected client is immediately seeded with the current state.
        let Some(IpcEvent::StateFromMaster(seed)) = client_in.next().await else {
            panic!("client did not receive the seed state");
        };
        assert_eq!(seed, SharedState::default());

        // The master's state changes are pushed to the client.
        let changed = SharedState {
            caffeinated: true,
            timer_end_millis: Some(epoch_millis_now() + 60_000),
            remember_state: false,
        };
        engine_tx
            .send(ToEngine::Broadcast(changed.clone()))
            .unwrap();
        let Some(IpcEvent::StateFromMaster(received)) = client_in.next().await else {
            panic!("client did not receive the broadcast state");
        };
        assert_eq!(received, changed);

        // A client's command is forwarded to the master as a remote command.
        client_tx
            .send(ToEngine::SendCommand(ClientMessage::SetTimerMinutes(5)))
            .unwrap();
        let Some(IpcEvent::RemoteCommand(ClientMessage::SetTimerMinutes(5))) =
            master_in.next().await
        else {
            panic!("master did not receive the forwarded command");
        };

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn try_become_master_recovers_a_stale_socket() {
        let path = test_socket_path("stale");
        let _ = std::fs::remove_file(&path);

        // A listener that is bound and then dropped without unlinking leaves
        // exactly the kind of stale socket file a crashed master would.
        drop(UnixListener::bind(&path).unwrap());

        let listener = try_become_master(&path)
            .await
            .expect("a stale socket with nobody listening should be reclaimed");
        drop(listener);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn try_become_master_defers_to_a_live_master() {
        let path = test_socket_path("live");
        let _ = std::fs::remove_file(&path);

        let listener = UnixListener::bind(&path).unwrap();
        assert!(
            try_become_master(&path).await.is_err(),
            "a second instance must not steal the role while the first is alive"
        );
        drop(listener);
        let _ = std::fs::remove_file(&path);
    }
}
