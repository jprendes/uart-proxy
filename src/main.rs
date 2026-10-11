use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::sleep;
use tokio_serial::{SerialPort, SerialPortBuilderExt};
#[cfg(target_os = "linux")]
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};

const PROTOCOL_MAGIC: &[u8; 11] = b"UARTPROXY\x02\n";
const FRAME_DATA: u8 = 1;
const FRAME_MODEM_CONTROL: u8 = 2;
const MODEM_DTR: u8 = 1 << 0;
const MODEM_RTS: u8 = 1 << 1;
const MAX_FRAME_SIZE: usize = 64 * 1024;
const DEFAULT_LISTEN: &str = "127.0.0.1:7000";
const SINGLE_PORT_ROUTE: &str = "serial";

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Expose one or more physical serial ports over WebSocket.
    Server(ServerArgs),
    /// Expose a remote serial port as a local Linux PTY.
    Client(ClientArgs),
}

#[derive(Debug, Args)]
struct ServerArgs {
    /// Local serial device, such as /dev/ttyACM0 or COM3.
    #[arg(long, required_unless_present = "config", conflicts_with = "config")]
    serial: Option<String>,

    /// TOML file containing named serial-port mappings.
    #[arg(long, conflicts_with = "serial")]
    config: Option<PathBuf>,

    /// Serial baud rate.
    #[arg(long, default_value_t = 115_200, conflicts_with = "config")]
    baud: u32,

    /// Address on which to listen, overriding the configuration file.
    #[arg(long)]
    listen: Option<SocketAddr>,

    /// Pulse DTR whenever a client connects.
    #[arg(long, conflicts_with = "config")]
    dtr_reset: bool,
}

#[derive(Debug, Args)]
struct ClientArgs {
    /// Remote WebSocket URL, such as ws://server:7000/com1.
    #[arg(long)]
    connect: String,

    /// Optional stable symlink to the generated PTY.
    #[arg(long)]
    link: Option<PathBuf>,

    /// Delay before retrying a lost or refused TCP connection.
    #[arg(long, default_value_t = 1_000)]
    reconnect_delay_ms: u64,

    /// Disable propagation of PTY open/close state to physical DTR.
    #[arg(long)]
    no_modem_control: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct SerialEndpoint {
    device: String,
    #[serde(default = "default_baud")]
    baud: u32,
    #[serde(default)]
    dtr_reset: bool,
}

#[derive(Debug, Deserialize)]
struct ServerFileConfig {
    listen: Option<SocketAddr>,
    ports: HashMap<String, SerialEndpoint>,
}

fn default_baud() -> u32 {
    115_200
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Server(args) => run_server(args).await,
        Command::Client(args) => run_client(args).await,
    }
}

async fn run_server(args: ServerArgs) -> Result<()> {
    let (listen, ports) = load_server_settings(args).await?;
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("failed to listen on {listen}"))?;
    let ports = Arc::new(ports);

    println!("serving {} serial port(s) at ws://{listen}", ports.len());
    for (route, endpoint) in ports.iter() {
        println!("  /{route} -> {} ({} baud)", endpoint.device, endpoint.baud);
    }

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("failed to accept TCP connection")?;
                let ports = Arc::clone(&ports);
                tokio::spawn(async move {
                    if let Err(error) = handle_websocket_connection(stream, peer, ports).await {
                        eprintln!("client {peer} disconnected with error: {error:#}");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for Ctrl-C")?;
                println!("shutting down");
                return Ok(());
            }
        }
    }
}

async fn load_server_settings(
    args: ServerArgs,
) -> Result<(SocketAddr, HashMap<String, SerialEndpoint>)> {
    if let Some(path) = args.config {
        let contents = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("failed to read configuration {}", path.display()))?;
        let config: ServerFileConfig = toml::from_str(&contents)
            .with_context(|| format!("failed to parse configuration {}", path.display()))?;
        validate_routes(&config.ports)?;
        if config.ports.is_empty() {
            anyhow::bail!("configuration must define at least one port");
        }
        let listen = args
            .listen
            .or(config.listen)
            .unwrap_or(DEFAULT_LISTEN.parse().unwrap());
        return Ok((listen, config.ports));
    }

    let endpoint = SerialEndpoint {
        device: args.serial.context("--serial is required")?,
        baud: args.baud,
        dtr_reset: args.dtr_reset,
    };
    let listen = args.listen.unwrap_or(DEFAULT_LISTEN.parse().unwrap());
    Ok((
        listen,
        HashMap::from([(SINGLE_PORT_ROUTE.into(), endpoint)]),
    ))
}

fn validate_routes(ports: &HashMap<String, SerialEndpoint>) -> Result<()> {
    for route in ports.keys() {
        if route.is_empty()
            || route.contains('/')
            || route.contains('?')
            || route.contains('#')
            || route == "."
            || route == ".."
        {
            anyhow::bail!("invalid serial-port route {route:?}");
        }
    }
    Ok(())
}

async fn handle_websocket_connection(
    stream: tokio::net::TcpStream,
    peer: SocketAddr,
    ports: Arc<HashMap<String, SerialEndpoint>>,
) -> Result<()> {
    stream
        .set_nodelay(true)
        .context("failed to configure TCP connection")?;
    let requested_path = Arc::new(Mutex::new(None));
    let callback_path = Arc::clone(&requested_path);
    let mut websocket = accept_hdr_async(
        stream,
        move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
              response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            *callback_path.lock().expect("request path mutex poisoned") =
                Some(request.uri().path().trim_matches('/').to_owned());
            Ok(response)
        },
    )
    .await
    .context("WebSocket handshake failed")?;
    let route = requested_path
        .lock()
        .expect("request path mutex poisoned")
        .take()
        .context("WebSocket request did not contain a path")?;
    let endpoint = ports
        .get(&route)
        .with_context(|| format!("no serial port is configured at /{route}"))?
        .clone();

    println!("client {peer} connected to /{route}");
    server_handshake(&mut websocket).await?;
    proxy_serial_connection(&endpoint, websocket).await?;
    println!("client {peer} disconnected from /{route}");
    Ok(())
}

async fn proxy_serial_connection(
    endpoint: &SerialEndpoint,
    websocket: WebSocketStream<tokio::net::TcpStream>,
) -> Result<()> {
    let mut serial = tokio_serial::new(&endpoint.device, endpoint.baud)
        .open_native_async()
        .with_context(|| {
            format!(
                "failed to open serial port {} at {} baud",
                endpoint.device, endpoint.baud
            )
        })?;

    if endpoint.dtr_reset {
        serial
            .write_data_terminal_ready(false)
            .context("failed to clear DTR")?;
        sleep(Duration::from_millis(50)).await;
        serial
            .write_data_terminal_ready(true)
            .context("failed to assert DTR")?;
        sleep(Duration::from_millis(50)).await;
        serial
            .write_data_terminal_ready(false)
            .context("failed to finish DTR pulse")?;
    }

    let (mut websocket_writer, mut websocket_reader) = websocket.split();
    let mut serial_buffer = [0_u8; 8192];
    let mut from_client = 0_u64;
    let mut from_serial = 0_u64;

    loop {
        tokio::select! {
            message = websocket_reader.next() => {
                match read_websocket_frame(message)? {
                    Some(Frame::Data(data)) => {
                        serial.write_all(&data).await.context("failed to write to serial port")?;
                        from_client += data.len() as u64;
                    }
                    Some(Frame::ModemControl { mask, values }) => {
                        apply_modem_control(&mut serial, mask, values)?;
                    }
                    Some(Frame::Ping(payload)) => {
                        websocket_writer
                            .send(Message::Pong(payload))
                            .await
                            .context("failed to send WebSocket pong")?;
                    }
                    Some(Frame::Ignore) => {}
                    None => {
                        println!(
                            "forwarded {from_client} bytes to serial and {from_serial} bytes to TCP"
                        );
                        return Ok(());
                    }
                }
            }
            read = serial.read(&mut serial_buffer) => {
                let bytes_read = read.context("failed to read from serial port")?;
                if bytes_read == 0 {
                    anyhow::bail!("serial port reached end of stream");
                }
                write_data_frame(&mut websocket_writer, &serial_buffer[..bytes_read]).await?;
                from_serial += bytes_read as u64;
            }
        }
    }
}

async fn server_handshake(websocket: &mut WebSocketStream<tokio::net::TcpStream>) -> Result<()> {
    let message = websocket
        .next()
        .await
        .context("client disconnected before protocol handshake")?
        .context("failed to read protocol handshake")?;
    if !matches!(message, Message::Binary(payload) if payload.as_ref() == PROTOCOL_MAGIC) {
        anyhow::bail!("client uses an unsupported protocol");
    }
    websocket
        .send(Message::Binary(PROTOCOL_MAGIC.to_vec().into()))
        .await
        .context("failed to send protocol handshake")
}

fn apply_modem_control(
    serial: &mut tokio_serial::SerialStream,
    mask: u8,
    values: u8,
) -> Result<()> {
    if mask & !(MODEM_DTR | MODEM_RTS) != 0 {
        anyhow::bail!("unsupported modem-control mask {mask:#04x}");
    }
    if mask & MODEM_DTR != 0 {
        serial
            .write_data_terminal_ready(values & MODEM_DTR != 0)
            .context("failed to change DTR")?;
    }
    if mask & MODEM_RTS != 0 {
        serial
            .write_request_to_send(values & MODEM_RTS != 0)
            .context("failed to change RTS")?;
    }
    Ok(())
}

enum Frame {
    Data(Vec<u8>),
    ModemControl { mask: u8, values: u8 },
    Ping(tokio_tungstenite::tungstenite::Bytes),
    Ignore,
}

fn read_websocket_frame(
    message: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
) -> Result<Option<Frame>> {
    let message = match message {
        Some(message) => message.context("failed to read WebSocket message")?,
        None => return Ok(None),
    };
    let payload = match message {
        Message::Binary(payload) => payload,
        Message::Close(_) => return Ok(None),
        Message::Ping(payload) => return Ok(Some(Frame::Ping(payload))),
        Message::Pong(_) => return Ok(Some(Frame::Ignore)),
        Message::Text(_) => anyhow::bail!("text WebSocket messages are not supported"),
        Message::Frame(_) => anyhow::bail!("unexpected raw WebSocket frame"),
    };
    if payload.is_empty() {
        anyhow::bail!("empty WebSocket protocol frame");
    }
    if payload.len() > MAX_FRAME_SIZE + 1 {
        anyhow::bail!("WebSocket frame exceeds maximum size");
    }

    match payload[0] {
        FRAME_DATA => Ok(Some(Frame::Data(payload[1..].to_vec()))),
        FRAME_MODEM_CONTROL if payload.len() == 3 => Ok(Some(Frame::ModemControl {
            mask: payload[1],
            values: payload[2],
        })),
        FRAME_MODEM_CONTROL => {
            anyhow::bail!("invalid modem-control frame length {}", payload.len() - 1)
        }
        frame_type => anyhow::bail!("unknown frame type {frame_type}"),
    }
}

async fn write_frame<S>(writer: &mut S, frame_type: u8, payload: &[u8]) -> Result<()>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    writer
        .send(encode_frame(frame_type, payload)?)
        .await
        .context("failed to write WebSocket frame")
}

fn encode_frame(frame_type: u8, payload: &[u8]) -> Result<Message> {
    if payload.len() > MAX_FRAME_SIZE {
        anyhow::bail!(
            "frame payload length {} exceeds maximum {MAX_FRAME_SIZE}",
            payload.len()
        );
    }
    let mut message = Vec::with_capacity(payload.len() + 1);
    message.push(frame_type);
    message.extend_from_slice(payload);
    Ok(Message::Binary(message.into()))
}

async fn write_data_frame<S>(writer: &mut S, data: &[u8]) -> Result<()>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    write_frame(writer, FRAME_DATA, data).await
}

#[cfg(target_os = "linux")]
async fn write_modem_control_frame<S>(writer: &mut S, mask: u8, values: u8) -> Result<()>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    write_frame(writer, FRAME_MODEM_CONTROL, &[mask, values]).await
}

#[cfg(target_os = "linux")]
async fn run_client(args: ClientArgs) -> Result<()> {
    use std::fs;
    use std::os::fd::AsRawFd;

    use nix::fcntl::{FcntlArg, OFlag, fcntl};
    use nix::pty::openpty;
    use nix::sys::termios::{SetArg, cfmakeraw, tcgetattr, tcsetattr};

    let uri: tokio_tungstenite::tungstenite::http::Uri = args
        .connect
        .parse()
        .with_context(|| format!("invalid WebSocket URL {}", args.connect))?;
    if !matches!(uri.scheme_str(), Some("ws" | "wss")) {
        anyhow::bail!("client URL must use ws:// or wss://");
    }
    if uri.path().trim_matches('/').is_empty() {
        anyhow::bail!("client URL must include a serial-port path");
    }

    let pty = openpty(None, None).context("failed to create PTY")?;
    let mut settings = tcgetattr(&pty.slave).context("failed to read PTY settings")?;
    cfmakeraw(&mut settings);
    tcsetattr(&pty.slave, SetArg::TCSANOW, &settings).context("failed to put PTY into raw mode")?;
    fcntl(pty.master.as_raw_fd(), FcntlArg::F_SETFL(OFlag::O_NONBLOCK))
        .context("failed to make PTY nonblocking")?;

    let slave_path = fs::read_link(format!("/proc/self/fd/{}", pty.slave.as_raw_fd()))
        .context("failed to determine PTY slave path")?;
    let mut open_watcher = if args.no_modem_control {
        None
    } else {
        Some(PtyOpenWatcher::new(&slave_path)?)
    };
    let _link = match args.link {
        Some(path) => Some(PtyLink::create(path, &slave_path)?),
        None => None,
    };

    println!("local serial port: {}", slave_path.display());
    if let Some(link) = &_link {
        println!("stable link: {}", link.path.display());
    }

    let master = AsyncPty::new(pty.master)?;
    let (mut pty_reader, mut pty_writer) = tokio::io::split(master);
    let _slave_guard = pty.slave;
    let reconnect_delay = Duration::from_millis(args.reconnect_delay_ms);
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        println!("connecting to {}", args.connect);
        let mut websocket = loop {
            tokio::select! {
                connection = connect_async(args.connect.as_str()) => {
                    match connection {
                        Ok((websocket, _response)) => break websocket,
                        Err(error) => {
                            eprintln!("connection failed: {error}; retrying");
                            tokio::select! {
                                _ = sleep(reconnect_delay) => {}
                                signal = &mut shutdown => {
                                    signal.context("failed to listen for Ctrl-C")?;
                                    println!("shutting down");
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
                signal = &mut shutdown => {
                    signal.context("failed to listen for Ctrl-C")?;
                    println!("shutting down");
                    return Ok(());
                }
            }
        };

        client_handshake(&mut websocket).await?;
        if let Some(watcher) = &open_watcher {
            let values = if watcher.is_open() { MODEM_DTR } else { 0 };
            write_modem_control_frame(&mut websocket, MODEM_DTR, values).await?;
        }
        println!("connected to {}", args.connect);
        let (mut websocket_writer, mut websocket_reader) = websocket.split();
        let mut pty_buffer = [0_u8; 8192];

        loop {
            tokio::select! {
                message = websocket_reader.next() => {
                    match read_websocket_frame(message) {
                        Ok(Some(Frame::Data(data))) => {
                            if !data.is_empty() {
                                pty_writer
                                    .write_all(&data)
                                    .await
                                    .context("failed to write remote data to PTY")?;
                            }
                        }
                        Ok(Some(Frame::ModemControl { .. })) => {
                            anyhow::bail!("server sent an unexpected modem-control frame");
                        }
                        Ok(Some(Frame::Ping(payload))) => {
                            websocket_writer
                                .send(Message::Pong(payload))
                                .await
                                .context("failed to send WebSocket pong")?;
                        }
                        Ok(Some(Frame::Ignore)) => {}
                        Ok(None) => {
                            eprintln!("connection closed; retrying");
                            break;
                        }
                        Err(error) => {
                            eprintln!("connection lost: {error:#}; retrying");
                            break;
                        }
                    }
                }
                read = pty_reader.read(&mut pty_buffer) => {
                    let bytes_read = read.context("failed to read local PTY")?;
                    if bytes_read == 0 {
                        anyhow::bail!("local PTY reached end of stream");
                    }
                    if let Err(error) =
                        write_data_frame(&mut websocket_writer, &pty_buffer[..bytes_read]).await
                    {
                        eprintln!("connection lost: {error:#}; retrying");
                        break;
                    }
                }
                dtr = next_dtr_change(&mut open_watcher) => {
                    let asserted = dtr.context("failed to monitor PTY open state")?;
                    let values = if asserted { MODEM_DTR } else { 0 };
                    if let Err(error) =
                        write_modem_control_frame(&mut websocket_writer, MODEM_DTR, values).await
                    {
                        eprintln!("connection lost: {error:#}; retrying");
                        break;
                    }
                }
                signal = &mut shutdown => {
                    signal.context("failed to listen for Ctrl-C")?;
                    println!("shutting down");
                    return Ok(());
                }
            }
        }

        #[cfg(target_os = "linux")]
        async fn next_dtr_change(watcher: &mut Option<PtyOpenWatcher>) -> Result<bool> {
            match watcher {
                Some(watcher) => watcher.next_dtr_change().await,
                None => std::future::pending().await,
            }
        }

        tokio::select! {
            _ = sleep(reconnect_delay) => {}
            signal = &mut shutdown => {
                signal.context("failed to listen for Ctrl-C")?;
                println!("shutting down");
                return Ok(());
            }
        }
    }
}

#[cfg(target_os = "linux")]
async fn client_handshake<S>(websocket: &mut WebSocketStream<S>) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    websocket
        .send(Message::Binary(PROTOCOL_MAGIC.to_vec().into()))
        .await
        .context("failed to send protocol handshake")?;
    let message = websocket
        .next()
        .await
        .context("server disconnected before protocol handshake")?
        .context("failed to read protocol handshake")?;
    if !matches!(message, Message::Binary(payload) if payload.as_ref() == PROTOCOL_MAGIC) {
        anyhow::bail!("server uses an unsupported protocol");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
struct PtyOpenWatcher {
    inner: tokio::io::unix::AsyncFd<InotifyFd>,
    open_count: usize,
    pending_changes: std::collections::VecDeque<bool>,
}

#[cfg(target_os = "linux")]
struct InotifyFd(nix::sys::inotify::Inotify);

#[cfg(target_os = "linux")]
impl std::os::fd::AsRawFd for InotifyFd {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&std::os::fd::AsFd::as_fd(&self.0))
    }
}

#[cfg(target_os = "linux")]
impl PtyOpenWatcher {
    fn new(path: &std::path::Path) -> Result<Self> {
        use nix::sys::inotify::{AddWatchFlags, InitFlags, Inotify};

        let inotify = Inotify::init(InitFlags::IN_CLOEXEC | InitFlags::IN_NONBLOCK)
            .context("failed to initialize PTY watcher")?;
        inotify
            .add_watch(
                path,
                AddWatchFlags::IN_OPEN
                    | AddWatchFlags::IN_CLOSE_WRITE
                    | AddWatchFlags::IN_CLOSE_NOWRITE,
            )
            .with_context(|| format!("failed to watch PTY {}", path.display()))?;
        Ok(Self {
            inner: tokio::io::unix::AsyncFd::new(InotifyFd(inotify))
                .context("failed to register PTY watcher with the async runtime")?,
            open_count: 0,
            pending_changes: std::collections::VecDeque::new(),
        })
    }

    fn is_open(&self) -> bool {
        self.open_count > 0
    }

    async fn next_dtr_change(&mut self) -> Result<bool> {
        use nix::sys::inotify::AddWatchFlags;

        loop {
            if let Some(change) = self.pending_changes.pop_front() {
                return Ok(change);
            }

            let mut guard = self
                .inner
                .readable()
                .await
                .context("failed waiting for PTY open event")?;
            let events = match guard.try_io(|inner| {
                inner
                    .get_ref()
                    .0
                    .read_events()
                    .map_err(std::io::Error::from)
            }) {
                Ok(result) => result.context("failed to read PTY open events")?,
                Err(_) => continue,
            };

            for event in events {
                if event.mask.contains(AddWatchFlags::IN_OPEN) {
                    let was_open = self.is_open();
                    self.open_count += 1;
                    if !was_open {
                        self.pending_changes.push_back(true);
                    }
                }
                if event
                    .mask
                    .intersects(AddWatchFlags::IN_CLOSE_WRITE | AddWatchFlags::IN_CLOSE_NOWRITE)
                {
                    let was_open = self.is_open();
                    self.open_count = self.open_count.saturating_sub(1);
                    if was_open && !self.is_open() {
                        self.pending_changes.push_back(false);
                    }
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
struct AsyncPty {
    inner: tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>,
}

#[cfg(target_os = "linux")]
impl AsyncPty {
    fn new(fd: std::os::fd::OwnedFd) -> Result<Self> {
        Ok(Self {
            inner: tokio::io::unix::AsyncFd::new(fd)
                .context("failed to register PTY with the async runtime")?,
        })
    }
}

#[cfg(target_os = "linux")]
impl tokio::io::AsyncRead for AsyncPty {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        loop {
            let mut guard = std::task::ready!(self.inner.poll_read_ready(context))?;
            match guard.try_io(|inner| {
                let destination = buffer.initialize_unfilled();
                nix::unistd::read(
                    std::os::fd::AsRawFd::as_raw_fd(inner.get_ref()),
                    destination,
                )
                .map_err(std::io::Error::from)
            }) {
                Ok(Ok(bytes_read)) => {
                    buffer.advance(bytes_read);
                    return std::task::Poll::Ready(Ok(()));
                }
                Ok(Err(error)) => return std::task::Poll::Ready(Err(error)),
                Err(_) => continue,
            }
        }
    }
}

#[cfg(target_os = "linux")]
impl tokio::io::AsyncWrite for AsyncPty {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        loop {
            let mut guard = std::task::ready!(self.inner.poll_write_ready(context))?;
            match guard.try_io(|inner| {
                nix::unistd::write(inner.get_ref(), buffer).map_err(std::io::Error::from)
            }) {
                Ok(result) => return std::task::Poll::Ready(result),
                Err(_) => continue,
            }
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(not(target_os = "linux"))]
async fn run_client(_args: ClientArgs) -> Result<()> {
    anyhow::bail!("the client currently requires Linux PTY support")
}

#[cfg(target_os = "linux")]
struct PtyLink {
    path: PathBuf,
}

#[cfg(target_os = "linux")]
impl PtyLink {
    fn create(path: PathBuf, target: &std::path::Path) -> Result<Self> {
        if std::fs::symlink_metadata(&path).is_ok() {
            anyhow::bail!("refusing to replace existing path {}", path.display());
        }

        std::os::unix::fs::symlink(target, &path).with_context(|| {
            format!(
                "failed to create PTY link {} -> {}",
                path.display(),
                target.display()
            )
        })?;
        Ok(Self { path })
    }
}

#[cfg(target_os = "linux")]
impl Drop for PtyLink {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path) {
            eprintln!("failed to remove PTY link {}: {error}", self.path.display());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_server_options() {
        let cli = Cli::try_parse_from([
            "uart-proxy",
            "server",
            "--serial",
            "/dev/ttyACM0",
            "--baud",
            "9600",
            "--listen",
            "0.0.0.0:8000",
            "--dtr-reset",
        ])
        .unwrap();

        let Command::Server(args) = cli.command else {
            panic!("expected server command");
        };
        assert_eq!(args.serial.as_deref(), Some("/dev/ttyACM0"));
        assert!(args.config.is_none());
        assert_eq!(args.baud, 9600);
        assert_eq!(args.listen, Some("0.0.0.0:8000".parse().unwrap()));
        assert!(args.dtr_reset);
    }

    #[test]
    fn parses_client_options() {
        let cli = Cli::try_parse_from([
            "uart-proxy",
            "client",
            "--connect",
            "ws://192.0.2.1:7000/arduino",
            "--link",
            "/tmp/remote-arduino",
        ])
        .unwrap();

        let Command::Client(args) = cli.command else {
            panic!("expected client command");
        };
        assert_eq!(args.connect, "ws://192.0.2.1:7000/arduino");
        assert_eq!(args.link, Some(PathBuf::from("/tmp/remote-arduino")));
        assert_eq!(args.reconnect_delay_ms, 1_000);
        assert!(!args.no_modem_control);
    }

    #[tokio::test]
    async fn round_trips_data_frame() {
        let message = encode_frame(FRAME_DATA, b"serial data").unwrap();
        let Some(Frame::Data(data)) = read_websocket_frame(Some(Ok(message))).unwrap() else {
            panic!("expected data frame");
        };
        assert_eq!(data, b"serial data");
    }

    #[tokio::test]
    async fn round_trips_modem_control_frame() {
        let message =
            encode_frame(FRAME_MODEM_CONTROL, &[MODEM_DTR | MODEM_RTS, MODEM_DTR]).unwrap();
        let Some(Frame::ModemControl { mask, values }) =
            read_websocket_frame(Some(Ok(message))).unwrap()
        else {
            panic!("expected modem-control frame");
        };
        assert_eq!(mask, MODEM_DTR | MODEM_RTS);
        assert_eq!(values, MODEM_DTR);
    }

    #[test]
    fn parses_multi_port_configuration() {
        let config: ServerFileConfig = toml::from_str(
            r#"
                listen = "0.0.0.0:7000"

                [ports.com1]
                device = "COM1"

                [ports.arduino]
                device = "/dev/ttyACM0"
                baud = 57600
                dtr_reset = true
            "#,
        )
        .unwrap();

        assert_eq!(config.listen, Some("0.0.0.0:7000".parse().unwrap()));
        assert_eq!(config.ports["com1"].device, "COM1");
        assert_eq!(config.ports["com1"].baud, 115_200);
        assert_eq!(config.ports["arduino"].baud, 57_600);
        assert!(config.ports["arduino"].dtr_reset);
        validate_routes(&config.ports).unwrap();
    }
}
