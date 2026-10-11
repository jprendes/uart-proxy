use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;
use tokio_serial::{SerialPort, SerialPortBuilderExt};

const PROTOCOL_MAGIC: &[u8; 11] = b"UARTPROXY\x01\n";
const FRAME_DATA: u8 = 1;
const FRAME_MODEM_CONTROL: u8 = 2;
const MODEM_DTR: u8 = 1 << 0;
const MODEM_RTS: u8 = 1 << 1;
const MAX_FRAME_SIZE: usize = 64 * 1024;

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Expose a physical serial port over TCP.
    Server(ServerArgs),
    /// Expose a remote serial port as a local Linux PTY.
    Client(ClientArgs),
}

#[derive(Debug, Args)]
struct ServerArgs {
    /// Local serial device, such as /dev/ttyACM0 or COM3.
    #[arg(long)]
    serial: String,

    /// Serial baud rate.
    #[arg(long, default_value_t = 115_200)]
    baud: u32,

    /// TCP address on which to listen.
    #[arg(long, default_value = "127.0.0.1:7000")]
    listen: SocketAddr,

    /// Pulse DTR whenever a client connects.
    #[arg(long)]
    dtr_reset: bool,
}

#[derive(Debug, Args)]
struct ClientArgs {
    /// Remote uart-proxy server address.
    #[arg(long)]
    connect: SocketAddr,

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

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Server(args) => run_server(args).await,
        Command::Client(args) => run_client(args).await,
    }
}

async fn run_server(args: ServerArgs) -> Result<()> {
    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("failed to listen on {}", args.listen))?;

    println!(
        "serving {} at {} ({} baud)",
        args.serial, args.listen, args.baud
    );

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted.context("failed to accept TCP connection")?;
                println!("client connected: {peer}");

                tokio::select! {
                    result = proxy_serial_connection(&args, stream) => {
                        if let Err(error) = result {
                            eprintln!("client {peer} disconnected with error: {error:#}");
                        } else {
                            println!("client disconnected: {peer}");
                        }
                    }
                    signal = tokio::signal::ctrl_c() => {
                        signal.context("failed to listen for Ctrl-C")?;
                        println!("shutting down");
                        return Ok(());
                    }
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for Ctrl-C")?;
                println!("shutting down");
                return Ok(());
            }
        }
    }
}

async fn proxy_serial_connection(args: &ServerArgs, mut stream: TcpStream) -> Result<()> {
    stream
        .set_nodelay(true)
        .context("failed to configure TCP connection")?;

    let mut serial = tokio_serial::new(&args.serial, args.baud)
        .open_native_async()
        .with_context(|| {
            format!(
                "failed to open serial port {} at {} baud",
                args.serial, args.baud
            )
        })?;

    if args.dtr_reset {
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

    server_handshake(&mut stream).await?;
    let mut serial_buffer = [0_u8; 8192];
    let mut from_client = 0_u64;
    let mut from_serial = 0_u64;

    loop {
        tokio::select! {
            frame = read_frame(&mut stream) => {
                match frame? {
                    Some(Frame::Data(data)) => {
                        serial.write_all(&data).await.context("failed to write to serial port")?;
                        from_client += data.len() as u64;
                    }
                    Some(Frame::ModemControl { mask, values }) => {
                        apply_modem_control(&mut serial, mask, values)?;
                    }
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
                write_data_frame(&mut stream, &serial_buffer[..bytes_read]).await?;
                from_serial += bytes_read as u64;
            }
        }
    }
}

async fn server_handshake(stream: &mut TcpStream) -> Result<()> {
    let mut magic = [0_u8; PROTOCOL_MAGIC.len()];
    stream
        .read_exact(&mut magic)
        .await
        .context("failed to read protocol handshake")?;
    if &magic != PROTOCOL_MAGIC {
        anyhow::bail!("client uses an unsupported protocol");
    }
    stream
        .write_all(PROTOCOL_MAGIC)
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
}

async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Frame>> {
    let frame_type = match reader.read_u8().await {
        Ok(frame_type) => frame_type,
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error).context("failed to read frame type"),
    };
    let length = reader
        .read_u32()
        .await
        .context("failed to read frame length")? as usize;
    if length > MAX_FRAME_SIZE {
        anyhow::bail!("frame length {length} exceeds maximum {MAX_FRAME_SIZE}");
    }

    let mut payload = vec![0_u8; length];
    reader
        .read_exact(&mut payload)
        .await
        .context("failed to read frame payload")?;

    match frame_type {
        FRAME_DATA => Ok(Some(Frame::Data(payload))),
        FRAME_MODEM_CONTROL if payload.len() == 2 => Ok(Some(Frame::ModemControl {
            mask: payload[0],
            values: payload[1],
        })),
        FRAME_MODEM_CONTROL => {
            anyhow::bail!("invalid modem-control frame length {}", payload.len())
        }
        _ => anyhow::bail!("unknown frame type {frame_type}"),
    }
}

async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame_type: u8,
    payload: &[u8],
) -> Result<()> {
    if payload.len() > MAX_FRAME_SIZE {
        anyhow::bail!(
            "frame payload length {} exceeds maximum {MAX_FRAME_SIZE}",
            payload.len()
        );
    }
    writer
        .write_u8(frame_type)
        .await
        .context("failed to write frame type")?;
    writer
        .write_u32(payload.len() as u32)
        .await
        .context("failed to write frame length")?;
    writer
        .write_all(payload)
        .await
        .context("failed to write frame payload")
}

async fn write_data_frame<W: AsyncWrite + Unpin>(writer: &mut W, data: &[u8]) -> Result<()> {
    write_frame(writer, FRAME_DATA, data).await
}

#[cfg(any(target_os = "linux", test))]
async fn write_modem_control_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    mask: u8,
    values: u8,
) -> Result<()> {
    write_frame(writer, FRAME_MODEM_CONTROL, &[mask, values]).await
}

#[cfg(target_os = "linux")]
async fn run_client(args: ClientArgs) -> Result<()> {
    use std::fs;
    use std::os::fd::AsRawFd;

    use nix::fcntl::{FcntlArg, OFlag, fcntl};
    use nix::pty::openpty;
    use nix::sys::termios::{SetArg, cfmakeraw, tcgetattr, tcsetattr};

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
        let mut stream = loop {
            tokio::select! {
                connection = TcpStream::connect(args.connect) => {
                    match connection {
                        Ok(stream) => break stream,
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

        stream
            .set_nodelay(true)
            .context("failed to configure TCP connection")?;
        client_handshake(&mut stream).await?;
        if let Some(watcher) = &open_watcher {
            let values = if watcher.is_open() { MODEM_DTR } else { 0 };
            write_modem_control_frame(&mut stream, MODEM_DTR, values).await?;
        }
        println!("connected to {}", args.connect);
        let (mut tcp_reader, mut tcp_writer) = stream.into_split();
        let mut pty_buffer = [0_u8; 8192];

        loop {
            tokio::select! {
                frame = read_frame(&mut tcp_reader) => {
                    match frame {
                        Ok(Some(Frame::Data(data))) => {
                            pty_writer
                                .write_all(&data)
                                .await
                                .context("failed to write remote data to PTY")?;
                        }
                        Ok(Some(Frame::ModemControl { .. })) => {
                            anyhow::bail!("server sent an unexpected modem-control frame");
                        }
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
                        write_data_frame(&mut tcp_writer, &pty_buffer[..bytes_read]).await
                    {
                        eprintln!("connection lost: {error:#}; retrying");
                        break;
                    }
                }
                dtr = next_dtr_change(&mut open_watcher) => {
                    let asserted = dtr.context("failed to monitor PTY open state")?;
                    let values = if asserted { MODEM_DTR } else { 0 };
                    if let Err(error) =
                        write_modem_control_frame(&mut tcp_writer, MODEM_DTR, values).await
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
async fn client_handshake(stream: &mut TcpStream) -> Result<()> {
    stream
        .write_all(PROTOCOL_MAGIC)
        .await
        .context("failed to send protocol handshake")?;
    let mut magic = [0_u8; PROTOCOL_MAGIC.len()];
    stream
        .read_exact(&mut magic)
        .await
        .context("failed to read protocol handshake")?;
    if &magic != PROTOCOL_MAGIC {
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
        assert_eq!(args.serial, "/dev/ttyACM0");
        assert_eq!(args.baud, 9600);
        assert_eq!(args.listen, "0.0.0.0:8000".parse().unwrap());
        assert!(args.dtr_reset);
    }

    #[test]
    fn parses_client_options() {
        let cli = Cli::try_parse_from([
            "uart-proxy",
            "client",
            "--connect",
            "192.0.2.1:7000",
            "--link",
            "/tmp/remote-arduino",
        ])
        .unwrap();

        let Command::Client(args) = cli.command else {
            panic!("expected client command");
        };
        assert_eq!(args.connect, "192.0.2.1:7000".parse().unwrap());
        assert_eq!(args.link, Some(PathBuf::from("/tmp/remote-arduino")));
        assert_eq!(args.reconnect_delay_ms, 1_000);
        assert!(!args.no_modem_control);
    }

    #[tokio::test]
    async fn round_trips_data_frame() {
        let (mut writer, mut reader) = tokio::io::duplex(128);
        write_data_frame(&mut writer, b"serial data").await.unwrap();

        let Some(Frame::Data(data)) = read_frame(&mut reader).await.unwrap() else {
            panic!("expected data frame");
        };
        assert_eq!(data, b"serial data");
    }

    #[tokio::test]
    async fn round_trips_modem_control_frame() {
        let (mut writer, mut reader) = tokio::io::duplex(128);
        write_modem_control_frame(&mut writer, MODEM_DTR | MODEM_RTS, MODEM_DTR)
            .await
            .unwrap();

        let Some(Frame::ModemControl { mask, values }) = read_frame(&mut reader).await.unwrap()
        else {
            panic!("expected modem-control frame");
        };
        assert_eq!(mask, MODEM_DTR | MODEM_RTS);
        assert_eq!(values, MODEM_DTR);
    }
}
