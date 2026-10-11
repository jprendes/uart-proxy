# uart-proxy

[![CI](https://github.com/jprendes/uart-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/jprendes/uart-proxy/actions/workflows/ci.yml)

`uart-proxy` makes a serial device available over a TCP connection. It is aimed
at workflows where VS Code and the Arduino toolchain run on a remote Linux
machine while the board is attached to another computer.

The physical-port server supports Linux and Windows through `tokio-serial`. The
client currently supports Linux and creates a pseudo-terminal (PTY) that tools
can open like a serial port.

## Build

Install a current Rust toolchain, then run:

```sh
cargo build --release
```

The binary is written to `target/release/uart-proxy` (or
`target\release\uart-proxy.exe` on Windows).

Precompiled x86-64 binaries are published for statically linked Linux musl and
Windows on each tagged GitHub release.

## Usage

On the computer to which the board is physically connected:

```sh
# Linux
uart-proxy server --serial /dev/ttyACM0 --baud 115200 --listen 127.0.0.1:7000

# Windows
uart-proxy.exe server --serial COM3 --baud 115200 --listen 127.0.0.1:7000
```

On the remote Linux development machine:

```sh
uart-proxy client --connect 127.0.0.1:7000 --link /tmp/remote-arduino
```

Use the printed `/dev/pts/...` path or `/tmp/remote-arduino` as the port in the
Arduino tooling. The symlink is optional, must not already exist, and is removed
when the client exits.

The default loopback addresses are intentional. With VS Code Remote SSH,
forward TCP port 7000 between the machines rather than exposing an unauthenticated
serial port to a network. If direct connectivity is required, set `--listen
0.0.0.0:7000` and protect the connection with a firewall, VPN, or SSH tunnel.

Run `uart-proxy server --help` and `uart-proxy client --help` for all options.

## Arduino notes

The protocol transports framed serial data and modem-control messages. Configure
`--baud` on the server to the rate expected by the target and keep it fixed for
that server process.

Many Arduino bootloaders require a DTR transition immediately before an upload.
By default, the Linux client asserts physical DTR when the first application
opens its PTY and clears DTR when the last application closes it. This reproduces
the normal Arduino upload reset behavior without restarting the proxy. The
server's `--dtr-reset` option remains available to additionally pulse DTR when a
TCP client connects.

Linux PTYs do not implement the `TIOCMGET` or `TIOCMSET` ioctls, so arbitrary
DTR/RTS changes requested through those ioctls cannot be observed. PTY
open/close events are therefore used for DTR. The wire protocol supports both
DTR and RTS for future client backends. Use `--no-modem-control` on the client
for serial drivers that reject DTR changes.

## Current limitations

- One TCP client is served at a time.
- TCP traffic is neither encrypted nor authenticated.
- Baud-rate changes made to the client PTY are not propagated to the physical
  port.
- Linux PTY open/close state is propagated as DTR, but arbitrary PTY DTR/RTS
  ioctls are unavailable in the Linux kernel.
- The server uses the usual default 8-N-1 serial settings.
- The client PTY implementation is Linux-only. Server support for macOS may work
  through `tokio-serial`, but is not currently tested.
- The framed protocol is not compatible with the earlier raw-TCP prototype.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
