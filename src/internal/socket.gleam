import gleam/bytes_tree
import gleam/dynamic
import gleam/erlang/charlist
import gleam/erlang/process

/// Raw TCP or SSL socket handle.
pub type Socket

/// POSIX socket error reason.
pub type SocketReason {
  Closed
  Timeout
  Badarg
  Terminated
  Eaddrinuse
  Eaddrnotavail
  Eafnosupport
  Ealready
  Econnaborted
  Econnrefused
  Econnreset
  Edestaddrreq
  Ehostdown
  Ehostunreach
  Einprogress
  Eisconn
  Emsgsize
  Enetdown
  Enetunreach
  Enopkg
  Enoprotoopt
  Enotconn
  Enotty
  Enotsock
  Eproto
  Eprotonosupport
  Eprototype
  Esocktnosupport
  Etimedout
  Ewouldblock
  Exbadport
  Exbadseq
}

/// Convert a socket error reason to a human-readable string.
pub fn reason_to_string(reason: SocketReason) -> String {
  case reason {
    Closed -> "connection closed"
    Timeout -> "operation timed out"
    Badarg -> "bad argument"
    Terminated -> "process terminated"
    Eaddrinuse -> "address already in use"
    Eaddrnotavail -> "address not available"
    Eafnosupport -> "address family not supported"
    Ealready -> "operation already in progress"
    Econnaborted -> "connection aborted"
    Econnrefused -> "connection refused"
    Econnreset -> "connection reset by peer"
    Edestaddrreq -> "destination address required"
    Ehostdown -> "host is down"
    Ehostunreach -> "host is unreachable"
    Einprogress -> "operation in progress"
    Eisconn -> "already connected"
    Emsgsize -> "message too long"
    Enetdown -> "network is down"
    Enetunreach -> "network is unreachable"
    Enopkg -> "package not installed"
    Enoprotoopt -> "protocol not available"
    Enotconn -> "not connected"
    Enotty -> "inappropriate ioctl for device"
    Enotsock -> "not a socket"
    Eproto -> "protocol error"
    Eprotonosupport -> "protocol not supported"
    Eprototype -> "wrong protocol type for socket"
    Esocktnosupport -> "socket type not supported"
    Etimedout -> "connection timed out"
    Ewouldblock -> "operation would block"
    Exbadport -> "bad port"
    Exbadseq -> "bad sequence"
  }
}

/// Socket shutdown direction.
pub type Shutdown {
  Read
  Write
  ReadWrite
}

/// Socket active mode for receiving data.
pub type ActiveMode {
  Once
  Passive
  Count(Int)
  Active
}

/// Socket data format.
pub type PacketMode {
  Binary
}

/// TLS certificate verification mode.
pub type VerifyMode {
  VerifyPeer
  VerifyNone
}

/// Socket configuration option.
pub type Option {
  ActiveMode(ActiveMode)
  Mode(PacketMode)
  /// In milliseconds.
  SendTimeout(Int)
  SendTimeoutClose(Bool)
  Reuseaddr(Bool)
  /// Disable Nagle's algorithm.
  Nodelay(Bool)
  Verify(VerifyMode)
  Cacerts(dynamic.Dynamic)
  CustomizeHostnameCheck(dynamic.Dynamic)
  ServerNameIndication(dynamic.Dynamic)
}

/// Erlang-native socket option.
pub type ErlangOption

/// Default options for a WebSocket client.
pub const default_options = [
  ActiveMode(Passive),
  Mode(Binary),
  SendTimeout(30_000),
  SendTimeoutClose(True),
  Reuseaddr(True),
  Nodelay(True),
]

/// Convert options to Erlang-native format.
@external(erlang, "websocket_ffi", "to_erl_options")
pub fn to_erl_options(options: List(Option)) -> List(ErlangOption)

/// Retrieve system CA certificates for TLS.
@external(erlang, "public_key", "cacerts_get")
pub fn get_system_cacerts() -> dynamic.Dynamic

/// Get the HTTPS hostname verification match function.
@external(erlang, "websocket_ffi", "custom_sni_matcher")
pub fn get_custom_hostname_check() -> dynamic.Dynamic

/// Transport protocol.
pub type Transport {
  Tcp
  Ssl
}

/// Connect to a host and port.
pub fn connect(
  transport: Transport,
  host: charlist.Charlist,
  port: Int,
  options: List(Option),
  timeout: Int,
) -> Result(Socket, SocketReason) {
  let options = to_erl_options(options)
  case transport {
    Tcp -> tcp_connect(host, port, options, timeout)
    Ssl -> ssl_connect(host, port, options, timeout)
  }
}

/// Send data through the socket.
pub fn send(
  transport: Transport,
  socket: Socket,
  data: bytes_tree.BytesTree,
) -> Result(Nil, SocketReason) {
  case transport {
    Tcp -> tcp_send(socket, data)
    Ssl -> ssl_send(socket, data)
  }
}

/// Receive data from the socket.
pub fn receive(
  transport: Transport,
  socket: Socket,
  length: Int,
) -> Result(BitArray, SocketReason) {
  case transport {
    Tcp -> tcp_receive(socket, length)
    Ssl -> ssl_receive(socket, length)
  }
}

/// Receive data from the socket with a timeout.
pub fn receive_timeout(
  transport: Transport,
  socket: Socket,
  length: Int,
  timeout: Int,
) -> Result(BitArray, SocketReason) {
  case transport {
    Tcp -> tcp_receive_timeout(socket, length, timeout)
    Ssl -> ssl_receive_timeout(socket, length, timeout)
  }
}

/// Close the socket.
pub fn close(transport: Transport, socket: Socket) -> Result(Nil, SocketReason) {
  case transport {
    Tcp -> tcp_close(socket)
    Ssl -> ssl_close(socket)
  }
}

/// Shut down the socket in the given direction.
pub fn shutdown(
  transport: Transport,
  socket: Socket,
  how: Shutdown,
) -> Result(Nil, SocketReason) {
  case transport {
    Tcp -> tcp_shutdown(socket, how)
    Ssl -> ssl_shutdown(socket, how)
  }
}

/// Set socket options.
pub fn set_opts(
  transport: Transport,
  socket: Socket,
  options: List(Option),
) -> Result(Nil, SocketReason) {
  let options = to_erl_options(options)
  case transport {
    Tcp -> tcp_set_opts(socket, options)
    Ssl -> ssl_set_opts(socket, options)
  }
}

/// Transfer socket ownership to another process.
pub fn controlling_process(
  transport: Transport,
  socket: Socket,
  new_owner: process.Pid,
) -> Result(Nil, SocketReason) {
  case transport {
    Tcp -> tcp_controlling_process(socket, new_owner)
    Ssl -> ssl_controlling_process(socket, new_owner)
  }
}

@external(erlang, "gen_tcp", "connect")
fn tcp_connect(
  address: charlist.Charlist,
  port: Int,
  options: List(ErlangOption),
  timeout: Int,
) -> Result(Socket, SocketReason)

@external(erlang, "websocket_ffi", "tcp_send")
fn tcp_send(
  socket: Socket,
  packet: bytes_tree.BytesTree,
) -> Result(Nil, SocketReason)

@external(erlang, "gen_tcp", "recv")
fn tcp_receive(socket: Socket, length: Int) -> Result(BitArray, SocketReason)

@external(erlang, "gen_tcp", "recv")
fn tcp_receive_timeout(
  socket: Socket,
  length: Int,
  timeout: Int,
) -> Result(BitArray, SocketReason)

@external(erlang, "websocket_ffi", "tcp_close")
fn tcp_close(socket: Socket) -> Result(Nil, SocketReason)

@external(erlang, "websocket_ffi", "tcp_shutdown")
fn tcp_shutdown(socket: Socket, how: Shutdown) -> Result(Nil, SocketReason)

@external(erlang, "websocket_ffi", "tcp_set_opts")
fn tcp_set_opts(
  socket: Socket,
  opts: List(ErlangOption),
) -> Result(Nil, SocketReason)

@external(erlang, "websocket_ffi", "tcp_controlling_process")
fn tcp_controlling_process(
  socket: Socket,
  new_owner: process.Pid,
) -> Result(Nil, SocketReason)

@external(erlang, "ssl", "connect")
fn ssl_connect(
  address: charlist.Charlist,
  port: Int,
  options: List(ErlangOption),
  timeout: Int,
) -> Result(Socket, SocketReason)

@external(erlang, "websocket_ffi", "ssl_send")
fn ssl_send(
  socket: Socket,
  packet: bytes_tree.BytesTree,
) -> Result(Nil, SocketReason)

@external(erlang, "ssl", "recv")
fn ssl_receive(socket: Socket, length: Int) -> Result(BitArray, SocketReason)

@external(erlang, "ssl", "recv")
fn ssl_receive_timeout(
  socket: Socket,
  length: Int,
  timeout: Int,
) -> Result(BitArray, SocketReason)

@external(erlang, "websocket_ffi", "ssl_close")
fn ssl_close(socket: Socket) -> Result(Nil, SocketReason)

@external(erlang, "websocket_ffi", "ssl_shutdown")
fn ssl_shutdown(socket: Socket, how: Shutdown) -> Result(Nil, SocketReason)

@external(erlang, "websocket_ffi", "ssl_set_opts")
fn ssl_set_opts(
  socket: Socket,
  opts: List(ErlangOption),
) -> Result(Nil, SocketReason)

@external(erlang, "websocket_ffi", "ssl_controlling_process")
fn ssl_controlling_process(
  socket: Socket,
  new_owner: process.Pid,
) -> Result(Nil, SocketReason)
