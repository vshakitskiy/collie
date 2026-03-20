import gleam/bytes_tree
import gleam/erlang/charlist
import gleam/erlang/process
import gleam/http
import gleam/http/request
import gleam/http/response
import gleam/int
import gleam/list
import gleam/option
import gleam/otp/actor
import internal/http as http_
import internal/socket

pub opaque type Next(state, message) {
  Continue(state: state, selector: option.Option(process.Selector(message)))
  NormalStop
  AbnormalStop(reason: String)
}

pub fn continue(state: state) -> Next(state, message) {
  Continue(state:, selector: option.None)
}

pub fn continue_with_selector(
  state: state,
  selector: process.Selector(message),
) -> Next(state, message) {
  Continue(state:, selector: option.Some(selector))
}

pub fn stop() -> Next(state, message) {
  NormalStop
}

pub fn stop_abnormal(reason: String) -> Next(state, message) {
  AbnormalStop(reason:)
}

pub opaque type Initialised(state, message) {
  Initialised(state: state, selector: option.Option(process.Selector(message)))
}

pub fn initialised(state: state) -> Initialised(state, message) {
  Initialised(state:, selector: option.None)
}

pub fn selecting(
  initialised: Initialised(state, old_message),
  selector: process.Selector(message),
) -> Initialised(state, message) {
  Initialised(..initialised, selector: option.Some(selector))
}

pub type Message(message) {
  Text(String)
  Binary(BitArray)
  User(message)
}

pub opaque type Connection {
  Connection(transport: socket.Transport, socket: socket.Socket)
}

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

fn to_socket_reason(reason: socket.SocketReason) -> SocketReason {
  case reason {
    socket.Closed -> Closed
    socket.Timeout -> Timeout
    socket.Badarg -> Badarg
    socket.Terminated -> Terminated
    socket.Eaddrinuse -> Eaddrinuse
    socket.Eaddrnotavail -> Eaddrnotavail
    socket.Eafnosupport -> Eafnosupport
    socket.Ealready -> Ealready
    socket.Econnaborted -> Econnaborted
    socket.Econnrefused -> Econnrefused
    socket.Econnreset -> Econnreset
    socket.Edestaddrreq -> Edestaddrreq
    socket.Ehostdown -> Ehostdown
    socket.Ehostunreach -> Ehostunreach
    socket.Einprogress -> Einprogress
    socket.Eisconn -> Eisconn
    socket.Emsgsize -> Emsgsize
    socket.Enetdown -> Enetdown
    socket.Enetunreach -> Enetunreach
    socket.Enopkg -> Enopkg
    socket.Enoprotoopt -> Enoprotoopt
    socket.Enotconn -> Enotconn
    socket.Enotty -> Enotty
    socket.Enotsock -> Enotsock
    socket.Eproto -> Eproto
    socket.Eprotonosupport -> Eprotonosupport
    socket.Eprototype -> Eprototype
    socket.Esocktnosupport -> Esocktnosupport
    socket.Etimedout -> Etimedout
    socket.Ewouldblock -> Ewouldblock
    socket.Exbadport -> Exbadport
    socket.Exbadseq -> Exbadseq
  }
}

pub type CloseReason {
  NormalClosure(data: BitArray)
  GoingAway(data: BitArray)
  ProtocolError(data: BitArray)
  UnsupportedData(data: BitArray)
  InvalidPayloadData(data: BitArray)
  PolicyViolation(data: BitArray)
  MessageTooBig(data: BitArray)
  MandatoryExtension(data: BitArray)
  InternalError(data: BitArray)
  ServiceRestart(data: BitArray)
  TryAgainLater(data: BitArray)
  BadGateway(data: BitArray)
  TLSHandshake(data: BitArray)
  CustomCloseCode(code: Int, data: BitArray)
  NoCloseReason
}

pub opaque type Builder(body, state, message) {
  Builder(
    request: request.Request(body),
    connection_timeout: Int,
    initialise: fn() -> Result(Initialised(state, message), String),
    handler: fn(Connection, state, Message(message)) -> Next(state, message),
    on_close: fn(state, CloseReason) -> Nil,
  )
}

pub fn new(
  request: request.Request(body),
  state: state,
) -> Builder(body, state, message) {
  Builder(
    request:,
    connection_timeout: 5000,
    initialise: fn() { Ok(initialised(state)) },
    handler: fn(_conn, state, _message) { continue(state) },
    on_close: fn(_state, _reason) { Nil },
  )
}

pub fn new_with_initialiser(
  request: request.Request(body),
  initialise: fn() -> Result(Initialised(state, message), String),
) -> Builder(body, state, message) {
  Builder(
    request:,
    connection_timeout: 5000,
    initialise:,
    handler: fn(_conn, state, _message) { continue(state) },
    on_close: fn(_state, _reason) { Nil },
  )
}

pub fn with_connection_timeout(
  builder: Builder(body, state, message),
  connection_timeout: Int,
) -> Builder(body, state, message) {
  Builder(..builder, connection_timeout:)
}

pub fn on_message(
  builder: Builder(body, state, message),
  handler: fn(Connection, state, Message(message)) -> Next(state, message),
) -> Builder(body, state, message) {
  Builder(..builder, handler:)
}

pub fn on_close(
  builder: Builder(body, state, message),
  on_close: fn(state, CloseReason) -> Nil,
) -> Builder(body, state, message) {
  Builder(..builder, on_close:)
}

pub type StartError {
  ActorFailed(actor.StartError)
  SocketFailed(SocketReason)
  UpgradeFailed
}

pub fn start(
  builder: Builder(body, state, message),
) -> Result(actor.Started(process.Subject(message)), StartError) {
  let transport = case builder.request.scheme {
    http.Https -> socket.Ssl
    http.Http -> socket.Tcp
  }

  use response <- handshake(
    builder.request,
    builder.connection_timeout,
    transport,
  )

  todo
}

fn handshake(
  request: request.Request(body),
  connection_timeout: Int,
  transport: socket.Transport,
  handle_response: fn(#(response.Response(BitArray), BitArray)) ->
    Result(actor.Started(process.Subject(message)), StartError),
) -> Result(actor.Started(process.Subject(message)), StartError) {
  let #(options, default_port) = case transport {
    socket.Ssl -> #(
      [
        socket.Cacerts(socket.get_system_cacerts()),
        socket.ServerNameIndication(socket.get_custom_hostname_check()),
      ],
      443,
    )
    socket.Tcp -> #([], 80)
  }

  use socket <- unwrap_socket(socket.connect(
    transport,
    charlist.from_string(request.host),
    option.unwrap(request.port, default_port),
    list.append(socket.default_options, options),
    connection_timeout,
  ))

  let data = http_.construct_upgrade(request)
  use _nil <- unwrap_socket(socket.send(transport, socket, data))

  case http_.decode_response(transport, socket, connection_timeout) {
    Ok(#(response, remaining)) -> {
      case response.status {
        101 -> handle_response(#(response, remaining))
        _ -> Error(UpgradeFailed)
      }
    }
    Error(http_.SocketFailed(reason)) ->
      Error(SocketFailed(to_socket_reason(reason)))
    Error(http_.MalformedRequest) -> Error(UpgradeFailed)
  }
}

fn unwrap_socket(
  result: Result(return, socket.SocketReason),
  handle_return: fn(return) -> Result(continue, StartError),
) -> Result(continue, StartError) {
  case result {
    Ok(return) -> handle_return(return)
    Error(reason) -> Error(SocketFailed(to_socket_reason(reason)))
  }
}
