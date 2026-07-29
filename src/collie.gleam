//// <script>
//// const docs = [
////   {
////     header: "Builder",
////     functions: [
////       "new",
////       "new_with_initialiser",
////       "with_connection_timeout",
////       "with_limits",
////       "named",
////       "on_message",
////       "on_close"
////     ]
////   },
////   {
////     header: "Initialiser",
////     functions: ["initialised", "selecting", "returning"]
////   },
////   {
////     header: "Start",
////     functions: ["start", "supervised", "factory"]
////   },
////   {
////     header: "Handler",
////     functions: [
////       "continue",
////       "continue_with_selector",
////       "stop",
////       "stop_abnormal",
////       "send_ping",
////       "send_text_frame",
////       "send_binary_frame",
////       "send_close_frame"
////     ]
////   },
////   {
////     header: "User messages",
////     functions: ["to_user_message"]
////   },
////   {
////     header: "To string",
////     functions: [
////       "close_code_to_string",
////       "close_reason_to_string",
////       "socket_reason_to_string"
////     ]
////   }
//// ]
////
//// const callback = () => {
////   const list = document.querySelector(".sidebar > ul:last-of-type")
////   const sortedLists = document.createDocumentFragment()
////   const sortedMembers = document.createDocumentFragment()
////
////   for (const section of docs) {
////     sortedLists.append((() => {
////       const node = document.createElement("h3")
////       node.append(section.header)
////       return node
////     })())
////     sortedMembers.append((() => {
////       const node = document.createElement("h2")
////       node.append(section.header)
////       return node
////     })())
////
////     const sortedList = document.createElement("ul")
////     sortedLists.append(sortedList)
////
////     const sortedFunctions = [...section.functions].sort()
////
////     for (const funcName of sortedFunctions) {
////       const href = `#${funcName}`
////       const member = document.querySelector(
////         `.member:has(h2 > a[href="${href}"])`
////       )
////       const sidebar = list.querySelector(`li:has(a[href="${href}"])`)
////       sortedList.append(sidebar)
////       sortedMembers.append(member)
////     }
////   }
////
////   document.querySelector(".sidebar").insertBefore(sortedLists, list)
////   document
////     .querySelector(".module-members:has(#module-values)")
////     .insertBefore(
////       sortedMembers,
////       document.querySelector("#module-values").nextSibling
////     )
//// }
////
//// document.readyState !== "loading"
////   ? callback()
////   : document.addEventListener(
////     "DOMContentLoaded",
////     callback,
////     { once: true }
////   )
//// </script>

import collie/internal/http as http_
import collie/internal/socket
import exception
import gleam/bytes_tree
import gleam/crypto
import gleam/dynamic
import gleam/erlang/charlist
import gleam/erlang/process
import gleam/http
import gleam/http/request
import gleam/http/response
import gleam/int
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/supervision
import gleam/result
import logging
import websocks

/// Represents an instruction on how WebSocket connection should proceed.
/// - continue processing the WebSocket connection.
/// - continue processing the WebSocket connection with selector for custom
/// messages.
/// - stop the WebSocket connection.
/// - stop the WebSocket connection with abnormal reason.
pub opaque type Next(state, message) {
  Continue(state: state, selector: option.Option(process.Selector(message)))
  NormalStop
  AbnormalStop(reason: String)
}

/// Instructs WebSocket connection to continue processing.
pub fn continue(state: state) -> Next(state, message) {
  Continue(state:, selector: option.None)
}

/// Instructs WebSocket connection to continue processing, with selector for
/// custom messages. New selector replaces any existing one that was previously
/// given.
pub fn continue_with_selector(
  state: state,
  selector: process.Selector(message),
) -> Next(state, message) {
  Continue(state:, selector: option.Some(selector))
}

/// Instructs WebSocket connection to stop.
pub fn stop() -> Next(state, message) {
  NormalStop
}

/// Instructs WebSocket connection to stop with abnormal reason.
pub fn stop_abnormal(reason: String) -> Next(state, message) {
  AbnormalStop(reason:)
}

/// A type returned from the initialiser, containing the WebSocket state and a
/// selector to receive messages with.
///
/// Use `initialised`, `selecting` and `returning` functions to construct this 
/// type.
pub opaque type Initialised(state, message, return) {
  Initialised(
    state: state,
    selector: option.Option(process.Selector(message)),
    return: return,
  )
}

/// Takes the post-initialisation state. This state will be passed to the
/// `on_message` callback each time the message is received.
pub fn initialised(state: state) -> Initialised(state, message, Nil) {
  Initialised(state:, selector: option.None, return: Nil)
}

/// Adds a selector to receive messages with.
pub fn selecting(
  initialised: Initialised(state, old_message, return),
  selector: process.Selector(message),
) -> Initialised(state, message, return) {
  Initialised(..initialised, selector: option.Some(selector))
}

/// Adds the data to return to the parent process.
pub fn returning(
  initialised: Initialised(state, message, old_return),
  return: return,
) -> Initialised(state, message, return) {
  Initialised(..initialised, return:)
}

/// Represents a message the websocket actor can receive.
pub type Message(message) {
  /// Indicates that text frame has been received.
  Text(String)
  /// Indicates that binary frame has been received.
  Binary(BitArray)
  /// Indicates that user message has been received from WebSocket selector.
  User(message)
}

/// Represents a WebSocket connection between a client and a server.
pub opaque type Connection {
  Connection(
    transport: socket.Transport,
    socket: socket.Socket,
    context: websocks.Context,
  )
}

/// Error codes that can occur during socket operations such as connecting,
/// sending, or receiving data.
pub type SocketReason {
  /// Connection was closed by the remote peer.
  Closed
  /// Operation exceeded the specified timeout.
  Timeout
  /// Invalid argument provided to socket operation.
  Badarg
  /// Process was terminated.
  Terminated
  /// Address is already in use.
  Eaddrinuse
  /// Requested address is not available.
  Eaddrnotavail
  /// Address family is not supported.
  Eafnosupport
  /// Connection attempt is already in progress.
  Ealready
  /// Connection was aborted by the system.
  Econnaborted
  /// Connection was refused by the remote host.
  Econnrefused
  /// Connection was reset by the remote peer.
  Econnreset
  /// Destination address is required.
  Edestaddrreq
  /// Remote host is down.
  Ehostdown
  /// Remote host is unreachable.
  Ehostunreach
  /// Operation is currently in progress.
  Einprogress
  /// Socket is already connected.
  Eisconn
  /// Message size is too large.
  Emsgsize
  /// Network is down.
  Enetdown
  /// Network is unreachable.
  Enetunreach
  /// Required package is not installed.
  Enopkg
  /// Protocol option is not available.
  Enoprotoopt
  /// Socket is not connected.
  Enotconn
  /// Inappropriate I/O control operation.
  Enotty
  /// File descriptor is not a socket.
  Enotsock
  /// Protocol error occurred.
  Eproto
  /// Protocol is not supported.
  Eprotonosupport
  /// Protocol type is incorrect for socket.
  Eprototype
  /// Socket type is not supported.
  Esocktnosupport
  /// Connection attempt timed out.
  Etimedout
  /// Operation would block in non-blocking mode.
  Ewouldblock
  /// Invalid port number.
  Exbadport
  /// Invalid sequence number.
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

/// Converts a socket error to a human-readable string.
pub fn socket_reason_to_string(reason: SocketReason) -> String {
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

/// WebSocket status codes that may appear in a close frame.
///
/// The codes reserved for local use only, such as 1005, 1006 and 1015, are
/// absent. They must never be sent, and receiving one is a protocol violation.
pub type CloseCode {
  /// The connection successfully completed its purpose and is closing normally.
  NormalClosure
  /// The endpoint is going away, either due to server shutdown or browser
  /// navigation.
  GoingAway
  /// A WebSocket protocol violation was detected.
  ProtocolError
  /// The endpoint received data it cannot accept.
  UnsupportedData
  /// The message data doesn’t match the declared type.
  InvalidPayloadData
  /// Generic status for policy violations when no other code applies.
  PolicyViolation
  /// Message exceeds the maximum size the endpoint can handle.
  MessageTooBig
  /// The server encountered an unexpected condition preventing request
  /// fulfillment.
  MandatoryExtension
  /// The server encountered an unexpected error.
  InternalError
  /// Server is restarting.
  ServiceRestart
  /// Temporary server overload.
  TryAgainLater
  /// Gateway/proxy received invalid response.
  BadGateway
  /// An application-specific code, between 3000 and 4999.
  ApplicationCode(code: Int)
}

/// Why the connection is closing. A close frame is allowed to carry neither
/// code nor reason.
pub type CloseReason {
  /// A close frame with an empty payload.
  NoCloseReason
  /// A close frame carrying a status code and a description. The description
  /// must not exceed 123 bytes.
  CloseReason(code: CloseCode, reason: String)
}

fn to_close_code(code: websocks.CloseCode) -> CloseCode {
  case code {
    websocks.NormalClosure -> NormalClosure
    websocks.GoingAway -> GoingAway
    websocks.ProtocolError -> ProtocolError
    websocks.UnsupportedData -> UnsupportedData
    websocks.InvalidPayloadData -> InvalidPayloadData
    websocks.PolicyViolation -> PolicyViolation
    websocks.MessageTooBig -> MessageTooBig
    websocks.MandatoryExtension -> MandatoryExtension
    websocks.InternalError -> InternalError
    websocks.ServiceRestart -> ServiceRestart
    websocks.TryAgainLater -> TryAgainLater
    websocks.BadGateway -> BadGateway
    websocks.ApplicationCode(code:) -> ApplicationCode(code:)
  }
}

fn to_internal_close_code(code: CloseCode) -> websocks.CloseCode {
  case code {
    NormalClosure -> websocks.NormalClosure
    GoingAway -> websocks.GoingAway
    ProtocolError -> websocks.ProtocolError
    UnsupportedData -> websocks.UnsupportedData
    InvalidPayloadData -> websocks.InvalidPayloadData
    PolicyViolation -> websocks.PolicyViolation
    MessageTooBig -> websocks.MessageTooBig
    MandatoryExtension -> websocks.MandatoryExtension
    InternalError -> websocks.InternalError
    ServiceRestart -> websocks.ServiceRestart
    TryAgainLater -> websocks.TryAgainLater
    BadGateway -> websocks.BadGateway
    ApplicationCode(code:) -> websocks.ApplicationCode(code:)
  }
}

fn to_close_reason(reason: websocks.CloseReason) -> CloseReason {
  case reason {
    websocks.NoCloseReason -> NoCloseReason
    websocks.CloseReason(code:, reason:) ->
      CloseReason(code: to_close_code(code), reason:)
  }
}

fn to_internal_close_reason(reason: CloseReason) -> websocks.CloseReason {
  case reason {
    NoCloseReason -> websocks.NoCloseReason
    CloseReason(code:, reason:) ->
      websocks.CloseReason(code: to_internal_close_code(code), reason:)
  }
}

/// Converts a close code to a human-readable string.
pub fn close_code_to_string(code: CloseCode) -> String {
  case code {
    NormalClosure -> "normal closure"
    GoingAway -> "going away"
    ProtocolError -> "protocol error"
    UnsupportedData -> "unsupported data"
    InvalidPayloadData -> "invalid payload data"
    PolicyViolation -> "policy violation"
    MessageTooBig -> "message too big"
    MandatoryExtension -> "mandatory extension"
    InternalError -> "internal error"
    ServiceRestart -> "service restart"
    TryAgainLater -> "try again later"
    BadGateway -> "bad gateway"
    ApplicationCode(code:) -> "application close code " <> int.to_string(code)
  }
}

/// Converts a close reason to a human-readable string.
pub fn close_reason_to_string(reason: CloseReason) -> String {
  case reason {
    NoCloseReason -> "no close reason"
    CloseReason(code:, reason: "") -> close_code_to_string(code)
    CloseReason(code:, reason:) -> close_code_to_string(code) <> ": " <> reason
  }
}

/// Caps on how much data the server can make this client hold at once. Without
/// them a server can declare an arbitrarily large frame, or fragment a single
/// message indefinitely, and exhaust memory.
pub type Limits {
  Limits(
    /// Largest payload a single frame may declare.
    max_frame_size: Int,
    /// Largest payload a fragmented message may accumulate to.
    max_message_size: Int,
  )
}

/// 16 MiB per frame, 64 MiB per reassembled message. Use the `with_limits`
/// function to override them.
pub const default_limits: Limits = Limits(
  max_frame_size: 16_777_216,
  max_message_size: 67_108_864,
)

/// Contains all client configurations, can be adjusted by different builder
/// functions.
pub opaque type Builder(body, state, message, return) {
  Builder(
    request: request.Request(body),
    named: option.Option(process.Name(WebsocketMessage(message))),
    connection_timeout: Int,
    limits: Limits,
    initialise: fn(process.Subject(WebsocketMessage(message))) ->
      Result(Initialised(state, message, return), String),
    handler: fn(Connection, state, Message(message)) -> Next(state, message),
    on_close: fn(state, CloseReason) -> Nil,
  )
}

/// Creates a new builder to set up WebSocket client with default configuration
/// without a custom initialiser. Use `new_with_initialiser` to create a builder
/// with some initialisation logic that runs before the client starts handling
/// messages.
pub fn new(
  request: request.Request(body),
  state: state,
) -> Builder(body, state, message, process.Subject(WebsocketMessage(message))) {
  Builder(
    request:,
    named: option.None,
    connection_timeout: 5000,
    limits: default_limits,
    initialise: fn(self) { initialised(state) |> returning(self) |> Ok },
    handler: fn(_conn, state, _message) { continue(state) },
    on_close: fn(_state, _reason) { Nil },
  )
}

/// Creates a new builder to set up WebSocket client with a custom initialiser
/// that runs before the client starts handling messages.
///
/// The actor's default subject is passed to the initialiser function. You can
/// use it to send custom messages via `to_user_message` or ignore it
/// completely.
///
/// If a custom selector is given using the `selecting` function, this expands
/// the default selector to handle custom messages.
pub fn new_with_initialiser(
  request: request.Request(body),
  initialise: fn(process.Subject(WebsocketMessage(message))) ->
    Result(Initialised(state, message, return), String),
) -> Builder(body, state, message, return) {
  Builder(
    request:,
    named: option.None,
    connection_timeout: 5000,
    limits: default_limits,
    initialise:,
    handler: fn(_conn, state, _message) { continue(state) },
    on_close: fn(_state, _reason) { Nil },
  )
}

/// Sets the maximum amount of time for the handshake to happen in milliseconds.
/// The initialiser function also has `timeout + 1000` milliseconds to run.
/// Default value is `5000`.
pub fn with_connection_timeout(
  builder: Builder(body, state, message, return),
  connection_timeout: Int,
) -> Builder(body, state, message, return) {
  Builder(..builder, connection_timeout:)
}

/// Sets the caps on how much data a single frame and a single reassembled
/// message may hold. A server that exceeds them closes the connection with
/// `MessageTooBig`. Defaults to `default_limits`.
pub fn with_limits(
  builder: Builder(body, state, message, return),
  limits: Limits,
) -> Builder(body, state, message, return) {
  Builder(..builder, limits:)
}

/// Provides a name for the client actor to be registered, enabling it to
/// receive messages via a named subject.
pub fn named(
  builder: Builder(body, state, message, return),
  name: process.Name(WebsocketMessage(message)),
) -> Builder(body, state, message, return) {
  Builder(..builder, named: option.Some(name))
}

/// Sets the message handler for the client. The callback function will be
/// called each time the client receives a message. It must return an
/// instruction on how the WebSocket connection should proceed.
pub fn on_message(
  builder: Builder(body, state, message, return),
  handler: fn(Connection, state, Message(message)) -> Next(state, message),
) -> Builder(body, state, message, return) {
  Builder(..builder, handler:)
}

/// Sets the handler that is called when the connection is closed. The callback
/// accepts the last value for the state and the closing reason.
pub fn on_close(
  builder: Builder(body, state, message, return),
  on_close: fn(state, CloseReason) -> Nil,
) -> Builder(body, state, message, return) {
  Builder(..builder, on_close:)
}

type WebsocketState(state, message) {
  WebsocketState(
    conn: Connection,
    user: state,
    context: websocks.Context,
    handler: fn(Connection, state, Message(message)) -> Next(state, message),
    on_close: fn(state, CloseReason) -> Nil,
  )
}

/// Messages received by the underlying actor. This type is exposed so
/// users are allowed to send custom messages. See `to_user_message` to
/// construct it.
pub opaque type WebsocketMessage(message) {
  Packet(BitArray)
  UserMessage(message)
  Passive
  SocketError(socket.SocketReason)
  Close
}

/// Maps custom message to the `WebsocketMessage` opaque type, allowing to send
/// custom messages to the client's process.
pub fn to_user_message(message: message) -> WebsocketMessage(message) {
  UserMessage(message)
}

fn create_socket_selector(
  self: process.Subject(WebsocketMessage(message)),
  user: option.Option(process.Selector(message)),
) -> process.Selector(WebsocketMessage(message)) {
  let selector =
    process.new_selector()
    |> process.select(self)
    |> process.select_record(Tcp, 2, coerce_socket_message)
    |> process.select_record(Ssl, 2, coerce_socket_message)
    |> process.select_record(TcpClosed, 1, coerce_socket_message)
    |> process.select_record(SslClosed, 1, coerce_socket_message)
    |> process.select_record(TcpPassive, 1, coerce_socket_message)
    |> process.select_record(SslPassive, 1, coerce_socket_message)
    |> process.select_record(TcpError, 2, coerce_socket_message)
    |> process.select_record(SslError, 2, coerce_socket_message)

  case user {
    option.Some(user) ->
      process.map_selector(user, UserMessage)
      |> process.merge_selector(selector)
    option.None -> selector
  }
}

type SelectRecord {
  Tcp
  Ssl
  TcpClosed
  SslClosed
  TcpPassive
  SslPassive
  TcpError
  SslError
}

const socket_mode = [socket.ActiveMode(socket.Count(100))]

@external(erlang, "collie_ffi", "coerce_socket_message")
fn coerce_socket_message(record: dynamic.Dynamic) -> WebsocketMessage(message)

/// Starts the WebSocket connection with the provided configurations.
pub fn start(
  builder: Builder(body, state, message, return),
) -> Result(actor.Started(return), actor.StartError) {
  let transport = case builder.request.scheme {
    http.Https -> socket.Ssl
    http.Http -> socket.Tcp
  }

  let actor =
    actor.new_with_initialiser(builder.connection_timeout + 1000, fn(self) {
      use #(response, socket, remaining) <- handshake(
        builder.request,
        builder.connection_timeout,
        transport,
      )

      logging.log(logging.Debug, "WebSocket handshake completed successfully")

      case remaining {
        <<>> -> Nil
        remaining -> actor.send(self, Packet(remaining))
      }

      use _ <- unwrap_socket(socket.set_opts(transport, socket, socket_mode))

      let extensions =
        response.get_header(response, "sec-websocket-extensions")
        |> result.unwrap("")

      logging.log(logging.Debug, "Calling initialiser function")
      use Initialised(state, selector, return) <- result.try(builder.initialise(
        self,
      ))
      logging.log(logging.Debug, "Initialiser returned successfully")

      let compression = case websocks.has_deflate(extensions) {
        True -> {
          logging.log(
            logging.Debug,
            "Using permessage-deflate for the WebSocket connection",
          )
          option.Some(websocks.get_compression_extensions(extensions))
        }
        False -> option.None
      }
      let Limits(max_frame_size:, max_message_size:) = builder.limits
      let context =
        websocks.create_context(compression, websocks.Client)
        |> websocks.with_limits(websocks.Limits(
          max_frame_size:,
          max_message_size:,
        ))

      WebsocketState(
        conn: Connection(transport:, socket:, context:),
        user: state,
        context:,
        handler: builder.handler,
        on_close: builder.on_close,
      )
      |> actor.initialised
      |> actor.selecting(create_socket_selector(self, selector))
      |> actor.returning(return)
      |> Ok
    })
    |> actor.on_message(handle_message)

  let actor = case builder.named {
    option.Some(name) -> actor.named(actor, name)
    option.None -> actor
  }

  actor.start(actor)
}

/// Returns a child specification for use in a supervision tree.
pub fn supervised(
  builder: Builder(body, state, message, return),
) -> supervision.ChildSpecification(return) {
  supervision.worker(fn() { start(builder) })
}

/// Returns a factory supervisor builder for dynamically starting WebSocket 
/// connections.
pub fn factory(
  build: fn(start_args) -> Builder(body, state, message, return),
) -> factory.Builder(start_args, return) {
  factory.worker_child(fn(args) { start(build(args)) })
}

fn handshake(
  request: request.Request(body),
  connection_timeout: Int,
  transport: socket.Transport,
  handle_response: fn(#(response.Response(BitArray), socket.Socket, BitArray)) ->
    Result(continue, String),
) -> Result(continue, String) {
  let #(options, default_port) = case transport {
    socket.Ssl -> #(
      [
        socket.Cacerts(socket.get_system_cacerts()),
        socket.CustomizeHostnameCheck(socket.get_custom_hostname_check()),
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
        101 -> handle_response(#(response, socket, remaining))
        status -> {
          let message =
            "WebSocket handshake failed with status " <> int.to_string(status)
          logging.log(logging.Error, message)
          Error(message)
        }
      }
    }
    Error(http_.SocketFailed(reason)) -> {
      let message =
        "WebSocket handshake failed due to socket: "
        <> socket.reason_to_string(reason)
      logging.log(logging.Error, message)
      Error(message)
    }
    Error(http_.MalformedRequest) -> {
      let message = "WebSocket handshake failed due to malformed request"
      logging.log(logging.Error, message)
      Error(message)
    }
  }
}

fn unwrap_socket(
  result: Result(return, socket.SocketReason),
  handle_return: fn(return) -> Result(continue, String),
) -> Result(continue, String) {
  case result {
    Ok(return) -> handle_return(return)
    Error(reason) -> {
      let message =
        "Websocket handshake failed due to socket: "
        <> socket.reason_to_string(reason)
      logging.log(logging.Error, message)
      Error(message)
    }
  }
}

fn handle_message(
  state: WebsocketState(state, message),
  message: WebsocketMessage(message),
) -> actor.Next(WebsocketState(state, message), WebsocketMessage(message)) {
  case message {
    Packet(data) -> handle_packet(data, state)

    UserMessage(message) -> {
      logging.log(logging.Debug, "Received user message from selector")
      let resolved = call_handler(new_resolve_state(state), User(message))
      resolve_next(resolved, state)
    }
    Passive -> {
      let options =
        socket.set_opts(state.conn.transport, state.conn.socket, socket_mode)
      case options {
        Ok(Nil) -> actor.continue(state)
        Error(reason) -> {
          let reason = socket.reason_to_string(reason)
          logging.log(logging.Error, "Failed to set socket options: " <> reason)

          CloseReason(InternalError, reason)
          |> handle_close(state, _, option.Some(reason))
        }
      }
    }

    SocketError(reason) -> {
      let reason = socket.reason_to_string(reason)
      logging.log(logging.Error, "Socket error: " <> reason)

      CloseReason(InternalError, reason)
      |> handle_close(state, _, option.Some(reason))
    }
    Close -> {
      logging.log(logging.Debug, "Socket closed by remote peer")
      handle_close(state, NoCloseReason, option.None)
    }
  }
}

type ResolveState(state, message) {
  ResolveState(
    conn: Connection,
    handler: fn(Connection, state, Message(message)) -> Next(state, message),
    next: Next(state, message),
    reason: option.Option(CloseReason),
  )
}

fn new_resolve_state(state: WebsocketState(state, message)) {
  ResolveState(
    conn: state.conn,
    handler: state.handler,
    next: Continue(state: state.user, selector: option.None),
    reason: option.None,
  )
}

fn handle_packet(
  data: BitArray,
  state: WebsocketState(state, message),
) -> actor.Next(WebsocketState(state, message), WebsocketMessage(message)) {
  websocks.push_data(state.context, data)
  |> drain_frames(state, new_resolve_state(state))
}

// `next_frame` hands back a single frame at a time, so keep draining the
// buffer until it holds no further frame, or a frame stops the connection.
fn drain_frames(
  context: websocks.Context,
  state: WebsocketState(state, message),
  resolved: ResolveState(state, message),
) -> actor.Next(WebsocketState(state, message), WebsocketMessage(message)) {
  case websocks.next_frame(context) {
    Ok(websocks.MoreData(context:)) ->
      resolve_next(resolved, with_context(state, context))

    Ok(websocks.Decoded(frame:, context:)) -> {
      let resolved = handle_frame(resolved, context, frame)
      case resolved.next {
        Continue(..) -> drain_frames(context, state, resolved)
        NormalStop | AbnormalStop(..) ->
          resolve_next(resolved, with_context(state, context))
      }
    }

    Error(violation) -> {
      let #(code, reason) = case violation {
        websocks.DecodeFailed(websocks.InvalidFrame) -> #(
          ProtocolError,
          "Malformed wire format",
        )
        websocks.DecodeFailed(websocks.FrameTooLarge(length:, limit:)) -> #(
          MessageTooBig,
          "Frame declares "
            <> int.to_string(length)
            <> " bytes, over the limit of "
            <> int.to_string(limit),
        )
        websocks.DecodeFailed(websocks.NotEnoughData(_data)) ->
          panic as "Unreachable branch for `next_frame`!"

        websocks.ResolveFailed(websocks.NotUtf8) -> #(
          InvalidPayloadData,
          "Text frame payload isn't valid UTF-8",
        )
        websocks.ResolveFailed(websocks.OrphanedContinuation) -> #(
          ProtocolError,
          "Continuation frame without a preceding fragmented start",
        )
        websocks.ResolveFailed(websocks.FragmentationInterrupted) -> #(
          ProtocolError,
          "Complete text/binary frame received mid-fragmentation",
        )
        websocks.ResolveFailed(websocks.ConcurrentFragmentation) -> #(
          ProtocolError,
          "New fragmented frame started while another is in progress",
        )
        websocks.ResolveFailed(websocks.MessageTooLarge(size:, limit:)) -> #(
          MessageTooBig,
          "Message accumulated "
            <> int.to_string(size)
            <> " bytes, over the limit of "
            <> int.to_string(limit),
        )
        websocks.ResolveFailed(websocks.DecompressionFailed) -> #(
          ProtocolError,
          "Payload isn't a valid deflate stream, or inflates past the message limit",
        )
      }

      logging.log(logging.Warning, "Protocol violation: " <> reason)
      handle_close(
        with_context(state, context),
        CloseReason(code:, reason:),
        option.Some(reason),
      )
    }
  }
}

fn with_context(
  state: WebsocketState(state, message),
  context: websocks.Context,
) -> WebsocketState(state, message) {
  let conn = Connection(..state.conn, context:)
  WebsocketState(..state, conn:, context:)
}

fn resolve_next(
  resolved: ResolveState(state, message),
  state: WebsocketState(state, message),
) -> actor.Next(WebsocketState(state, message), WebsocketMessage(message)) {
  case resolved.next {
    Continue(user, selector) -> {
      let next = actor.continue(WebsocketState(..state, user:))
      case selector {
        option.Some(selector) ->
          process.map_selector(selector, UserMessage)
          |> actor.with_selector(next, _)
        option.None -> next
      }
    }
    NormalStop -> {
      let reason = option.unwrap(resolved.reason, NoCloseReason)
      handle_close(state, reason, option.None)
    }
    AbnormalStop(reason: reason_string) -> {
      let reason =
        option.unwrap(
          resolved.reason,
          CloseReason(InternalError, reason_string),
        )
      handle_close(state, reason, option.Some(reason_string))
    }
  }
}

fn handle_close(
  state: WebsocketState(state, message),
  reason: CloseReason,
  abnormal: option.Option(String),
) {
  let stop = case abnormal {
    option.Some(reason) -> {
      logging.log(logging.Warning, "Closing connection abnormally: " <> reason)
      actor.stop_abnormal(reason)
    }
    option.None -> {
      logging.log(
        logging.Debug,
        "Closing connection: " <> close_reason_to_string(reason),
      )
      actor.stop()
    }
  }

  websocks.close_context(state.context)
  state.on_close(state.user, reason)

  stop
}

fn handle_frame(
  state: ResolveState(state, message),
  context: websocks.Context,
  frame: websocks.Frame,
) -> ResolveState(state, message) {
  let conn = Connection(..state.conn, context:)
  let state = ResolveState(..state, conn:)

  case frame {
    websocks.Control(websocks.Ping(payload)) -> {
      logging.log(logging.Debug, "Received ping frame")
      let pong =
        websocks.encode_pong_frame(
          payload:,
          masking: option.Some(crypto.strong_random_bytes(4)),
        )
        |> bytes_tree.from_bit_array

      case socket.send(state.conn.transport, state.conn.socket, pong) {
        Ok(Nil) -> {
          logging.log(logging.Debug, "Sent pong frame")
          state
        }
        Error(reason) -> {
          let reason =
            "failed to send pong: " <> socket.reason_to_string(reason)
          logging.log(logging.Error, reason)
          let next = AbnormalStop(reason)
          let reason = option.Some(CloseReason(InternalError, reason))

          ResolveState(..state, next:, reason:)
        }
      }
    }

    websocks.Control(websocks.Close(reason)) -> {
      logging.log(
        logging.Debug,
        "Received close frame: "
          <> close_reason_to_string(to_close_reason(reason)),
      )

      let _sent =
        websocks.encode_close_frame(
          reason:,
          masking: option.Some(crypto.strong_random_bytes(4)),
        )
        |> bytes_tree.from_bit_array
        |> socket.send(state.conn.transport, state.conn.socket, _)

      let reason = option.Some(to_close_reason(reason))
      ResolveState(..state, next: NormalStop, reason:)
    }

    websocks.Control(websocks.Pong(_)) -> {
      logging.log(logging.Debug, "Received pong frame")
      state
    }

    websocks.Text(payload) ->
      call_handler(state, Text(unsafe_to_string(payload)))

    websocks.Binary(payload) -> call_handler(state, Binary(payload))

    websocks.Continuation(_) -> state
  }
}

@external(erlang, "gleam_stdlib", "identity")
fn unsafe_to_string(a: BitArray) -> String

fn call_handler(
  state: ResolveState(state, message),
  message: Message(message),
) -> ResolveState(state, message) {
  let assert Continue(user_state, selector) = state.next

  let call =
    exception.rescue(fn() { state.handler(state.conn, user_state, message) })
  case call {
    Ok(Continue(user_state, new_selector)) -> {
      let selector = option.or(new_selector, selector)
      ResolveState(..state, next: Continue(user_state, selector))
    }
    Ok(NormalStop) -> ResolveState(..state, next: NormalStop)
    Ok(AbnormalStop(reason)) ->
      ResolveState(..state, next: AbnormalStop(reason))
    Error(exception) -> {
      let reason = case exception {
        exception.Errored(_dynamic) ->
          "An error was raised in the handler. This can be caused by calling the erlang:error/1 function, or some other runtime error."
        exception.Thrown(_dynamic) ->
          "A value was thrown in the handler. This can be caused by calling the erlang:throw/1 function."
        exception.Exited(_dynamic) ->
          "A process exited in the handler. This can be caused by calling the erlang:exit/1 function."
      }
      logging.log(logging.Error, "Handler exception: " <> reason)
      let next = AbnormalStop(reason)
      let reason = option.Some(CloseReason(InternalError, reason))

      ResolveState(..state, next:, reason:)
    }
  }
}

/// Sends a ping frame to the WebSocket server.
pub fn send_ping(
  conn: Connection,
  data: BitArray,
) -> Result(Nil, SocketReason) {
  option.Some(crypto.strong_random_bytes(4))
  |> websocks.encode_ping_frame(data, _)
  |> bytes_tree.from_bit_array
  |> socket.send(conn.transport, conn.socket, _)
  |> result.map_error(to_socket_reason)
}

/// Sends a text frame to the WebSocket server.
pub fn send_text_frame(
  conn: Connection,
  text: String,
) -> Result(Nil, SocketReason) {
  option.Some(crypto.strong_random_bytes(4))
  |> websocks.encode_text_frame(<<text:utf8>>, conn.context, _)
  |> bytes_tree.from_bit_array
  |> socket.send(conn.transport, conn.socket, _)
  |> result.map_error(to_socket_reason)
}

/// Sends a binary frame to the WebSocket server.
pub fn send_binary_frame(
  conn: Connection,
  bits: BitArray,
) -> Result(Nil, SocketReason) {
  option.Some(crypto.strong_random_bytes(4))
  |> websocks.encode_binary_frame(bits, conn.context, _)
  |> bytes_tree.from_bit_array
  |> socket.send(conn.transport, conn.socket, _)
  |> result.map_error(to_socket_reason)
}

/// Sends a close frame to the WebSocket server. Once called, no other frames 
/// can be sent on this connection. Stop the actor after calling this.
pub fn send_close_frame(
  conn: Connection,
  reason: CloseReason,
) -> Result(Nil, SocketReason) {
  websocks.encode_close_frame(
    reason: to_internal_close_reason(reason),
    masking: option.Some(crypto.strong_random_bytes(4)),
  )
  |> bytes_tree.from_bit_array
  |> socket.send(conn.transport, conn.socket, _)
  |> result.map_error(to_socket_reason)
}
