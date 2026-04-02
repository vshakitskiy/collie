import exception
import gleam/bit_array
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
import gleam/otp/supervision
import gleam/result
import gleam/string
import internal/http as http_
import internal/socket
import websocks

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
  Connection(
    transport: socket.Transport,
    socket: socket.Socket,
    context: websocks.Context,
  )
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

fn to_close_reason(reason: websocks.CloseReason) -> CloseReason {
  case reason {
    websocks.NormalClosure(data) -> NormalClosure(data)
    websocks.GoingAway(data:) -> GoingAway(data:)
    websocks.ProtocolError(data:) -> ProtocolError(data:)
    websocks.UnsupportedData(data:) -> UnsupportedData(data:)
    websocks.InvalidPayloadData(data:) -> InvalidPayloadData(data:)
    websocks.PolicyViolation(data:) -> PolicyViolation(data:)
    websocks.MessageTooBig(data:) -> MessageTooBig(data:)
    websocks.MandatoryExtension(data:) -> MandatoryExtension(data:)
    websocks.InternalError(data:) -> InternalError(data:)
    websocks.ServiceRestart(data:) -> ServiceRestart(data:)
    websocks.TryAgainLater(data:) -> TryAgainLater(data:)
    websocks.BadGateway(data:) -> BadGateway(data:)
    websocks.TLSHandshake(data:) -> TLSHandshake(data:)
    websocks.CustomCloseCode(code:, data:) -> CustomCloseCode(code:, data:)
    websocks.NoCloseReason -> NoCloseReason
  }
}

fn to_internal_close_reason(reason: CloseReason) -> websocks.CloseReason {
  case reason {
    NormalClosure(data:) -> websocks.NormalClosure(data:)
    GoingAway(data:) -> websocks.GoingAway(data:)
    ProtocolError(data:) -> websocks.ProtocolError(data:)
    UnsupportedData(data:) -> websocks.UnsupportedData(data:)
    InvalidPayloadData(data:) -> websocks.InvalidPayloadData(data:)
    PolicyViolation(data:) -> websocks.PolicyViolation(data:)
    MessageTooBig(data:) -> websocks.MessageTooBig(data:)
    MandatoryExtension(data:) -> websocks.MandatoryExtension(data:)
    InternalError(data:) -> websocks.InternalError(data:)
    ServiceRestart(data:) -> websocks.ServiceRestart(data:)
    TryAgainLater(data:) -> websocks.TryAgainLater(data:)
    BadGateway(data:) -> websocks.BadGateway(data:)
    TLSHandshake(data:) -> websocks.TLSHandshake(data:)
    CustomCloseCode(code:, data:) -> websocks.CustomCloseCode(code:, data:)
    NoCloseReason -> websocks.NoCloseReason
  }
}

pub opaque type Builder(body, state, message) {
  Builder(
    request: request.Request(body),
    named: option.Option(process.Name(WebsocketMessage(message))),
    connection_timeout: Int,
    initialise: fn(process.Subject(WebsocketMessage(message))) ->
      Result(Initialised(state, message), String),
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
    named: option.None,
    connection_timeout: 5000,
    initialise: fn(_self) { Ok(initialised(state)) },
    handler: fn(_conn, state, _message) { continue(state) },
    on_close: fn(_state, _reason) { Nil },
  )
}

pub fn new_with_initialiser(
  request: request.Request(body),
  initialise: fn(process.Subject(WebsocketMessage(message))) ->
    Result(Initialised(state, message), String),
) -> Builder(body, state, message) {
  Builder(
    request:,
    named: option.None,
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

pub fn named(
  builder: Builder(body, state, message),
  name: process.Name(WebsocketMessage(message)),
) -> Builder(body, state, message) {
  Builder(..builder, named: option.Some(name))
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

type WebsocketState(state, message) {
  WebsocketState(
    conn: Connection,
    user: state,
    context: websocks.Context,
    handler: fn(Connection, state, Message(message)) -> Next(state, message),
    on_close: fn(state, CloseReason) -> Nil,
  )
}

pub opaque type WebsocketMessage(message) {
  Packet(BitArray)
  UserMessage(message)
  Passive
  SocketError(socket.SocketReason)
  Close
}

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

pub fn start(
  builder: Builder(body, state, message),
) -> Result(
  actor.Started(process.Subject(WebsocketMessage(message))),
  actor.StartError,
) {
  let transport = case builder.request.scheme {
    http.Https -> socket.Ssl
    http.Http -> socket.Tcp
  }

  let actor =
    actor.new_with_initialiser(1000, fn(self) {
      use #(response, socket, remaining) <- handshake(
        builder.request,
        builder.connection_timeout,
        transport,
      )

      case remaining {
        <<>> -> Nil
        remaining -> actor.send(self, Packet(remaining))
      }

      use _ <- unwrap_socket(socket.set_opts(transport, socket, socket_mode))

      let extensions =
        response.get_header(response, "sec-websocket-extensions")
        |> result.map(string.split(_, ";"))
        |> result.unwrap([])
        |> list.map(string.trim)

      use Initialised(state, selector) <- result.try(builder.initialise(self))

      let compression = case websocks.has_deflate(extensions) {
        True -> option.Some(websocks.get_compression_extensions(extensions))
        False -> option.None
      }
      let context = websocks.create_context(compression, websocks.Client)

      WebsocketState(
        conn: Connection(transport:, socket:, context:),
        user: state,
        context:,
        handler: builder.handler,
        on_close: builder.on_close,
      )
      |> actor.initialised
      |> actor.selecting(create_socket_selector(self, selector))
      |> actor.returning(self)
      |> Ok
    })
    |> actor.on_message(handle_message)

  let actor = case builder.named {
    option.Some(name) -> actor.named(actor, name)
    option.None -> actor
  }

  actor.start(actor)
}

pub fn supervised(
  builder: Builder(body, state, message),
) -> supervision.ChildSpecification(process.Subject(WebsocketMessage(message))) {
  supervision.supervisor(fn() { start(builder) })
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
        101 -> handle_response(#(response, socket, remaining))
        status ->
          { "Websocket handshake failed with status " <> int.to_string(status) }
          |> Error
      }
    }
    Error(http_.SocketFailed(reason)) -> reason_to_string(reason)
    Error(http_.MalformedRequest) ->
      Error("WebSocket handshake failed due to malformed request")
  }
}

fn unwrap_socket(
  result: Result(return, socket.SocketReason),
  handle_return: fn(return) -> Result(continue, String),
) -> Result(continue, String) {
  case result {
    Ok(return) -> handle_return(return)
    Error(reason) -> reason_to_string(reason)
  }
}

fn reason_to_string(reason: socket.SocketReason) -> Result(a, String) {
  Error(
    "Websocket handshake failed due to socket: "
    <> socket.reason_to_string(reason),
  )
}

fn handle_message(
  state: WebsocketState(state, message),
  message: WebsocketMessage(message),
) -> actor.Next(WebsocketState(state, message), WebsocketMessage(message)) {
  case message {
    Packet(data) -> handle_packet(data, state)

    UserMessage(message) -> {
      let resolved = call_handler(new_resolve_state(state), User(message))
      resolve_next(resolved.state, state)
    }
    Passive -> {
      let options =
        socket.set_opts(state.conn.transport, state.conn.socket, socket_mode)
      case options {
        Ok(Nil) -> actor.continue(state)
        Error(reason) -> {
          let reason = socket.reason_to_string(reason)

          InternalError(bit_array.from_string(reason))
          |> handle_close(state, _, option.Some(reason))
        }
      }
    }

    SocketError(reason) -> {
      let reason = socket.reason_to_string(reason)

      InternalError(bit_array.from_string(reason))
      |> handle_close(state, _, option.Some(reason))
    }
    Close -> handle_close(state, NoCloseReason, option.None)
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
  let processed =
    new_resolve_state(state)
    |> websocks.process_incoming_frames(data, state.context, _, handle_frame)

  case processed {
    Ok(#(resolved, context)) -> {
      let conn = Connection(..state.conn, context:)
      resolve_next(resolved, WebsocketState(..state, conn:, context:))
    }
    Error(violation) -> {
      let #(variant, reason) = case violation {
        websocks.DecodeFailed(websocks.InvalidFrame) -> #(
          ProtocolError,
          "Malformed wire format",
        )
        websocks.DecodeFailed(websocks.NotEnoughData(_data)) ->
          panic as "Unreachable branch for `process_incoming_frames`!"

        websocks.ResolveFailed(websocks.NotUtf8) -> #(
          InvalidPayloadData,
          "Text frame payload isn't valid UTF-8",
        )
        websocks.ResolveFailed(websocks.OrphanedContinuation) -> #(
          ProtocolError,
          "Continuation frame without a preceding fragmented start",
        )
        websocks.ResolveFailed(websocks.ControlFrameFragmented) -> #(
          ProtocolError,
          "Control frame was fragmented",
        )
        websocks.ResolveFailed(websocks.FragmentationInterrupted) -> #(
          ProtocolError,
          "Complete text/binary frame received mid-fragmentation",
        )
        websocks.ResolveFailed(websocks.ConcurrentFragmentation) -> #(
          ProtocolError,
          "New fragmented frame started while another is in progress",
        )
        websocks.ResolveFailed(websocks.CompressedContinuation) -> #(
          ProtocolError,
          "Continuation frame has RSV1 set",
        )
      }

      handle_close(state, variant(<<reason:utf8>>), option.Some(reason))
    }
  }
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
        option.unwrap(resolved.reason, InternalError(<<reason_string:utf8>>))
      handle_close(state, reason, option.Some(reason_string))
    }
  }
}

fn handle_close(
  state: WebsocketState(state, message),
  reason: CloseReason,
  abnormal: option.Option(String),
) {
  websocks.close_context(state.context)
  state.on_close(state.user, reason)

  case abnormal {
    option.Some(reason) -> actor.stop_abnormal(reason)
    option.None -> actor.stop()
  }
}

fn handle_frame(
  state: ResolveState(state, message),
  context: websocks.Context,
  frame: websocks.Frame,
) -> websocks.ResolveNext(ResolveState(state, message)) {
  let conn = Connection(..state.conn, context:)
  let state = ResolveState(..state, conn:)

  case frame {
    websocks.Control(websocks.Ping(payload)) -> {
      case bit_array.byte_size(payload) {
        size if size > 125 -> {
          let next = AbnormalStop("control frame payload exceeds 125 octets")
          let reason =
            ProtocolError(<<"control frame payload exceeds 125 octets">>)
            |> option.Some

          websocks.Stop(ResolveState(..state, next:, reason:))
        }
        _ -> {
          let pong =
            option.Some(crypto.strong_random_bytes(4))
            |> websocks.encode_pong_frame(payload:)
            |> bytes_tree.from_bit_array

          case socket.send(state.conn.transport, state.conn.socket, pong) {
            Ok(Nil) -> websocks.Continue(state)
            Error(reason) -> {
              let reason =
                "failed to send pong: " <> socket.reason_to_string(reason)
              let next = AbnormalStop(reason)
              let reason = option.Some(InternalError(<<reason:utf8>>))

              websocks.Stop(ResolveState(..state, next:, reason:))
            }
          }
        }
      }
    }

    websocks.Control(websocks.Close(reason)) -> {
      let _sent =
        option.Some(crypto.strong_random_bytes(4))
        |> websocks.encode_close_frame(reason:)
        |> bytes_tree.from_bit_array
        |> socket.send(state.conn.transport, state.conn.socket, _)

      let reason = option.Some(to_close_reason(reason))
      websocks.Stop(ResolveState(..state, next: NormalStop, reason:))
    }

    websocks.Control(websocks.Pong(_)) -> websocks.Continue(state)

    websocks.Text(payload) ->
      call_handler(state, Text(unsafe_to_string(payload)))
    websocks.Binary(payload) -> call_handler(state, Binary(payload))

    websocks.Continuation(_) -> websocks.Continue(state)
  }
}

@external(erlang, "gleam_stdlib", "identity")
fn unsafe_to_string(a: BitArray) -> String

fn call_handler(
  state: ResolveState(state, message),
  message: Message(message),
) -> websocks.ResolveNext(ResolveState(state, message)) {
  let assert Continue(user_state, selector) = state.next

  let call =
    exception.rescue(fn() { state.handler(state.conn, user_state, message) })
  case call {
    Ok(Continue(user_state, new_selector)) -> {
      let selector = option.or(new_selector, selector)
      ResolveState(..state, next: Continue(user_state, selector))
      |> websocks.Continue
    }
    Ok(NormalStop) -> websocks.Stop(ResolveState(..state, next: NormalStop))
    Ok(AbnormalStop(reason)) ->
      websocks.Stop(ResolveState(..state, next: AbnormalStop(reason)))
    Error(exception) -> {
      let reason = case exception {
        exception.Errored(_dynamic) ->
          "An error was raised in the handler. This can be caused by calling the erlang:error/1 function, or some other runtime error."
        exception.Thrown(_dynamic) ->
          "A value was thrown in the handler. This can be caused by calling the erlang:throw/1 function."
        exception.Exited(_dynamic) ->
          "A process exited in the handler. This can be caused by calling the erlang:exit/1 function."
      }
      let next = AbnormalStop(reason)
      let reason = option.Some(InternalError(<<reason:utf8>>))

      websocks.Stop(ResolveState(..state, next:, reason:))
    }
  }
}

pub fn send_ping(conn: Connection, data: BitArray) -> Result(Nil, SocketReason) {
  option.Some(crypto.strong_random_bytes(4))
  |> websocks.encode_ping_frame(data, _)
  |> bytes_tree.from_bit_array
  |> socket.send(conn.transport, conn.socket, _)
  |> result.map_error(to_socket_reason)
}

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

pub fn send_close_frame(
  conn: Connection,
  reason: CloseReason,
) -> Next(state, message) {
  let sent =
    to_internal_close_reason(reason)
    |> websocks.encode_close_frame(option.Some(crypto.strong_random_bytes(4)))
    |> bytes_tree.from_bit_array()
    |> socket.send(conn.transport, conn.socket, _)

  case sent {
    Ok(Nil) -> NormalStop
    Error(reason) -> {
      let reason = socket.reason_to_string(reason)
      AbnormalStop("Errored while trying to send close frame: " <> reason)
    }
  }
}
