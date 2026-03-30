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
    named: process.Name(WebsocketMessage(message)),
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
    named: process.new_name("client"),
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
    named: process.new_name("client"),
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
  Builder(..builder, named: name)
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
    transport: socket.Transport,
    socket: socket.Socket,
    user: state,
    context: websocks.Context,
    handler: fn(Connection, state, Message(message)) -> Next(state, message),
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

@external(erlang, "websocket_ffi", "coerce_socket_message")
fn coerce_socket_message(record: dynamic.Dynamic) -> WebsocketMessage(message)

pub fn start(builder: Builder(body, state, message)) {
  let transport = case builder.request.scheme {
    http.Https -> socket.Ssl
    http.Http -> socket.Tcp
  }

  actor.new_with_initialiser(1000, fn(self) {
    use #(response, socket, remaining) <- handshake(
      builder.request,
      builder.connection_timeout,
      transport,
    )

    use _ <- unwrap_socket(socket.set_opts(transport, socket, socket_mode))

    case remaining {
      <<>> -> Nil
      remaining -> actor.send(self, Packet(remaining))
    }

    let extensions =
      response.get_header(response, "sec-websocket-extensions")
      |> result.map(string.split(_, ";"))
      |> result.unwrap([])

    use Initialised(state, selector) <- result.try(builder.initialise())

    let compression = case websocks.has_deflate(extensions) {
      True -> option.Some(websocks.get_context_takeovers(extensions))
      False -> option.None
    }
    let context = websocks.create_context(compression)

    WebsocketState(
      transport:,
      socket:,
      user: state,
      context:,
      handler: builder.handler,
    )
    |> actor.initialised
    |> actor.selecting(create_socket_selector(self, selector))
    |> actor.returning(self)
    |> Ok
  })
  |> actor.named(builder.named)
  |> actor.on_message(handle_message)
  |> actor.start
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
  case echo message {
    Packet(data) -> handle_packet(data, state, message)
    UserMessage(_) -> todo
    Passive ->
      case socket.set_opts(state.transport, state.socket, socket_mode) {
        Ok(Nil) -> actor.continue(state)
        Error(_reason) -> actor.stop()
      }
    SocketError(reason) -> {
      websocks.close_context(state.context)

      socket.reason_to_string(reason)
      |> actor.stop_abnormal
    }
    Close -> {
      websocks.close_context(state.context)

      actor.stop()
    }
  }

  actor.continue(state)
}

type ResolveState(state, message) {
  ResolveState(
    transport: socket.Transport,
    socket: socket.Socket,
    handler: fn(Connection, state, Message(message)) -> Next(state, message),
    next: Next(state, message),
  )
}

fn handle_packet(
  data: BitArray,
  state: WebsocketState(state, message),
  message: WebsocketMessage(message),
) -> actor.Next(WebsocketState(state, message), WebsocketMessage(message)) {
  let conn = Connection(transport: state.transport, socket: state.socket)

  let processed =
    websocks.process_incoming_frames(
      data,
      state.context,
      ResolveState(
        transport: state.transport,
        socket: state.socket,
        handler: state.handler,
        next: Continue(state: state.user, selector: option.None),
      ),
      handle_frame,
    )

  todo
}

fn handle_frame(
  state: ResolveState(state, message),
  _context: websocks.Context,
  frame: websocks.Frame,
) -> websocks.ResolveNext(ResolveState(state, message)) {
  case frame {
    websocks.Control(websocks.Ping(payload)) -> {
      case bit_array.byte_size(payload) {
        size if size > 125 ->
          websocks.Stop(
            ResolveState(
              ..state,
              next: AbnormalStop("control frame payload exceeds 125 octets"),
            ),
          )
        _ -> {
          let mask = option.Some(crypto.strong_random_bytes(4))
          let pong =
            websocks.encode_pong_frame(payload:, masking: mask)
            |> bytes_tree.from_bit_array

          case socket.send(state.transport, state.socket, pong) {
            Ok(Nil) -> websocks.Continue(state)
            Error(reason) ->
              websocks.Stop(
                ResolveState(
                  ..state,
                  next: AbnormalStop(
                    "failed to send pong: " <> socket.reason_to_string(reason),
                  ),
                ),
              )
          }
        }
      }
    }

    websocks.Control(websocks.Close(reason)) -> {
      let mask = option.Some(crypto.strong_random_bytes(4))
      let close =
        websocks.encode_close_frame(reason:, masking: mask)
        |> bytes_tree.from_bit_array
      let _sent = socket.send(state.transport, state.socket, close)
      websocks.Stop(ResolveState(..state, next: NormalStop))
    }

    websocks.Control(websocks.Pong(_)) -> websocks.Continue(state)

    websocks.Text(payload) -> {
      case bit_array.to_string(payload) {
        Ok(text) -> call_handler(state, Text(text))
        Error(Nil) ->
          websocks.Stop(
            ResolveState(
              ..state,
              next: AbnormalStop("received invalid UTF-8 in text frame"),
            ),
          )
      }
    }

    websocks.Binary(payload) -> call_handler(state, Binary(payload))

    websocks.Continuation(_) -> websocks.Continue(state)
  }
}

fn call_handler(
  state: ResolveState(state, message),
  message: Message(message),
) -> websocks.ResolveNext(ResolveState(state, message)) {
  let assert Continue(user_state, selector) = state.next
  let conn = Connection(transport: state.transport, socket: state.socket)

  let call = exception.rescue(fn() { state.handler(conn, user_state, message) })
  case call {
    Ok(Continue(user_state, new_selector)) -> {
      let selector = option.or(new_selector, selector)
      websocks.Continue(
        ResolveState(..state, next: Continue(user_state, selector)),
      )
    }
    Ok(NormalStop) -> websocks.Stop(ResolveState(..state, next: NormalStop))
    Ok(AbnormalStop(reason)) ->
      websocks.Stop(ResolveState(..state, next: AbnormalStop(reason)))
    Error(_) -> todo
  }
}
