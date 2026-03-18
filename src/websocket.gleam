import gleam/erlang/process
import gleam/http/request
import gleam/option
import glisten/socket
import glisten/transport

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
  Connection(transport: transport.Transport, socket: socket.Socket)
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
