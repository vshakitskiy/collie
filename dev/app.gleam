import ewe
import gleam/erlang/process
import gleam/http/request
import gleam/http/response
import gleam/int
import gleam/io
import gleam/list
import gleam/string
import websocket

pub type Message {
  Tick
}

pub fn main() {
  let assert Ok(_started) =
    ewe.new(server)
    |> ewe.bind("0.0.0.0")
    |> ewe.listening(port: 8080)
    |> ewe.start

  let assert Ok(request) = request.to("http://0.0.0.0:8080")
  let name = process.new_name("joe")

  let assert Ok(_start) =
    websocket.new_with_initialiser(request, fn(self) {
      process.send_after(self, 1000, websocket.to_user_message(Tick))

      websocket.initialised(self)
      |> Ok
    })
    |> websocket.named(name)
    |> websocket.on_message(client)
    |> websocket.start

  // let subject = process.named_subject(name)
  // websocket.to_user_message("Anus")
  // |> process.send(subject, _)

  process.sleep_forever()
}

fn client(
  _connection: websocket.Connection,
  self: process.Subject(websocket.WebsocketMessage(Message)),
  message: websocket.Message(Message),
) -> websocket.Next(
  process.Subject(websocket.WebsocketMessage(Message)),
  Message,
) {
  case message {
    websocket.User(Tick) -> {
      io.println("tick.")
      process.send_after(self, 1000, websocket.to_user_message(Tick))

      websocket.continue(self)
    }
    _ -> websocket.continue(self)
  }
}

fn server(
  request: request.Request(ewe.Connection),
) -> response.Response(ewe.ResponseBody) {
  ewe.upgrade_websocket(
    request,
    on_init: fn(conn, selector) {
      let _ = ewe.send_text_frame(conn, "Ping")

      #(Nil, selector)
    },
    handler: fn(conn, state, msg) {
      case msg {
        ewe.Text("Pong") -> {
          let _ = ewe.send_text_frame(conn, "Ping")
          ewe.websocket_continue(state)
        }
        ewe.Text(text_frame) -> {
          let _ = ewe.send_text_frame(conn, text_frame)
          ewe.websocket_continue(state)
        }
        ewe.Binary(binary_frame) -> {
          let _ = ewe.send_binary_frame(conn, binary_frame)
          ewe.websocket_continue(state)
        }
        _ -> ewe.websocket_continue(state)
      }
    },
    on_close: fn(_conn, _state) { Nil },
  )
}
