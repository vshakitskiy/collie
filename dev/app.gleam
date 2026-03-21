import gleam/erlang/process
import gleam/http/request
import websocket

pub fn main() {
  let assert Ok(request) = request.to("http://0.0.0.0:8080")

  echo websocket.new(request, Nil)
    |> websocket.start

  process.sleep_forever()
}
