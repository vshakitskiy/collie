import collie
import gleam/erlang/process
import gleam/function
import gleam/http/request
import gleam/int
import gleam/io
import gleam/list
import gleam/otp/actor
import gleam/result
import gleam/string
import stratus

const base = "http://127.0.0.1:9001"

type Client {
  Client(
    agent: String,
    runner: fn(request.Request(String)) -> Result(Nil, String),
  )
}

const clients = [
  Client(agent: "collie@1", runner: collie_adapter),
  Client(agent: "stratus@2", runner: stratus_adapter),
]

pub fn main() {
  process.trap_exits(True)

  let total_cases = get_case_count()

  { "Running " <> int.to_string(total_cases) <> " autobahn test cases\n" }
  |> io.println

  list.each(clients, handle_adapters(_, total_cases))

  io.println("Done! Check autobahn/index.html for results.")
}

fn get_case_count() -> Int {
  let assert Ok(req) = request.to(base <> "/getCaseCount")

  let result = process.new_subject()
  let assert Ok(actor.Started(pid:, ..)) =
    collie.new(req, Nil)
    |> collie.on_message(fn(_conn, state, message) {
      case message {
        collie.Text(count) -> {
          process.send(result, count)
          collie.continue(state)
        }
        _ -> collie.continue(state)
      }
    })
    |> collie.start

  let monitor = process.monitor(pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, function.identity)
  process.selector_receive_forever(selector)

  let assert Ok(count) =
    process.receive_forever(result)
    |> int.parse
    as "received invalid payload from /getCaseCount"

  count
}

fn handle_adapters(client: Client, total_cases: Int) -> Nil {
  io.println("--- Testing: " <> client.agent <> " ---")

  int.range(from: 1, to: total_cases + 1, with: Nil, run: fn(_nil, case_number) {
    io.print(
      "Case "
      <> int.to_string(case_number)
      <> "/"
      <> int.to_string(total_cases)
      <> "... ",
    )

    let path =
      "/runCase?case="
      <> int.to_string(case_number)
      <> "&agent="
      <> client.agent
    let assert Ok(req) = request.to(base <> path)

    case client.runner(req) {
      Ok(_) -> io.println("✓")
      Error(reason) -> io.println("✗ (" <> reason <> ")")
    }

    Nil
  })

  update_reports(client.agent)
  io.println("Reports updated for " <> client.agent <> "\n")
}

fn handle_started(started: Result(actor.Started(any), actor.StartError)) {
  case started {
    Ok(actor.Started(pid:, ..)) -> {
      let monitor = process.monitor(pid)
      let selector =
        process.new_selector()
        |> process.select_specific_monitor(monitor, fn(_down) { Nil })

      process.selector_receive(selector, 120_000)
      |> result.replace_error("timeout")
    }
    Error(actor.InitFailed(reason)) -> Error(reason)
    Error(actor.InitTimeout) -> Error("actor init timeout")
    Error(actor.InitExited(reason)) ->
      Error("actor init exited: " <> string.inspect(reason))
  }
}

fn collie_adapter(req: request.Request(String)) -> Result(Nil, String) {
  collie.new(req, Nil)
  |> collie.on_message(fn(conn, state, message) {
    case message {
      collie.Text(text) -> {
        let _ = collie.send_text_frame(conn, text)
        collie.continue(state)
      }
      collie.Binary(data) -> {
        let _ = collie.send_binary_frame(conn, data)
        collie.continue(state)
      }
      collie.User(_) -> collie.continue(state)
    }
  })
  |> collie.start
  |> handle_started
}

fn stratus_adapter(req: request.Request(String)) -> Result(Nil, String) {
  stratus.new(req, Nil)
  |> stratus.on_message(fn(state, message, conn) {
    case message {
      stratus.Text(text) -> {
        let _ = stratus.send_text_message(conn, text)
        stratus.continue(state)
      }
      stratus.Binary(data) -> {
        let _ = stratus.send_binary_message(conn, data)
        stratus.continue(Nil)
      }
      _ -> stratus.continue(Nil)
    }
  })
  |> stratus.start
  |> result.map_error(fn(error) {
    case error {
      stratus.ActorFailed(error) -> error
      stratus.HandshakeFailed(error) ->
        actor.InitFailed("handshake failed: " <> string.inspect(error))
      stratus.FailedToTransferSocket(error) ->
        actor.InitFailed("socket error: " <> string.inspect(error))
    }
  })
  |> handle_started
}

fn update_reports(agent: String) -> Nil {
  let assert Ok(req) = request.to(base <> "/updateReports?agent=" <> agent)

  let assert Ok(actor.Started(pid:, ..)) = collie.new(req, Nil) |> collie.start

  let monitor = process.monitor(pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_down) { Nil })
  process.selector_receive_forever(selector)
}
