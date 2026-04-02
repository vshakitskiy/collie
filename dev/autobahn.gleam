import collie
import gleam/erlang/process
import gleam/function
import gleam/http/request
import gleam/int
import gleam/io
import gleam/list
import gleam/otp/actor
import gleam/result
import stratus

const base = "http://127.0.0.1:9001"

type Adapter {
  Adapter(agent: String, runner: fn(Int) -> Result(Nil, String))
}

const clients = [
  Adapter(agent: "collie", runner: collie_adapter),
  Adapter(agent: "stratus", runner: stratus_adapter),
]

pub fn main() {
  process.trap_exits(True)

  let case_count = get_case_count()

  { "Running " <> int.to_string(case_count) <> " autobahn test cases\n" }
  |> io.println

  list.each(clients, handle_adapters(_, case_count))

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

fn handle_adapters(client: Adapter, case_count: Int) -> Nil {
  io.println("--- Testing: " <> client.agent <> " ---")

  int.range(from: 1, to: case_count + 1, with: Nil, run: fn(_nil, case_number) {
    io.print(
      "Case "
      <> int.to_string(case_number)
      <> "/"
      <> int.to_string(case_count)
      <> "... ",
    )

    case client.runner(case_number) {
      Ok(_) -> io.println("✓")
      Error(reason) -> io.println("✗ (" <> reason <> ")")
    }

    Nil
  })

  update_reports(client.agent)
  io.println("Reports updated for " <> client.agent <> "\n")
}

fn collie_adapter(case_number: Int) -> Result(Nil, String) {
  let path = "/runCase?case=" <> int.to_string(case_number) <> "&agent=collie"
  let assert Ok(req) = request.to(base <> path)

  let started =
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

  case started {
    Ok(actor.Started(pid:, ..)) -> {
      let monitor = process.monitor(pid)
      let selector =
        process.new_selector()
        |> process.select_specific_monitor(monitor, fn(_down) { Nil })

      process.selector_receive(selector, 120_000)
      |> result.replace_error("timeout")
    }
    Error(_) -> Error("failed to start")
  }
}

fn stratus_adapter(case_number: Int) -> Result(Nil, String) {
  let path = "/runCase?case=" <> int.to_string(case_number) <> "&agent=stratus"
  let assert Ok(req) = request.to(base <> path)

  let started =
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

  case started {
    Ok(actor.Started(pid:, ..)) -> {
      let monitor = process.monitor(pid)
      let selector =
        process.new_selector()
        |> process.select_specific_monitor(monitor, fn(_down) { Nil })

      process.selector_receive(selector, 120_000)
      |> result.replace_error("timeout")
    }
    Error(_) -> Error("failed to start")
  }
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
