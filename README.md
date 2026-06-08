# 🐕 Collie

A WebSocket client for Gleam.

[![Package Version](https://img.shields.io/hexpm/v/collie)](https://hex.pm/packages/collie)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/collie/)

## Installation

```sh
gleam add collie@2
```

## Autobahn

Collie passes all Autobahn WebSocket test suite cases, ensuring full compliance with the WebSocket protocol specification. You can view detailed test results and compare them with other Gleam WebSocket implementations like Stratus at:

**[https://vshakitskiy.github.io/collie](https://vshakitskiy.github.io/collie)**

## Usage

### Basic Client

This example connects to an echo server, sends random 4 bytes as binary data, and waits to receive them back before closing the connection:

```gleam
import collie
import gleam/crypto
import gleam/erlang/process
import gleam/http/request
import gleam/io
import gleam/otp/actor
import logging

pub fn main() {
  logging.configure()
  logging.set_level(logging.Info)

  // Create a WebSocket request
  let assert Ok(req) = request.to("https://echo.websocket.org")

  // Build and start the client
  let client =
    collie.new(req, crypto.strong_random_bytes(4))
    |> collie.on_message(handle_message)
    |> collie.start()

  case client {
    Ok(actor.Started(pid:, ..)) -> {
      // Keep the main process alive while the WebSocket client is alive
      let monitor = process.monitor(pid)
      let selector =
        process.new_selector()
        |> process.select_specific_monitor(monitor, fn(_down) { Nil })
      process.selector_receive_forever(selector)
    }
    Error(_) -> io.println_error("Server is busy, try again later")
  }
}

fn handle_message(
  conn: collie.Connection,
  key: BitArray,
  message: collie.Message(BitArray),
) -> collie.Next(BitArray, BitArray) {
  case message {
    collie.Text(text) -> {
      io.println(text)

      case collie.send_binary_frame(conn, key) {
        Ok(Nil) -> collie.continue(key)
        Error(reason) -> {
          let reason = collie.socket_reason_to_string(reason)

          io.println_error(reason)
          collie.stop_abnormal(reason)
        }
      }
    }

    collie.Binary(bin) if bin == key -> collie.stop()

    _ -> collie.stop_abnormal("Got unexpected binary payload")
  }
}
```

### Named Clients and Sending Messages

This example shows how to create a named WebSocket client that can receive messages from other processes. By using `collie.named`, you can register the client with a name and then send it custom messages using `collie.to_user_message` from anywhere in your application:

```gleam
import collie
import gleam/erlang/process
import gleam/http/request
import gleam/io
import gleam/otp/actor
import logging

pub type Message {
  Send(String)
  Shutdown
}

pub fn main() {
  logging.configure()
  logging.set_level(logging.Info)

  let assert Ok(req) = request.to("https://echo.websocket.org")

  // Create a name for the client
  let name = process.new_name("collie")

  // Start a named client
  let client =
    collie.new(req, Nil)
    |> collie.named(name)
    |> collie.on_message(handle_message)
    |> collie.start()

  case client {
    Ok(actor.Started(pid:, ..)) -> {
      let monitor = process.monitor(pid)
      let selector =
        process.new_selector()
        |> process.select_specific_monitor(monitor, fn(_down) { Nil })

      // Send messages to the client from other processes
      let subject = process.named_subject(name)

      process.send(subject, collie.to_user_message(Send("Hello!")))
      process.send(subject, collie.to_user_message(Shutdown))

      process.selector_receive_forever(selector)
    }
    Error(_) -> io.println_error("Server is busy, try again later")
  }
}

fn handle_message(conn, state, message) {
  case message {
    collie.Text(text) -> {
      io.println(text)
      collie.continue(state)
    }

    collie.User(Send(text)) -> {
      io.println("Sending `" <> text <> "`")
      // Send the message through WebSocket
      let _ = collie.send_text_frame(conn, text)
      collie.continue(state)
    }

    collie.User(Shutdown) ->
      collie.send_close_frame(conn, collie.NormalClosure(<<>>))

    collie.Binary(_) -> collie.continue(state)
  }
}
```

### Using an Initialiser and Custom Selectors

This example demonstrates usage with `collie.new_with_initialiser` to perform setup before the client starts handling messages. It creates a custom timer process and uses a selector to receive timer messages, sending a ping to the server every second:

```gleam
import collie
import gleam/erlang/process
import gleam/http/request
import gleam/io

pub type Message {
  Tick
}

pub type State {
  State(timer: process.Subject(Message))
}

pub fn main() {
  let assert Ok(req) = request.to("https://echo.websocket.org")

  let assert Ok(_client) =
    collie.new_with_initialiser(req, fn(self) {
      // Spawn a timer process
      let timer = process.new_subject()
      process.spawn(fn() { timer_loop(timer) })

      // Create a selector for timer messages
      let selector =
        process.new_selector()
        |> process.select(timer)

      // Return state with the selector and return self subject
      let state = State(timer:)
      collie.initialised(state)
      |> collie.selecting(selector)
      |> collie.returning(self)
      |> Ok
    })
    |> collie.on_message(handle_message)
    |> collie.start

  process.sleep_forever()
}

fn timer_loop(subject) {
  process.sleep(1000)
  process.send(subject, Tick)
  timer_loop(subject)
}

fn handle_message(conn, state, message) {
  case message {
    collie.Text(text) -> {
      io.println(text)
      collie.continue(state)
    }

    collie.User(Tick) -> {
      io.println("tick")
      let _ = collie.send_ping(conn, <<"ping">>)
      collie.continue(state)
    }

    collie.Binary(_) -> collie.continue(state)
  }
}
```

## API Documentation

For full API documentation, see [hexdocs.pm/collie](https://hexdocs.pm/collie/).
