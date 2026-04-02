import collie/internal/socket
import gleam/bit_array
import gleam/bytes_tree
import gleam/http/request
import gleam/http/response
import gleam/int
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import websocks

pub fn construct_upgrade(request: request.Request(body)) -> bytes_tree.BytesTree {
  let headers =
    list.fold(request.headers, "", fn(acc, pair) {
      let #(key, value) = pair

      case key {
        "host"
        | "upgrade"
        | "connection"
        | "sec-websocket-key"
        | "sec-websocket-version"
        | "sec-websocket-extensions" -> acc
        key -> acc <> key <> ": " <> value <> "\r\n"
      }
    })
    <> "\r\n"

  let port =
    option.map(request.port, fn(port) { ":" <> int.to_string(port) })
    |> option.unwrap("")

  let path = case request.path {
    "" -> "/"
    path -> path
  }

  let query = case option.then(request.query, validate_query) {
    option.None | option.Some("") -> ""
    option.Some(query) -> "?" <> query
  }

  let extensions_header =
    "sec-websocket-extensions: permessage-deflate; client_max_window_bits\r\n"

  bytes_tree.new()
  |> bytes_tree.append_string("GET " <> path <> query <> " HTTP/1.1\r\n")
  |> bytes_tree.append_string("host: " <> request.host <> port <> "\r\n")
  |> bytes_tree.append_string("connection: upgrade\r\n")
  |> bytes_tree.append_string("upgrade: websocket\r\n")
  |> bytes_tree.append_string(
    "sec-websocket-key: " <> websocks.websocket_key() <> "\r\n",
  )
  |> bytes_tree.append_string("sec-websocket-version: 13\r\n")
  |> bytes_tree.append_string(extensions_header)
  |> bytes_tree.append_string(headers)
}

@external(erlang, "collie_ffi", "validate_query")
fn validate_query(query: String) -> option.Option(String)

pub type DecodeError {
  SocketFailed(socket.SocketReason)
  MalformedRequest
}

pub type DecoderError {
  More(length: Int)
  HttpError(reason: String)
}

pub type PacketType {
  HttphBin
  HttpBin
}

pub type Packet {
  HttpResponse(version: #(Int, Int), status: Int, text: String)
  HttpHeader(idx: Int, field: BitArray, value: BitArray)
  HttpEoh
}

@external(erlang, "collie_ffi", "decode_packet")
fn decode_packet(
  kind: PacketType,
  bin: BitArray,
) -> Result(#(Packet, BitArray), DecoderError)

pub fn decode_response(
  transport: socket.Transport,
  socket: socket.Socket,
  timeout: Int,
) -> Result(#(response.Response(BitArray), BitArray), DecodeError) {
  do_decode_response(transport, socket, timeout, 0, <<>>)
}

fn do_decode_response(
  transport: socket.Transport,
  socket: socket.Socket,
  timeout: Int,
  length: Int,
  buffer: BitArray,
) {
  case socket.receive_timeout(transport, socket, length, timeout) {
    Ok(data) ->
      case decode_packet(HttpBin, data) {
        Ok(#(HttpResponse(_version, status, _text), remaining)) -> {
          use #(headers, remaining) <- result.try(
            decode_headers(transport, socket, timeout, remaining, []),
          )

          let content_length =
            list.key_find(headers, "content-length")
            |> result.try(int.parse)
            |> result.unwrap(0)

          use #(body, remaining) <- result.try(decode_body(
            transport,
            socket,
            timeout,
            content_length,
            remaining,
          ))

          Ok(#(response.Response(status:, headers:, body:), remaining))
        }
        Error(More(length)) ->
          do_decode_response(transport, socket, timeout, length, <<
            buffer:bits,
            data:bits,
          >>)
        Ok(_) | Error(_) -> Error(MalformedRequest)
      }
    Error(reason) -> Error(SocketFailed(reason))
  }
}

fn decode_headers(
  transport: socket.Transport,
  socket: socket.Socket,
  timeout: Int,
  buffer: BitArray,
  headers: List(#(String, String)),
) -> Result(#(List(#(String, String)), BitArray), DecodeError) {
  case decode_packet(HttphBin, buffer) {
    Ok(#(HttpEoh, remaining)) -> Ok(#(list.reverse(headers), remaining))
    Ok(#(HttpHeader(idx:, field:, value:), remaining)) -> {
      use field <- result.try(case formatted_field_by_idx(idx) {
        Ok(field) -> Ok(field)
        Error(Nil) -> {
          bit_array.to_string(field)
          |> result.map(string.lowercase)
          |> result.replace_error(MalformedRequest)
        }
      })

      use value <- result.try(
        validate_field_value(value) |> result.replace_error(MalformedRequest),
      )

      decode_headers(transport, socket, timeout, remaining, [
        #(field, value),
        ..headers
      ])
    }
    Error(More(length)) -> {
      case socket.receive_timeout(transport, socket, length, timeout) {
        Ok(data) ->
          decode_headers(
            transport,
            socket,
            timeout,
            <<buffer:bits, data:bits>>,
            headers,
          )
        Error(reason) -> Error(SocketFailed(reason))
      }
    }
    Ok(_) | Error(_) -> Error(MalformedRequest)
  }
}

fn decode_body(
  transport: socket.Transport,
  socket: socket.Socket,
  timeout: Int,
  content_length: Int,
  buffer: BitArray,
) -> Result(#(BitArray, BitArray), DecodeError) {
  case buffer {
    <<data:bytes-size(content_length), remaining:bits>> ->
      Ok(#(data, remaining))
    _ ->
      case socket.receive_timeout(transport, socket, 0, timeout) {
        Ok(data) ->
          decode_body(transport, socket, timeout, content_length, <<
            buffer:bits,
            data:bits,
          >>)
        Error(reason) -> Error(SocketFailed(reason))
      }
  }
}

@external(erlang, "collie_ffi", "validate_field_value")
fn validate_field_value(value: BitArray) -> Result(String, Nil)

fn formatted_field_by_idx(idx: Int) -> Result(String, Nil) {
  case idx {
    0 -> Error(Nil)
    1 -> Ok("cache-control")
    2 -> Ok("connection")
    3 -> Ok("date")
    4 -> Ok("pragma")
    5 -> Ok("transfer-encoding")
    6 -> Ok("upgrade")
    7 -> Ok("via")
    8 -> Ok("accept")
    9 -> Ok("accept-charset")
    10 -> Ok("accept-encoding")
    11 -> Ok("accept-language")
    12 -> Ok("authorization")
    13 -> Ok("from")
    14 -> Ok("host")
    15 -> Ok("if-modified-since")
    16 -> Ok("if-match")
    17 -> Ok("if-none-match")
    18 -> Ok("if-range")
    19 -> Ok("if-unmodified-since")
    20 -> Ok("max-forwards")
    21 -> Ok("proxy-authorization")
    22 -> Ok("range")
    23 -> Ok("referer")
    24 -> Ok("user-agent")
    25 -> Ok("age")
    26 -> Ok("location")
    27 -> Ok("proxy-authenticate")
    28 -> Ok("public")
    29 -> Ok("retry-after")
    30 -> Ok("server")
    31 -> Ok("vary")
    32 -> Ok("warning")
    33 -> Ok("www-authenticate")
    34 -> Ok("allow")
    35 -> Ok("content-base")
    36 -> Ok("content-encoding")
    37 -> Ok("content-language")
    38 -> Ok("content-length")
    39 -> Ok("content-location")
    40 -> Ok("content-md5")
    41 -> Ok("content-range")
    42 -> Ok("content-type")
    43 -> Ok("etag")
    44 -> Ok("expires")
    45 -> Ok("last-modified")
    46 -> Ok("accept-ranges")
    47 -> Ok("set-cookie")
    48 -> Ok("set-cookie2")
    49 -> Ok("x-forwarded-for")
    50 -> Ok("cookie")
    51 -> Ok("keep-alive")
    52 -> Ok("proxy-connection")
    _ -> Error(Nil)
  }
}
