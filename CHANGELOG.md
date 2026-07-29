# v3.0.0 - 29.07.2026

- Split `CloseReason` into `CloseCode` and `CloseReason`, carrying a `String`
  description instead of a `BitArray` payload
- Remove `TLSHandshake` close code, as 1015 must never be sent
- Change `CustomCloseCode` to `ApplicationCode`
- Add `close_code_to_string`
- Add `with_limits` to cap the frame and the message size, defaulting to
  `default_limits`
- Add `MessageTooBig` closure for frames and messages over the limits

# v2.0.1 - 11.06.2026

- Fix a bug with `supervised` using `supervisor` function instead of a `worker`

# v2.0.0 - 08.06.2026

- Add `returning` to return custom data on initialisation
- Change documentation line for `Message` type
- Add `factory` for dynamically starting WebSocket connections
- Change `send_close_frame` to match other `send_` functions return value

# v1.0.0 - 02.04.2026

- Initial release