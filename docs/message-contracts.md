# Message payload contracts

Wire payload records live in `org.prats.definitions.records`. Producers serialize
records with `MessageParser.toPayload`; consumers decode them with `fromPayload`.
The desktop `LoginController` demonstrates the existing login contract.

| Operation | Payload |
| --- | --- |
| Command request | `CommandRequest(body)` |
| Command response | `CommandResponse(body)` |
| TCP handshake | `HandshakePayload(body)` |
| EC lifecycle request | `ControlRequest(ecName)` |
| EC lifecycle success or progress | `ControlResponse(body, status)` |
| Database query result | `QueryResponse(ok, columns, rows, rowCount, truncated, body)` |
| Database update result | `UpdateResponse(ok, affectedRows, body)` |
| Authentication | `LoginRequest` / `LoginResponse` |
| Health reporting | Existing health report records |

`Message.status` describes completion (`SUCCESS`, `IN_PROGRESS`, or `FAILED`).
The lifecycle payload's `status` remains descriptive progress text for wire
compatibility. Database rows retain maps because their columns are dynamic.

Failures use `Message.error.errorCode` and `Message.error.errorMessage`, with a
null payload, `FAILED` status, and a terminal response. Use
`MessageParser.createErrorResponse` for correlated failures, or `attachError`
when retaining an existing envelope. Consumers check `hasError()` before decoding
success payloads. Forwarders preserve errors when converting lifecycle replies.

Clients that previously read `payload.errorCode`, `payload.errorDescription`, or
an error in `payload.body` must read the envelope error instead. Successful wire
field names are preserved. Plain TCP command replies render envelope error text
when the response retains the COMMAND operation.
