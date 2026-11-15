# Platform commands

Run these commands through the platform's application TCP connection (port 15000).

```text
mng help
mng start ec ec01
mng stop ec ec01
mng restart ec ec01
mng health report ec01
mng health report platform
mng status
mng restart services all
mng shutdown services all
mng kill
```

All management commands require the `mng` prefix. Keywords are case insensitive;
container IDs retain their configured case. Unprefixed commands and the old
`get health report` / `health ec` syntax are rejected.

`mng health report <ec-id>` displays the latest heartbeat received by the platform, its age,
the EC's reported overall status, and service health details. It does not send an
on-demand probe. ECs send heartbeats every 15 seconds; after 45 seconds without a
heartbeat the report is marked STALE and current health is unknown. Missing
heartbeats are reported as UNKNOWN, and unconfigured EC IDs are rejected.

After a platform restart, allow an EC to reconnect and send a heartbeat before
expecting a report. `mng health report platform` shows the platform's own internal
health. On a direct EC connection, `mng health report <that-ec-id>` shows its local
report. `mng status`, service commands, and `mng kill` act on the connected JVM.

Overall health combines independent service statuses. Failure counters from
different services are never compared as if they were one service's history.
Only an explicit failed-recovery status is reported as `RECOVERY_FAILED`.

Platform management now requires an account session. Use `mng register <username> <password>`, then `mng login <username> <password>` on the same connection. `mng help` remains available before login. See [desktop and operations](desktop-and-operations.md) for the Swing client, diagnostics, capacity limits and watchdog recovery.
