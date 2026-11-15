# Desktop console and platform operations

Build both artifacts with JDK 25 LTS:

```sh
./gradlew test shadowJar desktopJar integrationTest
java -jar build/libs/mediation-desktop.jar
```

`mediation-desktop.jar` is a standalone Swing application. It packages the shared
engine classes and dependencies so the desktop can reuse `TCPService` in outbound
client mode. The desktop starts only its transport; it opens no listening socket
and initializes no database or container services.
Connect to the platform's application endpoint, normally `127.0.0.1:15000`.
The client never connects to watchdogs or ECs. Socket reads/writes run outside
Swing's event-dispatch thread, and responses are correlated by request UUID.

The platform request path is TCPService → TCPApplicationProcessor → MessageService
→ RequestProcessor → MessageProcessor → GUIForwarder → GUIService → GUIHandler.
GUIService manages admission, execution, lifecycle and health. GUIHandler owns
GUI actions, authorization requests and response formatting.

GUIHandler sends account and authorization requests through MessageService →
AuthenticationForwarder → AuthenticationService → AuthenticationHandler. Its
AccountManager builds the existing structured CREATE/SEARCH database messages;
these return through MessageService → DatabaseForwarder → DatabaseService →
DatabaseRequestHandler. JDBC execution and account schema initialization belong
to the database handler. GUI code contains no table names or query fields.
Each nested request has a separate correlation ID and FutureReplyChannel. GUI
and authentication continuations compose futures without waiting on workers.
Responses preserve the original GUI correlation and return through MessageService.

Diagnostics also enter MessageService. DiagnosticsForwarder chooses the local
DiagnosticsService or an existing control-channel peer. PlatformDiagnostics
aggregates the replies asynchronously; GUIHandler does not call TCP directly.

The desktop PlatformClient implements a protocol processor for the shared
TCPService. Connection establishment and shutdown run on its connector executor;
transport workers perform framing, reads and writes. The desktop retains its
32-request limit and two-million-character response limit.

## Accounts over nc

```sh
nc 127.0.0.1 15000
```

```text
mng help
mng register operator <password-of-10-to-128-characters>
mng login operator <password>
mng whoami
mng health report platform
mng health report ec01
mng logout
```

Accounts are stored in the platform's `users` table. The password column is
widened in place to 255 characters; new passwords are salted PBKDF2-HMAC-SHA256
hashes with 600,000 iterations. Existing plaintext passwords are never accepted
by the new login implementation. Usernames are case-sensitive, 3–30 characters.
Sessions expire after 30 minutes and are removed when a TCP connection closes.
Five failed login attempts throttle that username for one minute. Passwords,
session tokens and SQL parameter values are excluded from application logs.

`mng help`, registration and login work without a session. Platform management
and database requests require login. Currently every registered active account
is an operator; role-based administration is not implemented. Account tables
cannot be accessed through generic database messages.

The existing TCP protocol is not encrypted. Keep local listeners bound to
loopback; remote access needs a trusted tunnel or transport-level TLS.

## Health monitoring

The monitoring tab includes the platform, configured watchdogs and all configured
ECs, including disconnected JVMs. It refreshes every 15 seconds and distinguishes
platform connectivity from health. CPU, memory and heap values are live readings;
unavailable metrics display N/A. JVMs on one machine naturally share host-load
measurements, while JVM CPU and heap figures remain process-specific.

Double-click a JVM to inspect memory pools, open TCP connections, JVM arguments,
host load, collector statistics, registry capacity and service metrics. Run GC
requires confirmation and sends an explicit request to that JVM; a JVM may ignore
the request. Configured JVMs must load the updated engine JAR to support diagnostics.

## Capacity and long-running operation

Defaults are conservative bounds, not a claim of proven multiyear uptime. Tune
them with workload measurements and soak tests. Each JVM accepts these `-D` properties:

| Property | Default | Bounds |
| --- | ---: | --- |
| `mediation.admission.max-tasks` | 1024 | Queued plus running tasks per worker pool |
| `mediation.correlation.limit` | 4096 | Application in-flight request entries |
| `mediation.correlation.control-limit` | 1024 | Control request entries |
| `mediation.retry.limit` | 1024 | Failed-response retry entries |
| `mediation.retry.max-bytes` | 16777216 | Estimated retained retry payload bytes |
| `mediation.tcp.max-connections` | 256 | Accepted connections per TCP service |
| `mediation.tcp.max-frame-chars` | 65536 | Incoming line length |
| `mediation.tcp.max-outbound-bytes` | 16777216 | Estimated retained outbound payload bytes per TCP service |
| `mediation.tcp.write-timeout-seconds` | 10 | Slow-client write timeout; timed-out sockets close |
| `mediation.tcp.read-idle-seconds` | 300 | Accepted application connection idle timeout |

GUIService additionally caps concurrent GUI/account requests at 64. Request and
response/maintenance workers have separate admission pools so request saturation
does not consume response capacity. Overload returns a terminal rejection;
correlations are removed on final response, expiry or connection close. The
timeout-refresh operation cannot resurrect a removed correlation. Application
correlations expire after 60 seconds, with cleanup every five seconds. Control
correlations expire after two minutes. Retry age and attempt limits still apply.
Map capacity remains bounded after heavy traffic, even if a ConcurrentHashMap
retains its expanded internal table. Queue/result/byte limits, rather than forced
GC or frequent restarts, provide the memory bound. JDBC result sets are limited
to 1,000 rows and text cells to 4,096 characters; truncation is explicit.

Request admission, rejection counts, registry occupancy and retained payload
estimates are visible in diagnostics. Maintained logs rotate at their configured
file-size and retention limits. More representative sustained-load and failure
testing is still needed before claiming production longevity.

## Watchdog recovery and identity files

Each updated EC atomically writes `run/<ec-id>.process.properties` beneath the
installation directory. It records PID, process start instant, EC ID and home.
The watchdog verifies process liveness, start time and EC JVM arguments before
reattaching. It can also discover older ECs without identity files and persist
their identity. Ambiguous duplicate EC identities are rejected, never guessed.
Stale files may remain after termination; a file alone is never proof of liveness.
The registry uses ProcessHandle so adopted ECs can subsequently be stopped or
restarted even though they are no longer child processes of the new watchdog.

On macOS the watchdog LaunchAgent must include:

```xml
<key>AbandonProcessGroup</key>
<true/>
```

Without this setting launchd kills the watchdog's remaining process group when
the watchdog exits. The existing job must be reloaded once to activate a changed
definition. A job already loaded with the old setting can still stop its ECs
during that one-time migration; plan an EC restart for that migration. Subsequent
watchdog restarts preserve ECs and recover their registry entries.

`python3 ops/prepare_watchdog_agent.py` prepares the corrected definition under
`build/launchd/` without modifying or reloading the running job. Install and reload
that prepared definition only during the one-time migration window.

Tests cover bounded/concurrent admissions, expiry and disconnect cleanup,
password storage, prepared CRUD and errors, oversized/stalled TCP writes, and an
isolated platform/watchdog/EC test that verifies diagnostics, GC, login and the
same EC PID surviving watchdog termination and reattachment. Swing components
also have a headless initialization check; visual desktop QA is separate.

## Local deployment

`./deploy-local.sh` selects the installed Java 25 JDK, runs unit tests, builds
both JARs, and updates `~/Desktop/MediationEngine/mediation-engine.jar` and
`mediation-desktop.jar`. It also rebuilds `/Applications/Mediation Engine.app`
with a bundled Java 25 runtime and the latest GUI JAR. Reopen the app after
deployment. Override destinations with `MEDIATION_INSTALL_DIR` and
`MEDIATION_APP_DIR`. Service restarts remain a separate operation.
