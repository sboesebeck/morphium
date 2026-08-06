# Wire Proxy — Fault Injection & Wire-Level Monitoring

Morphium's test sources ship a small, reusable TCP proxy for the MongoDB wire protocol:
`WireProxy`. It sits between any wire-protocol client (Morphium, the official drivers,
`mongosh`) and any wire-protocol backend (MongoDB **or** PoppyDB) and gives you three things
that are otherwise hard to get in a test:

1. **Fault injection** — freeze, reset, or cleanly close connections at runtime, without
   touching the server process. This is how `DriverFailoverProxyTest` reproduces failovers
   (clean stepdown, hard kill, frozen socket) in the normal CI matrix, with no `kill -9` and
   no hand-built infrastructure.
2. **Wire-level monitoring** — observe every server response frame as a parsed
   `WireProtocolMessage`, e.g. to log exactly what a server sends during a test, or to assert
   on protocol-level behavior your API-level test can't see.
3. **Response manipulation** — rewrite server replies before the client sees them: change
   topology information (that is how the failover suite works), mutate documents, or inject
   deliberately malformed replies to test client robustness.

It lives in `morphium-core`'s **test** sources — package
`de.caluga.test.morphium.testutil.proxy` — so it is available to every test in this repository.
It is not (yet) published as a standalone artifact; if you want to use it outside this repo,
copy the package (it has no dependencies beyond `WireProtocolMessage`) or open an issue.

## Quick start

```java
// Proxy in front of any wire-protocol server (MongoDB or PoppyDB)
WireProxy proxy = new WireProxy("localhost", 27017);
proxy.addObserver(new Slf4jFrameObserver());   // log every server response frame
proxy.start();

// Point the client at the proxy, not the server
MorphiumConfig cfg = new MorphiumConfig();
cfg.clusterSettings().setHostSeed("localhost:" + proxy.getListenPort());

// ... run the test ...

proxy.stop();   // severs all connections, joins every pump thread before returning
```

`WireProxy` implements `AutoCloseable`, so try-with-resources works too. `stop()` guarantees
that every internal pump thread has exited before it returns — no thread leakage across tests.

## Fault injection

Faults are switched at runtime via `proxy.setFaultMode(...)`:

| `FaultMode` | Existing connections | New connection attempts | Simulates |
|---|---|---|---|
| `passthrough` | forwarded normally | accepted | healthy network (default) |
| `freeze` | left open, never answered, never closed | accepted, then silence | frozen process (`kill -STOP`), network partition, paused VM — the failure a client cannot distinguish from a slow server |
| `reset` | severed with a hard RST | refused | dead process (`kill -9`), closed port |
| `close` | severed with a clean FIN | refused | this route to the node is gone (the node itself may live on — e.g. after a clean stepdown) |

Semantics worth knowing before you build a test on them:

- **`freeze` is one-way per connection.** A connection accepted (or already open) during a
  freeze stays parked until `stop()` — switching back to `passthrough` only affects
  connections opened *after* the switch. That mirrors reality: a socket to a frozen process
  does not spring back to life; the client has to time out and reconnect.
- **`close` and `reset` both refuse new connections** — they differ only in how existing ones
  are severed (FIN vs. RST). "Nothing reachable behind this proxy right now" is the contract.
- **`stop()` always severs with RST**, regardless of the configured fault mode, so a client
  blocked in a read sees a definite error rather than a clean EOF that would look like an
  orderly shutdown.

## Replica sets: the address-rewriting trick

A proxy per node is not enough for a replica set: drivers do server discovery via `hello`, and
the server answers with the **real** addresses (`hosts`, `primary`, `me`). After the first
`hello`, a driver would connect straight past your proxies.

`AddressRewriter` fixes that. It is a `ResponseRewriter` that detects `hello`/`isMaster`-shaped
replies structurally (`setName` + `hosts` present) and maps every real address to its proxy
address:

```java
// one proxy per RS member
Map<String, String> backendToProxy = Map.of(
    "mongo1:27017", "localhost:" + p1.getListenPort(),
    "mongo2:27017", "localhost:" + p2.getListenPort(),
    "mongo3:27017", "localhost:" + p3.getListenPort());

AddressRewriter rewriter = new AddressRewriter(backendToProxy);
p1.setRewriter(rewriter);
p2.setRewriter(rewriter);
p3.setRewriter(rewriter);
```

The driver now lives in a consistent alternate topology consisting entirely of proxies — every
connection it ever opens, including discovery-triggered ones, flows through a fault gate.
The map keys must be the **exact** `host:port` strings the server reports (watch out for
hostname vs. IP mismatches). `DriverFailoverProxyTest.assertOnlyConnectedThroughProxies` shows
how to verify no traffic leaks around the proxies.

To drive the *real* replica set while the driver only sees proxies (e.g. trigger a genuine
`replSetStepDown`, poll `replSetGetStatus`), use `ControlChannel` — an auth-aware direct
connection to the real nodes, deliberately separate from the proxied data path.

## Monitoring: `FrameObserver`

```java
proxy.addObserver((dir, msg, ctx) ->
    log.info("[{}] {} -> {}", ctx.listenPort(), dir, msg.getClass().getSimpleName()));
```

Observers are read-only by contract — they must not mutate the frame (rewriting is
`ResponseRewriter`'s job; the interfaces are deliberately separate: *fault = state,
rewrite = strategy, observe = listener*). `Slf4jFrameObserver` is a ready-made logging
implementation. Observer exceptions never kill a proxy thread.

Today only `BACKEND_TO_CLIENT` frames fire: client→backend traffic is forwarded as raw,
length-prefixed bytes without parsing — deliberate pass-through fidelity, the proxy cannot
distort what it does not interpret. The `CLIENT_TO_BACKEND` direction exists in the enum and is
reserved for a consumer that actually needs it.

## Injecting invalid or manipulated replies

`ResponseRewriter` receives every parsed server reply and returns what the client should see —
including something intentionally broken:

```java
proxy.setRewriter(reply -> {
    if (reply instanceof OpMsg msg && msg.getFirstDoc() != null
            && msg.getFirstDoc().containsKey("cursor")) {
        msg.getFirstDoc().put("ok", 0.0);            // flip a find reply into an error
        msg.getFirstDoc().put("errmsg", "injected"); // ... or corrupt it any way you like
    }
    return reply;
});
```

This is the hook for robustness tests: truncated cursors, unexpected error codes, protocol
violations, replies claiming a different topology than reality. Only the backend→client
direction can be rewritten — requests pass through untouched by design.

## Limitations (honest list)

- No latency injection — a frame is forwarded immediately or not at all. If you need slow-link
  simulation, that would be a new `FaultMode`.
- No request (client→backend) rewriting or observation — see above, deliberate.
- `freeze` is not reversible per connection (matches reality, but don't expect a parked
  connection to resume).
- It is test infrastructure: no TLS termination, no config file, one backend per proxy
  instance.

## Reference consumer

`DriverFailoverProxyTest` (tag `wire-failover`) is the full-scale example: three proxies in
front of a real replica set, address rewriting, freeze/reset/close scenarios, stepdown via
`ControlChannel`, and read/write/messaging recovery assertions. It runs in the normal test
matrix against both MongoDB and PoppyDB — see the
[Developer Testing Guide](developer-testing-guide.md) for how the tags fit together.
