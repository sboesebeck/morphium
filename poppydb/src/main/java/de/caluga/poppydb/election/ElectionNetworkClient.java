package de.caluga.poppydb.election;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.wire.ConnectionType;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import javax.net.ssl.SSLContext;

/**
 * Handles network communication between PoppyDB nodes for elections.
 * Creates connections to peer servers and sends vote requests / heartbeats.
 */
public class ElectionNetworkClient {

    private static final Logger log = LoggerFactory.getLogger(ElectionNetworkClient.class);

    private final ElectionManager electionManager;
    private final ExecutorService executor;

    // Connection pool to peers (host:port -> driver)
    final ConcurrentHashMap<String, SingleMongoConnectDriver> peerConnections = new ConcurrentHashMap<>();

    // Consecutive failed heartbeats per peer - drives the throttled WARN below, so a leader
    // that cannot reach a follower says so instead of failing silently.
    private final ConcurrentHashMap<String, AtomicLong> heartbeatFailures = new ConcurrentHashMap<>();

    // Connection timeout. The reply timeout is election-derived - see
    // ElectionManager#getRpcTimeoutMs for why it is no longer a fixed 500ms.
    private static final int CONNECT_TIMEOUT_MS = 1000;

    // Heartbeats run every few hundred ms - the first WARN waits for a few consecutive
    // failures (one is routine: a peer's idle-closed socket, a reply that lost a scheduling
    // race under load), then only every Nth failure is logged.
    private static final int HEARTBEAT_FAILURE_WARN_AFTER = 3;
    private static final int HEARTBEAT_FAILURE_LOG_INTERVAL = 20;

    // One heartbeat in flight per peer: the reply wait may now exceed the heartbeat interval,
    // and a peer that is slow (or down, with its connect timeout) must not accumulate a
    // backlog of tasks all queueing for the same single connection.
    private final ConcurrentHashMap<String, AtomicBoolean> heartbeatInFlight = new ConcurrentHashMap<>();

    // Effectively disables the driver's own heartbeat for peer connections (#311). We cannot
    // pass "off", so this is a period nothing in a process lifetime reaches.
    private static final int PEER_DRIVER_HEARTBEAT_MS = (int) TimeUnit.DAYS.toMillis(1);

    private volatile boolean running = false;

    // RS-internal connection security, set once via setInternalConnectionSecurity() before
    // start() - see docs/superpowers/specs/2026-08-05-poppydb-rs-internal-auth-tls-design.md.
    // Defaults (auth off, no SSL context) reproduce today's plaintext/unauthenticated behavior.
    private volatile boolean authEnabled = false;
    private volatile String authUser = null;
    private volatile String authPassword = null;
    private volatile SSLContext internalSslContext = null;

    public ElectionNetworkClient(ElectionManager electionManager) {
        this.electionManager = electionManager;
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "ElectionNetwork-" + electionManager.getMyAddress());
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Configure how outbound connections to peers authenticate/encrypt themselves. Call before
     * {@link #start()}. {@code internalSslContext} of {@code null} means the connection stays
     * plaintext even if {@code authEnabled} is true.
     */
    public void setInternalConnectionSecurity(boolean authEnabled, String authUser, String authPassword,
            SSLContext internalSslContext) {
        this.authEnabled = authEnabled;
        this.authUser = authUser;
        this.authPassword = authPassword;
        this.internalSslContext = internalSslContext;
    }

    /**
     * Start the network client and wire up election callbacks.
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;

        // Wire up callbacks for sending messages
        electionManager.setSendVoteRequest(this::sendVoteRequest);
        electionManager.setSendAppendEntries(this::sendAppendEntries);
        electionManager.setSendRestoreStatusProbe(this::sendRestoreStatusProbe);

        log.info("ElectionNetworkClient started for {}", electionManager.getMyAddress());
    }

    /**
     * Stop the network client and close all connections.
     */
    public void stop() {
        // Deliberately NOT short-circuiting on !running: connections can exist without the
        // client ever having been started (a caller that only used getOrCreateConnection), and
        // returning early there left drivers, sockets and their schedulers alive - visible as
        // heartbeat chatter long after the owner was done with them. Closing is idempotent, so
        // making this method independent of the lifecycle flag costs nothing and always frees
        // what was actually allocated.
        running = false;

        // Close all peer connections
        for (Map.Entry<String, SingleMongoConnectDriver> entry : peerConnections.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                log.debug("Error closing connection to {}: {}", entry.getKey(), e.getMessage());
            }
        }
        peerConnections.clear();

        executor.shutdownNow();
        log.info("ElectionNetworkClient stopped");
    }

    /**
     * Send a vote request to a peer.
     */
    private void sendVoteRequest(String peer, VoteRequest request) {
        if (!running) {
            return;
        }

        executor.submit(() -> {
            try {
                Map<String, Object> requestMap = request.toMap();
                log.debug("Sending vote request to {}", peer);
                Map<String, Object> response = exchange(peer, requestMap);
                if (response != null) {
                    VoteResponse voteResponse = VoteResponse.fromMap(response);
                    voteResponse.setVoterId(peer);
                    // The request is handed back alongside the response (#306 P1-3): its
                    // roundId/preVote let the ElectionManager credit the response to the round
                    // it actually answers - and discard it if that round is already over.
                    electionManager.handleVoteResponse(peer, request, voteResponse);
                } else {
                    // Same as the exception path below: an unreachable peer is a denial, not
                    // silence - the round's bookkeeping should say so.
                    log.debug("No response from {} for vote request - treating as denied", peer);
                    electionManager.handleVoteResponse(peer, request, new VoteResponse(0, false, peer));
                }
            } catch (Exception e) {
                log.debug("Failed to send vote request to {}: {}", peer, e.getMessage());
                // Treat as vote denied
                electionManager.handleVoteResponse(peer, request, new VoteResponse(0, false, peer));
            }
        });
    }

    /**
     * Ask a peer for its restore finding (#391) - sent by a node that holds back candidacy for
     * an incomplete restore, once per election timeout. Anything but a proper answer (peer down,
     * or an older version that does not know the command and answers with an error) is
     * reported to nobody: the ElectionManager treats a missing answer as unknown, which blocks
     * the automatic acceptance.
     */
    private void sendRestoreStatusProbe(String peer) {
        if (!running) {
            return;
        }

        executor.submit(() -> {
            try {
                Map<String, Object> response = exchange(peer, RestoreStatus.requestMap(electionManager.getMyAddress()));
                RestoreStatus status = RestoreStatus.fromMap(response);
                if (status != null) {
                    electionManager.handleRestoreStatusResponse(peer, status);
                } else {
                    log.debug("No restore status from {} (answer: {}) - treating as unknown", peer, response);
                }
            } catch (Exception e) {
                log.debug("Failed to probe restore status of {}: {}", peer, e.getMessage());
            }
        });
    }

    /**
     * Send an append entries (heartbeat) to a peer.
     */
    private void sendAppendEntries(String peer, AppendEntriesRequest request) {
        if (!running) {
            return;
        }

        AtomicBoolean inFlight = heartbeatInFlight.computeIfAbsent(peer, p -> new AtomicBoolean());
        if (!inFlight.compareAndSet(false, true)) {
            // The previous heartbeat to this peer has not returned yet - skipping this tick
            // costs nothing (the next one carries the same information), while queueing would
            // pile up tasks behind one slow or dead peer.
            log.trace("Heartbeat to {} still in flight - skipping this tick", peer);
            return;
        }

        try {
            executor.submit(() -> heartbeat(peer, request, inFlight));
        } catch (RejectedExecutionException e) {
            // stop() shut the executor down between the running check and here
            inFlight.set(false);
        }
    }

    private void heartbeat(String peer, AppendEntriesRequest request, AtomicBoolean inFlight) {
        {
            try {
                Map<String, Object> response = exchange(peer, request.toMap());
                if (response != null) {
                    AppendEntriesResponse aeResponse = AppendEntriesResponse.fromMap(response);
                    aeResponse.setFollowerId(peer);
                    electionManager.handleAppendEntriesResponse(peer, aeResponse);
                    noteHeartbeatReachable(peer);
                } else {
                    noteHeartbeatFailure(peer, "no response");
                }
            } catch (Exception e) {
                // The lease still decides whether we keep leading - but a peer we cannot reach
                // must not be a silent condition: a follower without heartbeats campaigns
                // forever without ever winning, and the only visible symptom used to be the
                // election timeouts on the FOLLOWER, with nothing at all on the leader.
                noteHeartbeatFailure(peer, String.valueOf(e.getMessage()));
            } finally {
                inFlight.set(false);
            }
        }
    }

    /**
     * One request/response exchange with a peer, tolerant of a stale cached connection.
     *
     * <p>A follower's peer connections idle whenever it is neither leader nor candidate, and
     * the peer closes idle connections after its idle timeout. The driver cannot know that
     * until it uses the socket, so the first request after a failover found a dead socket:
     * EOF or reset, no answer. Losing that request costs a whole election round (the next
     * election timeout), or a heartbeat tick - and it was exactly what the captured slow
     * failovers showed ("Null response from localhost:NNN for vote request" for a peer that was
     * up and answering everyone else). So: if the first attempt went over a CACHED connection
     * and came back empty or broken, dial fresh and send once more. A fresh dial that failed is
     * not retried - that peer is down, and the next tick will try again anyway.
     */
    Map<String, Object> exchange(String peer, Map<String, Object> command) throws Exception {
        SingleMongoConnectDriver existing = peerConnections.get(peer);
        boolean cached = existing != null && existing.isConnected();
        Map<String, Object> response;
        try {
            response = sendCommand(peer, command);
        } catch (Exception first) {
            if (!cached || !running) {
                throw first;
            }
            log.debug("Cached connection to {} failed ({}) - dialing again for one retry", peer, first.getMessage());
            response = null;
        }
        if (response == null && cached && running) {
            // sendCommand evicted the stale driver, so this dials fresh.
            response = sendCommand(peer, command);
        }
        return response;
    }

    /**
     * Send a command to a peer and get the response.
     */
    private Map<String, Object> sendCommand(String peer, Map<String, Object> command) throws Exception {
        SingleMongoConnectDriver driver = getOrCreateConnection(peer);
        if (driver == null) {
            log.debug("No driver connection to peer {}", peer);
            return null;
        }

        try {
            MongoConnection conn = driver.getConnection();
            if (conn == null) {
                // Connection failed, remove from cache
                log.debug("No connection available to peer {}", peer);
                removeConnection(peer, driver);
                return null;
            }

            GenericCommand cmd = new GenericCommand(conn);
            cmd.setDb("admin");
            cmd.setCmdData(Doc.of(command));

            // Send command and read response
            int msgId = conn.sendCommand(cmd);
            Map<String, Object> result = conn.readSingleAnswer(msgId);
            cmd.releaseConnection();

            if (result == null) {
                // No answer means the connection is gone - readNextMessage closes it on both an
                // expired deadline and on EOF. Evict it now, or the next request would poll a
                // driver that only learns about the closed socket by failing again.
                removeConnection(peer, driver);
            }

            return result;
        } catch (Exception e) {
            // Deliberately Exception, not MorphiumDriverException: a driver that lost its
            // connection throws a plain RuntimeException from the wrapper it handed out
            // ("Cannot get delegate"), and that used to slip past this eviction - leaving the
            // dead driver in the cache for good.
            log.trace("Command to {} failed: {}", peer, e.getMessage());
            removeConnection(peer, driver);
            throw e;
        }
    }

    /**
     * Get or create a connection to a peer.
     * Does not cache null values to allow retry on transient failures.
     */
    SingleMongoConnectDriver getOrCreateConnection(String peer) {
        // First check if we have an existing connection - and that it is still usable.
        // A cached driver whose connection died does NOT repair itself: close() nulls the
        // connection AND cancels the driver's own heartbeat, after which getConnection()
        // hands out a wrapper around null whose first use throws a plain RuntimeException -
        // which never matched the MorphiumDriverException-only eviction in sendCommand(). The
        // leader then "sent" heartbeats into a dead driver forever and the restarted peer
        // stayed cut off until the LEADER was restarted (ACC message-bus set, 2026-08-18).
        SingleMongoConnectDriver existing = peerConnections.get(peer);
        if (existing != null) {
            if (existing.isConnected()) {
                return existing;
            }

            log.info("Connection to peer {} is no longer connected - dialing again", peer);
            removeConnection(peer, existing);
        }

        // Try to create a new connection
        try {
            String[] parts = peer.split(":");
            String peerHost = parts[0];
            int peerPort = Integer.parseInt(parts[1]);

            SingleMongoConnectDriver driver = new SingleMongoConnectDriver();
            driver.setHostSeed(peerHost + ":" + peerPort);
            driver.setConnectionTimeout(CONNECT_TIMEOUT_MS);
            driver.setMaxWaitTime(electionManager.getRpcTimeoutMs());
            // Use ANY connection type - during elections nodes may not yet be primary
            driver.setConnectionType(ConnectionType.ANY);
            if (authEnabled) {
                driver.setCredentials("admin", authUser, authPassword);
            }
            if (internalSslContext != null) {
                driver.setUseSSL(true);
                driver.setSslContext(internalSslContext);
                driver.setSslInvalidHostNameAllowed(true);
            }
            // One attempt per dial: we retry on every heartbeat tick anyway, so the driver's
            // own retry loop (5 tries with a 100ms pause by default) would only pile up threads
            // blocked for seconds against a peer that is simply down.
            driver.setRetriesOnNetworkError(1);

            // No driver-side self-repair for peer connections (#311). Liveness is ours now: we
            // probe every heartbeat tick and redial. The driver's own recovery task, in
            // contrast, runs close() -> sleep -> connect() on its own schedule, and a driver we
            // evicted mid-recovery would reconnect afterwards and live on as an orphan - with
            // an open socket nothing references, kept alive by that very heartbeat. Worse, the
            // reconnect walks a host seed that the first successful connect enlarged to every
            // RS member (the peer's hello answers with all of them), with ConnectionType.ANY -
            // so the revived orphan can end up attached to a different node than the peer it
            // was created for. Without the internal heartbeat, none of that machinery runs.
            driver.setHeartbeatFrequency(PEER_DRIVER_HEARTBEAT_MS);

            driver.connect();

            // Pin the seed back to this one peer. connect() enlarges it from the hello answer
            // to every RS member - the peer's own address first - and on a failed attempt
            // connect() walks to the next seed entry with ConnectionType.ANY. A driver dialed
            // for a peer that is down therefore attaches to whatever else answers, up to and
            // including THIS node, while we go on believing we are talking to the peer: on
            // 2026-08-18 a candidate's vote request for a restarting peer was answered by the
            // candidate itself and counted as that peer's grant, which won it an election that
            // the only up-to-date node had explicitly denied. Nothing here needs the wider
            // seed - this driver only ever talks to one peer.

            driver.setHostSeed(peer);

            log.debug("Created connection to peer {}", peer);
            // Only cache if connection was successful
            SingleMongoConnectDriver raced = peerConnections.putIfAbsent(peer, driver);

            if (raced != null) {
                // A concurrent dial for the same peer won. Handing back its driver is fine -
                // but ours must be closed, or it lives on as an orphaned socket keeping itself
                // alive with its own heartbeat, on both ends, until the process exits.
                closeQuietly(driver);
                return raced;
            }

            return driver;
        } catch (Exception e) {
            log.debug("Failed to connect to peer {}: {}", peer, e.getMessage());
            return null;
        }
    }

    /**
     * Record a failed heartbeat to {@code peer}. Logged once the streak reaches
     * {@link #HEARTBEAT_FAILURE_WARN_AFTER} and then once per
     * {@link #HEARTBEAT_FAILURE_LOG_INTERVAL} attempts - often enough to be noticed in an
     * incident, rarely enough not to drown the log at heartbeat cadence, and never for a
     * single miss that the next tick repairs.
     */
    private void noteHeartbeatFailure(String peer, String reason) {
        long failures = heartbeatFailures.computeIfAbsent(peer, p -> new AtomicLong()).incrementAndGet();

        if (failures == HEARTBEAT_FAILURE_WARN_AFTER || failures % HEARTBEAT_FAILURE_LOG_INTERVAL == 0) {
            log.warn("Cannot reach peer {} with heartbeats ({} consecutive failures): {} - "
                    + "that peer sees no leader and cannot rejoin until contact is restored",
                    peer, failures, reason);
        }
    }

    /**
     * Record a heartbeat that got through, closing out any failure streak.
     */
    private void noteHeartbeatReachable(String peer) {
        AtomicLong failures = heartbeatFailures.get(peer);

        if (failures != null && failures.getAndSet(0) >= HEARTBEAT_FAILURE_WARN_AFTER) {
            log.info("Peer {} is reachable again", peer);
        }
    }

    /**
     * Remove a failed connection from the cache.
     */
    private void removeConnection(String peer, SingleMongoConnectDriver expected) {
        // Conditional remove: two heartbeat tasks can observe the same broken driver, and the
        // slower one must not tear out the healthy replacement the faster one just installed -
        // that would kill a connection in mid-command right when the peer came back.
        if (expected != null && !peerConnections.remove(peer, expected)) {
            return;
        }

        if (expected == null) {
            expected = peerConnections.remove(peer);
        }

        closeQuietly(expected);
    }

    private void closeQuietly(SingleMongoConnectDriver driver) {
        if (driver == null) {
            return;
        }

        try {
            driver.close();
        } catch (Exception e) {
            // ignore - we are discarding this driver anyway
        }
    }
}
