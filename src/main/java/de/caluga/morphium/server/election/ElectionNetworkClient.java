package de.caluga.morphium.server.election;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.wire.ConnectionType;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

/**
 * Handles network communication between MorphiumServer nodes for elections.
 * Creates connections to peer servers and sends vote requests / heartbeats.
 */
public class ElectionNetworkClient {

    private static final Logger log = LoggerFactory.getLogger(ElectionNetworkClient.class);

    private final ElectionManager electionManager;
    private final ExecutorService executor;

    // Connection pool to peers (host:port -> driver)
    private final ConcurrentHashMap<String, SingleMongoConnectDriver> peerConnections = new ConcurrentHashMap<>();

    // Connection timeout
    private static final int CONNECT_TIMEOUT_MS = 1000;
    private static final int COMMAND_TIMEOUT_MS = 500;

    private volatile boolean running = false;

    public ElectionNetworkClient(ElectionManager electionManager) {
        this.electionManager = electionManager;
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "ElectionNetwork-" + electionManager.getMyAddress());
            t.setDaemon(true);
            return t;
        });
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

        log.info("ElectionNetworkClient started for {}", electionManager.getMyAddress());
    }

    /**
     * Stop the network client and close all connections.
     */
    public void stop() {
        if (!running) {
            return;
        }
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
                Map<String, Object> response = sendCommand(peer, requestMap);
                if (response != null) {
                    VoteResponse voteResponse = VoteResponse.fromMap(response);
                    voteResponse.setVoterId(peer);
                    electionManager.handleVoteResponse(peer, voteResponse);
                } else {
                    log.debug("Null response from {} for vote request", peer);
                }
            } catch (Exception e) {
                log.debug("Failed to send vote request to {}: {}", peer, e.getMessage());
                // Treat as vote denied
                electionManager.handleVoteResponse(peer, new VoteResponse(0, false, peer));
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

        executor.submit(() -> {
            try {
                Map<String, Object> response = sendCommand(peer, request.toMap());
                if (response != null) {
                    AppendEntriesResponse aeResponse = AppendEntriesResponse.fromMap(response);
                    aeResponse.setFollowerId(peer);
                    electionManager.handleAppendEntriesResponse(peer, aeResponse);
                }
            } catch (Exception e) {
                log.trace("Failed to send heartbeat to {}: {}", peer, e.getMessage());
                // Don't report failure - leader lease will handle it
            }
        });
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
                removeConnection(peer);
                return null;
            }

            GenericCommand cmd = new GenericCommand(conn);
            cmd.setDb("admin");
            cmd.setCmdData(Doc.of(command));

            // Send command and read response
            int msgId = conn.sendCommand(cmd);
            Map<String, Object> result = conn.readSingleAnswer(msgId);
            cmd.releaseConnection();

            return result;
        } catch (MorphiumDriverException e) {
            log.trace("Command to {} failed: {}", peer, e.getMessage());
            removeConnection(peer);
            throw e;
        }
    }

    /**
     * Get or create a connection to a peer.
     * Does not cache null values to allow retry on transient failures.
     */
    private SingleMongoConnectDriver getOrCreateConnection(String peer) {
        // First check if we have an existing connection
        SingleMongoConnectDriver existing = peerConnections.get(peer);
        if (existing != null) {
            return existing;
        }

        // Try to create a new connection
        try {
            String[] parts = peer.split(":");
            String peerHost = parts[0];
            int peerPort = Integer.parseInt(parts[1]);

            SingleMongoConnectDriver driver = new SingleMongoConnectDriver();
            driver.setHostSeed(peerHost + ":" + peerPort);
            driver.setConnectionTimeout(CONNECT_TIMEOUT_MS);
            driver.setMaxWaitTime(COMMAND_TIMEOUT_MS);
            // Use ANY connection type - during elections nodes may not yet be primary
            driver.setConnectionType(ConnectionType.ANY);
            driver.connect();

            log.debug("Created connection to peer {}", peer);
            // Only cache if connection was successful
            peerConnections.put(peer, driver);
            return driver;
        } catch (Exception e) {
            log.debug("Failed to connect to peer {}: {}", peer, e.getMessage());
            return null;
        }
    }

    /**
     * Remove a failed connection from the cache.
     */
    private void removeConnection(String peer) {
        SingleMongoConnectDriver driver = peerConnections.remove(peer);
        if (driver != null) {
            try {
                driver.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
