package de.caluga.morphium.server;

import static java.lang.Thread.sleep;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumTransactionContext;
import de.caluga.morphium.driver.bson.MongoTimestamp;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.HelloResult;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wire.SingleMongoConnection;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.OpQuery;
import de.caluga.morphium.driver.wireprotocol.OpReply;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

public class MorphiumServer {

    private static Logger log = LoggerFactory.getLogger(MorphiumServer.class);
    private InMemoryDriver drv;
    private int port;
    private String host;
    private AtomicInteger msgId = new AtomicInteger(1000);
    private AtomicInteger cursorId = new AtomicInteger(1000);
    // Map cursor IDs to their change stream queues for getMore handling
    private final Map<Long, java.util.concurrent.BlockingQueue<Map<String, Object>>> watchCursors = new java.util.concurrent.ConcurrentHashMap<>();

    // Track transactions by session ID (lsid) for proper multi-threaded transaction support
    // Since InMemoryDriver uses ThreadLocal for transactions, we need to track them here
    // and set the context on the current thread before processing each command
    private final Map<String, MorphiumTransactionContext> sessionTransactions = new java.util.concurrent.ConcurrentHashMap<>();

    private ThreadPoolExecutor executor;
    private Thread heartbeatThread;
    private static final int HEARTBEAT_INTERVAL_MS = 2000;
    private static final int HEARTBEAT_TIMEOUT_MS = 5000;
    private volatile boolean running = true;
    private ServerSocket serverSocket;
    private int compressorId;
    private String rsName = "";
    private String hostSeed = "";
    private List<String> hosts = new ArrayList<>();
    private Map<String, Integer> hostPriorities = new java.util.concurrent.ConcurrentHashMap<>();
    private boolean primary = true;
    private String primaryHost;
    private int priority = 0;
    private volatile boolean replicationStarted = false;
    private volatile boolean initialSyncDone = true;
    private static final Set<String> WRITE_COMMANDS = Set.of(
                        "insert", "update", "delete", "findandmodify",
                        "createindexes", "create", "drop", "dropindexes", "dropdatabase", "bulkwrite"
        );
    private static final Set<String> READ_COMMANDS = Set.of(
                        "find", "aggregate", "count", "distinct", "countdocuments"
        );

    // Cached connection for forwarding reads to primary (avoids creating new connections per request)
    private volatile SingleMongoConnectDriver forwardingDriver;
    private volatile MongoConnection forwardingConnection;
    private final Object forwardingLock = new Object();

    // Note: Synchronous replication creates connections per-request to avoid contention

    // Note: Each MorphiumServer instance has its own isolated InMemoryDriver.
    // Secondaries will not have the same data as primary unless replication is implemented.
    // For tests, use primary read preference or connect directly to the primary.

    // SSL configuration
    private boolean sslEnabled = false;
    private javax.net.ssl.SSLContext sslContext = null;

    // Persistence configuration
    private java.io.File dumpDirectory = null;
    private long dumpIntervalMs = 0; // 0 = disabled
    private java.util.concurrent.ScheduledExecutorService dumpScheduler = null;
    private volatile long lastDumpTime = 0;

    /**
     * Extract session ID string from a document's lsid field.
     * The lsid is typically { "id": UUID } structure.
     */
    private String extractSessionId(Map<String, Object> doc) {
        Object lsid = doc.get("lsid");
        if (lsid instanceof Map) {
            Object id = ((Map<?, ?>) lsid).get("id");
            if (id != null) {
                return id.toString();
            }
        }
        return null;
    }

    /**
     * Set up transaction context for the current thread if this command is part of a transaction.
     * Returns the session ID if a transaction context was set, null otherwise.
     */
    private String setupTransactionContext(Map<String, Object> doc) {
        String sessionId = extractSessionId(doc);
        if (sessionId == null) {
            return null;
        }

        // Check if this starts a new transaction
        Object startTxn = doc.get("startTransaction");
        boolean isStartTransaction = Boolean.TRUE.equals(startTxn) ||
            (startTxn instanceof Number && ((Number)startTxn).intValue() == 1);

        if (isStartTransaction) {
            // Start a new transaction
            log.debug("MorphiumServer: Starting transaction for session {}", sessionId);
            MorphiumTransactionContext ctx = drv.startTransaction(false);
            sessionTransactions.put(sessionId, ctx);
            return sessionId;
        }

        // Check if this is part of an existing transaction
        Object txnNumber = doc.get("txnNumber");
        Object autocommit = doc.get("autocommit");
        boolean isPartOfTransaction = txnNumber != null && Boolean.FALSE.equals(autocommit);

        if (isPartOfTransaction && sessionTransactions.containsKey(sessionId)) {
            // Set the existing transaction context on this thread
            MorphiumTransactionContext ctx = sessionTransactions.get(sessionId);
            drv.setTransactionContext(ctx);
            log.debug("MorphiumServer: Using existing transaction context for session {}", sessionId);
            return sessionId;
        }

        return null;
    }

    /**
     * Clean up transaction context after command execution.
     */
    private void cleanupTransactionContext(String sessionId) {
        if (sessionId != null) {
            // Don't clear the ThreadLocal - the context needs to stay associated with the session
            // We only clear it when the transaction ends (commit/abort)
        }
    }

    /**
     * Handle transaction abort - rollback changes and clean up.
     */
    private void handleAbortTransaction(String sessionId) {
        if (sessionId != null && sessionTransactions.containsKey(sessionId)) {
            log.debug("MorphiumServer: Aborting transaction for session {}", sessionId);
            MorphiumTransactionContext ctx = sessionTransactions.remove(sessionId);
            drv.setTransactionContext(ctx);
            try {
                drv.abortTransaction();
            } catch (Exception e) {
                log.error("Error aborting transaction", e);
            }
        }
    }

    /**
     * Handle transaction commit - apply changes and clean up.
     */
    private void handleCommitTransaction(String sessionId) {
        if (sessionId != null && sessionTransactions.containsKey(sessionId)) {
            log.debug("MorphiumServer: Committing transaction for session {}", sessionId);
            MorphiumTransactionContext ctx = sessionTransactions.remove(sessionId);
            drv.setTransactionContext(ctx);
            try {
                drv.commitTransaction();
            } catch (Exception e) {
                log.error("Error committing transaction", e);
            }
        }
    }

    public MorphiumServer(int port, String host, int maxThreads, int minThreads, int compressorId) {
        this.drv = new InMemoryDriver();
        this.port = port;
        this.host = host;
        this.compressorId = compressorId;
        drv.connect();
        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        executor.setMaximumPoolSize(maxThreads);
        executor.setCorePoolSize(minThreads);
    }

    public MorphiumServer(int port, String host, int maxThreads, int minThreads) {
        this(port, host, maxThreads, minThreads, OpCompressed.COMPRESSOR_NOOP);
    }

    public MorphiumServer() {
        this(17017, "localhost", 100, 10, OpCompressed.COMPRESSOR_NOOP);
    }



    private InMemoryDriver getDriver() {
        return drv;
    }

    // SSL configuration methods
    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public javax.net.ssl.SSLContext getSslContext() {
        return sslContext;
    }

    public void setSslContext(javax.net.ssl.SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    // Persistence configuration methods

    /**
     * Set the directory for periodic database dumps.
     * @param dir Directory to store dump files
     */
    public void setDumpDirectory(java.io.File dir) {
        this.dumpDirectory = dir;
    }

    public java.io.File getDumpDirectory() {
        return dumpDirectory;
    }

    /**
     * Set the interval for periodic database dumps.
     * @param intervalMs Interval in milliseconds (0 = disabled)
     */
    public void setDumpIntervalMs(long intervalMs) {
        this.dumpIntervalMs = intervalMs;
    }

    public long getDumpIntervalMs() {
        return dumpIntervalMs;
    }

    /**
     * Get the timestamp of the last successful dump.
     */
    public long getLastDumpTime() {
        return lastDumpTime;
    }

    /**
     * Manually trigger a database dump to the configured directory.
     * @return Number of databases dumped
     */
    public int dumpNow() throws IOException {
        if (dumpDirectory == null) {
            throw new IOException("Dump directory not configured");
        }
        int count = drv.dumpAllToDirectory(dumpDirectory);
        lastDumpTime = System.currentTimeMillis();
        log.info("Dumped {} databases to {}", count, dumpDirectory.getAbsolutePath());
        return count;
    }

    /**
     * Restore databases from the configured dump directory.
     * Should be called before start() to restore previous state.
     * @return Number of databases restored
     */
    public int restoreFromDump() throws IOException {
        if (dumpDirectory == null) {
            throw new IOException("Dump directory not configured");
        }
        try {
            int count = drv.restoreAllFromDirectory(dumpDirectory);
            log.info("Restored {} databases from {}", count, dumpDirectory.getAbsolutePath());
            return count;
        } catch (org.json.simple.parser.ParseException e) {
            throw new IOException("Failed to parse dump file", e);
        }
    }

    /**
     * Start the periodic dump scheduler.
     */
    private void startDumpScheduler() {
        if (dumpIntervalMs <= 0 || dumpDirectory == null) {
            return;
        }

        dumpScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "MorphiumServer-DumpScheduler");
            t.setDaemon(true);
            return t;
        });

        dumpScheduler.scheduleAtFixedRate(() -> {
            try {
                int count = drv.dumpAllToDirectory(dumpDirectory);
                lastDumpTime = System.currentTimeMillis();
                if (count > 0) {
                    log.info("Periodic dump: {} databases saved to {}", count, dumpDirectory.getAbsolutePath());
                }
            } catch (Exception e) {
                log.error("Failed to dump databases: {}", e.getMessage(), e);
            }
        }, dumpIntervalMs, dumpIntervalMs, java.util.concurrent.TimeUnit.MILLISECONDS);

        log.info("Dump scheduler started: interval={}ms, directory={}", dumpIntervalMs, dumpDirectory.getAbsolutePath());
    }

    /**
     * Stop the periodic dump scheduler.
     */
    private void stopDumpScheduler() {
        if (dumpScheduler != null) {
            dumpScheduler.shutdown();
            try {
                if (!dumpScheduler.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    dumpScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                dumpScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            dumpScheduler = null;
        }
    }

    /**
     * Create an SSLServerSocket with the configured SSLContext.
     * If no SSLContext is set, uses the default SSLContext.
     */
    private ServerSocket createSslServerSocket(int port) throws IOException {
        javax.net.ssl.SSLContext ctx = sslContext;
        if (ctx == null) {
            try {
                ctx = javax.net.ssl.SSLContext.getDefault();
            } catch (java.security.NoSuchAlgorithmException e) {
                throw new IOException("Failed to get default SSLContext", e);
            }
        }

        javax.net.ssl.SSLServerSocketFactory factory = ctx.getServerSocketFactory();
        javax.net.ssl.SSLServerSocket sslServerSocket = (javax.net.ssl.SSLServerSocket) factory.createServerSocket(port);

        // Configure SSL parameters - require TLS 1.2 or higher
        String[] enabledProtocols = sslServerSocket.getSupportedProtocols();
        List<String> secureProtocols = new ArrayList<>();
        for (String protocol : enabledProtocols) {
            if (protocol.equals("TLSv1.2") || protocol.equals("TLSv1.3")) {
                secureProtocols.add(protocol);
            }
        }
        if (!secureProtocols.isEmpty()) {
            sslServerSocket.setEnabledProtocols(secureProtocols.toArray(new String[0]));
        }

        log.info("SSL ServerSocket created with protocols: {}", Arrays.toString(sslServerSocket.getEnabledProtocols()));
        return sslServerSocket;
    }

    private HelloResult getHelloResult() {
        HelloResult res = new HelloResult();
        res.setHelloOk(true);
        res.setLocalTime(new Date());
        res.setOk(1.0);

        if (hosts == null || hosts.isEmpty()) {
            res.setHosts(Arrays.asList(host + ":" + port));
        } else {
            res.setHosts(hosts);
        }

        res.setConnectionId(1);
        res.setMaxWireVersion(17);
        res.setMinWireVersion(13);
        res.setMaxMessageSizeBytes(100000);
        res.setMaxBsonObjectSize(10000);
        res.setWritablePrimary(primary);
        res.setSecondary(!primary);
        res.setSetName(rsName);
        res.setPrimary(primary ? host + ":" + port : primaryHost);
        res.setMe(host + ":" + port);
        res.setLogicalSessionTimeoutMinutes(30);
        // res.setMsg("ok");
        res.setMsg("MorphiumServer V0.1ALPHA");
        return res;
    }

    public int getConnectionCount() {
        return executor.getActiveCount();
    }

    public void start() throws IOException, InterruptedException {
        log.info("Opening port " + port + (sslEnabled ? " (SSL enabled)" : ""));
        if (sslEnabled) {
            serverSocket = createSslServerSocket(port);
        } else {
            serverSocket = new ServerSocket(port);
        }
        drv.setHostSeed(host + ":" + port);
        drv.setReplicaSet(rsName != null && !rsName.isEmpty());
        drv.setReplicaSetName(rsName == null ? "" : rsName);
        executor.prestartAllCoreThreads();

        // Start periodic dump scheduler if configured
        startDumpScheduler();

        log.info("Port opened, waiting for incoming connections");
        new Thread(()-> {
            while (running) {
                Socket s = null;

                try {
                    s = serverSocket.accept();
                } catch (IOException e) {
                    if (e.getMessage().contains("Socket closed")) {
                        log.info("Server socket closed");
                        break;
                    }

                    log.error("Serversocket error", e);
                    terminate();
                    break;
                }

                log.info("Incoming connection: " + executor.getPoolSize());
                Socket finalS = s;
                //new Thread(() -> incoming(finalS)).start();
                executor.execute(() -> incoming(finalS));
            }

        }).start();

        boolean replicaConfigured = rsName != null && !rsName.isEmpty();
        if (replicaConfigured) {
            String myAddress = host + ":" + port;
            List<String> reachablePeers = getReachablePeers();

            if (!reachablePeers.isEmpty()) {
                String detectedPrimary = detectPrimaryFromPeers(reachablePeers);

                if (detectedPrimary != null) {
                    primaryHost = detectedPrimary;
                } else {
                    String fallback = pickHighestPriorityPeer(reachablePeers);

                    if (fallback != null) {
                        primaryHost = fallback;
                    } else {
                        primaryHost = reachablePeers.get(0);
                    }

                    log.info("Could not detect current primary, using {} as sync source", primaryHost);
                }

                if (primary) {
                    // Do NOT step down just because peers exist. This can lead to a "no primary" situation
                    // when all nodes start around the same time.
                    if (primaryHost != null && !primaryHost.equals(myAddress)) {
                        int otherPrio = hostPriorities.getOrDefault(primaryHost, 0);
                        if (otherPrio > priority) {
                            log.info("Detected primary {} with higher priority ({}), staying secondary for initial sync",
                                     primaryHost, otherPrio);
                            primary = false;
                            initialSyncDone = false;
                            startReplicaReplication();
                        } else {
                            log.info("No higher-priority primary detected, staying primary");
                            primary = true;
                            primaryHost = myAddress;
                            initialSyncDone = true;
                        }
                    } else {
                        // Either no primary detected yet or it's us.
                        primaryHost = myAddress;
                        initialSyncDone = true;
                    }
                } else {
                    startReplicaReplication();
                }
            } else {
                log.info("No reachable peers detected - acting as primary");
                primary = true;
                primaryHost = myAddress;
                initialSyncDone = true;
            }

            if (hosts != null && hosts.size() > 1) {
                startHeartbeat();
            }
        }
    }

    private void startHeartbeat() {
        heartbeatThread = Thread.ofVirtual().name("heartbeat").start(() -> {
            while (running) {
                try {
                    Thread.sleep(HEARTBEAT_INTERVAL_MS);

                    String myAddress = host + ":" + port;

                    if (primary) {
                        // Primary: check if there's another primary
                        // This handles split-brain when a node comes back after being down
                        for (var entry : hostPriorities.entrySet()) {
                            String otherHost = entry.getKey();

                            if (otherHost.equals(myAddress)) {
                                continue;
                            }

                            // Check if another host thinks it's primary
                            if (checkHostAlive(otherHost) && checkIfPrimary(otherHost)) {
                                // Two primaries detected! The one with lower priority steps down
                                int otherPrio = entry.getValue();
                                if (otherPrio > priority) {
                                    log.warn("Found another primary {} with higher priority ({}), stepping down", otherHost, otherPrio);
                                    becomesSecondary(otherHost);
                                } else {
                                    log.warn("Found another primary {} with lower priority ({}), they should step down", otherHost, otherPrio);
                                    // Don't do anything - the other node should detect us and step down
                                }
                                break;
                            }
                        }
                        continue;
                    }

                    // Secondary: check if primary is still alive
                    if (primaryHost != null && !primaryHost.equals(myAddress)) {
                        boolean primaryAlive = checkHostAlive(primaryHost);
                        if (!primaryAlive) {
                            log.warn("Primary {} appears to be down, initiating election", primaryHost);
                            initiateElection();
                        } else {
                            // Primary is alive - check if we should be primary (higher priority)
                            int primaryPrio = hostPriorities.getOrDefault(primaryHost, 0);
                            if (priority > primaryPrio) {
                                if (initialSyncDone) {
                                    log.info("This node has higher priority ({}) than current primary {} ({}), taking over",
                                             priority, primaryHost, primaryPrio);
                                    becomesPrimary();
                                } else {
                                    log.info("Detected higher priority than current primary {}, waiting for initial sync to finish", primaryHost);
                                }
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Heartbeat error", e);
                }
            }
        });
    }

    private List<String> getReachablePeers() {
        if (hosts == null || hosts.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> reachable = new ArrayList<>();
        String myAddress = host + ":" + port;

        for (String h : hosts) {
            if (h.equals(myAddress)) {
                continue;
            }

            if (checkHostAlive(h)) {
                reachable.add(h);
            }
        }

        return reachable;
    }

    private String detectPrimaryFromPeers(List<String> peers) {
        for (String peer : peers) {
            try {
                if (checkIfPrimary(peer)) {
                    return peer;
                }
            } catch (Exception e) {
                log.debug("Could not determine if {} is primary: {}", peer, e.getMessage());
            }
        }

        return null;
    }

    private String pickHighestPriorityPeer(List<String> peers) {
        String best = null;
        int bestPrio = Integer.MIN_VALUE;

        for (String peer : peers) {
            int prio = hostPriorities.getOrDefault(peer, 0);

            if (prio > bestPrio) {
                bestPrio = prio;
                best = peer;
            }
        }

        return best;
    }

    private boolean checkIfPrimary(String hostPort) {
        String[] parts = hostPort.split(":");
        String checkHost = parts[0];
        int checkPort = Integer.parseInt(parts[1]);

        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(checkHost, checkPort), HEARTBEAT_TIMEOUT_MS);
            socket.setSoTimeout(HEARTBEAT_TIMEOUT_MS);

            var out = socket.getOutputStream();
            var in = socket.getInputStream();

            // Send isMaster/hello command
            OpMsg helloMsg = new OpMsg();
            helloMsg.setMessageId(msgId.incrementAndGet());
            helloMsg.setFirstDoc(Doc.of("hello", 1, "$db", "admin"));
            out.write(helloMsg.bytes());
            out.flush();

            // Read response using parseFromStream
            var response = WireProtocolMessage.parseFromStream(in);
            if (response instanceof OpMsg) {
                Map<String, Object> doc = ((OpMsg) response).getFirstDoc();
                Boolean isWritablePrimary = (Boolean) doc.get("isWritablePrimary");
                return isWritablePrimary != null && isWritablePrimary;
            }
            return false;
        } catch (Exception e) {
            log.debug("Could not check if {} is primary: {}", hostPort, e.getMessage());
            return false;
        }
    }

    private void becomesSecondary(String newPrimaryHost) {
        log.info("Stepping down {} to secondary, new primary is {}", host + ":" + port, newPrimaryHost);
        primary = false;
        primaryHost = newPrimaryHost;
        replicationStarted = false;
        startReplicaReplication();
    }

    private boolean checkHostAlive(String hostPort) {
        String[] parts = hostPort.split(":");
        String checkHost = parts[0];
        int checkPort = Integer.parseInt(parts[1]);

        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(checkHost, checkPort), HEARTBEAT_TIMEOUT_MS);
            socket.setSoTimeout(HEARTBEAT_TIMEOUT_MS);

            // Send a simple ping command
            var out = socket.getOutputStream();
            var in = socket.getInputStream();

            OpMsg pingMsg = new OpMsg();
            pingMsg.setMessageId(msgId.incrementAndGet());
            pingMsg.setFirstDoc(Doc.of("ping", 1, "$db", "admin"));
            out.write(pingMsg.bytes());
            out.flush();

            // Try to read response
            byte[] header = new byte[16];
            int read = in.read(header);
            return read > 0;
        } catch (Exception e) {
            log.debug("Host {} not reachable: {}", hostPort, e.getMessage());
            return false;
        }
    }

    private void initiateElection() {
        // Find the highest priority host that is alive (excluding current primary)
        String newPrimary = null;
        int highestPrio = Integer.MIN_VALUE;

        for (var entry : hostPriorities.entrySet()) {
            String candidate = entry.getKey();
            int prio = entry.getValue();

            // Skip the failed primary
            if (candidate.equals(primaryHost)) {
                continue;
            }

            // Check if this candidate has higher priority
            if (prio > highestPrio) {
                // Check if candidate is alive (or it's us)
                if (candidate.equals(host + ":" + port) || checkHostAlive(candidate)) {
                    highestPrio = prio;
                    newPrimary = candidate;
                }
            }
        }

        if (newPrimary == null) {
            log.error("No eligible candidate for new primary found");
            return;
        }

        String myAddress = host + ":" + port;
        if (newPrimary.equals(myAddress)) {
            // We are the new primary!
            if (initialSyncDone) {
                log.info("This node ({}) is becoming the new primary", myAddress);
                becomesPrimary();
            } else {
                log.info("Election result favors this node, but initial sync is not complete yet - remaining secondary");
            }
        } else {
            // Another node should become primary
            log.info("Node {} should become the new primary (higher priority)", newPrimary);
            primaryHost = newPrimary;
        }
    }

    private void becomesPrimary() {
        log.info("Promoting {} to primary", host + ":" + port);
        primary = true;
        primaryHost = host + ":" + port;
        initialSyncDone = true;

        // Stop replication - we're now the primary
        replicationStarted = false;
    }

    public void incoming(Socket s) {
        log.info("handling incoming connection...{}", executor.getPoolSize());

        try {
            s.setSoTimeout(0);
            s.setTcpNoDelay(true);
            s.setKeepAlive(true);
            // Use buffered streams for better I/O performance
            var in = new java.io.BufferedInputStream(s.getInputStream(), 65536);
            var out = new java.io.BufferedOutputStream(s.getOutputStream(), 65536);
            int id = 0;
            //            OpMsg r = new OpMsg();
            //            r.setFlags(2);
            //            r.setMessageId(msgId.incrementAndGet());
            //            r.setResponseTo(id);
            var answer = getHelloResult().toMsg();

            //            r.setFirstDoc(answer);
            //            log.info("flush...");
            //            out.write(r.bytes());
            //            out.flush();
            //            log.info("Sent hello result");
            while (s.isConnected()) {
                log.debug("Thread {} waiting for incoming message", Thread.currentThread().getId());
                var msg = WireProtocolMessage.parseFromStream(in);
                log.debug("---> Thread {} got message", Thread.currentThread().getId());

                //probably closed
                if (msg == null) {
                    log.info("Null");
                    break;
                }

                // log.debug("got incoming msg: " + msg.getClass().getSimpleName());
                Map<String, Object> doc = null;

                if (msg instanceof OpQuery) {
                    var q = (OpQuery) msg;
                    id = q.getMessageId();
                    doc = q.getDoc();

                    if (doc.containsKey("ismaster") || doc.containsKey("isMaster")) {
                        // ismaster via OpQuery (legacy)
                        log.debug("OpQuery->isMaster");
                        var r = new OpReply();
                        r.setFlags(2);
                        r.setMessageId(msgId.incrementAndGet());
                        r.setResponseTo(id);
                        r.setNumReturned(1);
                        var res = getHelloResult();
                        var resMsg = res.toMsg();
                        // Add MorphiumServer-specific fields for client detection
                        resMsg.put("morphiumServer", true);
                        resMsg.put("inMemoryBackend", true);
                        log.debug("Sending isMaster response via OpReply: {}", resMsg);
                        r.setDocuments(Arrays.asList(resMsg));

                        if (compressorId != OpCompressed.COMPRESSOR_NOOP) {
                            OpCompressed cmp = new OpCompressed();
                            cmp.setMessageId(r.getMessageId());
                            cmp.setResponseTo(id);
                            cmp.setOriginalOpCode(r.getOpCode());
                            cmp.setCompressorId(compressorId);
                            byte[] originalPayload = r.getPayload();
                            cmp.setUncompressedSize(originalPayload.length);
                            cmp.setCompressedMessage(originalPayload);
                            byte[] compressedBytes = cmp.bytes();
                            log.debug("Sending compressed OpReply: {} bytes (uncompressed: {} bytes)", compressedBytes.length, originalPayload.length);
                            out.write(compressedBytes);
                        } else {
                            byte[] replyBytes = r.bytes();
                            log.debug("Sending OpReply: {} bytes", replyBytes.length);
                            out.write(replyBytes);
                        }

                        out.flush();
                        log.debug("Sent isMaster result via OpQuery");
                        continue;
                    }

                    var r = new OpReply();
                    Doc d = Doc.of("$err", "OP_QUERY is no longer supported. The client driver may require an upgrade.", "code", 5739101, "ok", 0.0);
                    r.setFlags(2);
                    r.setMessageId(msgId.incrementAndGet());
                    r.setResponseTo(id);
                    r.setNumReturned(1);
                    r.setDocuments(Arrays.asList(d));
                    out.write(r.bytes());
                    out.flush();
                    log.debug("Sent out error because OPQuery not allowed anymore!");
                    log.debug(Utils.toJsonString(doc));
                    // Thread.sleep(1000);
                    continue;
                } else if (msg instanceof OpMsg) {
                    var m = (OpMsg) msg;
                    doc = ((OpMsg) msg).getFirstDoc();
                    log.debug("Message flags: " + m.getFlags());
                    id = m.getMessageId();
                }

                log.debug("Incoming " + Utils.toJsonString(doc));
                String cmd = doc.keySet().stream().findFirst().get();
                log.debug("Handling command " + cmd);
                OpMsg reply = new OpMsg();
                reply.setResponseTo(msg.getMessageId());
                reply.setMessageId(msgId.incrementAndGet());

                switch (cmd) {
                    case "getCmdLineOpts":
                        answer = Doc.of("argv", List.of(), "parsed", Map.of());
                        break;

                    case "buildInfo":
                        answer = Doc.of("version", "5.0.0-ALPHA", "buildEnvironment", Doc.of("distarch", "java", "targetarch", "java"));
                        answer.put("ok", 1.0);
                        reply.setFirstDoc(answer);
                        break;

                    case "ismaster":
                    case "isMaster":
                    case "hello":
                        log.debug("OpMsg->hello/ismaster");
                        answer = getHelloResult().toMsg();
                        // Add MorphiumServer-specific fields for client detection
                        answer.put("morphiumServer", true);
                        answer.put("inMemoryBackend", true);
                        log.debug("Hello response: {}", answer);
                        break;

                    case "getFreeMonitoringStatus":
                        answer = Doc.of("state", "disabled", "message", "", "url", "", "userReminder", "", "ok", 1.0);
                        break;

                    case "ping":
                        answer = Doc.of("ok", 1.0);
                        break;

                    case "listDatabases":
                        // Return list of databases from the in-memory driver
                        List<Map<String, Object>> dbList = new ArrayList<>();
                        for (String dbName : drv.listDatabases()) {
                            dbList.add(Doc.of("name", dbName, "sizeOnDisk", 0, "empty", false));
                        }
                        answer = Doc.of("databases", dbList, "totalSize", 0, "ok", 1.0);
                        break;

                    case "endSessions":
                        // Session management - just acknowledge since we don't track sessions
                        answer = Doc.of("ok", 1.0);
                        break;

                    case "startSession":
                        // Session management - return a dummy session
                        answer = Doc.of("id", Map.of("id", java.util.UUID.randomUUID()), "timeoutMinutes", 30, "ok", 1.0);
                        break;

                    case "refreshSessions":
                        // Session refresh - just acknowledge
                        answer = Doc.of("ok", 1.0);
                        break;

                    case "abortTransaction":
                        // Handle transaction abort with session-based tracking
                        String abortSessionId = extractSessionId(doc);
                        handleAbortTransaction(abortSessionId);
                        answer = Doc.of("ok", 1.0);
                        break;

                    case "commitTransaction":
                        // Handle transaction commit with session-based tracking
                        String commitSessionId = extractSessionId(doc);
                        handleCommitTransaction(commitSessionId);
                        answer = Doc.of("ok", 1.0);
                        break;

                    case "getMore":
                        // For change streams, getMore should wait for data from the queue
                        long getMoreCursorId = ((Number) doc.get("getMore")).longValue();
                        int maxTimeMs = doc.containsKey("maxTimeMS") ? ((Number) doc.get("maxTimeMS")).intValue() : 30000;
                        String getMoreCollection = (String) doc.get("collection");
                        String getMoreDb = (String) doc.get("$db");

                        var queue = watchCursors.get(getMoreCursorId);

                        if (queue != null) {
                            // This is a change stream cursor - handle with blocking queue
                            List<Map<String, Object>> batch = new ArrayList<>();
                            try {
                                // Wait for first event with timeout
                                Map<String, Object> event = queue.poll(maxTimeMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                                if (event != null) {
                                    batch.add(event);
                                    // Drain any additional events that are ready
                                    queue.drainTo(batch, 99);
                                    log.debug("getMore returning {} events for cursor {}", batch.size(), getMoreCursorId);
                                }
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                            var getMoreCursor = Doc.of("nextBatch", batch, "ns", getMoreDb + "." + getMoreCollection, "id", getMoreCursorId);
                            answer = Doc.of("ok", 1.0, "cursor", getMoreCursor);
                        } else {
                            // Not a change stream cursor - forward to InMemoryDriver for regular query cursors
                            log.debug("getMore for regular cursor {} - forwarding to driver", getMoreCursorId);
                            int getMoreMsgId = drv.runCommand(new GenericCommand(drv).fromMap(doc));
                            var crs = drv.readSingleAnswer(getMoreMsgId);
                            answer = Doc.of("ok", 1.0);
                            if (crs != null) answer.putAll(crs);
                        }
                        break;

                    case "replSetStepDown":
                        stepDown();
                        answer = Doc.of("ok", 1.0, "primary", primaryHost);
                        break;

                    case "getLog":
                        if (doc.get(cmd).equals("startupWarnings")) {
                            answer = Doc.of("totalLinesWritten", 0, "log", List.of(), "ok", 1.0);
                            break;
                        } else {
                            log.warn("Unknown log " + doc.get(cmd));
                            answer = Doc.of("ok", 0, "errmsg", "unknown logr");
                        }

                        break;

                    case "getParameter":
                        if (Integer.valueOf(1).equals(doc.get("featureCompatibilityVersion"))) {
                            answer = Doc.of("version", "5.0", "ok", 1.0);
                        } else {
                            answer = Doc.of("ok", 0, "errmsg", "no such parameter");
                        }

                        break;

                    default:
                        try {
                            // Reject writes to secondaries, unless it's a replication command from primary
                            boolean isReplicationFromPrimary = Boolean.TRUE.equals(doc.get("$fromPrimary"));
                            if (!primary && isWriteCommand(cmd) && !isReplicationFromPrimary) {
                                answer = Doc.of("ok", 0.0, "errmsg", "not primary", "code", 10107, "codeName", "NotWritablePrimary");
                                break;
                            }

                            // Forward listIndexes and listCollections to primary when running as secondary
                            // This is needed because indexes and collection metadata (capped status) are only created on primary
                            if (!primary && (cmd.equalsIgnoreCase("listindexes") || cmd.equalsIgnoreCase("listcollections")) && primaryHost != null) {
                                answer = forwardCommandToPrimary(doc, cmd);
                                if (answer != null) {
                                    break;
                                }
                                // Fall through to local execution if forwarding failed
                            }

                            // Forward ALL read commands to primary for strong consistency
                            // This ensures secondaries always return the same data as primary
                            if (!primary && READ_COMMANDS.contains(cmd.toLowerCase()) && primaryHost != null) {
                                log.debug("Forwarding {} to primary {} for consistency", cmd, primaryHost);
                                answer = forwardCommandToPrimary(doc, cmd);
                                if (answer != null) {
                                    break;
                                }
                                log.warn("Forwarding {} failed, falling back to local", cmd);
                                // Fall through to local execution if forwarding failed
                            }

                            AtomicInteger msgid = new AtomicInteger(0);

                            if (doc.containsKey("pipeline") && ((Map)((List)doc.get("pipeline")).get(0)).containsKey("$changeStream")) {
                                WatchCommand wcmd = new WatchCommand(drv).fromMap(doc);
                                final long myCursorId = cursorId.incrementAndGet();

                                // Create queue for this cursor's events
                                var eventQueue = new java.util.concurrent.LinkedBlockingQueue<Map<String, Object>>();
                                watchCursors.put(myCursorId, eventQueue);
                                log.info("Created watch cursor {} for {}.{}", myCursorId, wcmd.getDb(), wcmd.getColl());

                                // Set up callback to queue events
                                wcmd.setCb(new DriverTailableIterationCallback() {
                                    @Override
                                    public void incomingData(Map<String, Object> data, long dur) {
                                        log.info("Watch callback: queueing event for cursor {} - data: {}", myCursorId, data);
                                        eventQueue.offer(data);
                                    }

                                    @Override
                                    public boolean isContinued() {
                                        return running && watchCursors.containsKey(myCursorId);
                                    }
                                });

                                // Run the watch in a separate thread
                                Thread.ofVirtual().name("watch-" + myCursorId).start(() -> {
                                    try {
                                        log.info("Starting watch thread for cursor {}, db={}, coll={}", myCursorId, wcmd.getDb(), wcmd.getColl());
                                        drv.runCommand(wcmd);
                                        log.info("Watch thread ended for cursor {}", myCursorId);
                                    } catch (Exception e) {
                                        log.error("Watch command error for cursor {}", myCursorId, e);
                                    } finally {
                                        watchCursors.remove(myCursorId);
                                    }
                                });

                                // Send initial empty response
                                var initialCursor = Doc.of("firstBatch", List.of(), "ns", wcmd.getDb() + "." + wcmd.getColl(), "id", myCursorId);
                                answer = Doc.of("ok", 1.0, "cursor", initialCursor);
                                // Skip normal command processing
                            } else {
                                // Set up transaction context if this command is part of a transaction
                                String txnSessionId = setupTransactionContext(doc);

                                // Log database drop commands at INFO level for debugging test isolation issues
                                if (cmd.equalsIgnoreCase("dropDatabase") || cmd.equalsIgnoreCase("drop")) {
                                    log.info("MorphiumServer[{}:{}]: Processing {} command for db={}, coll={}, drvHash={}",
                                             host, port, cmd, doc.get("$db"), doc.get(cmd), System.identityHashCode(drv));
                                    log.info("MorphiumServer[{}:{}]: Databases BEFORE drop: {}", host, port, drv.listDatabases());
                                }
                                log.debug("MorphiumServer: Executing command '{}' with filter: {}", cmd, doc.get("filter"));
                                msgid.set(drv.runCommand(new GenericCommand(drv).fromMap(doc)));
                                log.debug("MorphiumServer: Command returned msgid={}, reading answer...", msgid.get());
                                var crs = drv.readSingleAnswer(msgid.get());
                                log.debug("MorphiumServer: Got answer: {}", crs != null ? "cursor with " + (crs.containsKey("cursor") ? "data" : "no cursor") : "null");
                                answer = Doc.of("ok", 1.0);
                                if (crs != null) answer.putAll(crs);
                                // Log after drop
                                if (cmd.equalsIgnoreCase("dropDatabase") || cmd.equalsIgnoreCase("drop")) {
                                    log.info("MorphiumServer[{}:{}]: Databases AFTER drop: {}", host, port, drv.listDatabases());
                                }

                                // Synchronous replication: if primary and this is a write command, replicate to secondaries
                                // Skip if this is already a replication from primary (to avoid infinite loop)
                                if (primary && WRITE_COMMANDS.contains(cmd.toLowerCase()) && !Boolean.TRUE.equals(doc.get("$fromPrimary"))) {
                                    replicateToSecondaries(doc);
                                }
                            }
                        } catch (Exception e) {
                            // Check if it's a duplicate key error for write commands
                            // Need to traverse the entire cause chain to find the actual error
                            String duplicateKeyMsg = null;
                            Throwable current = e;
                            while (current != null) {
                                String errMsg = current.getMessage();
                                if (errMsg != null && errMsg.contains("Duplicate _id")) {
                                    duplicateKeyMsg = errMsg;
                                    break;
                                }
                                current = current.getCause();
                            }

                            if (duplicateKeyMsg != null) {
                                // Return proper MongoDB duplicate key error format
                                var writeError = Doc.of("index", 0, "code", 11000, "errmsg", "E11000 duplicate key error: " + duplicateKeyMsg);
                                answer = Doc.of("ok", 1.0, "n", 0, "writeErrors", List.of(writeError));
                                log.debug("Duplicate key error for command {}: {}", cmd, duplicateKeyMsg);
                            } else {
                                // Find the deepest cause message or use the original
                                String actualError = e.getMessage();
                                current = e;
                                while (current.getCause() != null) {
                                    current = current.getCause();
                                    if (current.getMessage() != null) {
                                        actualError = current.getMessage();
                                    }
                                }
                                answer = Doc.of("ok", 0, "errmsg", actualError != null ? actualError : "Command failed: " + cmd);
                                log.error("Error executing command {}: {}", cmd, actualError, e);
                            }
                        }

                        break;
                }

                answer.put("$clusterTime", Doc.of("clusterTime", new MongoTimestamp(System.currentTimeMillis())));
                answer.put("operationTime", new MongoTimestamp(System.currentTimeMillis()));
                reply.setFirstDoc(answer);
                log.debug("Final response being sent: {}", answer);

                if (compressorId != OpCompressed.COMPRESSOR_NOOP) {
                    OpCompressed cmsg = new OpCompressed();
                    cmsg.setMessageId(reply.getMessageId());
                    cmsg.setResponseTo(reply.getResponseTo());
                    cmsg.setCompressorId(compressorId);
                    cmsg.setOriginalOpCode(reply.getOpCode());
                    byte[] originalPayload = reply.getPayload();
                    cmsg.setUncompressedSize(originalPayload.length);
                    cmsg.setCompressedMessage(originalPayload);
                    var b = cmsg.bytes();
                    log.debug("Server sending {} bytes (compressed), uncompressed: {} bytes, responseTo: {}", b.length, originalPayload.length, cmsg.getResponseTo());
                    out.write(b);
                } else {
                    var b = reply.bytes();
                    log.debug("Server sending {} bytes, responseTo: {}", b.length, reply.getResponseTo());
                    out.write(b);
                }

                out.flush();
                log.debug("Sent answer for cmd: {}", cmd);
            }

            s.close();
            in.close();
            out.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
            // e.printStackTrace();
            // System.exit(0);
        }

        log.info("Thread finished!");
        s = null;
    }

    public void terminate() {
        running = false;

        // Stop dump scheduler
        stopDumpScheduler();

        // Final dump on shutdown if persistence is configured
        if (dumpDirectory != null) {
            try {
                log.info("Performing final dump before shutdown...");
                int count = drv.dumpAllToDirectory(dumpDirectory);
                log.info("Final dump completed: {} databases saved", count);
            } catch (Exception e) {
                log.error("Failed to perform final dump: {}", e.getMessage(), e);
            }
        }

        if (serverSocket != null) {
            try {
                serverSocket.close();
                serverSocket = null;
            } catch (IOException e) {
                //swallow
            }
        }

        executor.shutdownNow();
        executor = null;
        cleanupSecondaryDrivers();
    }

    public void stepDown() {
        if (!primary) {
            return;
        }

        log.info("Stepping down {}", host + ":" + port);

        // For standalone servers (no replica set or single host), stepDown is essentially a no-op
        // since there's no other node to become primary. We briefly step down and immediately
        // become primary again to simulate MongoDB's behavior for single-member replica sets.
        boolean isStandalone = rsName == null || rsName.isEmpty() || hosts == null || hosts.size() <= 1;

        if (isStandalone) {
            log.info("Standalone server - immediately becoming primary again after stepDown");
            // Reset priority to normal and stay as primary
            hostPriorities.put(host + ":" + port, 0);
            primaryHost = host + ":" + port;
            primary = true;
            return;
        }

        primary = false;
        hostPriorities.put(host + ":" + port, Integer.MIN_VALUE);
        primaryHost = findPrimaryHost(host + ":" + port);
        replicationStarted = false;
        startReplicaReplication();
    }

    private String findPrimaryHost(String exclude) {
        String best = null;
        int bestPrio = Integer.MIN_VALUE;

        for (var e : hostPriorities.entrySet()) {
            if (exclude != null && exclude.equals(e.getKey())) {
                continue;
            }

            if (e.getValue() != null && e.getValue() > bestPrio) {
                bestPrio = e.getValue();
                best = e.getKey();
            }
        }

        if (best == null && hosts != null && !hosts.isEmpty()) {
            best = hosts.get(0);
        }

        return best == null ? host + ":" + port : best;
    }

    public boolean isPrimary() {
        return primary;
    }

    public String getPrimaryHost() {
        return primaryHost;
    }

    public boolean isRunning() {
        return running;
    }

    public void configureReplicaSet(String name, List<String> hostList, Map<String, Integer> priorities) {
        rsName = name == null ? "" : name;
        hosts = hostList == null ? new ArrayList<>() : hostList;

        if (priorities != null) {
            hostPriorities.clear();
            hostPriorities.putAll(priorities);
        }

        primaryHost = findPrimaryHost(null);
        priority = hostPriorities.getOrDefault(host + ":" + port, 0);
        primary = rsName.isEmpty() || (host + ":" + port).equals(primaryHost);
        initialSyncDone = primary || rsName.isEmpty();
        drv.setReplicaSet(!rsName.isEmpty());
        drv.setReplicaSetName(rsName);
        drv.setHostSeed(hosts);
    }
    private boolean isWriteCommand(String cmd) {
        return WRITE_COMMANDS.contains(cmd.toLowerCase());
    }

    /**
     * Get or create a cached connection to the primary for forwarding.
     */
    private MongoConnection getForwardingConnection() {
        // Already inside synchronized block from caller, so just check the cached values
        if (forwardingConnection != null && forwardingDriver != null) {
            return forwardingConnection;
        }
        try {
            // Close old driver if exists
            if (forwardingDriver != null) {
                try { forwardingDriver.close(); } catch (Exception ignore) {}
                forwardingDriver = null;
                forwardingConnection = null;
            }
            log.debug("Creating forwarding connection to primary {}", primaryHost);
            forwardingDriver = new SingleMongoConnectDriver();
            forwardingDriver.setHostSeed(primaryHost);
            forwardingDriver.setConnectionTimeout(5000);
            forwardingDriver.setReadTimeout(30000);
            forwardingDriver.connect();
            var conn = forwardingDriver.getPrimaryConnection(null);
            if (conn == null) {
                log.warn("getPrimaryConnection returned null for {}", primaryHost);
                forwardingDriver.close();
                forwardingDriver = null;
                return null;
            }
            forwardingConnection = conn;
            log.info("Created forwarding connection to primary {}", primaryHost);
            return forwardingConnection;
        } catch (Exception e) {
            log.warn("Failed to create forwarding connection: {} - {}", e.getClass().getSimpleName(), e.getMessage());
            if (forwardingDriver != null) {
                try { forwardingDriver.close(); } catch (Exception ignore) {}
                forwardingDriver = null;
            }
            forwardingConnection = null;
            return null;
        }
    }

    /**
     * Reset the forwarding connection (called on error to force reconnect).
     */
    private void resetForwardingConnection() {
        synchronized (forwardingLock) {
            if (forwardingDriver != null) {
                try { forwardingDriver.close(); } catch (Exception ignore) {}
            }
            forwardingDriver = null;
            forwardingConnection = null;
        }
    }

    /**
     * Forward a command to primary server.
     * Uses cached connection for efficiency. Synchronized to prevent concurrent access issues.
     * Returns null on failure (caller should fall back to local execution).
     */
    private Map<String, Object> forwardCommandToPrimary(Map<String, Object> doc, String cmdName) {
        if (primaryHost == null || primaryHost.isEmpty()) {
            return null;
        }

        // Synchronize to prevent multiple threads from using the connection simultaneously
        synchronized (forwardingLock) {
            try {
                var conn = getForwardingConnection();
                if (conn == null) {
                    log.warn("Could not get forwarding connection for {}", cmdName);
                    return null;
                }

                GenericCommand cmd = new GenericCommand(conn).fromMap(doc);
                conn.sendCommand(cmd);

                // Read the full response without cursor extraction
                var reply = conn.readNextMessage(forwardingDriver.getReadTimeout());

                if (reply == null) {
                    log.warn("Forwarding {} got null reply, resetting connection", cmdName);
                    resetForwardingConnection();
                    return null;
                }
                if (reply.getFirstDoc() == null) {
                    log.warn("Forwarding {} got null firstDoc, resetting connection", cmdName);
                    resetForwardingConnection();
                    return null;
                }

                Map<String, Object> response = reply.getFirstDoc();
                // Log response details for debugging
                if (response.containsKey("cursor")) {
                    var cursor = (Map<?, ?>) response.get("cursor");
                    var batch = cursor.get("firstBatch");
                    if (batch instanceof List) {
                        log.debug("Forwarded {} to primary: cursor with {} results", cmdName, ((List<?>) batch).size());
                    }
                } else {
                    log.debug("Forwarded {} to primary successfully: {}", cmdName, response.keySet());
                }
                return response;
            } catch (Exception e) {
                log.warn("Failed to forward {} to primary: {} - {}", cmdName, e.getClass().getSimpleName(), e.getMessage());
                // Reset connection on error to force reconnect next time
                resetForwardingConnection();
            }
            return null;  // Return null to fall back to local execution
        }
    }

    // private boolean isWriteCommand(String cmd) {
    //     return List.of("insert", "update", "delete", "findandmodify", "findAndModify", "createIndexes", "create")
    //            .contains(cmd);
    // }

    /**
     * Synchronously replicate a write command to all secondaries.
     * Called by the primary after executing a write locally.
     * This ensures strong consistency - secondaries have the data before the client gets a response.
     */
    private void replicateToSecondaries(Map<String, Object> commandDoc) {
        if (!primary || hosts == null || hosts.size() <= 1) {
            return; // Not primary or no secondaries
        }

        String myHost = host + ":" + port;
        List<String> secondaries = new ArrayList<>();
        for (String h : hosts) {
            if (!h.equals(myHost) && !h.equals("localhost:" + port) && !h.equals("127.0.0.1:" + port)) {
                secondaries.add(h);
            }
        }

        if (secondaries.isEmpty()) {
            return;
        }

        // Replicate to each secondary in parallel
        List<java.util.concurrent.CompletableFuture<Void>> futures = new ArrayList<>();
        for (String secondary : secondaries) {
            futures.add(java.util.concurrent.CompletableFuture.runAsync(() -> {
                try {
                    replicateToSecondary(secondary, commandDoc);
                } catch (Exception e) {
                    log.warn("Failed to replicate to secondary {}: {}", secondary, e.getMessage());
                }
            }));
        }

        // Wait for all replications to complete (with timeout)
        try {
            java.util.concurrent.CompletableFuture.allOf(futures.toArray(new java.util.concurrent.CompletableFuture[0]))
                .get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Replication to secondaries did not complete in time: {}", e.getMessage());
        }
    }

    /**
     * Replicate a command to a specific secondary.
     * Creates a DIRECT connection (bypassing RS topology discovery) to ensure
     * the command goes to the secondary, not redirected back to primary.
     */
    private void replicateToSecondary(String secondaryHost, Map<String, Object> commandDoc) {
        SingleMongoConnection conn = null;
        SingleMongoConnectDriver tmpDriver = null;
        try {
            // Parse host:port
            String[] parts = secondaryHost.split(":");
            String secHost = parts[0];
            int secPort = parts.length > 1 ? Integer.parseInt(parts[1]) : 27017;

            // Create a temporary driver just for connection settings (timeouts, etc.)
            // We don't call driver.connect() to avoid RS topology discovery
            tmpDriver = new SingleMongoConnectDriver();
            tmpDriver.setConnectionTimeout(2000);
            tmpDriver.setReadTimeout(5000);

            // Create DIRECT connection to secondary - this bypasses RS topology discovery
            // which would otherwise redirect write commands to the primary
            conn = new SingleMongoConnection();
            conn.connect(tmpDriver, secHost, secPort);

            if (!conn.isConnected()) {
                log.warn("Could not establish direct connection to secondary {}", secondaryHost);
                return;
            }

            // Mark this as a replication command so secondary doesn't reject or forward it
            // Use LinkedHashMap to preserve key order - command name must be first key
            Map<String, Object> replicaDoc = new java.util.LinkedHashMap<>(commandDoc);
            replicaDoc.put("$fromPrimary", true);

            String firstKey = replicaDoc.keySet().iterator().next();
            log.info("Replicating command to {}: firstKey={}, keys={}", secondaryHost, firstKey, replicaDoc.keySet());

            int msgId = conn.sendCommand(new GenericCommand(conn).fromMap(replicaDoc));
            var response = conn.readSingleAnswer(msgId);

            if (response != null && response.get("ok") != null) {
                Object ok = response.get("ok");
                if (ok instanceof Number && ((Number) ok).doubleValue() >= 1.0) {
                    log.debug("Replicated to secondary {} successfully", secondaryHost);
                } else {
                    log.warn("Replication to {} returned ok={}, errmsg={}", secondaryHost, ok, response.get("errmsg"));
                }
            }
        } catch (Exception e) {
            log.warn("Error replicating to secondary {}: {}", secondaryHost, e.getMessage());
        } finally {
            if (conn != null) {
                try { conn.close(); } catch (Exception ignore) {}
            }
            if (tmpDriver != null) {
                try { tmpDriver.close(); } catch (Exception ignore) {}
            }
        }
    }

    /**
     * Clean up secondary driver connections.
     * Note: With the current implementation, connections are created per-request and closed immediately,
     * so this is a no-op. Kept for potential future optimizations.
     */
    private void cleanupSecondaryDrivers() {
        // Currently connections are created and closed per-request, so nothing to clean up
    }

    public void startReplicaReplication() {
        if (replicationStarted || primary || hosts == null || hosts.isEmpty()) {
            return;
        }

        initialSyncDone = false;
        replicationStarted = true;
        new Thread(() -> {
            while (running) {
                try {
                    String target = primaryHost != null ? primaryHost : hosts.get(0);
                    log.info("Starting replication from {}", target);
                    // Use longer timeouts for replication - 30s max wait
                    MorphiumConfig cfg = new MorphiumConfig("admin", 10, 10000, 30000);
                    cfg.setDriverName(SingleMongoConnectDriver.driverName);
                    cfg.setHostSeed(target);
                    Morphium morphium = new Morphium(cfg);

                    // Initial sync of existing databases
                    var databases = morphium.listDatabases();
                    log.info("Found {} databases on primary: {}", databases.size(), databases);

                    for (String db : databases) {
                        if (db.equals("admin") || db.equals("local") || db.equals("config")) {
                            log.debug("Skipping system database: {}", db);
                            continue;
                        }
                        log.info("Performing initial sync for database: {}", db);
                        initialSyncDatabase(morphium, db);
                    }

                    // NOTE: Ongoing replication is handled by synchronous replication.
                    // When the primary receives a write command, it forwards it to all secondaries
                    // before returning success to the client (see replicateToSecondaries method).
                    // This includes createIndexes, drop, etc. - all write commands are replicated.
                    // This provides strong consistency without the complexity of change streams.

                    // Close the morphium connection used for initial sync
                    morphium.close();

                    log.info("Initial sync and replication setup complete");
                    initialSyncDone = true;
                    break;
                } catch (Exception e) {
                    log.error("Could not setup replication, retrying", e);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }).start();
    }

    private void initialSyncDatabase(Morphium sourceMorphium, String db) {
        try {
            var con = sourceMorphium.getDriver().getPrimaryConnection(null);

            // List all collections in this database
            var listCmd = new de.caluga.morphium.driver.commands.ListCollectionsCommand(con)
            .setDb(db)
            .setNameOnly(false);  // Need full info to get collection names
            var collections = listCmd.execute();
            // Connection is released by execute()

            if (collections == null || collections.isEmpty()) {
                log.info("No collections in database {}", db);
                return;
            }

            log.info("Found {} collections in database {}", collections.size(), db);

            for (Map<String, Object> collInfo : collections) {
                String collectionName = (String) collInfo.get("name");
                if (collectionName == null || collectionName.startsWith("system.")) {
                    continue;
                }

                // Check if collection is capped and sync capped metadata
                Map<String, Object> options = (Map<String, Object>) collInfo.get("options");
                if (options != null && Boolean.TRUE.equals(options.get("capped"))) {
                    int size = options.get("size") != null ? ((Number) options.get("size")).intValue() : 1000000;
                    int max = options.get("max") != null ? ((Number) options.get("max")).intValue() : 0;
                    log.info("Registering capped collection: {}.{} (size={}, max={})", db, collectionName, size, max);
                    drv.registerCappedCollection(db, collectionName, size, max);
                }

                log.info("Syncing collection: {}.{}", db, collectionName);
                syncCollection(sourceMorphium, db, collectionName);
            }
        } catch (Exception e) {
            log.error("Error during initial sync of database {}", db, e);
        }
    }

    private void syncCollection(Morphium sourceMorphium, String db, String collectionName) {
        try {
            var con = sourceMorphium.getDriver().getPrimaryConnection(null);

            // Fetch all documents from the source collection
            var findCmd = new de.caluga.morphium.driver.commands.FindCommand(con)
            .setDb(db)
            .setColl(collectionName)
            .setFilter(Map.of())
            .setBatchSize(1000);

            var cursor = findCmd.executeIterable(1000);

            int count = 0;
            int errors = 0;

            while (cursor.hasNext()) {
                Map<String, Object> doc = cursor.next();
                count++;

                try {
                    // Use upsert to handle duplicates gracefully
                    // This replaces the entire document if it exists
                    Object id = doc.get("_id");
                    if (id != null) {
                        drv.update(db, collectionName, Map.of("_id", id), null,
                                   Doc.of("$set", doc), false, true, null, null);
                    } else {
                        drv.insert(db, collectionName, List.of(doc), null);
                    }
                } catch (MorphiumDriverException e) {
                    errors++;
                    log.warn("Error syncing document in {}.{}: {}", db, collectionName, e.getMessage());
                }
            }

            log.info("Synced {} documents from {}.{} ({} errors)", count, db, collectionName, errors);
            cursor.close();

            // Sync indexes for this collection
            syncIndexes(sourceMorphium, db, collectionName);
        } catch (Exception e) {
            log.error("Error syncing collection {}.{}", db, collectionName, e);
        }
    }

    /**
     * Sync indexes from primary to this secondary (initial sync - no deletion).
     * This ensures secondaries have the same indexes for efficient query execution.
     */
    private void syncIndexes(Morphium sourceMorphium, String db, String collectionName) {
        try {
            var con = sourceMorphium.getDriver().getPrimaryConnection(null);

            // List all indexes from primary
            var listIdxCmd = new de.caluga.morphium.driver.commands.ListIndexesCommand(con)
                .setDb(db)
                .setColl(collectionName);
            var indexes = listIdxCmd.execute();

            if (indexes == null || indexes.isEmpty()) {
                log.debug("No indexes to sync for {}.{}", db, collectionName);
                return;
            }

            int synced = 0;
            for (var idx : indexes) {
                // Skip the _id index - it's created automatically
                String idxName = idx.getName();
                if (idxName != null && idxName.equals("_id_")) {
                    continue;
                }

                Map<String, Object> key = idx.getKey();
                if (key == null || key.isEmpty()) {
                    continue;
                }

                try {
                    // Build options map from IndexDescription
                    Map<String, Object> options = new java.util.HashMap<>();
                    if (idx.getName() != null) options.put("name", idx.getName());
                    if (idx.getUnique() != null && idx.getUnique()) options.put("unique", true);
                    if (idx.getSparse() != null && idx.getSparse()) options.put("sparse", true);
                    if (idx.getExpireAfterSeconds() != null) options.put("expireAfterSeconds", idx.getExpireAfterSeconds());
                    if (idx.getWeights() != null) options.put("weights", idx.getWeights());

                    drv.createIndex(db, collectionName, key, options);
                    synced++;
                    log.debug("Synced index {} on {}.{}", idxName, db, collectionName);
                } catch (Exception e) {
                    log.warn("Failed to create index {} on {}.{}: {}", idxName, db, collectionName, e.getMessage());
                }
            }

            if (synced > 0) {
                log.info("Synced {} indexes for {}.{}", synced, db, collectionName);
            }
        } catch (Exception e) {
            log.warn("Error syncing indexes for {}.{}: {}", db, collectionName, e.getMessage());
        }
    }
}
