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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.Utils;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.bson.MongoTimestamp;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.HelloResult;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
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

    private ThreadPoolExecutor executor;
    private Thread heartbeatThread;
    private static final int HEARTBEAT_INTERVAL_MS = 2000;
    private static final int HEARTBEAT_TIMEOUT_MS = 5000;
    private boolean running = true;
    private ServerSocket serverSocket;
    private int compressorId;
    private String rsName = "";
    private String hostSeed = "";
    private List<String> hosts = new ArrayList<>();
    private Map<String, Integer> hostPriorities = new java.util.concurrent.ConcurrentHashMap<>();
    private boolean primary = true;
    private String primaryHost;
    private int priority = 0;
    private final List<ChangeStreamMonitor> replicationMonitors = new ArrayList<>();
    private volatile boolean replicationStarted = false;
    private volatile boolean initialSyncDone = true;

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
        log.info("Opening port " + port);
        serverSocket = new ServerSocket(port);
        drv.setHostSeed(host + ":" + port);
        drv.setReplicaSet(rsName != null && !rsName.isEmpty());
        drv.setReplicaSetName(rsName == null ? "" : rsName);
        executor.prestartAllCoreThreads();
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
                    log.info("Peers detected - stepping down to secondary to perform initial sync");
                    primary = false;
                    initialSyncDone = false;
                }

                startReplicaReplication();
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
        for (var monitor : replicationMonitors) {
            monitor.terminate();
        }
        replicationMonitors.clear();
        replicationStarted = false;
    }

    public void incoming(Socket s) {
        log.info("handling incoming connection...{}", executor.getPoolSize());

        try {
            s.setSoTimeout(0);
            s.setTcpNoDelay(true);
            s.setKeepAlive(true);
            var in = s.getInputStream();
            var out = s.getOutputStream();
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
                        log.debug("Sending isMaster response via OpReply: {}", res.toMsg());
                        r.setDocuments(Arrays.asList(res.toMsg()));

                        if (compressorId != OpCompressed.COMPRESSOR_NOOP) {
                            OpCompressed cmp = new OpCompressed();
                            cmp.setMessageId(r.getMessageId());
                            cmp.setResponseTo(id);
                            cmp.setOriginalOpCode(r.getOpCode());
                            cmp.setCompressorId(compressorId);
                            byte[] originalPayload = r.getPayload();
                            cmp.setUncompressedSize(originalPayload.length);
                            cmp.setCompressedMessage(originalPayload);
                            log.debug("Sending compressed OpReply: {} bytes (uncompressed: {} bytes)", cmp.bytes().length, originalPayload.length);
                            out.write(cmp.bytes());
                        } else {
                            log.debug("Sending OpReply: {} bytes", r.bytes().length);
                            out.write(r.bytes());
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
                        log.debug("Hello response: {}", answer);
                        break;

                    case "getFreeMonitoringStatus":
                        answer = Doc.of("state", "disabled", "message", "", "url", "", "userReminder", "", "ok", 1.0);
                        break;

                    case "ping":
                        answer = Doc.of("ok", 1.0);
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
                    case "commitTransaction":
                        // Transaction commands - just acknowledge since we don't support transactions
                        answer = Doc.of("ok", 1.0);
                        break;

                    case "getMore":
                        // For change streams, getMore should wait for data from the queue
                        long getMoreCursorId = ((Number) doc.get("getMore")).longValue();
                        int maxTimeMs = doc.containsKey("maxTimeMS") ? ((Number) doc.get("maxTimeMS")).intValue() : 30000;
                        String getMoreCollection = (String) doc.get("collection");
                        String getMoreDb = (String) doc.get("$db");

                        var queue = watchCursors.get(getMoreCursorId);
                        List<Map<String, Object>> batch = new ArrayList<>();

                        if (queue != null) {
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
                        }

                        var getMoreCursor = Doc.of("nextBatch", batch, "ns", getMoreDb + "." + getMoreCollection, "id", getMoreCursorId);
                        answer = Doc.of("ok", 1.0, "cursor", getMoreCursor);
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
                            if (!primary && isWriteCommand(cmd)) {
                                answer = Doc.of("ok", 0.0, "errmsg", "not primary", "code", 10107, "codeName", "NotWritablePrimary");
                                break;
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
                                msgid.set(drv.runCommand(new GenericCommand(drv).fromMap(doc)));
                                var crs = drv.readSingleAnswer(msgid.get());
                                answer = Doc.of("ok", 1.0);
                                if (crs != null) answer.putAll(crs);
                            }
                        } catch (Exception e) {
                            answer = Doc.of("ok", 0, "errmsg", "no such command: '{}" + cmd + "'");
                            log.error("No such command {}", cmd, e);
                            // log.warn("errror running command " + cmd, e);
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
        replicationMonitors.forEach(ChangeStreamMonitor::terminate);
    }

    public void stepDown() {
        if (!primary) {
            return;
        }

        log.info("Stepping down {}", host + ":" + port);
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
        return List.of("insert", "update", "delete", "findandmodify", "findAndModify", "createIndexes", "create")
                   .contains(cmd);
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

                    // Start a single cluster-wide change stream for ALL databases
                    // Using admin database with null collection watches everything
                    log.info("Setting up cluster-wide change stream on admin database");
                    ChangeStreamMonitor mtr = new ChangeStreamMonitor(morphium, null, true);
                    mtr.addListener((evt)-> {
                        log.info("CHANGE STREAM EVENT RECEIVED: type={}, db={}, coll={}",
                                evt.getOperationType(), evt.getDbName(), evt.getCollectionName());
                        try {
                            String db = evt.getDbName();
                            String coll = evt.getCollectionName();

                            // Skip system databases
                            if (db == null || db.equals("admin") || db.equals("local") || db.equals("config")) {
                                return true;
                            }

                            log.info("Replication event: {} on {}.{}", evt.getOperationType(), db, coll);

                            if (evt.getOperationType().equals("insert")) {
                                var fullDoc = evt.getFullDocument();
                                if (fullDoc == null) {
                                    log.warn("Insert event has null fullDocument for {}.{}", db, coll);
                                } else {
                                    drv.insert(db, coll, List.of(fullDoc), null);
                                }
                            } else if (evt.getOperationType().equals("delete")) {
                                drv.delete(db, coll, Map.of("_id", evt.getDocumentKey()), null, false, null, null);
                            } else if (evt.getOperationType().equals("update")) {
                                Map<String, Object> updated = evt.getUpdatedFields();
                                if (updated == null || updated.isEmpty()) {
                                    log.warn("Update event has no updated fields for {}.{}", db, coll);
                                } else {
                                    drv.update(db, coll, Map.of("_id", evt.getDocumentKey()), null, Doc.of("$set", updated), false, false, null, null);
                                }
                            }
                        } catch (MorphiumDriverException e) {
                            log.error("Replication error: {}", e.getMessage());
                        }

                        return true;
                    });
                    mtr.start();
                    log.info("Started cluster-wide change stream monitor");
                    replicationMonitors.add(mtr);

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
        } catch (Exception e) {
            log.error("Error syncing collection {}.{}", db, collectionName, e);
        }
    }
}
