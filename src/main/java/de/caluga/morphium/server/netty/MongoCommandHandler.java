package de.caluga.morphium.server.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumTransactionContext;
import de.caluga.morphium.driver.bson.MongoTimestamp;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.server.ReplicationCoordinator;
import de.caluga.morphium.driver.wire.HelloResult;
import de.caluga.morphium.driver.wireprotocol.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Netty handler for processing MongoDB commands.
 * This is the async equivalent of MorphiumServer.incoming().
 */
public class MongoCommandHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(MongoCommandHandler.class);

    // Channel attributes for per-connection state
    private static final AttributeKey<MorphiumTransactionContext> TX_CONTEXT_KEY =
            AttributeKey.valueOf("txContext");
    private static final AttributeKey<String> SESSION_ID_KEY =
            AttributeKey.valueOf("sessionId");

    private static final Set<String> WRITE_COMMANDS = Set.of(
            "insert", "update", "delete", "findandmodify",
            "createindexes", "create", "drop", "dropindexes", "dropdatabase", "bulkwrite"
    );

    private final InMemoryDriver driver;
    private final WatchCursorManager cursorManager;
    private final AtomicInteger msgId;
    private final String host;
    private final int port;
    private final String rsName;
    private final List<String> hosts;
    private final boolean primary;
    private final String primaryHost;
    private final int compressorId;
    private final ReplicationCoordinator replicationCoordinator;

    public MongoCommandHandler(InMemoryDriver driver, WatchCursorManager cursorManager,
                               AtomicInteger msgId, String host, int port, String rsName,
                               List<String> hosts, boolean primary, String primaryHost,
                               int compressorId, ReplicationCoordinator replicationCoordinator) {
        this.driver = driver;
        this.cursorManager = cursorManager;
        this.msgId = msgId;
        this.host = host;
        this.port = port;
        this.rsName = rsName;
        this.hosts = hosts;
        this.primary = primary;
        this.primaryHost = primaryHost;
        this.compressorId = compressorId;
        this.replicationCoordinator = replicationCoordinator;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof WireProtocolMessage)) {
            ctx.fireChannelRead(msg);
            return;
        }

        WireProtocolMessage wireMsg = (WireProtocolMessage) msg;
        log.debug("Received {} message, id={}", wireMsg.getClass().getSimpleName(), wireMsg.getMessageId());

        try {
            processMessage(ctx, wireMsg);
        } catch (Exception e) {
            log.error("Error processing message: {}", e.getMessage(), e);
            sendError(ctx, wireMsg.getMessageId(), "Internal error: " + e.getMessage());
        }
    }

    private void processMessage(ChannelHandlerContext ctx, WireProtocolMessage msg) throws Exception {
        if (msg instanceof OpQuery) {
            processOpQuery(ctx, (OpQuery) msg);
        } else if (msg instanceof OpMsg) {
            processOpMsg(ctx, (OpMsg) msg);
        } else {
            log.warn("Unsupported message type: {}", msg.getClass().getSimpleName());
            sendError(ctx, msg.getMessageId(), "Unsupported operation");
        }
    }

    private void processOpQuery(ChannelHandlerContext ctx, OpQuery query) throws Exception {
        Map<String, Object> doc = query.getDoc();
        int requestId = query.getMessageId();

        if (doc.containsKey("ismaster") || doc.containsKey("isMaster")) {
            // isMaster via OpQuery (legacy)
            log.debug("OpQuery->isMaster");
            OpReply reply = new OpReply();
            reply.setFlags(2);
            reply.setMessageId(msgId.incrementAndGet());
            reply.setResponseTo(requestId);
            reply.setNumReturned(1);

            Map<String, Object> response = getHelloResult().toMsg();
            response.put("morphiumServer", true);
            response.put("inMemoryBackend", true);
            reply.setDocuments(Arrays.asList(response));

            ctx.writeAndFlush(reply);
            return;
        }

        // OpQuery is deprecated
        OpReply reply = new OpReply();
        reply.setFlags(2);
        reply.setMessageId(msgId.incrementAndGet());
        reply.setResponseTo(requestId);
        reply.setNumReturned(1);
        reply.setDocuments(Arrays.asList(Doc.of(
                "$err", "OP_QUERY is no longer supported. The client driver may require an upgrade.",
                "code", 5739101, "ok", 0.0)));

        ctx.writeAndFlush(reply);
    }

    private void processOpMsg(ChannelHandlerContext ctx, OpMsg opMsg) throws Exception {
        Map<String, Object> doc = opMsg.getFirstDoc();
        int requestId = opMsg.getMessageId();

        log.debug("Incoming {}", Utils.toJsonString(doc));

        String cmd = doc.keySet().stream().findFirst().orElse("unknown");
        log.debug("Handling command {}", cmd);

        Map<String, Object> answer;

        switch (cmd) {
            case "getCmdLineOpts":
                answer = Doc.of("argv", List.of(), "parsed", Map.of(), "ok", 1.0);
                break;

            case "buildInfo":
                answer = Doc.of("version", "5.0.0-ALPHA",
                        "buildEnvironment", Doc.of("distarch", "java", "targetarch", "java"),
                        "ok", 1.0);
                break;

            case "ismaster":
            case "isMaster":
            case "hello":
                log.debug("OpMsg->hello/ismaster");
                answer = getHelloResult().toMsg();
                answer.put("morphiumServer", true);
                answer.put("inMemoryBackend", true);
                break;

            case "getFreeMonitoringStatus":
                answer = Doc.of("state", "disabled", "message", "", "url", "", "userReminder", "", "ok", 1.0);
                break;

            case "ping":
                answer = Doc.of("ok", 1.0);
                break;

            case "listDatabases":
                List<Map<String, Object>> dbList = new ArrayList<>();
                for (String dbName : driver.listDatabases()) {
                    dbList.add(Doc.of("name", dbName, "sizeOnDisk", 0, "empty", false));
                }
                answer = Doc.of("databases", dbList, "totalSize", 0, "ok", 1.0);
                break;

            case "endSessions":
            case "startSession":
            case "refreshSessions":
                answer = Doc.of("ok", 1.0);
                break;

            case "abortTransaction":
                handleAbortTransaction(ctx);
                answer = Doc.of("ok", 1.0);
                break;

            case "commitTransaction":
                handleCommitTransaction(ctx);
                answer = Doc.of("ok", 1.0);
                break;

            case "getMore":
                processGetMore(ctx, doc, requestId);
                return; // Async response

            case "killCursors":
                answer = processKillCursors(doc);
                break;

            case "getLog":
                if ("startupWarnings".equals(doc.get(cmd))) {
                    answer = Doc.of("totalLinesWritten", 0, "log", List.of(), "ok", 1.0);
                } else {
                    answer = Doc.of("ok", 0, "errmsg", "unknown log");
                }
                break;

            case "getParameter":
                if (Integer.valueOf(1).equals(doc.get("featureCompatibilityVersion"))) {
                    answer = Doc.of("version", "5.0", "ok", 1.0);
                } else {
                    answer = Doc.of("ok", 0, "errmsg", "no such parameter");
                }
                break;

            case "replSetProgress":
                answer = processReplSetProgress(doc);
                break;

            default:
                answer = processDefaultCommand(ctx, doc, cmd);
        }

        sendResponse(ctx, requestId, answer);
    }

    private void processGetMore(ChannelHandlerContext ctx, Map<String, Object> doc, int requestId) {
        long cursorId = ((Number) doc.get("getMore")).longValue();
        int maxTimeMs = doc.containsKey("maxTimeMS") ? ((Number) doc.get("maxTimeMS")).intValue() : 5000;
        String collection = (String) doc.get("collection");
        String db = (String) doc.get("$db");

        // Cap wait time for better responsiveness
        int effectiveWaitMs = Math.min(maxTimeMs, 2000);

        if (cursorManager.hasCursor(cursorId)) {
            // Async getMore for watch/tailable cursors
            CompletableFuture<List<Map<String, Object>>> future = cursorManager.getMore(cursorId, effectiveWaitMs);

            future.whenComplete((batch, error) -> {
                Map<String, Object> answer;
                if (error != null) {
                    log.error("getMore error for cursor {}: {}", cursorId, error.getMessage());
                    answer = Doc.of("ok", 0.0, "errmsg", error.getMessage());
                } else {
                    log.debug("getMore returning {} events for cursor {}", batch.size(), cursorId);
                    var cursor = Doc.of("nextBatch", batch, "ns", db + "." + collection, "id", cursorId);
                    answer = Doc.of("ok", 1.0, "cursor", cursor);
                }
                sendResponse(ctx, requestId, answer);
            });
        } else {
            // Regular cursor - use driver
            try {
                int cmdMsgId = driver.runCommand(new GenericCommand(driver).fromMap(doc));
                var result = driver.readSingleAnswer(cmdMsgId);
                Map<String, Object> answer = Doc.of("ok", 1.0);
                if (result != null) answer.putAll(result);
                sendResponse(ctx, requestId, answer);
            } catch (Exception e) {
                log.error("getMore error: {}", e.getMessage());
                sendError(ctx, requestId, e.getMessage());
            }
        }
    }

    private Map<String, Object> processKillCursors(Map<String, Object> doc) {
        List<Long> cursorsToKill = new ArrayList<>();
        Object cursorsObj = doc.get("cursors");
        if (cursorsObj instanceof List) {
            for (Object cursorIdObj : (List<?>) cursorsObj) {
                if (cursorIdObj instanceof Number) {
                    cursorsToKill.add(((Number) cursorIdObj).longValue());
                }
            }
        }

        List<Long> killed = new ArrayList<>();
        List<Long> notFound = new ArrayList<>();

        for (Long cursorId : cursorsToKill) {
            if (cursorManager.killCursor(cursorId)) {
                killed.add(cursorId);
            } else {
                notFound.add(cursorId);
            }
        }

        return Doc.of("ok", 1.0, "cursorsKilled", killed, "cursorsNotFound", notFound,
                "cursorsAlive", List.of(), "cursorsUnknown", List.of());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> processDefaultCommand(ChannelHandlerContext ctx,
                                                       Map<String, Object> doc, String cmd) {
        try {
            boolean isWriteCommand = WRITE_COMMANDS.contains(cmd.toLowerCase());

            // Reject writes to secondaries
            if (!primary && isWriteCommand &&
                    !Boolean.TRUE.equals(doc.get("$fromPrimary"))) {
                return Doc.of("ok", 0.0, "errmsg", "not primary", "code", 10107, "codeName", "NotWritablePrimary");
            }

            // Check for change stream aggregation
            if (doc.containsKey("pipeline")) {
                Object pipelineObj = doc.get("pipeline");
                if (pipelineObj instanceof List && !((List<?>) pipelineObj).isEmpty()) {
                    Object firstStage = ((List<?>) pipelineObj).get(0);
                    if (firstStage instanceof Map && ((Map<?, ?>) firstStage).containsKey("$changeStream")) {
                        return processChangeStream(doc);
                    }
                }
            }

            // Set up transaction context
            setupTransactionContext(ctx, doc);

            // Execute command
            int cmdMsgId = driver.runCommand(new GenericCommand(driver).fromMap(doc));
            var result = driver.readSingleAnswer(cmdMsgId);

            Map<String, Object> answer = Doc.of("ok", 1.0);
            if (result != null) answer.putAll(result);

            // Handle write concern for write commands on primary
            if (isWriteCommand && primary && replicationCoordinator != null) {
                // Get the sequence from the InMemoryDriver's change stream
                // This is the sequence number of the change event that was just created
                long writeSeq = driver.getChangeStreamSequence();

                // Extract write concern from command
                int w = getWriteConcernW(doc);
                long wtimeout = getWriteConcernTimeout(doc);

                if (w > 1) {
                    boolean acknowledged = replicationCoordinator.waitForReplication(writeSeq, w, wtimeout);
                    if (!acknowledged) {
                        // Write succeeded but replication timed out
                        answer.put("writeConcernError", Doc.of(
                            "code", 64,
                            "codeName", "WriteConcernFailed",
                            "errmsg", "waiting for replication timed out",
                            "errInfo", Doc.of("wtimeout", true)
                        ));
                    }
                }
            }

            // Handle tailable cursors
            if (cmd.equalsIgnoreCase("find") && Boolean.TRUE.equals(doc.get("tailable"))) {
                setupTailableCursor(doc, answer);
            }

            return answer;

        } catch (Exception e) {
            // Handle duplicate key errors
            String duplicateKeyMsg = findDuplicateKeyError(e);
            if (duplicateKeyMsg != null) {
                var writeError = Doc.of("index", 0, "code", 11000, "errmsg", "E11000 duplicate key error: " + duplicateKeyMsg);
                return Doc.of("ok", 1.0, "n", 0, "writeErrors", List.of(writeError));
            }

            String errorMsg = getDeepestCauseMessage(e);
            log.error("Error executing command {}: {}", cmd, errorMsg, e);
            return Doc.of("ok", 0.0, "errmsg", errorMsg != null ? errorMsg : "Command failed: " + cmd);
        }
    }

    /**
     * Process replication progress report from a secondary.
     * This is called when a secondary sends its current replication sequence.
     */
    private Map<String, Object> processReplSetProgress(Map<String, Object> doc) {
        if (replicationCoordinator == null) {
            // Not a primary with replication enabled
            return Doc.of("ok", 0.0, "errmsg", "not primary or replication not enabled");
        }

        String secondaryAddress = (String) doc.get("secondaryAddress");
        Object seqObj = doc.get("sequenceNumber");

        if (secondaryAddress == null || seqObj == null) {
            return Doc.of("ok", 0.0, "errmsg", "missing secondaryAddress or sequenceNumber");
        }

        long sequenceNumber = seqObj instanceof Number ? ((Number) seqObj).longValue() : 0;

        log.debug("Received replication progress from {}: seq={}", secondaryAddress, sequenceNumber);
        replicationCoordinator.reportProgress(secondaryAddress, sequenceNumber);

        return Doc.of("ok", 1.0);
    }

    /**
     * Extract write concern 'w' value from command document.
     * Returns 1 if not specified (primary only).
     */
    @SuppressWarnings("unchecked")
    private int getWriteConcernW(Map<String, Object> doc) {
        Object wc = doc.get("writeConcern");
        if (wc instanceof Map) {
            Object w = ((Map<String, Object>) wc).get("w");
            if (w instanceof Number) {
                return ((Number) w).intValue();
            } else if ("majority".equals(w)) {
                // Majority = (replicaSetSize / 2) + 1
                return (hosts.size() / 2) + 1;
            }
        }
        // Default: w=1 (primary only acknowledgment)
        return 1;
    }

    /**
     * Extract write concern timeout from command document.
     * Returns 0 if not specified (use default).
     */
    @SuppressWarnings("unchecked")
    private long getWriteConcernTimeout(Map<String, Object> doc) {
        Object wc = doc.get("writeConcern");
        if (wc instanceof Map) {
            Object wtimeout = ((Map<String, Object>) wc).get("wtimeout");
            if (wtimeout instanceof Number) {
                return ((Number) wtimeout).longValue();
            }
        }
        return 0; // Use default timeout
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> processChangeStream(Map<String, Object> doc) {
        try {
            WatchCommand wcmd = new WatchCommand(driver).fromMap(doc);
            long cursorId = cursorManager.createWatchCursor(driver, wcmd);

            var initialCursor = Doc.of("firstBatch", List.of(),
                    "ns", wcmd.getDb() + "." + wcmd.getColl(), "id", cursorId);
            return Doc.of("ok", 1.0, "cursor", initialCursor);
        } catch (Exception e) {
            log.error("Error setting up change stream: {}", e.getMessage(), e);
            return Doc.of("ok", 0.0, "errmsg", e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void setupTailableCursor(Map<String, Object> doc, Map<String, Object> answer) {
        Map<String, Object> cursor = (Map<String, Object>) answer.get("cursor");
        if (cursor != null) {
            long tailCursorId = ((Number) cursor.get("id")).longValue();
            if (tailCursorId != 0) {
                String db = (String) doc.get("$db");
                String coll = (String) doc.get("find");
                Map<String, Object> filter = (Map<String, Object>) doc.get("filter");
                log.info("Setting up tailable cursor {} for {}.{}", tailCursorId, db, coll);
                // Register with cursor manager for async handling
                // The actual cursor ID is already created by the driver
            }
        }
    }

    private HelloResult getHelloResult() {
        HelloResult res = new HelloResult();
        res.setHelloOk(true);
        res.setLocalTime(new Date());
        res.setOk(1.0);

        String myAddress = host + ":" + port;
        if (hosts == null || hosts.isEmpty()) {
            res.setHosts(Arrays.asList(myAddress));
        } else {
            List<String> allHosts = new ArrayList<>(hosts);
            if (!allHosts.contains(myAddress)) {
                allHosts.add(0, myAddress);
            }
            res.setHosts(allHosts);
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
        res.setMsg("MorphiumServer V0.1ALPHA (Netty)");
        return res;
    }

    private void sendResponse(ChannelHandlerContext ctx, int requestId, Map<String, Object> answer) {
        answer.put("$clusterTime", Doc.of("clusterTime", new MongoTimestamp(System.currentTimeMillis())));
        answer.put("operationTime", new MongoTimestamp(System.currentTimeMillis()));

        OpMsg reply = new OpMsg();
        reply.setResponseTo(requestId);
        reply.setMessageId(msgId.incrementAndGet());
        reply.setFirstDoc(answer);

        log.debug("Sending response for request {}: {}", requestId, answer.keySet());
        ctx.writeAndFlush(reply);
    }

    private void sendError(ChannelHandlerContext ctx, int requestId, String message) {
        sendResponse(ctx, requestId, Doc.of("ok", 0.0, "errmsg", message));
    }

    // Transaction handling

    private void setupTransactionContext(ChannelHandlerContext ctx, Map<String, Object> doc) {
        String sessionId = extractSessionId(doc);
        if (sessionId == null) return;

        Object startTxn = doc.get("startTransaction");
        boolean isStartTransaction = Boolean.TRUE.equals(startTxn) ||
                (startTxn instanceof Number && ((Number) startTxn).intValue() == 1);

        if (isStartTransaction) {
            log.debug("Starting transaction for session {}", sessionId);
            MorphiumTransactionContext txCtx = driver.startTransaction(false);
            ctx.channel().attr(TX_CONTEXT_KEY).set(txCtx);
            ctx.channel().attr(SESSION_ID_KEY).set(sessionId);
            return;
        }

        // Check if part of existing transaction
        Object txnNumber = doc.get("txnNumber");
        Object autocommit = doc.get("autocommit");
        if (txnNumber != null && Boolean.FALSE.equals(autocommit)) {
            MorphiumTransactionContext txCtx = ctx.channel().attr(TX_CONTEXT_KEY).get();
            if (txCtx != null) {
                driver.setTransactionContext(txCtx);
            }
        }
    }

    private void handleAbortTransaction(ChannelHandlerContext ctx) {
        MorphiumTransactionContext txCtx = ctx.channel().attr(TX_CONTEXT_KEY).getAndSet(null);
        if (txCtx != null) {
            log.debug("Aborting transaction");
            driver.setTransactionContext(txCtx);
            try {
                driver.abortTransaction();
            } catch (Exception e) {
                log.error("Error aborting transaction", e);
            }
        }
    }

    private void handleCommitTransaction(ChannelHandlerContext ctx) {
        MorphiumTransactionContext txCtx = ctx.channel().attr(TX_CONTEXT_KEY).getAndSet(null);
        if (txCtx != null) {
            log.debug("Committing transaction");
            driver.setTransactionContext(txCtx);
            try {
                driver.commitTransaction();
            } catch (Exception e) {
                log.error("Error committing transaction", e);
            }
        }
    }

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

    // Error handling helpers

    private String findDuplicateKeyError(Throwable e) {
        Throwable current = e;
        while (current != null) {
            String msg = current.getMessage();
            if (msg != null && msg.contains("Duplicate _id")) {
                return msg;
            }
            current = current.getCause();
        }
        return null;
    }

    private String getDeepestCauseMessage(Throwable e) {
        String msg = e.getMessage();
        Throwable current = e;
        while (current.getCause() != null) {
            current = current.getCause();
            if (current.getMessage() != null) {
                msg = current.getMessage();
            }
        }
        return msg;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Handler error: {}", cause.getMessage());
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.debug("Channel inactive, cleaning up");
        // Abort any pending transaction
        handleAbortTransaction(ctx);
        super.channelInactive(ctx);
    }
}
