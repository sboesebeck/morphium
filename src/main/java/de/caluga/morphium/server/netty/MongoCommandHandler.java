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
import de.caluga.morphium.server.election.*;
import de.caluga.morphium.server.messaging.MessagingCollectionInfo;
import de.caluga.morphium.server.messaging.MessagingOptimizer;
import de.caluga.morphium.driver.wire.HelloResult;
import de.caluga.morphium.driver.wireprotocol.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Netty handler for processing MongoDB commands.
 * This is the async equivalent of MorphiumServer.incoming().
 */
public class MongoCommandHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(MongoCommandHandler.class);

    // Dedicated executor for replication waits - don't block Netty I/O threads
    private static final ExecutorService REPLICATION_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

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
    private final MessagingOptimizer messagingOptimizer;
    private final AtomicInteger msgId;
    private final String host;
    private final int port;
    private final String rsName;
    private final List<String> hosts;
    private final boolean primary;
    private final String primaryHost;
    private final int compressorId;
    private final ReplicationCoordinator replicationCoordinator;
    private final ElectionManager electionManager;

    public MongoCommandHandler(InMemoryDriver driver, WatchCursorManager cursorManager,
                               MessagingOptimizer messagingOptimizer,
                               AtomicInteger msgId, String host, int port, String rsName,
                               List<String> hosts, boolean primary, String primaryHost,
                               int compressorId, ReplicationCoordinator replicationCoordinator) {
        this(driver, cursorManager, messagingOptimizer, msgId, host, port, rsName, hosts, primary, primaryHost,
             compressorId, replicationCoordinator, null);
    }

    public MongoCommandHandler(InMemoryDriver driver, WatchCursorManager cursorManager,
                               MessagingOptimizer messagingOptimizer,
                               AtomicInteger msgId, String host, int port, String rsName,
                               List<String> hosts, boolean primary, String primaryHost,
                               int compressorId, ReplicationCoordinator replicationCoordinator,
                               ElectionManager electionManager) {
        this.driver = driver;
        this.cursorManager = cursorManager;
        this.messagingOptimizer = messagingOptimizer;
        this.msgId = msgId;
        this.host = host;
        this.port = port;
        this.rsName = rsName;
        this.hosts = hosts;
        this.primary = primary;
        this.primaryHost = primaryHost;
        this.compressorId = compressorId;
        this.replicationCoordinator = replicationCoordinator;
        this.electionManager = electionManager;
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

            case "registerMessagingCollection":
                answer = processRegisterMessagingCollection(doc);
                break;

            case "unregisterMessagingSubscriber":
                answer = processUnregisterMessagingSubscriber(doc);
                break;

            case "getMessagingStats":
                answer = messagingOptimizer != null ? messagingOptimizer.getStats() : Doc.of("ok", 0.0, "errmsg", "Messaging optimizer not available");
                answer.put("ok", 1.0);
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

            // Election protocol commands
            case "requestVote":
                answer = processRequestVote(doc);
                break;

            case "appendEntries":
                answer = processAppendEntries(doc);
                break;

            case "replSetGetStatus":
                answer = processReplSetGetStatus();
                break;

            case "replSetStepDown":
                answer = processReplSetStepDown(doc);
                break;

            case "replSetFreeze":
                answer = processReplSetFreeze(doc);
                break;

            default:
                // Handle write commands that need replication wait asynchronously
                processDefaultCommandAsync(ctx, doc, cmd, requestId);
                return; // Async response
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

    /**
     * Process commands asynchronously - handles write concern waits without blocking Netty I/O threads.
     */
    @SuppressWarnings("unchecked")
    private void processDefaultCommandAsync(ChannelHandlerContext ctx, Map<String, Object> doc,
                                            String cmd, int requestId) {
        try {
            boolean isWriteCommand = WRITE_COMMANDS.contains(cmd.toLowerCase());

            // Reject writes to secondaries (use dynamic election state if available)
            boolean isPrimary = isCurrentPrimary();
            if (!isPrimary && isWriteCommand &&
                    !Boolean.TRUE.equals(doc.get("$fromPrimary"))) {
                // Include primary host so client can redirect
                String currentPrimary = getCurrentPrimaryHost();
                Map<String, Object> errorResponse = Doc.of(
                    "ok", 0.0,
                    "errmsg", "not primary",
                    "code", 10107,
                    "codeName", "NotWritablePrimary"
                );
                if (currentPrimary != null) {
                    errorResponse.put("primaryHost", currentPrimary);
                }
                sendResponse(ctx, requestId, errorResponse);
                return;
            }

            // Check read preference for read commands on secondaries
            // If read preference requires primary, reject on secondaries to ensure consistency
            if (!isPrimary && !isWriteCommand) {
                @SuppressWarnings("unchecked")
                Map<String, Object> readPref = (Map<String, Object>) doc.get("$readPreference");
                if (readPref != null) {
                    String mode = (String) readPref.get("mode");
                    if ("primary".equalsIgnoreCase(mode)) {
                        String currentPrimary = getCurrentPrimaryHost();
                        Map<String, Object> errorResponse = Doc.of(
                            "ok", 0.0,
                            "errmsg", "not primary and read preference is primary",
                            "code", 10107,
                            "codeName", "NotPrimaryNoSecondaryOk"
                        );
                        if (currentPrimary != null) {
                            errorResponse.put("primaryHost", currentPrimary);
                        }
                        sendResponse(ctx, requestId, errorResponse);
                        return;
                    }
                }
            }

            // Check for change stream aggregation
            if (doc.containsKey("pipeline")) {
                Object pipelineObj = doc.get("pipeline");
                if (pipelineObj instanceof List && !((List<?>) pipelineObj).isEmpty()) {
                    Object firstStage = ((List<?>) pipelineObj).get(0);
                    if (firstStage instanceof Map && ((Map<?, ?>) firstStage).containsKey("$changeStream")) {
                        sendResponse(ctx, requestId, processChangeStream(doc));
                        return;
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

            // Notify tailable cursors about inserted documents
            if (cmd.equalsIgnoreCase("insert")) {
                notifyTailableCursorsOnInsert(doc);
                // Note: messaging cursors are notified via the normal change stream callback chain
                // The fast-path infrastructure is in place (notifyMessagingCursorsOnInsert) but disabled
                // to avoid duplicate delivery. Enable when proper coordination is implemented.
            }

            // Notify messaging cursors when a lock is deleted (exclusive message released).
            // This allows other subscribers to re-poll for the now-available message
            // without needing a separate lock-monitor change stream connection.
            if (cmd.equalsIgnoreCase("delete")) {
                notifyMessagingCursorsOnLockDelete(doc);
            }

            // Handle tailable cursors
            if (cmd.equalsIgnoreCase("find") && Boolean.TRUE.equals(doc.get("tailable"))) {
                setupTailableCursor(doc, answer);
            }

            // Handle write concern for write commands on primary - ASYNC to not block Netty I/O
            if (isWriteCommand && primary && replicationCoordinator != null) {
                long writeSeq = driver.getChangeStreamSequence();
                int w = getWriteConcernW(doc);
                long wtimeout = getWriteConcernTimeout(doc);

                if (w > 1) {
                    // Wait for replication on a separate thread to not block Netty I/O
                    final Map<String, Object> finalAnswer = answer;
                    log.debug("Starting async replication wait: cmd={}, w={}, wtimeout={}", cmd, w, wtimeout);
                    REPLICATION_EXECUTOR.execute(() -> {
                        try {
                            boolean acknowledged = replicationCoordinator.waitForReplication(writeSeq, w, wtimeout);
                            if (!acknowledged) {
                                finalAnswer.put("writeConcernError", Doc.of(
                                    "code", 64,
                                    "codeName", "WriteConcernFailed",
                                    "errmsg", "waiting for replication timed out",
                                    "errInfo", Doc.of("wtimeout", true)
                                ));
                            }
                        } finally {
                            // Send response back on the Netty event loop
                            ctx.executor().execute(() -> sendResponse(ctx, requestId, finalAnswer));
                        }
                    });
                    return; // Response will be sent asynchronously
                }
            }

            // No replication wait needed - send response immediately
            sendResponse(ctx, requestId, answer);

        } catch (Exception e) {
            // Handle duplicate key errors
            String duplicateKeyMsg = findDuplicateKeyError(e);
            if (duplicateKeyMsg != null) {
                var writeError = Doc.of("index", 0, "code", 11000, "errmsg", "E11000 duplicate key error: " + duplicateKeyMsg);
                sendResponse(ctx, requestId, Doc.of("ok", 1.0, "n", 0, "writeErrors", List.of(writeError)));
                return;
            }

            String errorMsg = getDeepestCauseMessage(e);
            log.error("Error executing command {}: {}", cmd, errorMsg, e);
            sendResponse(ctx, requestId, Doc.of("ok", 0.0, "errmsg", errorMsg != null ? errorMsg : "Command failed: " + cmd));
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

    /**
     * Register a collection as a messaging collection for optimizations.
     */
    private Map<String, Object> processRegisterMessagingCollection(Map<String, Object> doc) {
        if (messagingOptimizer == null) {
            return Doc.of("ok", 0.0, "errmsg", "Messaging optimizer not available");
        }

        String db = (String) doc.get("$db");
        String collection = (String) doc.get("registerMessagingCollection");
        String lockCollection = (String) doc.get("lockCollection");
        String senderId = (String) doc.get("senderId");

        if (db == null || collection == null) {
            return Doc.of("ok", 0.0, "errmsg", "Missing required fields: $db and collection name");
        }

        log.info("Registering messaging collection: {}.{} (lock: {}, subscriber: {})",
                 db, collection, lockCollection, senderId);

        return messagingOptimizer.registerMessagingCollection(db, collection, lockCollection, senderId);
    }

    /**
     * Unregister a subscriber from a messaging collection.
     */
    private Map<String, Object> processUnregisterMessagingSubscriber(Map<String, Object> doc) {
        if (messagingOptimizer == null) {
            return Doc.of("ok", 0.0, "errmsg", "Messaging optimizer not available");
        }

        String db = (String) doc.get("$db");
        String collection = (String) doc.get("unregisterMessagingSubscriber");
        String senderId = (String) doc.get("senderId");

        if (db == null || collection == null || senderId == null) {
            return Doc.of("ok", 0.0, "errmsg", "Missing required fields: $db, collection name, and senderId");
        }

        log.debug("Unregistering subscriber {} from messaging collection: {}.{}", senderId, db, collection);

        return messagingOptimizer.unregisterMessagingSubscriber(db, collection, senderId);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> processChangeStream(Map<String, Object> doc) {
        try {
            WatchCommand wcmd = new WatchCommand(driver).fromMap(doc);
            long cursorId = cursorManager.createWatchCursor(driver, wcmd);

            // Register as messaging cursor if this is a registered messaging collection
            if (messagingOptimizer != null &&
                messagingOptimizer.isMessagingCollection(wcmd.getDb(), wcmd.getColl())) {
                // Extract subscriber ID from pipeline filter if present (e.g., sender != "myId")
                // For now, register without subscriber ID - server-side filtering can be added later
                String subscriberId = extractSubscriberIdFromPipeline(doc);
                messagingOptimizer.registerMessagingCursor(cursorId, wcmd.getDb(), wcmd.getColl(), subscriberId);
                log.debug("Registered messaging cursor {} for {}.{}", cursorId, wcmd.getDb(), wcmd.getColl());
            }

            var initialCursor = Doc.of("firstBatch", List.of(),
                    "ns", wcmd.getDb() + "." + wcmd.getColl(), "id", cursorId);
            return Doc.of("ok", 1.0, "cursor", initialCursor);
        } catch (Exception e) {
            log.error("Error setting up change stream: {}", e.getMessage(), e);
            return Doc.of("ok", 0.0, "errmsg", e.getMessage());
        }
    }

    /**
     * Extract subscriber ID from change stream pipeline for server-side sender filtering.
     * Looks for patterns like: {"$match": {"fullDocument.sender": {"$ne": "subscriberId"}}}
     */
    @SuppressWarnings("unchecked")
    private String extractSubscriberIdFromPipeline(Map<String, Object> doc) {
        try {
            List<Map<String, Object>> pipeline = (List<Map<String, Object>>) doc.get("pipeline");
            if (pipeline == null) return null;

            for (Map<String, Object> stage : pipeline) {
                if (stage.containsKey("$match")) {
                    Map<String, Object> match = (Map<String, Object>) stage.get("$match");
                    // Look for sender filter: fullDocument.sender != subscriberId
                    Object senderFilter = match.get("fullDocument.sender");
                    if (senderFilter instanceof Map) {
                        Object ne = ((Map<String, Object>) senderFilter).get("$ne");
                        if (ne instanceof String) {
                            return (String) ne;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.trace("Could not extract subscriber ID from pipeline: {}", e.getMessage());
        }
        return null;
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
                // Register the driver's cursor with our cursor manager for notification handling
                cursorManager.registerTailableCursor(tailCursorId, db, coll, filter);
            }
        }
    }

    /**
     * Notify tailable cursors about newly inserted documents.
     */
    @SuppressWarnings("unchecked")
    private void notifyTailableCursorsOnInsert(Map<String, Object> doc) {
        try {
            String db = (String) doc.get("$db");
            String coll = (String) doc.get("insert");
            Object docsObj = doc.get("documents");

            if (db == null || coll == null || docsObj == null) {
                return;
            }

            List<Map<String, Object>> documents;
            if (docsObj instanceof List) {
                documents = (List<Map<String, Object>>) docsObj;
            } else {
                return;
            }

            if (!documents.isEmpty()) {
                log.debug("Notifying tailable cursors about {} inserted documents in {}.{}",
                         documents.size(), db, coll);
                cursorManager.notifyTailableCursors(db, coll, documents);
            }
        } catch (Exception e) {
            log.debug("Error notifying tailable cursors: {}", e.getMessage());
        }
    }

    /**
     * Fast-path notification for messaging collections.
     * This directly notifies waiting messaging cursors about new messages,
     * bypassing the normal change stream mechanism for lower latency.
     */
    @SuppressWarnings("unchecked")
    private void notifyMessagingCursorsOnInsert(Map<String, Object> doc) {
        if (messagingOptimizer == null) {
            return;
        }

        try {
            String db = (String) doc.get("$db");
            String coll = (String) doc.get("insert");
            Object docsObj = doc.get("documents");

            if (db == null || coll == null || docsObj == null) {
                return;
            }

            // Only process if this is a registered messaging collection
            if (!messagingOptimizer.isMessagingCollection(db, coll)) {
                return;
            }

            List<Map<String, Object>> documents;
            if (docsObj instanceof List) {
                documents = (List<Map<String, Object>>) docsObj;
            } else {
                return;
            }

            int notified = 0;
            for (Map<String, Object> document : documents) {
                if (messagingOptimizer.notifyMessageInsert(db, coll, document)) {
                    notified++;
                }
            }

            if (notified > 0) {
                log.debug("Fast-path: notified messaging cursors for {} documents in {}.{}",
                         notified, db, coll);
            }
        } catch (Exception e) {
            log.debug("Error notifying messaging cursors: {}", e.getMessage());
        }
    }

    /**
     * When a lock document is deleted from a lock collection, notify the messaging cursors
     * of the parent messaging collection. This sends a synthetic "lock_released" event
     * so clients can re-poll for exclusive messages without a separate lock-monitor connection.
     */
    @SuppressWarnings("unchecked")
    private void notifyMessagingCursorsOnLockDelete(Map<String, Object> doc) {
        if (messagingOptimizer == null) {
            return;
        }

        try {
            String db = (String) doc.get("$db");
            String lockColl = (String) doc.get("delete");

            if (db == null || lockColl == null) {
                return;
            }

            // Only process if this is a registered lock collection
            if (!messagingOptimizer.isLockCollection(db, lockColl)) {
                return;
            }

            // Look up the parent messaging collection
            MessagingCollectionInfo info = messagingOptimizer.getMessagingInfoByLockCollection(db, lockColl);
            if (info == null) {
                return;
            }

            // Build a synthetic "lock_released" event for the parent messaging collection
            Map<String, Object> event = Doc.of(
                "operationType", "lock_released",
                "ns", Doc.of("db", db, "coll", info.getCollection()),
                "lockCollection", lockColl
            );

            int notified = cursorManager.notifyMessagingEvent(db, info.getCollection(), event, null);
            if (notified > 0) {
                log.debug("Lock-release: notified {} cursors for lock delete in {}.{} -> {}.{}",
                         notified, db, lockColl, db, info.getCollection());
            }
        } catch (Exception e) {
            log.debug("Error notifying messaging cursors on lock delete: {}", e.getMessage());
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

        // Use dynamic election state if available
        boolean isPrimary = isCurrentPrimary();
        String currentPrimaryHost = getCurrentPrimaryHost();

        res.setConnectionId(1);
        res.setMaxWireVersion(17);
        res.setMinWireVersion(13);
        res.setMaxMessageSizeBytes(100000);
        res.setMaxBsonObjectSize(10000);
        res.setWritablePrimary(isPrimary);
        res.setSecondary(!isPrimary);
        res.setSetName(rsName);
        res.setPrimary(currentPrimaryHost != null ? currentPrimaryHost : (isPrimary ? myAddress : primaryHost));
        res.setMe(myAddress);
        res.setLogicalSessionTimeoutMinutes(30);
        res.setMsg("MorphiumServer V0.1ALPHA (Netty)");
        return res;
    }

    /**
     * Check if this node is currently the primary/leader.
     * Uses ElectionManager if available (dynamic elections), otherwise falls back to static configuration.
     */
    private boolean isCurrentPrimary() {
        if (electionManager != null) {
            return electionManager.isLeader();
        }
        return primary;
    }

    /**
     * Get the current primary host address.
     * Uses ElectionManager if available for dynamic primary detection.
     */
    private String getCurrentPrimaryHost() {
        if (electionManager != null) {
            String leader = electionManager.getCurrentLeader();
            if (leader != null) {
                return leader;
            }
            // If this node is leader, return own address
            if (electionManager.isLeader()) {
                return host + ":" + port;
            }
        }
        // Fall back to static configuration
        if (primary) {
            return host + ":" + port;
        }
        return primaryHost;
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
        StringBuilder sb = new StringBuilder();
        while (current != null) {
            String currentMsg = current.getMessage();
            if (currentMsg != null && !currentMsg.isEmpty()) {
                // Skip generic exception wrapper messages
                if (!currentMsg.equals("java.lang.reflect.InvocationTargetException") &&
                    !currentMsg.startsWith("java.lang.")) {
                    msg = currentMsg;
                }
            }
            // Also check class name for debugging
            sb.append(current.getClass().getSimpleName());
            if (current.getMessage() != null) {
                sb.append(": ").append(current.getMessage());
            }
            sb.append(" -> ");
            current = current.getCause();
        }
        log.debug("Exception chain: {}", sb.toString());
        return msg != null ? msg : e.getClass().getSimpleName();
    }

    // ==================== Election Protocol Handlers ====================

    /**
     * Handle vote request from a candidate.
     */
    private Map<String, Object> processRequestVote(Map<String, Object> doc) {
        if (electionManager == null) {
            log.warn("Election not enabled, rejecting vote request");
            return Doc.of("ok", 0, "errmsg", "Election not enabled on this node");
        }

        try {
            VoteRequest request = VoteRequest.fromMap(doc);
            log.debug("Processing vote request: {}", request);

            VoteResponse response = electionManager.handleVoteRequest(request);
            return response.toMap();
        } catch (Exception e) {
            log.error("Error processing vote request: {}", e.getMessage(), e);
            return Doc.of("ok", 0, "errmsg", "Error processing vote request: " + e.getMessage());
        }
    }

    /**
     * Handle append entries (heartbeat) from leader.
     */
    private Map<String, Object> processAppendEntries(Map<String, Object> doc) {
        if (electionManager == null) {
            log.warn("Election not enabled, rejecting append entries");
            return Doc.of("ok", 0, "errmsg", "Election not enabled on this node");
        }

        try {
            AppendEntriesRequest request = AppendEntriesRequest.fromMap(doc);
            log.trace("Processing append entries: {}", request);

            AppendEntriesResponse response = electionManager.handleAppendEntries(request);
            return response.toMap();
        } catch (Exception e) {
            log.error("Error processing append entries: {}", e.getMessage(), e);
            return Doc.of("ok", 0, "errmsg", "Error processing append entries: " + e.getMessage());
        }
    }

    /**
     * Get replica set status including election state.
     */
    private Map<String, Object> processReplSetGetStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("set", rsName);
        status.put("ok", 1.0);

        if (electionManager != null) {
            // Include election manager stats
            Map<String, Object> electionStats = electionManager.getStats();
            status.put("term", electionStats.get("term"));

            // Determine myState based on election state
            ElectionState state = electionManager.getState();
            int myState = switch (state) {
                case LEADER -> 1;    // PRIMARY
                case FOLLOWER -> 2;  // SECONDARY
                case CANDIDATE -> 3; // RECOVERING (closest match)
            };
            status.put("myState", myState);

            // Build members list
            List<Map<String, Object>> members = new ArrayList<>();
            String myAddress = electionManager.getMyAddress();
            String currentLeader = electionManager.getCurrentLeader();

            // Add self
            Map<String, Object> selfMember = new LinkedHashMap<>();
            selfMember.put("_id", 0);
            selfMember.put("name", myAddress);
            selfMember.put("state", myState);
            selfMember.put("stateStr", state.name());
            selfMember.put("self", true);
            members.add(selfMember);

            // Add peers
            int memberId = 1;
            for (String peer : electionManager.getPeerAddresses()) {
                Map<String, Object> peerMember = new LinkedHashMap<>();
                peerMember.put("_id", memberId++);
                peerMember.put("name", peer);

                // Determine peer state (we know leader, others are likely followers)
                if (peer.equals(currentLeader)) {
                    peerMember.put("state", 1);
                    peerMember.put("stateStr", "PRIMARY");
                } else {
                    peerMember.put("state", 2);
                    peerMember.put("stateStr", "SECONDARY");
                }
                members.add(peerMember);
            }

            status.put("members", members);
        } else {
            // No election manager - static configuration
            status.put("myState", primary ? 1 : 2);
            status.put("term", 0);

            List<Map<String, Object>> members = new ArrayList<>();
            int memberId = 0;
            for (String h : hosts) {
                Map<String, Object> member = new LinkedHashMap<>();
                member.put("_id", memberId++);
                member.put("name", h);
                boolean isPrimary = h.equals(primaryHost);
                member.put("state", isPrimary ? 1 : 2);
                member.put("stateStr", isPrimary ? "PRIMARY" : "SECONDARY");
                member.put("self", h.equals(host + ":" + port));
                members.add(member);
            }
            status.put("members", members);
        }

        return status;
    }

    /**
     * Process replSetStepDown command for graceful leadership handoff.
     * This is essential for rolling updates and maintenance operations.
     *
     * Command format:
     * {
     *   replSetStepDown: <seconds>,           // Time to refuse re-election
     *   secondaryCatchUpPeriodSecs: <seconds>,// Time to wait for secondaries to catch up
     *   force: <boolean>                      // Force stepdown even if no eligible secondary
     * }
     */
    private Map<String, Object> processReplSetStepDown(Map<String, Object> doc) {
        if (electionManager == null) {
            return Doc.of("ok", 0.0, "errmsg", "Election not enabled on this server", "code", 76);
        }

        if (!electionManager.isLeader()) {
            return Doc.of("ok", 0.0, "errmsg", "not primary so can't step down", "code", 10107);
        }

        // Parse step down duration (default 60 seconds)
        int stepDownSecs = 60;
        Object stepDownVal = doc.get("replSetStepDown");
        if (stepDownVal instanceof Number) {
            stepDownSecs = ((Number) stepDownVal).intValue();
        }

        // Parse secondary catch-up period (default 10 seconds)
        int catchUpSecs = 10;
        Object catchUpVal = doc.get("secondaryCatchUpPeriodSecs");
        if (catchUpVal instanceof Number) {
            catchUpSecs = ((Number) catchUpVal).intValue();
        }

        // Parse force flag
        boolean force = Boolean.TRUE.equals(doc.get("force"));

        log.info("Processing replSetStepDown: stepDownSecs={}, catchUpSecs={}, force={}",
                stepDownSecs, catchUpSecs, force);

        try {
            // Perform graceful stepdown
            boolean success = electionManager.stepDown(stepDownSecs, catchUpSecs, force);
            if (success) {
                log.info("Successfully stepped down as leader");
                return Doc.of("ok", 1.0);
            } else {
                return Doc.of("ok", 0.0, "errmsg", "Step down failed - no eligible secondary caught up", "code", 189);
            }
        } catch (Exception e) {
            log.error("Error during stepdown: {}", e.getMessage(), e);
            return Doc.of("ok", 0.0, "errmsg", "Step down error: " + e.getMessage(), "code", 1);
        }
    }

    /**
     * Process replSetFreeze command to temporarily prevent this node from seeking election.
     * Used for maintenance operations and controlled failover.
     *
     * Command format:
     * {
     *   replSetFreeze: <seconds>  // 0 to unfreeze, positive to freeze for N seconds
     * }
     */
    private Map<String, Object> processReplSetFreeze(Map<String, Object> doc) {
        if (electionManager == null) {
            return Doc.of("ok", 0.0, "errmsg", "Election not enabled on this server", "code", 76);
        }

        int freezeSecs = 0;
        Object freezeVal = doc.get("replSetFreeze");
        if (freezeVal instanceof Number) {
            freezeSecs = ((Number) freezeVal).intValue();
        }

        if (freezeSecs < 0) {
            return Doc.of("ok", 0.0, "errmsg", "freeze time must be >= 0", "code", 1);
        }

        log.info("Processing replSetFreeze: {} seconds", freezeSecs);

        try {
            if (freezeSecs == 0) {
                electionManager.unfreeze();
                log.info("Node unfrozen - can seek election");
            } else {
                electionManager.freeze(freezeSecs);
                log.info("Node frozen for {} seconds - will not seek election", freezeSecs);
            }
            return Doc.of("ok", 1.0);
        } catch (Exception e) {
            log.error("Error during freeze: {}", e.getMessage(), e);
            return Doc.of("ok", 0.0, "errmsg", "Freeze error: " + e.getMessage(), "code", 1);
        }
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
