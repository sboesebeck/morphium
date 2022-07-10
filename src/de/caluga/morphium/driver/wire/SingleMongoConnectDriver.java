package de.caluga.morphium.driver.wire;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.*;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.commands.result.CursorResult;
import de.caluga.morphium.driver.commands.result.ListResult;
import de.caluga.morphium.driver.commands.result.RunCommandResult;
import de.caluga.morphium.driver.commands.result.SingleElementResult;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: Stephan Bösebeck
 * Date: 02.12.15
 * Time: 23:47
 * <p>
 * connects to one node only!
 */
public class SingleMongoConnectDriver extends DriverBase {

    private final Logger log = LoggerFactory.getLogger(SingleMongoConnectDriver.class);
    private SingleMongoConnection connection;
    private ConnectionType connectionType = ConnectionType.PRIMARY;
    private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
        private AtomicLong l = new AtomicLong(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread ret = new Thread(r);
            ret.setName("MCon_" + (l.incrementAndGet()));
            ret.setDaemon(true);
            return ret;
        }
    });
    private ScheduledFuture<?> heartbeat;

    public ConnectionType getConnectionType() {
        return connectionType;
    }

    public SingleMongoConnectDriver setConnectionType(ConnectionType connectionType) {
        this.connectionType = connectionType;
        return this;
    }

    private String getHost(int hostSeedIndex) {
        return getHost(getHostSeed().get(hostSeedIndex));
    }

    private String getHost(String hostPort) {
        String h[] = hostPort.split(":");
        return h[0];
    }

    private int getPortFromHost(int hostSeedIdx) {
        return getPortFromHost(getHostSeed().get(hostSeedIdx));
    }

    private int getPortFromHost(String host) {
        String h[] = host.split(":");
        if (h.length == 1)
            return 27017;
        return Integer.parseInt(h[1]);
    }


    @Override
    public void connect() throws MorphiumDriverException {
        connect(null);
    }

    @Override
    public void connect(String replSet) throws MorphiumDriverException {
        int connectToIdx = 0;
        while (true) {
            try {
                String host = getHostSeed().get(connectToIdx);
                String h[] = host.split(":");
                int port = 27017;
                if (h.length > 1) {
                    port = Integer.parseInt(h[1]);
                }
                connection = new SingleMongoConnection();
                var hello = connection.connect(this, h[0], port);
                //checking hosts
                if (hello.getHosts() != null) {
                    for (String s : hello.getHosts()) {
                        if (!getHostSeed().contains(s)) getHostSeed().add(s);
                    }
                }
                if (hello.getPrimary() != null && !getHostSeed().contains(hello.getPrimary())) {
                    getHostSeed().add(hello.getPrimary());
                }
                if (connectionType.equals(ConnectionType.PRIMARY) && !Boolean.TRUE.equals(hello.getWritablePrimary())) {
                    log.info("want primary connection, got secondary, retrying");
                    disconnect();
                    Thread.sleep(1000);//slowing down
                    if (hello.getPrimary() != null) {
                        //checking for primary
                        connectToIdx = getHostSeed().indexOf(hello.getPrimary());
                    } else {
                        connectToIdx++;
                        if (connectToIdx >= getHostSeed().size()) {
                            log.info("End of hostseed, starting over");
                            connectToIdx = 0;
                        }
                    }
                    continue;
                } else if (connectionType.equals(ConnectionType.SECONDARY) && !Boolean.TRUE.equals(hello.getSecondary())) {
                    log.info("want secondary connection, got other - retrying");
                    disconnect();
                    Thread.sleep(1000);//Slowing down
                    connectToIdx++;
                    if (connectToIdx >= getHostSeed().size()) {
                        log.info("End of hostseed, starting over");
                        connectToIdx = 0;
                    }
                    continue;
                }
                setMaxBsonObjectSize(hello.getMaxBsonObjectSize());
                setMaxMessageSize(hello.getMaxMessageSizeBytes());
                setMaxWriteBatchSize(hello.getMaxWriteBatchSize());
                startHeartbeat();
                break;
            } catch (Exception e) {
                log.error("connection failed", e);
            }
        }
    }


    protected synchronized void startHeartbeat() {
        if (heartbeat == null) {
            log.info("Starting heartbeat ");
            heartbeat = executor.scheduleWithFixedDelay(() -> {

                log.info("checking connection");
                try {
                    HelloCommand cmd = new HelloCommand(getConnection())
                            .setHelloOk(true)
                            .setIncludeClient(false);
                    var hello = cmd.execute();
                    if (connectionType.equals(ConnectionType.PRIMARY) && !Boolean.TRUE.equals(hello.getWritablePrimary())) {
                        log.info("wanted primary connection, changed to secondary, retrying");
                        disconnect();
                        Thread.sleep(1000);
                        connect(getReplicaSetName());

                    } else if (connectionType.equals(ConnectionType.SECONDARY) && !Boolean.TRUE.equals(hello.getSecondary())) {
                        log.info("state changed, wanted secondary, got something differnt now -reconnecting");
                        disconnect();
                        Thread.sleep(1000);//Slowing down
                        connect(getReplicaSetName());

                    }
                } catch (MorphiumDriverException e) {
                    log.error("Error in heartbeat", e);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }, getHeartbeatFrequency(), getHeartbeatFrequency(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void watch(WatchCommand settings) throws MorphiumDriverException {
        connection.watch(settings);
    }
//
//    @Override
//    public Map<String, Object> readSingleAnswer(int msgId) throws MorphiumDriverException {
//        return connection.readSingleAnswer(msgId);
//    }
//
//    @Override
//    public List<Map<String, Object>> readAnswerFor(int msgId) throws MorphiumDriverException {
//        return connection.readAnswerFor(msgId);
//    }
//
//    @Override
//    public MorphiumCursor getAnswerFor(int msgId) throws MorphiumDriverException {
//        return connection.getAnswerFor(msgId);
//    }
//
//    @Override
//    public List<Map<String, Object>> readAnswerFor(MorphiumCursor crs) throws MorphiumDriverException {
//        return connection.readAnswerFor(crs);
//    }

    @Override
    public MongoConnection getConnection() {
        return connection;
    }


    @Override
    public String getName() {
        return "SingleMongoConnectDriver";
    }


    @Override
    public void setConnectionUrl(String connectionUrl) {

    }


    public boolean isConnected() {
        return connection != null && connection.isConnected();
    }

    @Override
    public void disconnect() {

        connection.disconnect();

    }

    @Override
    public void commitTransaction() throws MorphiumDriverException {
        if (getTransactionContext() == null)
            throw new IllegalArgumentException("No transaction in progress, cannot commit");
        MorphiumTransactionContext ctx = getTransactionContext();
        runCommand("admin", Doc.of("commitTransaction", 1, "txnNumber", ctx.getTxnNumber(), "autocommit", false, "lsid", Doc.of("id", ctx.getLsid())));
        clearTransactionContext();
    }

    @Override
    public void abortTransaction() throws MorphiumDriverException {
        if (getTransactionContext() == null)
            throw new IllegalArgumentException("No transaction in progress, cannot abort");
        MorphiumTransactionContext ctx = getTransactionContext();
        runCommand("admin", Doc.of("abortTransaction", 1, "txnNumber", ctx.getTxnNumber(), "autocommit", false, "lsid", Doc.of("id", ctx.getLsid())));
        clearTransactionContext();
    }

    @Override
    public SingleElementResult runCommandSingleResult(SingleResultCommand cmd) throws MorphiumDriverException {
        var start = System.currentTimeMillis();
        var m = cmd.asMap();
        var msg = getConnection().sendCommand(m);
        var res = getConnection().readSingleAnswer(msg);
        return new SingleElementResult().setResult(res).setDuration(System.currentTimeMillis() - start)
                .setServer(getConnection().getConnectedTo());
    }

    @Override
    public CursorResult runCommand(MultiResultCommand cmd) throws MorphiumDriverException {
        var start = System.currentTimeMillis();
        var msg = getConnection().sendCommand(cmd.asMap());
        return new CursorResult().setCursor(getConnection().getAnswerFor(msg))
                .setMessageId(msg).setDuration(System.currentTimeMillis() - start)
                .setServer(getConnection().getConnectedTo());
    }

    @Override
    public ListResult runCommandList(MultiResultCommand cmd) throws MorphiumDriverException {
        var start = System.currentTimeMillis();
        var msg = getConnection().sendCommand(cmd.asMap());
        return new ListResult().setResult(getConnection().readAnswerFor(msg))
                .setMessageId(msg).setDuration(System.currentTimeMillis() - start)
                .setServer(getConnection().getConnectedTo());
    }

    @Override
    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        cmd.put("$db", db);
        var start = System.currentTimeMillis();
        var msg = getConnection().sendCommand(cmd);
        return getConnection().readSingleAnswer(msg);
    }


    enum ConnectionType {
        PRIMARY, SECONDARY, ANY,
    }


    @Override
    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        return new NetworkCallHelper<Map<String, Object>>().doCall(() -> {
            var ret = runCommand("admin", Doc.of("replSetGetStatus", 1));
            @SuppressWarnings("unchecked") List<Doc> mem = (List) ret.get("members");
            if (mem == null) {
                return null;
            }
            //noinspection unchecked
            mem.stream().filter(d -> d.get("optime") instanceof Map).forEach(d -> d.put("optime", ((Map<String, Doc>) d.get("optime")).get("ts")));
            return ret;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }


    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        return runCommand(db, Doc.of("dbstats", 1));
    }


    @Override
    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        return runCommand(db, Doc.of("collStats", coll, "scale", 1024));
    }


    public Map<String, Object> currentOp(long threshold) throws MorphiumDriverException {
        return runCommand("admin", Doc.of("currentOp", 1, "secs_running", Doc.of("$gt", threshold)))
                ;
    }



    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        if (crs == null) {
            return;
        }
        killCursors(crs.getDb(), crs.getCollection(), crs.getCursorId());
    }

    protected void killCursors(String db, String coll, long... ids) throws MorphiumDriverException {
        List<Long> cursorIds = new ArrayList<>();
        for (long l : ids) {
            if (l != 0) {
                cursorIds.add(l);
            }
        }
        if (cursorIds.isEmpty()) {
            return;
        }

        KillCursorsCommand k = new KillCursorsCommand(connection)
                .setCursorIds(cursorIds)
                .setDb(db)
                .setColl(coll);

        var ret = k.execute();
        log.info("killed cursor");
    }

//    @Override
//    public RunCommandResult sendCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
//        OpMsg q = new OpMsg();
//        cmd.put("$db", db);
//        q.setMessageId(getNextId());
//        q.setFirstDoc(Doc.of(cmd));
//
//        OpMsg rep = null;
//        long start = System.currentTimeMillis();
//        connection.sendQuery(q);
//        return new RunCommandResult().setMessageId(q.getMessageId())
//                .setDuration(System.currentTimeMillis() - start)
//                .setServer(connection.getConnectedTo());
//    }

    private Map<String, Object> getSingleDocAndKillCursor(OpMsg msg) throws MorphiumDriverException {
        if (!msg.hasCursor()) return null;
        Map<String, Object> cursor = (Map<String, Object>) msg.getFirstDoc().get("cursor");
        Map<String, Object> ret = null;
        if (cursor.containsKey("firstBatch")) {
            ret = (Map<String, Object>) cursor.get("firstBatch");
        } else {
            ret = (Map<String, Object>) cursor.get("nextBatch");
        }
        String[] namespace = cursor.get("ns").toString().split("\\.");
        killCursors(namespace[0], namespace[1], (Long) cursor.get("id"));
        return ret;
    }


    private List<Map<String, Object>> readBatches(int waitingfor, int batchSize) throws MorphiumDriverException {
        List<Map<String, Object>> ret = new ArrayList<>();

        Map<String, Object> doc;
        String db = null;
        String coll = null;
        while (true) {
            OpMsg reply = connection.getReplyFor(waitingfor, getMaxWaitTime());
            if (reply.getResponseTo() != waitingfor) {
                log.error("Wrong answer - waiting for " + waitingfor + " but got " + reply.getResponseTo());
                log.error("Document: " + Utils.toJsonString(reply.getFirstDoc()));
                continue;
            }

            //                    replies.remove(i);
            @SuppressWarnings("unchecked") Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");
            if (cursor == null) {
                //trying result
                if (reply.getFirstDoc().get("result") != null) {
                    //noinspection unchecked
                    return (List<Map<String, Object>>) reply.getFirstDoc().get("result");
                }
                if (reply.getFirstDoc().containsKey("results")) {
                    return (List<Map<String, Object>>) reply.getFirstDoc().get("results");
                }
                throw new MorphiumDriverException("Mongo Error: " + reply.getFirstDoc().get("codeName") + " - " + reply.getFirstDoc().get("errmsg"));
            }
            if (db == null) {
                //getting ns
                String[] namespace = cursor.get("ns").toString().split("\\.");
                db = namespace[0];
                if (namespace.length > 1)
                    coll = namespace[1];
            }
            if (cursor.get("firstBatch") != null) {
                //noinspection unchecked
                ret.addAll((List) cursor.get("firstBatch"));
            } else if (cursor.get("nextBatch") != null) {
                //noinspection unchecked
                ret.addAll((List) cursor.get("nextBatch"));
            }
            if (((Long) cursor.get("id")) != 0) {
                //                        log.info("getting next batch for cursor " + cursor.get("id"));
                //there is more! Sending getMore!

                //there is more! Sending getMore!
                OpMsg q = new OpMsg();
                q.setFirstDoc(Doc.of("getMore", (Object) cursor.get("id"))
                        .add("$db", db)
                        .add("batchSize", batchSize)
                );
                if (coll != null) q.getFirstDoc().put("collection", coll);
                q.setMessageId(getNextId());
                waitingfor = q.getMessageId();
                connection.sendQuery(q);
            } else {
                break;
            }

        }
        return ret;
    }


    public boolean exists(String db) throws MorphiumDriverException {
        //noinspection EmptyCatchBlock
        try {
            getDBStats(db);
            return true;
        } catch (MorphiumDriverException e) {
        }
        return false;
    }


    public Map<String, Object> getDbStats(String db) throws MorphiumDriverException {
        return getDbStats(db, false);
    }


    public Map<String, Object> getDbStats(String db, boolean withStorage) throws MorphiumDriverException {
        return new NetworkCallHelper<Map<String, Object>>().doCall(() -> {
            OpMsg msg = new OpMsg();
            msg.setMessageId(getNextId());
            Map<String, Object> v = Doc.of("dbStats", 1, "scale", 1024);
            v.put("$db", db);
            if (withStorage) {
                v.put("freeStorage", 1);
            }
            msg.setFirstDoc(v);
            connection.sendQuery(msg);
            OpMsg reply = connection.getReplyFor(msg.getMessageId(), getMaxWaitTime());
            return reply.getFirstDoc();
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }


    private List<Map<String, Object>> getCollectionInfo(String db, String collection) throws MorphiumDriverException {

        //noinspection unchecked
        return new NetworkCallHelper<List<Map<String, Object>>>().doCall(() -> {
            Map<String, Object> cmd = new Doc();
            cmd.put("listCollections", 1);
            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());

            if (collection != null) {
                cmd.put("filter", Doc.of("name", collection));
            }
            cmd.put("$db", db);
            q.setFirstDoc(cmd);
            q.setFlags(0);
            q.setResponseTo(0);

            List<Map<String, Object>> ret;
            connection.sendQuery(q);
            ret = readBatches(q.getMessageId(), getMaxWriteBatchSize());
            return ret;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }


    @Override
    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        List<Map<String, Object>> lst = getCollectionInfo(db, coll);
        try {
            if (!lst.isEmpty() && lst.get(0).get("name").equals(coll)) {
                Object capped = ((Map) lst.get(0).get("options")).get("capped");
                return capped != null && capped.equals(true);
            }
        } catch (Exception e) {
            log.error("Error", e);
        }
        return false;
    }

    @Override
    public Map<String, Integer> getNumConnectionsByHost() {
        return UtilsMap.of(connection.getConnectedTo(), 1);
    }


    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return new BulkRequestContext(m) {
            private final List<BulkRequest> requests = new ArrayList<>();


            public Doc execute() {
                try {
                    for (BulkRequest r : requests) {
                        if (r instanceof InsertBulkRequest) {
                            InsertMongoCommand settings = new InsertMongoCommand(getConnection());
                            settings.setDb(db).setColl(collection)
                                    .setComment("Bulk insert")
                                    .setDocuments(((InsertBulkRequest) r).getToInsert());
                            settings.execute();
                        } else if (r instanceof UpdateBulkRequest) {
                            UpdateBulkRequest up = (UpdateBulkRequest) r;
                            UpdateMongoCommand upCmd = new UpdateMongoCommand(getConnection());
                            upCmd.setColl(collection).setDb(db)
                                    .setUpdates(Arrays.asList(Doc.of("q", up.getQuery(), "u", up.getCmd(), "upsert", up.isUpsert(), "multi", up.isMultiple())));
                            upCmd.execute();
                        } else if (r instanceof DeleteBulkRequest) {
                            DeleteBulkRequest dbr = ((DeleteBulkRequest) r);
                            DeleteMongoCommand del = new DeleteMongoCommand(getConnection());
                            del.setColl(collection).setDb(db)
                                    .setDeletes(Arrays.asList(Doc.of("q", dbr.getQuery(), "limit", dbr.isMultiple() ? 0 : 1)));
                            del.execute();
                        } else {
                            throw new RuntimeException("Unknown operation " + r.getClass().getName());
                        }
                    }
                } catch (MorphiumDriverException e) {
                    log.error("Got exception: ", e);
                }
                return new Doc();

            }


            public UpdateBulkRequest addUpdateBulkRequest() {
                UpdateBulkRequest up = new UpdateBulkRequest();
                requests.add(up);
                return up;
            }


            public InsertBulkRequest addInsertBulkRequest(List<Map<String, Object>> toInsert) {
                InsertBulkRequest in = new InsertBulkRequest(toInsert);
                requests.add(in);
                return in;
            }


            public DeleteBulkRequest addDeleteBulkRequest() {
                DeleteBulkRequest del = new DeleteBulkRequest();
                requests.add(del);
                return del;
            }
        };
    }


}
