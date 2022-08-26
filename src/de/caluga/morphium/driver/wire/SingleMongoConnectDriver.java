package de.caluga.morphium.driver.wire;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.AggregatorImpl;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.*;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    private Map<DriverStatsKey, AtomicDecimal> stats = new HashMap<>();
    private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {

        @Override
        public Thread newThread(Runnable r) {
            Thread ret = new Thread(r);
            ret.setName("MCon_" + (stats.get(DriverStatsKey.THREADS_CREATED).incrementAndGet()));
            ret.setDaemon(true);
            return ret;
        }
    });

    public SingleMongoConnectDriver() {
        for (var e : DriverStatsKey.values()) {
            stats.put(e, new AtomicDecimal(0));
        }
    }

    @Override
    public <T, R> Aggregator<T, R> createAggregator(Morphium morphium, Class<? extends T> type, Class<? extends R> resultType) {
        return new AggregatorImpl<>(morphium, type, resultType);
    }

    @Override
    public Map<DriverStatsKey, Double> getDriverStats() {
        Map<DriverStatsKey, Double> ret = new HashMap<>();
        //copy to avoid concurrent modification
        Map<DriverStatsKey, AtomicDecimal> hashMap = new HashMap(stats);
        for (var e : hashMap.entrySet()) {
            ret.put(e.getKey(), e.getValue().get());
        }
        if (connection != null) {
            for (var e : connection.getStats().entrySet()) {
                ret.putIfAbsent(e.getKey(), 0.0);
                ret.put(e.getKey(), ret.get(e.getKey()) + e.getValue());
            }
        }
        return ret;
    }

    public MongoConnection getConnection() {
        incStat(DriverStatsKey.CONNECTIONS_BORROWED);
        return new ConnectionWrapper(connection);
    }

    private ScheduledFuture<?> heartbeat;
    public final static String driverName = "SingleMongoConnectDriver";

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

    private double decStat(DriverStatsKey k) {
        return stats.get(k).decrementAndGet();
    }

    private double incStat(DriverStatsKey k) {
        return stats.get(k).incrementAndGet();
    }

    @Override
    public void connect(String replSet) throws MorphiumDriverException {
        //log.info("Connecting");
        int connectToIdx = 0;
        while (true) {
            try {
                incStat(DriverStatsKey.CONNECTIONS_OPENED);
                String host = getHostSeed().get(connectToIdx);
                String h[] = host.split(":");
                int port = 27017;
                if (h.length > 1) {
                    port = Integer.parseInt(h[1]);
                }
                connection = new SingleMongoConnection();
                if (getAuthDb() != null) {
                    connection.setCredentials(getAuthDb(), getUser(), getPassword());
                }
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
                    connection.close();
                    incStat(DriverStatsKey.CONNECTIONS_CLOSED);
                    connection = null;
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
                    connection.close();
                    incStat(DriverStatsKey.CONNECTIONS_CLOSED);
                    connection = null;
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
                incStat(DriverStatsKey.ERRORS);
                log.error("connection failed", e);
                connectToIdx++;
                if (connectToIdx > getHostSeed().size()) {
                    connectToIdx = 0;
                }
            }
        }
        incStat(DriverStatsKey.CONNECTIONS_IN_USE);
        // log.info("Connected! "+connection.getConnectedTo()+" / "+getHostSeed().get(connectToIdx));
    }


    protected synchronized void startHeartbeat() {
        if (heartbeat == null) {
            log.info("Starting heartbeat ");
            heartbeat = executor.scheduleWithFixedDelay(() -> {

                //log.info("checking connection");
                try {
                    HelloCommand cmd = new HelloCommand(connection)
                            .setHelloOk(true)
                            .setIncludeClient(false);
                    var hello = cmd.execute();
                    if (connectionType.equals(ConnectionType.PRIMARY) && !Boolean.TRUE.equals(hello.getWritablePrimary())) {
                        log.warn("wanted primary connection, changed to secondary, retrying");
                        incStat(DriverStatsKey.FAILOVERS);
                        connection.close();
                        decStat(DriverStatsKey.CONNECTIONS_IN_USE);
                        connection = null;
                        incStat(DriverStatsKey.CONNECTIONS_CLOSED);
                        Thread.sleep(1000);
                        connect(getReplicaSetName());

                    } else if (connectionType.equals(ConnectionType.SECONDARY) && !Boolean.TRUE.equals(hello.getSecondary())) {
                        log.warn("state changed, wanted secondary, got something differnt now -reconnecting");
                        connection.close();
                        decStat(DriverStatsKey.CONNECTIONS_IN_USE);
                        incStat(DriverStatsKey.CONNECTIONS_CLOSED);
                        connection = null;
                        incStat(DriverStatsKey.FAILOVERS);

                        Thread.sleep(1000);//Slowing down
                        connect(getReplicaSetName());

                    }
                } catch (MorphiumDriverException e) {
                    incStat(DriverStatsKey.ERRORS);
                    log.error("Connection error", e);
                    log.warn("Trying reconnect");
                    try {
                        close();
                    } catch (Exception ex) {
                        //swallow - maybe error because connection died
                    }

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        //really?
                    }
                    try {
                        connect();
                    } catch (MorphiumDriverException ex) {
                        log.error("Could not reconnect", ex);
                    }
                } catch (InterruptedException e) {
                    //e.printStackTrace();
                } catch (Exception e) {
                    incStat(DriverStatsKey.ERRORS);
                    e.printStackTrace();
                }


            }, getHeartbeatFrequency(), getHeartbeatFrequency(), TimeUnit.MILLISECONDS);
        } else {
            log.debug("Heartbeat already scheduled...");
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
    public void releaseConnection(MongoConnection con) {
        incStat(DriverStatsKey.CONNECTIONS_RELEASED);
        if (con instanceof ConnectionWrapper) {
            ((ConnectionWrapper) con).setDelegate(null);
        }
    }

    @Override
    public MongoConnection getReadConnection(ReadPreference rp) {
        return getConnection();
    }

    @Override
    public MongoConnection getPrimaryConnection(WriteConcern wc) {
        return getConnection();
    }

    @Override
    public void close() {
        incStat(DriverStatsKey.CONNECTIONS_CLOSED);
        decStat(DriverStatsKey.CONNECTIONS_IN_USE);
        try {
            if (connection != null)
                connection.close();
        } finally {
        }

        if (heartbeat != null)
            heartbeat.cancel(true);
        heartbeat = null;
    }

    @Override
    public String getName() {

        return driverName;
    }


    @Override
    public void setConnectionUrl(String connectionUrl) {

    }


    public boolean isConnected() {
        return connection != null && connection.isConnected();
    }


    @Override
    public void commitTransaction() throws MorphiumDriverException {
        if (getTransactionContext() == null)
            throw new IllegalArgumentException("No transaction in progress, cannot commit");
        MorphiumTransactionContext ctx = getTransactionContext();

        var cmd=new CommitTransactionCommand(connection).setTxnNumber(ctx.getTxnNumber()).setAutocommit(false)
                .setLsid(ctx.getLsid());
        cmd.execute();
        clearTransactionContext();

    }

    @Override
    public void abortTransaction() throws MorphiumDriverException {
        if (getTransactionContext() == null)
            throw new IllegalArgumentException("No transaction in progress, cannot abort");
        MorphiumTransactionContext ctx = getTransactionContext();
        var cmd=new AbortTransactionCommand(connection).setTxnNumber(ctx.getTxnNumber()).setAutocommit(false)
                .setLsid(ctx.getLsid());
        cmd.execute();
        clearTransactionContext();
    }

    @Override
    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        CollStatsCommand cmd = new CollStatsCommand(connection);
        return cmd.execute();
    }
//
//    @Override
//    public SingleElementResult runCommandSingleResult(SingleResultCommand cmd) throws MorphiumDriverException {
//        var start = System.currentTimeMillis();
//        var m = cmd.asMap();
//        var msg = getConnection().sendCommand(m);
//        var res = getConnection().readSingleAnswer(msg);
//        return new SingleElementResult().setResult(res).setDuration(System.currentTimeMillis() - start)
//                .setServer(getConnection().getConnectedTo()).setMetadata(cmd.getMetaData());
//    }
//
//    @Override
//    public CursorResult runCommand(MultiResultCommand cmd) throws MorphiumDriverException {
//        var start = System.currentTimeMillis();
//        var msg = getConnection().sendCommand(cmd.asMap());
//        return new CursorResult().setCursor(getConnection().getAnswerFor(msg, getDefaultBatchSize()))
//                .setMessageId(msg).setDuration(System.currentTimeMillis() - start)
//                .setServer(getConnection().getConnectedTo()).setMetadata(cmd.getMetaData());
//    }
//
//    @Override
//    public ListResult runCommandList(MultiResultCommand cmd) throws MorphiumDriverException {
//        var start = System.currentTimeMillis();
//        var msg = getConnection().sendCommand(cmd.asMap());
//        return new ListResult().setResult(getConnection().readAnswerFor(msg))
//                .setMessageId(msg).setDuration(System.currentTimeMillis() - start)
//                .setServer(getConnection().getConnectedTo()).setMetadata(cmd.getMetaData());
//    }
//
//    @Override
//    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
//        cmd.put("$db", db);
//        var start = System.currentTimeMillis();
//        var msg = getConnection().sendCommand(cmd);
//        return getConnection().readSingleAnswer(msg);
//    }


    @Override
    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {

        ReplicastStatusCommand cmd = new ReplicastStatusCommand(getPrimaryConnection(null));
        var result = cmd.execute();
        @SuppressWarnings("unchecked") List<Doc> mem = (List) result.get("members");
        if (mem == null) {
            return null;
        }
        //noinspection unchecked
        mem.stream().filter(d -> d.get("optime") instanceof Map).forEach(d -> d.put("optime", ((Map<String, Doc>) d.get("optime")).get("ts")));
        return result;
    }


    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        return new DbStatsCommand(getPrimaryConnection(null)).setDb(db).execute();
    }

    public List<Map<String, Object>> currentOp(int threshold) throws MorphiumDriverException {
        CurrentOpCommand cmd = new CurrentOpCommand(connection).setColl("admin").setSecsRunning(threshold);
        return cmd.execute();
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return new BulkRequestContext(m) {
            private final List<BulkRequest> requests = new ArrayList<>();


            public Doc execute() {
                try {
                    for (BulkRequest r : requests) {
                        if (r instanceof InsertBulkRequest) {
                            InsertMongoCommand settings = new InsertMongoCommand(connection);
                            settings.setDb(db).setColl(collection)
                                    .setComment("Bulk insert")
                                    .setDocuments(((InsertBulkRequest) r).getToInsert());
                            settings.execute();
                        } else if (r instanceof UpdateBulkRequest) {
                            UpdateBulkRequest up = (UpdateBulkRequest) r;
                            UpdateMongoCommand upCmd = new UpdateMongoCommand(connection);
                            upCmd.setColl(collection).setDb(db)
                                    .setUpdates(Arrays.asList(Doc.of("q", up.getQuery(), "u", up.getCmd(), "upsert", up.isUpsert(), "multi", up.isMultiple())));
                            upCmd.execute();
                        } else if (r instanceof DeleteBulkRequest) {
                            DeleteBulkRequest dbr = ((DeleteBulkRequest) r);
                            DeleteMongoCommand del = new DeleteMongoCommand(connection);
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
//
//    private Map<String, Object> getSingleDocAndKillCursor(OpMsg msg) throws MorphiumDriverException {
//        if (!msg.hasCursor()) return null;
//        Map<String, Object> cursor = (Map<String, Object>) msg.getFirstDoc().get("cursor");
//        Map<String, Object> ret = null;
//        if (cursor.containsKey("firstBatch")) {
//            ret = (Map<String, Object>) cursor.get("firstBatch");
//        } else {
//            ret = (Map<String, Object>) cursor.get("nextBatch");
//        }
//        String[] namespace = cursor.get("ns").toString().split("\\.");
//        killCursors(namespace[0], namespace[1], (Long) cursor.get("id"));
//        return ret;
//    }


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

    private class ConnectionWrapper implements MongoConnection {
        private MongoConnection delegate;

        public MongoConnection getDelegate() {
            if (delegate == null) throw new RuntimeException("Connection released!");
            return delegate;
        }

        public ConnectionWrapper(MongoConnection con) {
            delegate = con;
        }

        public void setDelegate(MongoConnection con) {
            delegate = con;
        }

        @Override
        public MorphiumDriver getDriver() {
            return SingleMongoConnectDriver.this;
        }

        @Override
        public int getSourcePort() {
            return 0;
        }

        @Override
        public void setCredentials(String authDb, String userName, String password) {
            delegate.setCredentials(authDb, userName, password);
        }

        @Override
        public HelloResult connect(MorphiumDriver drv, String host, int port) throws IOException, MorphiumDriverException {
            return getDelegate().connect(drv, host, port);
        }

        @Override
        public void close() {
            release();
            getDelegate().close();
        }

        @Override
        public boolean isConnected() {
            return getDelegate().isConnected();
        }

        @Override
        public String getConnectedTo() {
            return getDelegate().getConnectedTo();
        }

        @Override
        public String getConnectedToHost() {
            return getDelegate().getConnectedToHost();
        }

        @Override
        public int getConnectedToPort() {
            return getDelegate().getConnectedToPort();
        }

        @Override
        public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
            getDelegate().closeIteration(crs);
        }

        @Override
        public Map<String, Object> killCursors(String db, String coll, long... ids) throws MorphiumDriverException {
            return getDelegate().killCursors(db, coll, ids);
        }

        @Override
        public boolean replyAvailableFor(int msgId) {
            return getDelegate().replyAvailableFor(msgId);
        }

        @Override
        public OpMsg getReplyFor(int msgid, long timeout) throws MorphiumDriverException {
            return getDelegate().getReplyFor(msgid, timeout);
        }

        @Override
        public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
            return getDelegate().readSingleAnswer(id);
        }

        @Override
        public void watch(WatchCommand settings) throws MorphiumDriverException {
            getDelegate().watch(settings);
        }

        @Override
        public List<Map<String, Object>> readAnswerFor(int queryId) throws MorphiumDriverException {
            return getDelegate().readAnswerFor(queryId);
        }

        @Override
        public MorphiumCursor getAnswerFor(int queryId, int batchsize) throws MorphiumDriverException {
            return getDelegate().getAnswerFor(queryId, batchsize);
        }

        @Override
        public List<Map<String, Object>> readAnswerFor(MorphiumCursor crs) throws MorphiumDriverException {
            return getDelegate().readAnswerFor(crs);
        }

        @Override
        public int sendCommand(MongoCommand cmd) throws MorphiumDriverException {
            return getDelegate().sendCommand(cmd);
        }


        @Override
        public void release() {
            getDriver().releaseConnection(this);
        }
    }


}
