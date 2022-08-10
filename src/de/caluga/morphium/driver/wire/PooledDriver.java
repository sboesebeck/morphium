package de.caluga.morphium.driver.wire;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.*;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class PooledDriver extends DriverBase {
    public final static String driverName = "PooledDriver";
    private Map<String, List<Connection>> connectionPool;
    private Map<Integer, Connection> borrowedConnections;
    private Map<DriverStatsKey, AtomicDecimal> stats;
    private long fastestTime = 10000;
    private String fastestHost = "";
    private final Logger log = LoggerFactory.getLogger(SingleMongoConnectDriver.class);
    private String primaryNode;
    private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
        private AtomicLong l = new AtomicLong(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread ret = new Thread(r);
            ret.setName("MCon_" + (l.incrementAndGet()));
            ret.setDaemon(true);
            return ret;
        }

        //todo - heartbeat implementation
    });
    private int lastSecondaryNode;

    public PooledDriver() {
        connectionPool = new HashMap<>();
        borrowedConnections = Collections.synchronizedMap(new HashMap<>());
        stats = new ConcurrentHashMap<>();
        for (var e : DriverStatsKey.values()) {
            stats.put(e, new AtomicDecimal(0));
        }
    }

    @Override
    public void connect(String replSet) throws MorphiumDriverException {
        int connectToIdx = 0;
        FOR:
        for (String host : getHostSeed()) {
            for (int i = 0; i < getMinConnectionsPerHost(); i++) {
                try {
                    connectToHost(host);
                } catch (MorphiumDriverException e) {
                    log.error("Could not connect to " + host, e);
                }
            }
        }
        setReplicaSet(getHostSeed().size() > 1);

        startHeartbeat();


    }

    private void connectToHost(String host) throws MorphiumDriverException {

        String h = getHost(host);
        int port = getPortFromHost(host);

        var con = new SingleMongoConnection();
        long start = System.currentTimeMillis();
        var hello = con.connect(this, h, port);
        stats.get(DriverStatsKey.CONNECTIONS_OPENED).incrementAndGet();
        long dur = System.currentTimeMillis() - start;
        if (fastestTime > dur) {
            fastestTime = dur;
            fastestHost = host;
        }
        synchronized (connectionPool) {
            connectionPool.putIfAbsent(host, new CopyOnWriteArrayList<>());
            connectionPool.get(host).add(new Connection(con));
        }
        if (hello.getWritablePrimary()) {
            primaryNode = host;
        }
        setMaxBsonObjectSize(hello.getMaxBsonObjectSize());
        setMaxMessageSize(hello.getMaxMessageSizeBytes());
        setMaxWriteBatchSize(hello.getMaxWriteBatchSize());

    }

    private ScheduledFuture<?> heartbeat;


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

    protected synchronized void startHeartbeat() {
        if (heartbeat == null) {
            log.info("Starting heartbeat ");
            heartbeat = executor.scheduleWithFixedDelay(() -> {
                var copy = new HashMap<>(connectionPool); //avoid concurrent modification exception
                for (var e : copy.entrySet()) {
                    //checking max lifetime
                    List<Connection> connections = new ArrayList<>(e.getValue());
                    for (var c : connections) {
                        if (System.currentTimeMillis() - c.getCreated() > getMaxConnectionLifetime()) {
                            try {
                                //max lifetime exceeded
                                //log.info("Lifetime exceeded...");
                                if (copy.get(e.getKey()).remove(c)) {
                                    c.getCon().close();
                                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                }
                            } catch (Exception ex) {

                            }
                        } else if (System.currentTimeMillis() - c.getLastUsed() > getMaxConnectionIdleTime()) {
                            try {
                                log.info("Unused connection closed");
                                if (copy.get(e.getKey()).remove(c)) {
                                    c.getCon().close();
                                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                }
                            } catch (Exception ex) {

                            }
                        }
                        HelloCommand h = new HelloCommand(c.getCon()).setHelloOk(true).setIncludeClient(false);
                        try {
                            long start = System.currentTimeMillis();
                            var hello = h.execute();
                            long dur = System.currentTimeMillis() - start;
                            if (dur < fastestTime) {
                                fastestTime = dur;
                                fastestHost = e.getKey();
                            }
                            if (hello != null && hello.getWritablePrimary()) {
                                for (String hst : hello.getHosts()) {
                                    if (!connectionPool.containsKey(hst)) {
                                        log.info("new host needs to be added: " + hst);
                                        connectionPool.put(hst, new ArrayList<>());
                                    }
                                }
                                for (String k : connectionPool.keySet()) {
                                    if (!hello.getHosts().contains(k)) {
                                        log.warn("Host " + k + " is not part of the replicaset anymore!");
                                        List<Connection> lst = connectionPool.remove(k);
                                        if (fastestHost.equals(k)) {
                                            fastestHost = null;
                                            fastestTime = 10000;
                                        }
                                        for (Connection con : lst) {
                                            try {
                                                con.getCon().close();
                                                stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                            } catch (Exception ex) {
                                            }
                                        }
                                    }
                                }
                            }
                        } catch (MorphiumDriverException ex) {
                            if (!ex.getMessage().contains("closed")) {
                                log.error("Error talking to " + e.getKey(), ex);
                                copy.get(e.getKey()).remove(c);
                                try {
                                    c.getCon().close();
                                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                } catch (Exception exc) {
                                    //swallow - something was broken before already!
                                }
                            }
                        }
                    }
                    while (e.getValue().size() < getMinConnectionsPerHost()) {
                        //need to add connections!
                        try {
                            connectToHost(e.getKey());
                        } catch (MorphiumDriverException ex) {
                            log.error("Could not fill connection pool for " + e.getKey(), ex);
                            break;
                        }
                    }
                }


            }, getHeartbeatFrequency(), getHeartbeatFrequency(), TimeUnit.MILLISECONDS);
        } else {
            log.debug("Heartbeat already scheduled...");
        }
    }

    @Override
    public void watch(WatchCommand settings) throws MorphiumDriverException {
        MongoConnection con = null;
        try {
            con = getPrimaryConnection(null);
            con.watch(settings);
        } finally {
            if (con != null) {
                con.release();
            }
        }
    }

    private synchronized int getTotalConnectionsToHost(String h) {
        int borrowed = 0;
        for (var c : borrowedConnections.values()) {
            if (c.getCon().getConnectedTo().equals(h)) {
                borrowed++;
            }
        }
        return borrowed + connectionPool.get(h).size();
    }

    private synchronized MongoConnection borrowConnection(String host) throws MorphiumDriverException {
        Connection c = null;
        if (connectionPool.get(host).size() == 0) {
            //if too many connections were already borrowed, wait for some to return
            long start = System.currentTimeMillis();
            while (getTotalConnectionsToHost(host) > getMaxConnectionsPerHost()) {
                if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                    log.error("maxwaitTime exceeded while waiting for a connection - connectionpool exceeded!");
                    throw new MorphiumDriverException("Could not get connection in time");
                }
                Thread.yield();
            }
            try {
                String h = getHost(host);
                int port = getPortFromHost(host);

                var con = new SingleMongoConnection();
                var hello = con.connect(this, h, port);
                c = new Connection(con);
                stats.get(DriverStatsKey.CONNECTIONS_OPENED).incrementAndGet();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            }
        } else {
            c = connectionPool.get(host).remove(0);
        }
        borrowedConnections.put(c.getCon().getSourcePort(), c);
        stats.get(DriverStatsKey.CONNECTIONS_BORROWED).incrementAndGet();
        return c.getCon();

    }

    @Override
    public MongoConnection getReadConnection(ReadPreference rp) {
        try {
            if (getHostSeed().size() == 1 || !isReplicaSet()) {
                //no replicaset
                return borrowConnection(primaryNode);
            }
            if (rp == null) rp = getDefaultReadPreference();
            switch (rp.getType()) {
                case NEAREST:
                    //check fastest answer time
                    if (fastestHost != null) {
                        return borrowConnection(fastestHost);
                    }
                case PRIMARY:
                    return borrowConnection(primaryNode);
                case PRIMARY_PREFERRED:
                    if (connectionPool.get(primaryNode).size() != 0) {
                        return borrowConnection(primaryNode);
                    }
                case SECONDARY_PREFERRED:
                case SECONDARY:
                    //round-robin
                    if (lastSecondaryNode >= getHostSeed().size()) {
                        lastSecondaryNode = 0;
                    }
                    if (getHostSeed().get(lastSecondaryNode).equals(primaryNode)) {
                        lastSecondaryNode++;
                        if (lastSecondaryNode > getHostSeed().size()) {
                            lastSecondaryNode = 0;
                        }
                    }
                    return borrowConnection(getHostSeed().get(lastSecondaryNode++));
                default:
                    throw new IllegalArgumentException("Unhandeled Readpreferencetype " + rp.getType());
            }
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public MongoConnection getPrimaryConnection(WriteConcern wc) throws MorphiumDriverException {
        if (primaryNode == null) {
            throw new MorphiumDriverException("No primary node found - connection not established yet?");
        }
        return borrowConnection(primaryNode);
    }

    @Override
    public void releaseConnection(MongoConnection con) {
        if (!(con instanceof SingleMongoConnection)) {
            throw new IllegalArgumentException("Got connection of wrong type back!");
        }
        var c = borrowedConnections.remove(con.getSourcePort());

        if (c == null) {
            log.error("Returning not borrowed connection!?!?");
            c = new Connection((SingleMongoConnection) con);
        }
        connectionPool.get(con.getConnectedTo()).add(c);
        stats.get(DriverStatsKey.CONNECTIONS_RELEASED).incrementAndGet();
    }

    public boolean isConnected() {
        for (var c : connectionPool.keySet()) {
            if (getTotalConnectionsToHost(c) != 0) return true;
        }
        return false;
    }

    @Override
    public String getName() {
        return driverName;
    }


    @Override
    public void setConnectionUrl(String connectionUrl) {

    }

    @Override
    public void close() {
        for (var e : connectionPool.entrySet()) {
            for (var c : e.getValue()) {
                try {
                    c.getCon().close();
                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                } catch (Exception ex) {
                }
            }
            connectionPool.get(e.getKey()).clear();
        }

//        if (connection != null)
//            connection.close();
        if (heartbeat != null)
            heartbeat.cancel(true);
        heartbeat = null;
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

        KillCursorsCommand k = new KillCursorsCommand(null)
                .setCursorIds(cursorIds)
                .setDb(db)
                .setColl(coll);

        var ret = k.execute();
        log.info("killed cursor");
    }

    @Override
    public void commitTransaction() throws MorphiumDriverException {
        if (getTransactionContext() == null)
            throw new IllegalArgumentException("No transaction in progress, cannot commit");
        MorphiumTransactionContext ctx = getTransactionContext();
        MongoConnection con = getPrimaryConnection(null);
        var cmd = new CommitTransactionCommand(con).setTxnNumber(ctx.getTxnNumber())
                .setAutocommit(false)
                .setLsid(ctx.getLsid());
        cmd.execute();
//        getPrimaryConnection(null).sendCommand(Doc.of("commitTransaction", 1, "txnNumber", ctx.getTxnNumber(), "autocommit", false, "lsid", Doc.of("id", ctx.getLsid()), "$db", "admin"));
        clearTransactionContext();
        con.release();
    }

    @Override
    public void abortTransaction() throws MorphiumDriverException {
        if (getTransactionContext() == null)
            throw new IllegalArgumentException("No transaction in progress, cannot abort");
        MorphiumTransactionContext ctx = getTransactionContext();
        MongoConnection con = getPrimaryConnection(null);
        var cmd = new AbortTransactionCommand(con).setTxnNumber(ctx.getTxnNumber())
                .setAutocommit(false)
                .setLsid(ctx.getLsid());
        cmd.execute();
        //getPrimaryConnection(null).sendCommand(Doc.of("abortTransaction", 1, "txnNumber", ctx.getTxnNumber(), "autocommit", false, "lsid", Doc.of("id", ctx.getLsid(), "$db", "admin")));
        clearTransactionContext();
        con.release();
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

//    @Override
//    public CursorResult runCommand(MultiResultCommand cmd) throws MorphiumDriverException {
//        var start = System.currentTimeMillis();
//        var msg = getConnection().sendCommand(cmd.asMap());
//        return new CursorResult().setCursor(getConnection().getAnswerFor(msg, getDefaultBatchSize()))
//                .setMessageId(msg).setDuration(System.currentTimeMillis() - start)
//                .setServer(getConnection().getConnectedTo()).setMetadata(cmd.getMetaData());
//    }

//    @Override
//    public ListResult runCommandList(MultiResultCommand cmd) throws MorphiumDriverException {
//        var start = System.currentTimeMillis();
//        var msg = getConnection().sendCommand(cmd.asMap());
//        return new ListResult().setResult(getConnection().readAnswerFor(msg))
//                .setMessageId(msg).setDuration(System.currentTimeMillis() - start)
//                .setServer(getConnection().getConnectedTo()).setMetadata(cmd.getMetaData());
//    }

//    @Override
//    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
//        cmd.put("$db", db);
//        var start = System.currentTimeMillis();
//        var msg = getConnection().sendCommand(cmd);
//        return getConnection().readSingleAnswer(msg);
//    }

    @Override
    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        MongoConnection con = null;
        try {
            con = getPrimaryConnection(null);
            ReplicastStatusCommand cmd = new ReplicastStatusCommand(con);
            var result = cmd.execute();
            @SuppressWarnings("unchecked") List<Doc> mem = (List) result.get("members");
            if (mem == null) {
                return null;
            }
            //noinspection unchecked
            mem.stream().filter(d -> d.get("optime") instanceof Map).forEach(d -> d.put("optime", ((Map<String, Doc>) d.get("optime")).get("ts")));
            return result;
        } finally {
            if (con != null)
                con.release();
        }
    }

    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        MongoConnection con = null;
        try {
            con = getPrimaryConnection(null);
            return new DbStatsCommand(con).setDb(db).execute();
        } finally {
            if (con != null)
                con.release();
        }
    }

    @Override
    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        CollStatsCommand cmd = new CollStatsCommand(this);
        return cmd.execute();
    }

    public List<Map<String, Object>> currentOp(int threshold) throws MorphiumDriverException {
        CurrentOpCommand cmd = new CurrentOpCommand(getPrimaryConnection(null)).setColl("admin").setSecsRunning(threshold);
        return cmd.execute();
    }

    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        if (crs == null) {
            return;
        }
        killCursors(crs.getDb(), crs.getCollection(), crs.getCursorId());
    }

    private List<Map<String, Object>> readBatches(MongoConnection connection, int waitingfor, int batchSize) throws MorphiumDriverException {
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
                //there is more! Sending getMore!
                OpMsg q = new OpMsg();
                q.setFirstDoc(Doc.of("getMore", (Object) cursor.get("id"))
                        .add("$db", db)
                        .add("batchSize", batchSize)
                );
                if (coll != null) q.getFirstDoc().put("collection", coll);
                q.setMessageId(getNextId());
                waitingfor = q.getMessageId();
                // connection.sendQuery(q);
            } else {
                break;
            }

        }
        return ret;
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
            //  connection.sendQuery(msg);
            OpMsg reply = null; //connection.getReplyFor(msg.getMessageId(), getMaxWaitTime());
            return reply.getFirstDoc();
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
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

    private List<Map<String, Object>> getCollectionInfo(String db, String collection) throws MorphiumDriverException {

        //noinspection unchecked
        return new NetworkCallHelper<List<Map<String, Object>>>().doCall(() -> {
            var con = getPrimaryConnection(null);
            ListCollectionsCommand cmd = new ListCollectionsCommand(con);
            cmd.setDb(db);
            cmd.setFilter(Doc.of("name", collection));
            var ret = cmd.execute();
            con.release();
            return ret;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public Map<String, Integer> getNumConnectionsByHost() {
        Map<String, Integer> ret = new HashMap<>();
        for (var e : connectionPool.entrySet()) {
            ret.put(e.getKey(), e.getValue().size());
        }
        for (var e : borrowedConnections.values()) {
            ret.put(e.getCon().getConnectedTo(), ret.get(e.getCon().getConnectedTo()).intValue() + 1);
        }
        return ret;
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

    private class Connection {
        private SingleMongoConnection con;
        private long created;
        private long lastUsed;

        public Connection(SingleMongoConnection con) {
            this.con = con;
            created = System.currentTimeMillis();
            lastUsed = System.currentTimeMillis();
        }

        public void touch() {
            lastUsed = System.currentTimeMillis();
        }

        public SingleMongoConnection getCon() {
            return con;
        }

        public Connection setCon(SingleMongoConnection con) {
            this.con = con;
            return this;
        }

        public long getCreated() {
            return created;
        }

        public Connection setCreated(long created) {
            this.created = created;
            return this;
        }

        public long getLastUsed() {
            return lastUsed;
        }

        public Connection setLastUsed(long lastUsed) {
            this.lastUsed = lastUsed;
            return this;
        }
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return new BulkRequestContext(m) {
            private final List<BulkRequest> requests = new ArrayList<>();


            public Doc execute() {
                try {
                    for (BulkRequest r : requests) {
                        if (r instanceof InsertBulkRequest) {
                            InsertMongoCommand settings = new InsertMongoCommand(getPrimaryConnection(null));
                            settings.setDb(db).setColl(collection)
                                    .setComment("Bulk insert")
                                    .setDocuments(((InsertBulkRequest) r).getToInsert());
                            settings.execute();
                            settings.getConnection().release();
                        } else if (r instanceof UpdateBulkRequest) {
                            UpdateBulkRequest up = (UpdateBulkRequest) r;
                            UpdateMongoCommand upCmd = new UpdateMongoCommand(getPrimaryConnection(null));
                            upCmd.setColl(collection).setDb(db)
                                    .setUpdates(Arrays.asList(Doc.of("q", up.getQuery(), "u", up.getCmd(), "upsert", up.isUpsert(), "multi", up.isMultiple())));
                            upCmd.execute();
                            upCmd.getConnection().release();
                        } else if (r instanceof DeleteBulkRequest) {
                            DeleteBulkRequest dbr = ((DeleteBulkRequest) r);
                            DeleteMongoCommand del = new DeleteMongoCommand(getPrimaryConnection(null));
                            del.setColl(collection).setDb(db)
                                    .setDeletes(Arrays.asList(Doc.of("q", dbr.getQuery(), "limit", dbr.isMultiple() ? 0 : 1)));
                            del.execute();
                            del.getConnection().release();
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

    @Override
    public Map<DriverStatsKey, Double> getDriverStats() {
        Map<DriverStatsKey, Double> m = new HashMap<>();

        for (var e : stats.entrySet()) {
            m.put(e.getKey(), e.getValue().get());
        }

        synchronized (connectionPool) {
            for (var l : connectionPool.values()) {
                m.put(DriverStatsKey.CONNECTIONS_IN_USE, m.get(DriverStatsKey.CONNECTIONS_IN_USE) + l.size());
                for (var con : l) {
                    for (var entry : con.getCon().getStats().entrySet()) {
                        m.put(entry.getKey(), m.get(entry.getKey()).doubleValue() + entry.getValue());
                    }
                }
            }
        }
        return m;
    }


}
