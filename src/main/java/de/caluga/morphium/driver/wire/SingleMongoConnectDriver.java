package de.caluga.morphium.driver.wire;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.AggregatorImpl;
import de.caluga.morphium.annotations.Driver;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverNetworkException;
import de.caluga.morphium.driver.MorphiumTransactionContext;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.bulk.BulkRequest;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.bulk.DeleteBulkRequest;
import de.caluga.morphium.driver.bulk.InsertBulkRequest;
import de.caluga.morphium.driver.bulk.UpdateBulkRequest;
import de.caluga.morphium.driver.commands.AbortTransactionCommand;
import de.caluga.morphium.driver.commands.CollStatsCommand;
import de.caluga.morphium.driver.commands.CommitTransactionCommand;
import de.caluga.morphium.driver.commands.CurrentOpCommand;
import de.caluga.morphium.driver.commands.DbStatsCommand;
import de.caluga.morphium.driver.commands.DeleteMongoCommand;
import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.KillCursorsCommand;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.commands.ReplicastStatusCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.wireprotocol.OpMsg;

/**
 * User: Stephan Bösebeck
 * Date: 02.12.15
 * Time: 23:47
 * <p>
 * connects to one node only!
 */
@Driver(name = "SingleMongoConnectDriver", description = "simple driver only handling one connection")
public class SingleMongoConnectDriver extends DriverBase {

    @Override
    public int getServerSelectionTimeout() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setServerSelectionTimeout(int timeoutInMS) {
        // TODO Auto-generated method stub
    }

    private ScheduledFuture<?> heartbeat;
    public static final String driverName = "SingleMongoConnectDriver";

    private final Logger log = LoggerFactory.getLogger(SingleMongoConnectDriver.class);
    private SingleMongoConnection connection;
    private ConnectionType connectionType = ConnectionType.PRIMARY;
    private int idleSleepTime = 20;
    private boolean connectionInUse = false;
    private AtomicInteger waitingForHeartbeatCounter = new AtomicInteger(0);

    private Map<DriverStatsKey, AtomicDecimal> stats = new HashMap<>();
    private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5, r -> {
        Thread ret = new Thread(r);
        ret.setName("SCCon_" + (stats.get(DriverStatsKey.THREADS_CREATED).incrementAndGet()));
        ret.setDaemon(true);
        return ret;
    });

    public SingleMongoConnectDriver() {
        for (var e : DriverStatsKey.values()) {
            stats.put(e, new AtomicDecimal(0));
        }
    }

    @Override
    public <T, R> Aggregator<T, R> createAggregator(Morphium morphium, Class<? extends T > type, Class <? extends R > resultType) {
        return new AggregatorImpl<>(morphium, type, resultType);
    }

    @Override
    public Map<DriverStatsKey, Double> getDriverStats() {
        Map<DriverStatsKey, Double> ret = new HashMap<>();
        //copy to avoid concurrent modification
        Map<DriverStatsKey, AtomicDecimal> hashMap = new HashMap<>(stats);

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

    public MongoConnection getConnection() throws MorphiumDriverException {
        long waitUntil = System.currentTimeMillis() + getMaxWaitTime() * 5; //just to be sure - single connection!

        while (connectionInUse) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }

            if (System.currentTimeMillis() > waitUntil) {
                throw new MorphiumDriverException("could not get connection - still in use after " + getMaxWaitTime());
            }
        }

        while (connection != null && !connection.isConnected()) {
            try {
                log.info("Waiting for heartbeat to fix connection...");
                int waitingCount = waitingForHeartbeatCounter.incrementAndGet();

                if (waitingCount > 20) {
                    if (heartbeat != null) {
                        heartbeat.cancel(true);
                    }

                    heartbeat = null;
                    waitingForHeartbeatCounter.set(0);
                    startHeartbeat();
                }

                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }//waiting for heartbeat

            if (System.currentTimeMillis() > waitUntil) {
                throw new MorphiumDriverException("could not get connection - not connected after " + getMaxWaitTime());
            }
        }

        incStat(DriverStatsKey.CONNECTIONS_BORROWED);
        connectionInUse = true;
        return new ConnectionWrapper(connection);
    }

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
        String[] h = hostPort.split(":");
        return h[0];
    }

    private int getPortFromHost(int hostSeedIdx) {
        return getPortFromHost(getHostSeed().get(hostSeedIdx));
    }

    private int getPortFromHost(String host) {
        String[] h = host.split(":");

        if (h.length == 1) {
            return 27017;
        }

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
        int retries = 0;

        while (true) {
            try {
                incStat(DriverStatsKey.CONNECTIONS_OPENED);

                if (connectToIdx >= getHostSeed().size()) {
                    connectToIdx = 0;
                }

                if (getHostSeed().isEmpty()) {
                    log.error("All hosts unavailable...");
                    throw new MorphiumDriverException("Could not connect!");
                }

                String host = getHostSeed().get(connectToIdx);
                String[] h = host.split(":");
                int port = 27017;

                if (h.length > 1) {
                    port = Integer.parseInt(h[1]);
                }

                log.debug("Connecting to {}:{}", h[0], port);
                connection = new SingleMongoConnection();

                if (getAuthDb() != null) {
                    connection.setCredentials(getAuthDb(), getUser(), getPassword());
                }

                var hello = connection.connect(this, h[0], port);

                if (hello == null) {
                    log.error("did not get hello back...");
                    retries++;

                    if (retries > getRetriesOnNetworkError()) {
                        log.error("Max retries reached - aborting");
                        throw new MorphiumDriverNetworkException("Could not connect to " + h[0] + ":" + port);
                    }

                    Thread.sleep(getSleepBetweenErrorRetries() * 10000);
                    continue;
                }

                //checking hosts
                if (hello.getHosts() != null) {
                    for (String s : hello.getHosts()) {
                        if (!getHostSeed().contains(s)) {
                            addToHostSeed(s);
                            log.info("Adding {}", s);
                        }
                    }
                }

                for (String hst : new ArrayList<String>(getHostSeed())) {
                    if (!hst.equals(hello.getPrimary()) && !hello.getHosts().contains(hst)) {
                        log.debug("Host {} from hostseed is not part of replicaset anymore", hst);
                        removeFromHostSeed(hst);
                    }
                }

                if (hello.getPrimary() != null && !getHostSeed().contains(hello.getPrimary())) {
                    addToHostSeed(hello.getPrimary());
                }

                if (!getHostSeed().contains(connection.getConnectedTo())) {
                    log.debug("Hostname changed?!?!?");
                    close();
                    continue;
                }

                if (connectionType.equals(ConnectionType.PRIMARY) && !Boolean.TRUE.equals(hello.getWritablePrimary())) {
                    log.debug("connecting-> want primary connection, got secondary, retrying");
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
                            log.debug("End of hostseed, starting over");
                            connectToIdx = 0;
                        }
                    }

                    continue;
                } else if (connectionType.equals(ConnectionType.SECONDARY) && !Boolean.TRUE.equals(hello.getSecondary())) {
                    log.debug("want secondary connection, got other - retrying");
                    connection.close();
                    incStat(DriverStatsKey.CONNECTIONS_CLOSED);
                    connection = null;
                    Thread.sleep(1000);//Slowing down
                    connectToIdx++;

                    if (connectToIdx >= getHostSeed().size()) {
                        log.debug("End of hostseed, starting over");
                        connectToIdx = 0;
                    }

                    continue;
                }

                setMaxBsonObjectSize(hello.getMaxBsonObjectSize());
                setMaxMessageSize(hello.getMaxMessageSizeBytes());
                setMaxWriteBatchSize(hello.getMaxWriteBatchSize());
                break;
            } catch (Exception e) {
                incStat(DriverStatsKey.ERRORS);
                log.error("connection failed", e);
                connectToIdx++;

                if (connectToIdx > getHostSeed().size()) {
                    connectToIdx = 0;
                }

                retries++;

                if (retries > getRetriesOnNetworkError()) {
                    throw (new MorphiumDriverException("max retries exceeded", e));
                }

                try {
                    Thread.sleep(getSleepBetweenErrorRetries());
                } catch (InterruptedException e1) {
                    //swallow
                }
            }
        }

        startHeartbeat();
        incStat(DriverStatsKey.CONNECTIONS_IN_POOL);
        // log.info("Connected! "+connection.getConnectedTo()+" / "+getHostSeed().get(connectToIdx));
    }

    protected void startHeartbeat() {
        if (heartbeat == null) {
            // log.debug("Starting heartbeat ");
            heartbeat = executor.scheduleWithFixedDelay(()-> {
                try {
                    if (connectionInUse) {
                        return;
                    }

                    // log.info("checking connection");
                    if (connection == null)
                        return;

                    connectionInUse = true;

                    try {
                        HelloCommand cmd = new HelloCommand(connection).setHelloOk(true).setIncludeClient(false);
                        var hello = cmd.execute();

                        if (hello == null) {
                            log.warn("Could not run heartbeat!");
                            return;
                        }

                        if (connectionType.equals(ConnectionType.PRIMARY) && !Boolean.TRUE.equals(hello.getWritablePrimary())
                                || (connectionType.equals(ConnectionType.SECONDARY) && !Boolean.TRUE.equals(hello.getSecondary()))) {
                            log.warn("state change -> wanted {}, but changed, retrying", connectionType.name());
                            connection.close();
                            connection = null;
                            incStat(DriverStatsKey.FAILOVERS);
                            decStat(DriverStatsKey.CONNECTIONS_IN_POOL);
                            incStat(DriverStatsKey.CONNECTIONS_CLOSED);
                            Thread.sleep(1000);
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
                        log.error("Error during heartbeat", e);
                    } finally {
                        connectionInUse = false;
                    }
                } catch (Throwable e) {
                    log.error("Heartbeat caught error", e);
                }
            }, 10, getHeartbeatFrequency(), TimeUnit.MILLISECONDS);
        } else {
            log.debug("Heartbeat already scheduled...");
        }
    }

    @Override
    public void watch(WatchCommand settings) throws MorphiumDriverException {
        connection.watch(settings);
    }

    @Override
    public void releaseConnection(MongoConnection con) {
        incStat(DriverStatsKey.CONNECTIONS_RELEASED);
        connectionInUse = false;

        if (con instanceof ConnectionWrapper) {
            ((ConnectionWrapper) con).setDelegate(null);
        }
    }

    @Override
    public void closeConnection(MongoConnection con) {
        releaseConnection(con);
    }

    @Override
    public MongoConnection getReadConnection(ReadPreference rp) {
        try {
            return getConnection();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MongoConnection getPrimaryConnection(WriteConcern wc) {
        try {
            return getConnection();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        incStat(DriverStatsKey.CONNECTIONS_CLOSED);
        decStat(DriverStatsKey.CONNECTIONS_IN_POOL);

        try {
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (Exception e) {
            log.warn("Problem when closing connection", e);
        } finally {
        }

        if (heartbeat != null) {
            heartbeat.cancel(true);
        }

        heartbeat = null;
        connectionInUse = false;
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
        if (getTransactionContext() == null) {
            throw new IllegalArgumentException("No transaction in progress, cannot commit");
        }

        MorphiumTransactionContext ctx = getTransactionContext();
        var cmd = new CommitTransactionCommand(connection).setTxnNumber(ctx.getTxnNumber()).setAutocommit(false).setLsid(ctx.getLsid());
        cmd.execute();
        clearTransactionContext();
    }

    @Override
    public void abortTransaction() throws MorphiumDriverException {
        if (getTransactionContext() == null) {
            throw new IllegalArgumentException("No transaction in progress, cannot abort");
        }

        MorphiumTransactionContext ctx = getTransactionContext();
        var cmd = new AbortTransactionCommand(connection).setTxnNumber(ctx.getTxnNumber()).setAutocommit(false).setLsid(ctx.getLsid());
        cmd.execute();
        clearTransactionContext();
    }

    @Override
    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        CollStatsCommand cmd = new CollStatsCommand(connection);
        return cmd.execute();
    }

    @Override
    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        ReplicastStatusCommand cmd = null;

        try {
            cmd = new ReplicastStatusCommand(getPrimaryConnection(null));
            var result = cmd.execute();
            @SuppressWarnings("unchecked")
            List<Doc> mem = (List) result.get("members");

            if (mem == null) {
                return null;
            }

            //noinspection unchecked
            mem.stream().filter(d->d.get("optime") instanceof Map).forEach(d->d.put("optime", ((Map<String, Doc>) d.get("optime")).get("ts")));
            return result;
        } finally {
            if (cmd != null && cmd.getConnection() != null) {
                cmd.releaseConnection();
            }
        }
    }

    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        DbStatsCommand cmd = null;

        try {
            cmd = new DbStatsCommand(getPrimaryConnection(null)).setDb(db);
            return cmd.execute();
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }
    }

    public List<Map<String, Object>> currentOp(int threshold) throws MorphiumDriverException {
        CurrentOpCommand cmd = null;

        try {
            cmd = new CurrentOpCommand(connection).setColl("admin").setSecsRunning(threshold);
            return cmd.execute();
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return new BulkRequestContext(m) {
            private final List<BulkRequest> requests = new ArrayList<>();
            public Doc execute() {
                int delCount = 0;
                int matchedCount = 0;
                int insertCount = 0;
                int modifiedCount = 0;
                List<Object> upsertedIds = new ArrayList<>();

                try {
                    for (BulkRequest r : requests) {
                        if (r instanceof InsertBulkRequest) {
                            var c = getPrimaryConnection(wc);
                            InsertMongoCommand settings = new InsertMongoCommand(c);
                            settings.setDb(db).setColl(collection).setComment("Bulk insert").setDocuments(((InsertBulkRequest) r).getToInsert());
                            Map<String, Object> result = settings.execute();
                            settings.releaseConnection();
                            insertCount += ((InsertBulkRequest) r).getToInsert().size();
                        } else if (r instanceof UpdateBulkRequest) {
                            UpdateBulkRequest up = (UpdateBulkRequest) r;
                            var c = getPrimaryConnection(wc);
                            UpdateMongoCommand upCmd = new UpdateMongoCommand(c);
                            upCmd.setColl(collection).setDb(db).setUpdates(Arrays.asList(Doc.of("q", up.getQuery(), "u", up.getCmd(), "upsert", up.isUpsert(), "multi", up.isMultiple())));
                            Map<String, Object> result = upCmd.execute();
                            upCmd.releaseConnection();
                            if (result.containsKey("n")) {
                                matchedCount += ((Number) result.get("n")).intValue();
                            }
                            if (result.containsKey("nModified")) {
                                modifiedCount += ((Number) result.get("nModified")).intValue();
                            }
                            if (result.containsKey("upserted")) {
                                @SuppressWarnings("unchecked")
                                List<Map<String, Object>> upserted = (List<Map<String, Object>>) result.get("upserted");
                                for (Map<String, Object> u : upserted) {
                                    upsertedIds.add(u.get("_id"));
                                }
                            }
                        } else if (r instanceof DeleteBulkRequest) {
                            DeleteBulkRequest dbr = ((DeleteBulkRequest) r);
                            var c = getPrimaryConnection(wc);
                            DeleteMongoCommand del = new DeleteMongoCommand(c);
                            del.setColl(collection).setDb(db).setDeletes(Arrays.asList(Doc.of("q", dbr.getQuery(), "limit", dbr.isMultiple() ? 0 : 1)));
                            Map<String, Object> result = del.execute();
                            del.releaseConnection();
                            if (result.containsKey("n")) {
                                delCount += ((Number) result.get("n")).intValue();
                            }
                        } else {
                            throw new RuntimeException("Unknown operation " + r.getClass().getName());
                        }
                    }
                } catch (MorphiumDriverException e) {
                    log.error("Got exception: ", e);
                }

                // Build result document
                Doc res = Doc.of(
                    "num_deleted", delCount,
                    "num_matched", matchedCount,
                    "num_inserted", insertCount,
                    "num_modified", modifiedCount,
                    "num_upserts", upsertedIds.size()
                );

                if (!upsertedIds.isEmpty()) {
                    res.put("upsertedIds", upsertedIds);
                }

                return res;
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

        KillCursorsCommand k = new KillCursorsCommand(connection).setCursors(cursorIds).setDb(db).setColl(coll);
        var ret = k.execute();
        log.debug("killed cursor");
    }

    private List<Map<String, Object>> readBatches(int waitingfor, int batchSize) throws MorphiumDriverException {
        List<Map<String, Object>> ret = new ArrayList<>();
        String db = null;
        String coll = null;

        while (true) {
            OpMsg reply = connection.readNextMessage(getMaxWaitTime());//connection.getReplyFor(waitingfor, getMaxWaitTime());

            if (reply.getResponseTo() != waitingfor) {
                log.error("Wrong answer - waiting for {} but got {}", waitingfor, reply.getResponseTo());
                log.error("Document: {}", Utils.toJsonString(reply.getFirstDoc()));
                continue;
            }

            //                    replies.remove(i);
            @SuppressWarnings("unchecked")
            Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");

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

                if (namespace.length > 1) {
                    coll = namespace[1];
                }
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
                q.setFirstDoc(Doc.of("getMore", (Object) cursor.get("id")).add("$db", db).add("batchSize", batchSize));

                if (coll != null) {
                    q.getFirstDoc().put("collection", coll);
                }

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
        List<String> databases = listDatabases();
        return databases != null && databases.contains(db);
    }

    public Map<String, Object> getDbStats(String db) throws MorphiumDriverException {
        return getDbStats(db, false);
    }

    public Map<String, Object> getDbStats(String db, boolean withStorage) throws MorphiumDriverException {
        return new NetworkCallHelper<Map<String, Object>>().doCall(()-> {
            OpMsg msg = new OpMsg();
            msg.setMessageId(getNextId());
            Map<String, Object> v = Doc.of("dbStats", 1, "scale", 1024);
            v.put("$db", db);

            if (withStorage) {
                v.put("freeStorage", 1);
            }
            msg.setFirstDoc(v);
            connection.sendQuery(msg);
            OpMsg reply = connection.readNextMessage(getMaxWaitTime());//connection.getReplyFor(msg.getMessageId(), getMaxWaitTime());
            return reply.getFirstDoc();
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries(), (t)-> {
            close();
            connect();
        });
    }

    private List<Map<String, Object>> getCollectionInfo(String db, String collection) throws MorphiumDriverException {
        //noinspection unchecked
        return new NetworkCallHelper<List<Map<String, Object>>>().doCall(()-> {
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
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries(), (t)-> {
            close();
            connect();
        });
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
    public void setIdleSleepTime(int sl) {
        idleSleepTime = sl;
    }

    @Override
    public int getIdleSleepTime() {
        return idleSleepTime;
    }

    private class ConnectionWrapper implements MongoConnection {
        private MongoConnection delegate;

        public MongoConnection getDelegate() {
            if (delegate == null) {
                throw new RuntimeException("Cannot get delegate - Connection released!");
            }

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
            releaseConnection(getDelegate());
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
            return new NetworkCallHelper<Map<String, Object>>().doCall(()-> {
                return getDelegate().killCursors(db, coll, ids);
            }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries(), (t)-> {
                close();
                SingleMongoConnectDriver.this.connect();
            });
        }

        @Override
        public OpMsg readNextMessage(int timeout) throws MorphiumDriverException {
            return new NetworkCallHelper<OpMsg>().doCall(()-> {
                return getDelegate().readNextMessage(timeout);
            }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries(), (t)-> {
                close();
                SingleMongoConnectDriver.this.connect();
            });
        }

        @Override
        public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
            return new NetworkCallHelper<Map<String, Object>>().doCall(()-> {
                return getDelegate().readSingleAnswer(id);
            }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries(), (t)-> {
                close();
                SingleMongoConnectDriver.this.connect();
            });
        }

        @Override
        public void watch(WatchCommand settings) throws MorphiumDriverException {
            new NetworkCallHelper<Object>().doCall(()-> {
                getDelegate().watch(settings);
                return null;
            }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries(), (t)-> {
                if (t.getMessage().contains("Socket closed")) {
                    log.info("Socket closed");
                    close();
                }
                close();
                SingleMongoConnectDriver.this.connect();
            });
        }

        @Override
        public List<Map<String, Object>> readAnswerFor(int queryId) throws MorphiumDriverException {
            return new NetworkCallHelper<List<Map<String, Object>>>().doCall(()-> {
                return getDelegate().readAnswerFor(queryId);
            }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries(), (t)-> {
                close();
                SingleMongoConnectDriver.this.connect();
            });
        }

        @Override
        public MorphiumCursor getAnswerFor(int queryId, int batchsize) throws MorphiumDriverException {
            return new NetworkCallHelper<MorphiumCursor>().doCall(()-> {
                return getDelegate().getAnswerFor(queryId, batchsize);
            }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries(), (t)-> {
                close();
                SingleMongoConnectDriver.this.connect();
            });
        }

        @Override
        public List<Map<String, Object>> readAnswerFor(MorphiumCursor crs) throws MorphiumDriverException {
            return new NetworkCallHelper<List<Map<String, Object>>>().doCall(()-> {
                return getDelegate().readAnswerFor(crs);
            }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries(), (t)-> {
                close();
                SingleMongoConnectDriver.this.connect();
            });
        }

        @Override
        public int sendCommand(MongoCommand cmd) throws MorphiumDriverException {
            return new NetworkCallHelper<Integer>().doCall(()-> {
                return getDelegate().sendCommand(cmd);
            }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries(), (t)-> {
                close();
                SingleMongoConnectDriver.this.connect();
            });
        }

    }

}
