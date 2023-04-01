package de.caluga.morphium.driver.wire;

import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bson.BsonEncoder;
import de.caluga.morphium.driver.commands.ListCollectionsCommand;
import de.caluga.morphium.driver.commands.ListDatabasesCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.mongodb.Maximums;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * User: Stephan Bösebeck
 * Date: 03.12.15
 * Time: 22:36
 * <p>
 * <p>
 * Base for custom drivers
 */
@SuppressWarnings("WeakerAccess")
public abstract class DriverBase implements MorphiumDriver {
    private final Logger log = LoggerFactory.getLogger(DriverBase.class);
    private volatile AtomicInteger rqid = new AtomicInteger(Math.abs((int)(System.currentTimeMillis() % 100000)));
    private int maxWait = 1000;
    private int maxBsonObjectSize = 12 * 1025 * 1024;
    private int maxMessageSize = 16 * 1024 * 1024;
    private int maxWriteBatchSize = 1000;
    private ReadPreference defaultRP = ReadPreference.primary();
    private boolean replicaSet = false;
    private String replicaSetName = null;
    private int retriesOnNetworkError = 5;
    private int sleepBetweenRetries = 100;
    private boolean defaultJ = false;
    private int localThreshold = 0;
    private List<String> hostSeed;
    private int heartbeatFrequency = 2000;
    private boolean useSSL = false;
    private int defaultW = 1;
    private int connectionTimeout = 1000;
    private int maxConnectionIdleTime = 100000;
    private int maxConnectionLifetime = 600000;
    private int minConnectionsPerHost = 1;
    private int maxConnectionsPerHost = 100;
    private int defaultWriteTimeout = 10000;
    private int batchSize = 100;
    private boolean retryReads = false;
    private boolean retryWrites = true;
    private int readTimeout = 30000;

    private ThreadLocal<MorphiumTransactionContext> transactionContext = new ThreadLocal<>();

    private String authDb = null;
    private String user;
    private String password;

    public DriverBase() {
        //startHousekeeping();
    }

    @Override
    public void setConnectionUrl(String connectionUrl) throws MalformedURLException {
        URL u = new URL(connectionUrl);

        if (!u.getProtocol().equals("mongodb")) {
            throw new MalformedURLException("unsupported protocol: " + u.getProtocol());
        }
    }

    @Override
    public List<String> listCollections(String db, String regex) throws MorphiumDriverException {
        MongoConnection primaryConnection = getPrimaryConnection(null);
        if (primaryConnection==null){
            log.info("waiting");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
            }
            primaryConnection=getPrimaryConnection(null);
            if (primaryConnection==null) throw new IllegalArgumentException("could not get connection");
        }
        ListCollectionsCommand cmd=null;
        try {
             cmd = new ListCollectionsCommand(primaryConnection);
            cmd.setDb(db).setNameOnly(true);

            if (regex != null) {
                cmd.setFilter(Doc.of("name", Pattern.compile(regex)));
            }

            var lst = cmd.execute();
            List<String> colNames = new ArrayList<>();

            for (Map<String, Object> doc : lst) {
                String name = doc.get("name").toString();
                colNames.add(name);
            }

            return colNames;
        } finally {
            // if (cmd!=null) cmd.releaseConnection();
            //DO NOT RELEASE - Internally using cursor, you might release it twice!!
        }
    }

    @Override
    public void setHostSeed(String... hosts) {
        hostSeed = new ArrayList<>();

        for (String h : hosts) {
            hostSeed.add(h);
        }
    }

    @Override
    public int getRetriesOnNetworkError() {
        return retriesOnNetworkError;
    }

    @Override
    public MorphiumDriver setRetriesOnNetworkError(int r) {
        retriesOnNetworkError = r;
        return this;
    }

    @Override
    public int getSleepBetweenErrorRetries() {
        return sleepBetweenRetries;
    }

    @Override
    public MorphiumDriver setSleepBetweenErrorRetries(int s) {
        sleepBetweenRetries = s;
        return this;
    }

    @Override
    public void setCredentials(String db, String login, String pwd) {
        authDb = db;
        user = login;
        password = pwd;
    }

    @Override
    public int getMaxConnections() {
        return maxConnectionsPerHost;
    }

    public String getAuthDb() {
        return authDb;
    }

    public DriverBase setAuthDb(String authDb) {
        this.authDb = authDb;
        return this;
    }

    public String getUser() {
        return user;
    }

    public DriverBase setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public DriverBase setPassword(String password) {
        this.password = password;
        return this;
    }

    @Override
    public MorphiumDriver setMaxConnections(int maxConnections) {
        maxConnectionsPerHost = maxConnections;
        return this;
    }

    @Override
    public int getMinConnections() {
        return 1;
    }

    @Override
    public MorphiumDriver setMinConnections(int minConnections) {
        minConnectionsPerHost = minConnections;
        return this;
    }

    @Override
    public boolean isRetryReads() {
        return retryReads;
    }

    @Override
    public MorphiumDriver setRetryReads(boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    @Override
    public boolean isRetryWrites() {
        return retryWrites;
    }

    @Override
    public MorphiumDriver setRetryWrites(boolean retryWrites) {
        this.retryWrites = retryWrites;
        return this;
    }

    public String getUuidRepresentation() {
        return BsonEncoder.UUIDRepresentation.STANDARD.name();
    }

    public void setUuidRepresentation(String uuidRepresentation) {}

    @Override
    public int getReadTimeout() {
        return readTimeout;
    }

    @Override
    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    @Override
    public int getMinConnectionsPerHost() {
        return minConnectionsPerHost;
    }

    @Override
    public void setMinConnectionsPerHost(int minConnectionsPerHost) {
        this.minConnectionsPerHost = minConnectionsPerHost;
    }

    @Override
    public int getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    @Override
    public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
        this.maxConnectionsPerHost = maxConnectionsPerHost;
    }

    @Override
    public boolean isReplicaset() {
        return replicaSet;
    }

    @Override
    public List<String> listDatabases() throws MorphiumDriverException {
        if (!isConnected()) {
            return null;
        }

        MongoConnection primaryConnection = getPrimaryConnection(null);
ListDatabasesCommand cmd=null;
        try {
             cmd = new ListDatabasesCommand(primaryConnection);
            var msg = primaryConnection.sendCommand(cmd);
            Map<String, Object> res = primaryConnection.readSingleAnswer(msg);
            List<String> ret = new ArrayList<>();

            if (res.get("databases") != null) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> lst = (List<Map<String, Object>>) res.get("databases");

                for (Map<String, Object> db : lst) {
                    if (db.get("name") != null) {
                        ret.add(db.get("name").toString());
                    } else {
                        log.error("No DB Name for this entry...");
                    }
                }
            }

            return ret;
        } finally {
            if (cmd!=null) cmd.releaseConnection();
        }
    }

    public boolean exists(String db, String collection) throws MorphiumDriverException {
        var ret = listCollections(db, null);

        for (var c : ret) {
            if (c.equals(collection)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public List<String> getHostSeed() {
        if (hostSeed == null) {
            return null;
        }

        return hostSeed;
    }

    @Override
    public String getReplicaSetName() {
        return replicaSetName;
    }

    @Override
    public void setReplicaSetName(String replicaSetName) {
        this.replicaSetName = replicaSetName;
    }

    @Override
    @SuppressWarnings("unused")
    public int getMaxBsonObjectSize() {
        return maxBsonObjectSize;
    }

    @Override
    public void setMaxBsonObjectSize(int maxBsonObjectSize) {
        this.maxBsonObjectSize = maxBsonObjectSize;
    }

    @Override
    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    @Override
    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    public int getMaxWriteBatchSize() {
        return maxWriteBatchSize;
    }

    @Override
    public void setMaxWriteBatchSize(int maxWriteBatchSize) {
        this.maxWriteBatchSize = maxWriteBatchSize;
    }

    @Override
    @SuppressWarnings("unused")
    public boolean isReplicaSet() {
        return replicaSet;
    }

    @Override
    public void setReplicaSet(boolean replicaSet) {
        this.replicaSet = replicaSet;
    }

    public int getNextId() {
        synchronized (DriverBase.class) {
            return rqid.incrementAndGet();
        }
    }

    @Override
    public boolean getDefaultJ() {
        return defaultJ;
    }

    @Override
    public int getDefaultWriteTimeout() {
        return defaultWriteTimeout;
    }

    @Override
    public void setDefaultWriteTimeout(int wt) {
        defaultWriteTimeout = wt;
    }

    @Override
    public int getMaxWaitTime() {
        return this.maxWait;
    }

    @Override
    public void setMaxWaitTime(int maxWaitTime) {
        this.maxWait = maxWaitTime;
    }

    @Override
    public String[] getCredentials(String db) {
        return new String[0];
    }

    @Override
    public void setHostSeed(List<String> hosts) {
        hostSeed = hosts;
    }

    @Override
    public int getMaxConnectionLifetime() {
        return maxConnectionLifetime;
    }

    @Override
    public void setMaxConnectionLifetime(int timeout) {
        maxConnectionLifetime = timeout;
    }

    @Override
    public int getMaxConnectionIdleTime() {
        return maxConnectionIdleTime;
    }

    @Override
    public void setMaxConnectionIdleTime(int time) {
        maxConnectionIdleTime = time;
    }

    @Override
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    @Override
    public void setConnectionTimeout(int timeout) {
        connectionTimeout = timeout;
    }

    @Override
    public int getDefaultW() {
        return defaultW;
    }

    @Override
    public void setDefaultW(int w) {
        defaultW = w;
    }

    @Override
    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    @Override
    public void setHeartbeatFrequency(int heartbeatFrequency) {
        this.heartbeatFrequency = heartbeatFrequency;
    }

    public Maximums getMaximums() {
        Maximums max = new Maximums();
        max.setMaxBsonSize(maxBsonObjectSize);
        max.setMaxMessageSize(maxMessageSize);
        max.setMaxWriteBatchSize(maxWriteBatchSize);
        return max;
    }

    @Override
    public ReadPreference getDefaultReadPreference() {
        return defaultRP;
    }

    @Override
    public void setDefaultReadPreference(ReadPreference rp) {
        defaultRP = rp;
    }

    @Override
    public int getDefaultBatchSize() {
        return batchSize;
    }

    @Override
    public void setDefaultBatchSize(int defaultBatchSize) {
        this.batchSize = defaultBatchSize;
    }

    @Override
    public boolean isUseSSL() {
        return useSSL;
    }

    @Override
    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    @Override
    public boolean isDefaultJ() {
        return defaultJ;
    }

    @Override
    public void setDefaultJ(boolean j) {
        defaultJ = j;
    }

    public int getLocalThreshold() {
        return localThreshold;
    }

    public void setLocalThreshold(int thr) {
        localThreshold = thr;
    }

    public void heartBeatFrequency(int t) {
        heartbeatFrequency = t;
    }

    public void useSsl(boolean ssl) {
        useSSL = ssl;
    }

    //
    //    public String getHostAdress(String hn) throws UnknownHostException {
    //        String hst[] = hn.split(":");
    //        String h = hst[0];
    //        h = h.replaceAll(" ", "");
    //        int port = 27017;
    //        if (hst.length > 1) {
    //            port = Integer.parseInt(hst[1]);
    //        }
    //        InetAddress in = InetAddress.getByName(h);
    //        return in.getHostAddress() + ":" + port;
    //    }

    @Override
    public MorphiumTransactionContext startTransaction(boolean autoCommit) {
        if (this.transactionContext.get() != null) {
            throw new IllegalArgumentException("Transaction in progress");
        }

        MorphiumTransactionContextImpl ctx = new MorphiumTransactionContextImpl();
        ctx.setLsid(UUID.randomUUID());
        ctx.setTxnNumber((long) getNextId());
        this.transactionContext.set(ctx);
        return ctx;
    }

    @Override
    public MorphiumTransactionContext getTransactionContext() {
        return transactionContext.get();
    }

    @Override
    public void setTransactionContext(MorphiumTransactionContext ctx) {
        if (transactionContext.get() != null) {
            throw new IllegalArgumentException("Transaction already in progress!");
        }

        if (ctx instanceof MorphiumTransactionContextImpl) {
            transactionContext.set((MorphiumTransactionContextImpl) ctx);
        } else {
            throw new IllegalArgumentException("Transaction context of wrong type!");
        }
    }

    @Override
    public boolean isTransactionInProgress() {
        return transactionContext.get() != null;
    }

    protected void clearTransactionContext() {
        transactionContext.remove();
    }

    public abstract void watch(WatchCommand settings) throws MorphiumDriverException;

    //    public abstract void sendQuery(OpMsg q) throws MorphiumDriverException;

    //    public abstract OpMsg sendAndWaitForReply(OpMsg q) throws MorphiumDriverException;

    //    protected abstract OpMsg getReply(int waitingFor, int timeout) throws MorphiumDriverException;

    //
    //    public void tailableIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> s, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, int timeout, DriverTailableIterationCallback cb) throws MorphiumDriverException {
    //        if (s == null) {
    //            s = new HashMap<>();
    //        }
    //        final Map<String, Integer> sort = s;
    //        //noinspection unchecked
    //        new NetworkCallHelper().doCall(() -> {
    //            Doc doc = new Doc();
    //            doc.put("find", collection);
    //            doc.put("$db", db);
    //            if (limit > 0) {
    //                doc.put("limit", limit);
    //            }
    //            doc.put("skip", skip);
    //            if (!query.isEmpty()) {
    //                doc.put("filter", query);
    //            }
    //            if (projection != null) {
    //                doc.put("projection", projection);
    //            }
    //            int t = timeout;
    //            if (t == 0) {
    //                t = Integer.MAX_VALUE;
    //            }
    //            doc.put("sort", sort);
    //            doc.put("batchSize", batchSize);
    //            doc.put("maxTimeMS", t);
    //            doc.put("tailable", true);
    //            doc.put("awaitData", true);
    //
    //            OpMsg q = new OpMsg();
    //            q.setMessageId(getNextId());
    //            q.setFirstDoc(doc);
    //            q.setResponseTo(0);
    //
    //            long start = System.currentTimeMillis();
    //            List<Map<String, Object>> ret = null;
    //
    //            OpMsg reply;
    //            int waitingfor = q.getMessageId();
    //            long cursorId;
    //            log.info("Starting...");
    //
    //            while (true) {
    //                log.debug("reading result");
    //                reply = sendAndWaitForReply(q);
    //
    //                @SuppressWarnings("unchecked") Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");
    //                if (cursor == null) {
    //                    log.debug("no-cursor result");
    //                    //                    //trying result
    //                    if (reply.getFirstDoc().get("result") != null) {
    //                        //noinspection unchecked
    //                        for (Map<String, Object> d : (List<Map<String, Object>>) reply.getFirstDoc().get("result")) {
    //                            cb.incomingData(Doc.of(d), System.currentTimeMillis() - start);
    //                        }
    //                    }
    //                    log.error("did not get cursor. Data: " + Utils.toJsonString(reply.getFirstDoc()));
    //                    //                    throw new MorphiumDriverException("did not get any data, cursor == null!");
    //
    //                    log.debug("Retrying");
    //                    continue;
    //                }
    //                if (cursor.get("firstBatch") != null) {
    //                    log.debug("Firstbatch...");
    //                    //noinspection unchecked
    //                    for (Map<String, Object> d : (List<Map<String, Object>>) cursor.get("firstBatch")) {
    //                        cb.incomingData(Doc.of(d), System.currentTimeMillis() - start);
    //                    }
    //                } else if (cursor.get("nextBatch") != null) {
    //                    log.debug("NextBatch...");
    //                    //noinspection unchecked
    //                    for (Map<String, Object> d : (List<Map<String, Object>>) cursor.get("nextBatch")) {
    //                        cb.incomingData(Doc.of(d), System.currentTimeMillis() - start);
    //                    }
    //                }
    //                if (((Long) cursor.get("id")) != 0) {
    //                    //                        log.info("getting next batch for cursor " + cursor.get("id"));
    //                    //there is more! Sending getMore!
    //                    //there is more! Sending getMore!
    //
    //                    //                } else {
    //                    //                    break;
    //                    log.debug("CursorID:" + cursor.get("id").toString());
    //                    cursorId = Long.valueOf(cursor.get("id").toString());
    //                } else {
    //                    log.error("Cursor closed - reviving!");
    //                    try {
    //                        Thread.sleep(100);
    //                    } catch (InterruptedException e) {
    //                        e.printStackTrace();
    //                    }
    //                    q = new OpMsg();
    //
    //
    //                    doc = new Doc();
    //                    doc.put("find", collection);
    //                    doc.put("$db", db);
    //                    if (limit > 0) {
    //                        doc.put("limit", limit);
    //                    }
    //                    doc.put("skip", skip);
    //                    if (!query.isEmpty()) {
    //                        doc.put("filter", query);
    //                    }
    //                    if (projection != null) {
    //                        doc.put("projection", projection);
    //                    }
    //                    doc.put("sort", sort);
    //                    doc.put("batchSize", 1);
    //                    doc.put("maxTimeMS", timeout);
    //                    doc.put("tailable", true);
    //                    doc.put("awaitData", true);
    //                    doc.put("noCursorTimeout", true);
    //                    doc.put("allowPartialResults", false);
    //                    q.setMessageId(getNextId());
    //
    //                    q.setFirstDoc(doc);
    //                    q.setResponseTo(0);
    //                    sendQuery(q);
    //                    continue;
    //                }
    //                q = new OpMsg();
    //                q.setMessageId(getNextId());
    //
    //                doc = new Doc();
    //                doc.put("getMore", cursorId);
    //                doc.put("collection", collection);
    //                doc.put("batchSize", batchSize);
    //                doc.put("maxTimeMS", timeout);
    //                doc.put("limit", 1);
    //                doc.put("tailable", true);
    //                doc.put("awaitData", true);
    //                //doc.put("slaveOk")
    //                doc.put("noCursorTimeout", true);
    //                doc.put("$db", db);
    //
    //                q.setFirstDoc(doc);
    //                waitingfor = q.getMessageId();
    //                sendQuery(q);
    //
    //                log.debug("sent getmore....");
    //
    //            }
    //
    //        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    //
    //
    //    }

    //    public abstract OpMsg getReplyFor(int msgid, long timeout) throws MorphiumDriverException;

    //    public abstract boolean replyForMsgAvailable(int msg);

    //    public abstract List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException;

    public abstract boolean isCapped(String db, String coll) throws MorphiumDriverException;
}
