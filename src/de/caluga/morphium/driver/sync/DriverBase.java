package de.caluga.morphium.driver.sync;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.commands.WatchMongoCommand;
import de.caluga.morphium.driver.mongodb.Maximums;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
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
    private volatile AtomicInteger rqid = new AtomicInteger(1000);
    private int maxWait = 1000;
    private boolean keepAlive = true;
    private int soTimeout = 1000;
    private Map<String, Map<String, String>> credentials;
    private int maxBsonObjectSize;
    private int maxMessageSize = 16 * 1024 * 1024;
    private int maxWriteBatchSize = 1000;
    private ReadPreference defaultRP;
    private boolean replicaSet = false;
    private String replicaSetName = null;
    private int retriesOnNetworkError = 5;
    private int sleepBetweenRetries = 100;
    private boolean defaultJ = false;
    private int localThreshold = 0;
    private int heartbeatConnectionTimeout = 1000;
    private List<String> hostSeed;
    private int heartbeatSocketTimeout = 1000;
    private int heartbeatFrequency = 2000;
    private boolean useSSL = false;
    private int maxBlockingThreadsMultiplier = 5;
    private int defaultW = 1;
    private int connectionTimeout = 1000;
    private int maxConnectionIdleTime = 100000;
    private int maxConnectionLifetime = 600000;
    private int minConnectionsPerHost = 1;
    private int maxConnectionsPerHost = 100;
    private int defaultWriteTimeout = 10000;
    private boolean slaveOk = true;
    private int batchSize = 100;
    private boolean retryReads = false;
    private boolean retryWrites = true;
    private int readTimeout = 30000;


    @Override
    public void setConnectionUrl(String connectionUrl) {

    }

    @Override
    public int getRetriesOnNetworkError() {
        return MorphiumDriver.super.getRetriesOnNetworkError();
    }

    @Override
    public void setRetriesOnNetworkError(int r) {
        MorphiumDriver.super.setRetriesOnNetworkError(r);
    }

    @Override
    public int getSleepBetweenErrorRetries() {
        return MorphiumDriver.super.getSleepBetweenErrorRetries();
    }

    @Override
    public void setSleepBetweenErrorRetries(int s) {
        MorphiumDriver.super.setSleepBetweenErrorRetries(s);
    }

    @Override
    public void setCredentials(String db, String login, String pwd) {
        MorphiumDriver.super.setCredentials(db, login, pwd);
    }

    @Override
    public int getMaxConnections() {
        return maxConnectionsPerHost;
    }


    @Override
    public void setMaxConnections(int maxConnections) {
        maxConnectionsPerHost = maxConnections;
    }


    @Override
    public int getMinConnections() {
        return 1;
    }


    @Override
    public void setMinConnections(int minConnections) {
        minConnectionsPerHost = minConnections;

    }


    @Override
    public boolean isRetryReads() {
        return retryReads;
    }


    @Override
    public void setRetryReads(boolean retryReads) {
        this.retryReads = retryReads;
    }


    @Override
    public boolean isRetryWrites() {
        return retryWrites;
    }


    @Override
    public void setRetryWrites(boolean retryWrites) {
        this.retryWrites = retryWrites;
    }


    public String getUuidRepresentation() {
        return null;
    }


    public void setUuidRepresentation(String uuidRepresentation) {

    }


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
        Doc command = Doc.of("listDatabases", 1);
        Map<String, Object> res = runCommand("admin", command).next();
        List<String> ret = new ArrayList<>();
        if (res.get("databases") != null) {
            @SuppressWarnings("unchecked") List<Map<String, Object>> lst = (List<Map<String, Object>>) res.get("databases");
            for (Map<String, Object> db : lst) {
                if (db.get("name") != null) {
                    ret.add(db.get("name").toString());
                } else {
                    log.error("No DB Name for this entry...");
                }
            }
        }
        return ret;
    }


    @Override
    public List<String> listCollections(String db, String pattern) throws MorphiumDriverException {
        if (!isConnected()) {
            return null;
        }
        Doc command = Doc.of("listCollections", 1);
        if (pattern != null) {
            Map<String, Object> query = new HashMap<>();
            query.put("name", Pattern.compile(pattern));
            command.put("filter", query);
        }
        Map<String, Object> res = runCommand(db, command).next();
        List<Map<String, Object>> colList = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        addToListFromCursor(db, colList, res);

        for (Map<String, Object> col : colList) {
            colNames.add(col.get("name").toString());
        }
        return colNames;
    }

    private void addToListFromCursor(String db, List<Map<String, Object>> data, Map<String, Object> res) throws MorphiumDriverException {
        boolean valid;
        @SuppressWarnings("unchecked") Map<String, Object> crs = (Map<String, Object>) res.get("cursor");
        do {
            if (crs.get("firstBatch") != null) {
                //noinspection unchecked
                data.addAll((List<Map<String, Object>>) crs.get("firstBatch"));
            } else if (crs.get("nextBatch") != null) {
                //noinspection unchecked
                data.addAll((List<Map<String, Object>>) crs.get("firstBatch"));
            }
            //next iteration.
            Doc doc = new Doc();
            if (crs.get("id") != null && !crs.get("id").toString().equals("0")) {
                valid = true;
                doc.put("getMore", crs.get("id"));
                crs = runCommand(db, doc).next();
            } else {
                valid = false;
            }

        } while (valid);
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
    public Map<String, Map<String, String>> getCredentials() {
        return credentials;
    }

    @Override
    public void setCredentials(Map<String, Map<String, String>> credentials) {
        this.credentials = credentials;
    }

    @Override
    public void setCredentialsFor(String db, String user, String password) {
        credentials.putIfAbsent(db, new HashMap<>());
        credentials.get(db).put(user, password);
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
    public String[] getHostSeed() {
        if (hostSeed == null) {
            return null;
        }
        return hostSeed.toArray(new String[hostSeed.size()]);
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


    public String getHostAdress(String hn) throws UnknownHostException {
        String hst[] = hn.split(":");
        String h = hst[0];
        h = h.replaceAll(" ", "");
        int port = 27017;
        if (hst.length > 1) {
            port = Integer.parseInt(hst[1]);
        }
        InetAddress in = InetAddress.getByName(h);
        return in.getHostAddress() + ":" + port;
    }

//
//    
//    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query) throws MorphiumDriverException {
//        return mapReduce(db, collection, mapping, reducing, query, null);
//    }
//
//    
//    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing) throws MorphiumDriverException {
//        return mapReduce(db, collection, mapping, reducing, null, null);
//    }

//    
//    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query, Map<String, Object> sorting) throws MorphiumDriverException {
//        Map<String, Object> cmd = new LinkedHashMap<>();
//        /*
//         mapReduce: <collection>,
//                 map: <function>,
//                 reduce: <function>,
//                 finalize: <function>,
//                 out: <output>,
//                 query: <document>,
//                 sort: <document>,
//                 limit: <number>,
//                 scope: <document>,
//                 jsMode: <boolean>,
//                 verbose: <boolean>,
//                 bypassDocumentValidation: <boolean>
//         */
//
//        cmd.put("mapReduce", collection);
//        cmd.put("map", new MongoJSScript(mapping));
//        cmd.put("reduce", new MongoJSScript(reducing));
//        cmd.put("out", UtilsMap.of("inline", 1));
//        if (query != null) {
//            cmd.put("query", query);
//        }
//        if (sorting != null) {
//            cmd.put("sort", sorting);
//        }
//        Map<String, Object> result = runCommand(db, cmd);
//        if (result == null) {
//            throw new MorphiumDriverException("Could not get proper result");
//        }
//        @SuppressWarnings("unchecked") List<Map<String, Object>> results = (List<Map<String, Object>>) result.get("results");
//        if (results == null) {
//            return new ArrayList<>();
//        }
//
//
//        ArrayList<Map<String, Object>> ret = new ArrayList<>();
//        for (Map<String, Object> d : results) {
//            @SuppressWarnings("unchecked") Map<String, Object> value = (Map) d.get("value");
//            ret.add(value);
//        }
//
//        return ret;
//    }

    public abstract void watch(WatchMongoCommand settings) throws MorphiumDriverException;

    public abstract void sendQuery(OpMsg q) throws MorphiumDriverException;

    public abstract OpMsg sendAndWaitForReply(OpMsg q) throws MorphiumDriverException;

//    protected abstract OpMsg getReply(int waitingFor, int timeout) throws MorphiumDriverException;


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
        OpMsg q = new OpMsg();
        q.setMessageId(getNextId());
        q.setFirstDoc(Doc.of("killCursors", (Object) coll)
                .add("cursors", cursorIds)
                .add("$db", db)
        );
        sendQuery(q);

    }


    public void tailableIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> s, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, int timeout, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        if (s == null) {
            s = new HashMap<>();
        }
        final Map<String, Integer> sort = s;
        //noinspection unchecked
        new NetworkCallHelper().doCall(() -> {


            Doc doc = new Doc();
            doc.put("find", collection);
            doc.put("$db", db);
            if (limit > 0) {
                doc.put("limit", limit);
            }
            doc.put("skip", skip);
            if (!query.isEmpty()) {
                doc.put("filter", query);
            }
            if (projection != null) {
                doc.put("projection", projection);
            }
            int t = timeout;
            if (t == 0) {
                t = Integer.MAX_VALUE;
            }
            doc.put("sort", sort);
            doc.put("batchSize", batchSize);
            doc.put("maxTimeMS", t);
            doc.put("tailable", true);
            doc.put("awaitData", true);

            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());
            q.setFirstDoc(doc);
            q.setResponseTo(0);

            long start = System.currentTimeMillis();
            List<Map<String, Object>> ret = null;

            OpMsg reply;
            int waitingfor = q.getMessageId();
            long cursorId;
            log.info("Starting...");

            while (true) {
                log.debug("reading result");
                reply = sendAndWaitForReply(q);

                @SuppressWarnings("unchecked") Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");
                if (cursor == null) {
                    log.debug("no-cursor result");
                    //                    //trying result
                    if (reply.getFirstDoc().get("result") != null) {
                        //noinspection unchecked
                        for (Map<String, Object> d : (List<Map<String, Object>>) reply.getFirstDoc().get("result")) {
                            cb.incomingData(Doc.of(d), System.currentTimeMillis() - start);
                        }
                    }
                    log.error("did not get cursor. Data: " + Utils.toJsonString(reply.getFirstDoc()));
                    //                    throw new MorphiumDriverException("did not get any data, cursor == null!");

                    log.debug("Retrying");
                    continue;
                }
                if (cursor.get("firstBatch") != null) {
                    log.debug("Firstbatch...");
                    //noinspection unchecked
                    for (Map<String, Object> d : (List<Map<String, Object>>) cursor.get("firstBatch")) {
                        cb.incomingData(Doc.of(d), System.currentTimeMillis() - start);
                    }
                } else if (cursor.get("nextBatch") != null) {
                    log.debug("NextBatch...");
                    //noinspection unchecked
                    for (Map<String, Object> d : (List<Map<String, Object>>) cursor.get("nextBatch")) {
                        cb.incomingData(Doc.of(d), System.currentTimeMillis() - start);
                    }
                }
                if (((Long) cursor.get("id")) != 0) {
                    //                        log.info("getting next batch for cursor " + cursor.get("id"));
                    //there is more! Sending getMore!
                    //there is more! Sending getMore!

                    //                } else {
                    //                    break;
                    log.debug("CursorID:" + cursor.get("id").toString());
                    cursorId = Long.valueOf(cursor.get("id").toString());
                } else {
                    log.error("Cursor closed - reviving!");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    q = new OpMsg();


                    doc = new Doc();
                    doc.put("find", collection);
                    doc.put("$db", db);
                    if (limit > 0) {
                        doc.put("limit", limit);
                    }
                    doc.put("skip", skip);
                    if (!query.isEmpty()) {
                        doc.put("filter", query);
                    }
                    if (projection != null) {
                        doc.put("projection", projection);
                    }
                    doc.put("sort", sort);
                    doc.put("batchSize", 1);
                    doc.put("maxTimeMS", timeout);
                    doc.put("tailable", true);
                    doc.put("awaitData", true);
                    doc.put("noCursorTimeout", true);
                    doc.put("allowPartialResults", false);
                    q.setMessageId(getNextId());

                    q.setFirstDoc(doc);
                    q.setResponseTo(0);
                    sendQuery(q);
                    continue;
                }
                q = new OpMsg();
                q.setMessageId(getNextId());

                doc = new Doc();
                doc.put("getMore", cursorId);
                doc.put("collection", collection);
                doc.put("batchSize", batchSize);
                doc.put("maxTimeMS", timeout);
                doc.put("limit", 1);
                doc.put("tailable", true);
                doc.put("awaitData", true);
                //doc.put("slaveOk")
                doc.put("noCursorTimeout", true);
                doc.put("$db", db);

                q.setFirstDoc(doc);
                waitingfor = q.getMessageId();
                sendQuery(q);

                log.debug("sent getmore....");

            }

        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());


    }

    public abstract OpMsg getReplyFor(int msgid, long timeout);

    public abstract boolean replyForMsgAvailable(int msg);

}
