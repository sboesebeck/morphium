package de.caluga.morphium.driver.singleconnect;

import de.caluga.morphium.Logger;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bson.MongoJSScript;
import de.caluga.morphium.driver.mongodb.Maximums;
import de.caluga.morphium.driver.wireprotocol.OpQuery;
import de.caluga.morphium.driver.wireprotocol.OpReply;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

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
    private volatile int rqid = 10000;
    private int maxWait = 1000;
    private boolean keepAlive = true;
    private int soTimeout = 1000;

    private Map<String, Map<String, char[]>> credentials;
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


    private Logger log = new Logger(DriverBase.class);
    private boolean slaveOk = true;

    public boolean isSlaveOk() {
        return slaveOk;
    }

    public void setSlaveOk(boolean slaveOk) {
        this.slaveOk = slaveOk;
    }

    @Override
    public void setCredentials(String db, String login, char[] pwd) {
        if (credentials == null) {
            credentials = new HashMap<>();
        }
        Map<String, char[]> cred = new HashMap<>();
        cred.put(login, pwd);
        credentials.put(db, cred);
    }

    @Override
    public boolean isReplicaset() {
        return replicaSet;
    }


    public String getReplicaSetName() {
        return replicaSetName;
    }

    public void setReplicaSetName(String replicaSetName) {
        this.replicaSetName = replicaSetName;
    }


    @SuppressWarnings("unused")
    public Map<String, Map<String, char[]>> getCredentials() {
        return credentials;
    }

    @Override
    public void setCredentials(Map<String, String[]> credentials) {

    }

    @Override
    public int getRetriesOnNetworkError() {
        return retriesOnNetworkError;
    }

    @Override
    public void setRetriesOnNetworkError(int r) {
        if (r < 1) {
            r = 1;
        }
        retriesOnNetworkError = r;
    }

    @Override
    public int getSleepBetweenErrorRetries() {
        return sleepBetweenRetries;
    }

    @Override
    public void setSleepBetweenErrorRetries(int s) {
        if (s < 100) {
            s = 100;
        }
        sleepBetweenRetries = s;
    }

    @SuppressWarnings("unused")
    public int getMaxBsonObjectSize() {
        return maxBsonObjectSize;
    }

    public void setMaxBsonObjectSize(int maxBsonObjectSize) {
        this.maxBsonObjectSize = maxBsonObjectSize;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public int getMaxWriteBatchSize() {
        return maxWriteBatchSize;
    }

    public void setMaxWriteBatchSize(int maxWriteBatchSize) {
        this.maxWriteBatchSize = maxWriteBatchSize;
    }


    @SuppressWarnings("unused")
    public boolean isReplicaSet() {
        return replicaSet;
    }

    public void setReplicaSet(boolean replicaSet) {
        this.replicaSet = replicaSet;
    }


    public int getNextId() {
        synchronized (DriverBase.class) {
            return ++rqid;
        }
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
    public int getHeartbeatConnectTimeout() {
        return heartbeatConnectionTimeout;
    }

    @Override
    public void setHeartbeatConnectTimeout(int heartbeatConnectTimeout) {
        heartbeatConnectionTimeout = heartbeatConnectTimeout;
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
    public boolean isSocketKeepAlive() {
        return keepAlive;
    }

    @Override
    public void setSocketKeepAlive(boolean socketKeepAlive) {
        keepAlive = socketKeepAlive;
    }

    @Override
    public String[] getCredentials(String db) {
        return new String[0];
    }

    @Override
    public boolean isDefaultFsync() {
        return false;
    }

    @Override
    public void setDefaultFsync(boolean j) {
    }

    @Override
    public String[] getHostSeed() {
        if (hostSeed == null) {
            return null;
        }
        return hostSeed.toArray(new String[hostSeed.size()]);
    }

    @Override
    public void setHostSeed(String... host) {
        if (hostSeed == null) {
            hostSeed = new Vector<>();
        }
        for (String h : host) {
            try {
                hostSeed.add(getHostAdress(h));
            } catch (UnknownHostException e) {
                throw new RuntimeException("Could not add host", e);
            }
        }

    }

    @Override
    public int getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    @Override
    public void setMaxConnectionsPerHost(int mx) {
        maxConnectionsPerHost = mx;
    }

    @Override
    public int getMinConnectionsPerHost() {
        return minConnectionsPerHost;
    }

    @Override
    public void setMinConnectionsPerHost(int mx) {
        minConnectionsPerHost = mx;
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
    public int getSocketTimeout() {
        return soTimeout;
    }

    @Override
    public void setSocketTimeout(int timeout) {
        soTimeout = timeout;
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
    public int getMaxBlockintThreadMultiplier() {
        return maxBlockingThreadsMultiplier;
    }

    @Override
    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    @Override
    public void setHeartbeatFrequency(int heartbeatFrequency) {

    }

    public abstract Map<String, Object> update(String db, String collection, List<Map<String, Object>> updateCommand, boolean ordered, WriteConcern wc) throws MorphiumDriverException;

    @Override
    public Maximums getMaximums() {
        Maximums max = new Maximums();
        max.setMaxBsonSize(maxBsonObjectSize);
        max.setMaxMessageSize(maxMessageSize);
        max.setMaxWriteBatchSize(maxWriteBatchSize);
        return max;
    }

    public ReadPreference getDefaultReadPreference() {
        return defaultRP;
    }

    @Override
    public void setDefaultReadPreference(ReadPreference rp) {
        defaultRP = rp;
    }

    @Override
    public void setDefaultBatchSize(int defaultBatchSize) {

    }

    @Override
    public int getHeartbeatSocketTimeout() {
        return heartbeatSocketTimeout;
    }

    @Override
    public void setHeartbeatSocketTimeout(int heartbeatSocketTimeout) {
        this.heartbeatSocketTimeout = heartbeatSocketTimeout;
    }

    @Override
    public boolean isUseSSL() {
        return useSSL;
    }

    @Override
    public void setUseSSL(boolean useSSL) {

    }

    @Override
    public boolean isDefaultJ() {
        return defaultJ;
    }

    @Override
    public void setDefaultJ(boolean j) {
        defaultJ = j;
    }

    @Override
    public int getWriteTimeout() {
        return defaultWriteTimeout;
    }

    @Override
    public void setWriteTimeout(int writeTimeout) {

    }

    @Override
    public int getLocalThreshold() {
        return localThreshold;
    }

    @Override
    public void setLocalThreshold(int thr) {
        localThreshold = thr;
    }

    @Override
    public void setMaxBlockingThreadMultiplier(int m) {
        maxBlockingThreadsMultiplier = m;
    }

    @Override
    public void heartBeatFrequency(int t) {
        heartbeatFrequency = t;
    }

    @Override
    public void heartBeatSocketTimeout(int t) {
        heartbeatSocketTimeout = t;
    }

    @Override
    public void useSsl(boolean ssl) {
        useSSL = ssl;
    }


    public String getHostAdress(String hn) throws UnknownHostException {
        String hst[] = hn.split(":");
        String h = hst[0];
        int port = 27017;
        if (hst.length > 1) {
            port = Integer.valueOf(hst[1]);
        }
        InetAddress in = InetAddress.getByName(h);
        return in.getHostAddress() + ":" + port;
    }


    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query) throws MorphiumDriverException {
        return mapReduce(db, collection, mapping, reducing, query, null);
    }

    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing) throws MorphiumDriverException {
        return mapReduce(db, collection, mapping, reducing, null, null);
    }

    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query, Map<String, Object> sorting) throws MorphiumDriverException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        /*
         mapReduce: <collection>,
                 map: <function>,
                 reduce: <function>,
                 finalize: <function>,
                 out: <output>,
                 query: <document>,
                 sort: <document>,
                 limit: <number>,
                 scope: <document>,
                 jsMode: <boolean>,
                 verbose: <boolean>,
                 bypassDocumentValidation: <boolean>
         */

        cmd.put("mapReduce", collection);
        cmd.put("map", new MongoJSScript(mapping));
        cmd.put("reduce", new MongoJSScript(reducing));
        cmd.put("out", Utils.getMap("inline", 1));
        if (query != null) {
            cmd.put("query", query);
        }
        if (sorting != null) {
            cmd.put("sort", sorting);
        }
        Map<String, Object> result = runCommand(db, cmd);
        if (result == null) {
            throw new MorphiumDriverException("Could not get proper result");
        }
        List<Map<String, Object>> results = (List<Map<String, Object>>) result.get("results");
        if (results == null) {
            return new ArrayList<>();
        }


        ArrayList<Map<String, Object>> ret = new ArrayList<>();
        for (Map<String, Object> d : results) {
            Map<String, Object> value = (Map) d.get("value");
            ret.add(value);
        }

        return ret;
    }

    protected abstract void sendQuery(OpQuery q) throws MorphiumDriverException;

    protected abstract OpReply getReply(long waitingFor, int timeout) throws MorphiumDriverException;


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

        OpQuery q = new OpQuery();
        q.setDb(db);
        q.setColl("$cmd");
        q.setLimit(1);
        q.setSkip(0);
        q.setReqId(getNextId());

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("killCursors", coll);
        doc.put("cursors", cursorIds);
        q.setDoc(doc);
        sendQuery(q);

    }


    @Override
    public void tailableIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> s, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, int timeout, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        if (s == null) {
            s = new HashMap<>();
        }
        final Map<String, Integer> sort = s;
        //noinspection unchecked
        new NetworkCallHelper().doCall(() -> {
            OpQuery q = new OpQuery();
            q.setDb(db);
            q.setColl("$cmd");
            q.setLimit(1);
            q.setSkip(0);
            q.setReqId(getNextId());

            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("find", collection);
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
            q.setDoc(doc);
            q.setInReplyTo(0);
            q.setTailableCursor(true);
            q.setAwaitData(true);
            q.setNoCursorTimeout(true);

            long start = System.currentTimeMillis();
            List<Map<String, Object>> ret = null;
            sendQuery(q);

            OpReply reply = null;
            long waitingfor = q.getReqId();
            long cursorId = 0;
            log.info("Starting...");

            while (true) {
                log.info("reading result");
                reply = getReply(waitingfor, t);

                if (reply.getInReplyTo() != waitingfor) {
                    throw new MorphiumDriverNetworkException("Wrong answer - waiting for " + waitingfor + " but got " + reply.getInReplyTo());
                }
                @SuppressWarnings("unchecked") Map<String, Object> cursor = (Map<String, Object>) reply.getDocuments().get(0).get("cursor");
                if (cursor == null) {
                    log.info("no-cursor result");
                    //                    //trying result
                    if (reply.getDocuments().get(0).get("result") != null) {
                        //noinspection unchecked
                        for (Map<String, Object> d : (List<Map<String, Object>>) reply.getDocuments().get(0).get("result")) {
                            if (!cb.incomingData(d, System.currentTimeMillis() - start)) {
                                return null;
                            }
                        }
                    }
                    log.error("did not get cursor. Data: " + Utils.toJsonString(reply.getDocuments().get(0)));
                    //                    throw new MorphiumDriverException("did not get any data, cursor == null!");

                    log.info("Retrying");
                    continue;
                }
                if (cursor.get("firstBatch") != null) {
                    log.info("Firstbatch...");
                    //noinspection unchecked
                    for (Map<String, Object> d : (List<Map<String, Object>>) cursor.get("firstBatch")) {
                        if (!cb.incomingData(d, System.currentTimeMillis() - start)) {
                            return null;
                        }
                    }
                } else if (cursor.get("nextBatch") != null) {
                    log.info("NextBatch...");
                    //noinspection unchecked
                    for (Map<String, Object> d : (List<Map<String, Object>>) cursor.get("nextBatch")) {
                        if (!cb.incomingData(d, System.currentTimeMillis() - start)) {
                            return null;
                        }
                    }
                }
                if (((Long) cursor.get("id")) != 0) {
                    //                        log.info("getting next batch for cursor " + cursor.get("id"));
                    //there is more! Sending getMore!
                    //there is more! Sending getMore!

                    //                } else {
                    //                    break;
                    log.info("CursorID:" + cursor.get("id").toString());
                    cursorId = Long.valueOf(cursor.get("id").toString());
                } else {
                    log.error("Cursor closed - reviving!");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    q = new OpQuery();
                    q.setDb(db);
                    q.setColl("$cmd");
                    q.setLimit(1);
                    q.setSkip(0);
                    q.setReqId(getNextId());

                    doc = new LinkedHashMap<>();
                    doc.put("find", collection);
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
                    q.setDoc(doc);
                    q.setInReplyTo(0);
                    q.setTailableCursor(true);
                    q.setAwaitData(true);
                    q.setNoCursorTimeout(true);
                    sendQuery(q);
                    continue;
                }
                q = new OpQuery();
                q.setColl("$cmd");
                q.setDb(db);
                q.setReqId(getNextId());
                q.setSkip(0);
                q.setTailableCursor(true);
                q.setAwaitData(true);
                q.setNoCursorTimeout(true);
                q.setSlaveOk(false);
                q.setLimit(1);
                doc = new LinkedHashMap<>();
                doc.put("getMore", cursorId);
                doc.put("collection", collection);
                doc.put("batchSize", batchSize);
                doc.put("maxTimeMS", timeout);

                q.setDoc(doc);
                waitingfor = q.getReqId();
                sendQuery(q);

                log.info("sent getmore....");

            }

        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());


    }


}
