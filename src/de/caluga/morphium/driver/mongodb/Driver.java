package de.caluga.morphium.driver.mongodb;/**
 * Created by stephan on 05.11.15.
 */

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import de.caluga.morphium.Logger;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverOperation;
import de.caluga.morphium.driver.ReadPreference;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public class Driver implements MorphiumDriver {
    private Logger log = new Logger(Driver.class);
    private String[] hostSeed;
    private int maxConnectionsPerHost = 50;
    private int minConnectionsPerHost = 10;
    private int maxConnectionLifetime = 60000;
    private int maxConnectionIdleTime = 20000;
    private int socketTimeout = 1000;
    private int connectionTimeout = 1000;
    private int defaultW = 1;
    private int maxBlockintThreadMultiplier = 5;
    private int heartbeatFrequency = 1000;
    private int heartbeatSocketTimeout = 1000;
    private boolean useSSL = false;
    private boolean defaultJ = false;
    private int writeTimeout = 1000;
    private int localThreshold = 15;
    private boolean defaultFsync;
    private boolean socketKeepAlive;
    private int heartbeatConnectTimeout;
    private int maxWaitTime;

    private int defaultBatchSize = 100;
    private int retriesOnNetworkError = 2;
    private int sleepBetweenErrorRetries = 500;


    private Map<String, String[]> credentials = new HashMap<>();
    private MongoClient mongo;

    @Override
    public void setCredentials(String db, String login, char[] pwd) {
        String[] cred = new String[2];
        cred[0] = login;
        cred[1] = new String(pwd);
        credentials.put(db, cred);
    }

    @Override
    public String[] getCredentials(String db) {
        return credentials.get(db);
    }

    @Override
    public boolean isDefaultFsync() {
        return defaultFsync;
    }

    @Override
    public String[] getHostSeed() {
        return hostSeed;
    }

    @Override
    public int getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    @Override
    public int getMinConnectionsPerHost() {
        return minConnectionsPerHost;
    }

    @Override
    public int getMaxConnectionLifetime() {
        return maxConnectionLifetime;
    }

    @Override
    public int getMaxConnectionIdleTime() {
        return maxConnectionIdleTime;
    }

    @Override
    public int getSocketTimeout() {
        return socketTimeout;
    }

    @Override
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    @Override
    public int getDefaultW() {
        return defaultW;
    }

    @Override
    public int getMaxBlockintThreadMultiplier() {
        return maxBlockintThreadMultiplier;
    }

    @Override
    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    @Override
    public int getHeartbeatSocketTimeout() {
        return heartbeatSocketTimeout;
    }

    @Override
    public boolean isUseSSL() {
        return useSSL;
    }

    @Override
    public boolean isDefaultJ() {
        return defaultJ;
    }

    @Override
    public int getWriteTimeout() {
        return writeTimeout;
    }

    @Override
    public int getLocalThreshold() {
        return localThreshold;
    }

    @Override
    public void setHostSeed(String... host) {
        hostSeed = host;
    }

    @Override
    public void setMaxConnectionsPerHost(int mx) {
        maxConnectionsPerHost = mx;
    }

    @Override
    public void setMinConnectionsPerHost(int mx) {
        minConnectionsPerHost = mx;
    }

    @Override
    public void setMaxConnectionLifetime(int timeout) {
        maxConnectionLifetime = timeout;
    }

    @Override
    public void setMaxConnectionIdleTime(int time) {
        maxConnectionIdleTime = time;
    }

    @Override
    public void setSocketTimeout(int timeout) {
        socketTimeout = timeout;
    }

    @Override
    public void setConnectionTimeout(int timeout) {
        connectionTimeout = timeout;
    }

    @Override
    public void setDefaultW(int w) {
        defaultW = w;
    }

    @Override
    public void setMaxBlockingThreadMultiplier(int m) {
        maxBlockintThreadMultiplier = m;
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

    @Override
    public void connect() {

    }

    @Override
    public void connect(String replicasetName) throws MorphiumDriverException {
        try {
            MongoClientOptions.Builder o = MongoClientOptions.builder();
            WriteConcern w = new WriteConcern(getDefaultW(), getWriteTimeout(), isDefaultFsync(), isDefaultJ());
            o.writeConcern(w);
            o.socketTimeout(getSocketTimeout());
            o.connectTimeout(getConnectionTimeout());
            o.connectionsPerHost(getMaxConnectionsPerHost());
            o.socketKeepAlive(isSocketKeepAlive());
            o.threadsAllowedToBlockForConnectionMultiplier(getMaxBlockintThreadMultiplier());
//        o.cursorFinalizerEnabled(isCursorFinalizerEnabled()); //Deprecated?
//        o.alwaysUseMBeans(isAlwaysUseMBeans());
            o.heartbeatConnectTimeout(getHeartbeatConnectTimeout());
            o.heartbeatFrequency(getHeartbeatFrequency());
            o.heartbeatSocketTimeout(getHeartbeatSocketTimeout());
            o.minConnectionsPerHost(getMinConnectionsPerHost());
            o.minHeartbeatFrequency(getHeartbeatFrequency());
            o.localThreshold(getLocalThreshold());
            o.maxConnectionIdleTime(getMaxConnectionIdleTime());
            o.maxConnectionLifeTime(getMaxConnectionLifetime());
            if (replicasetName != null) {
                o.requiredReplicaSetName(replicasetName);
            }
            o.maxWaitTime(getMaxWaitTime());


            List<MongoCredential> lst = new ArrayList<>();
            for (Map.Entry<String, String[]> e : credentials.entrySet()) {
                MongoCredential cred = MongoCredential.createMongoCRCredential(e.getValue()[0], e.getKey(), e.getValue()[1].toCharArray());
                lst.add(cred);
            }


            if (hostSeed.length == 1) {
                ServerAddress adr = new ServerAddress(hostSeed[0]);
                mongo = new MongoClient(adr, lst, o.build());
            } else {
                List<ServerAddress> adrLst = new ArrayList<>();
                for (String h : hostSeed) {
                    adrLst.add(new ServerAddress(h));
                }
                mongo = new MongoClient(adrLst, lst, o.build());
            }
        } catch (Exception e) {
            throw new MorphiumDriverException("Error creating connection to mongo", e);
        }
    }

    @Override
    public boolean isConnected() {
        return mongo != null;
    }

    @Override
    public void setDefaultJ(boolean j) {
        defaultJ = j;
    }

    @Override
    public void setDefaultWriteTimeout(int wt) {
        writeTimeout = wt;
    }

    @Override
    public void setLocalThreshold(int thr) {
        localThreshold = thr;
    }

    @Override
    public void setDefaultFsync(boolean j) {
        defaultFsync = j;
    }

    @Override
    public void close() throws MorphiumDriverException {
        try {
            mongo.close();
        } catch (Exception e) {
            throw new MorphiumDriverException("error closing", e);
        }
    }

    @Override
    public Map<String, Object> getStats() {
        return null;
    }

    @Override
    public Map<String, Object> getOps(long threshold) {
        return null;
    }

    @Override
    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {

        return new DriverHelper().doCall(new MorphiumDriverOperation() {
            @Override
            public Map<String, Object> execute() {
                Document ret = mongo.getDatabase(db).runCommand(new BasicDBObject(cmd));
                return ret;
            }

        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }


    @Override
    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Integer> projection, int skip, int limit, int batchSize, de.caluga.morphium.driver.ReadPreference readPreference) {

        try {
            return (List<Map<String, Object>>) new DriverHelper().doCall(new MorphiumDriverOperation() {
                @Override
                public Map<String, Object> execute() {
                    MongoDatabase database = mongo.getDatabase(db);
                    MongoCollection<Document> coll = database.getCollection(collection);
                    FindIterable<Document> ret = coll.find(new BasicDBObject(query));
                    //TODO: Read Preference handling
                    if (sort != null)
                        ret = ret.sort(new BasicDBObject(sort));
                    if (skip != 0) ret = ret.skip(skip);
                    if (limit != 0) ret = ret.limit(limit);
                    if (batchSize != 0) ret.batchSize(batchSize);
                    else ret.batchSize(defaultBatchSize);
                    if (projection != null) ret.projection(new BasicDBObject(projection));
                    List<Map<String, Object>> values = new ArrayList<>();
                    for (Document d : ret) {
                        values.add(d);
                    }

                    Map<String, Object> r = new HashMap<String, Object>();
                    r.put("result", values);
                    return r;
                }
            }, retriesOnNetworkError, sleepBetweenErrorRetries);
        } catch (MorphiumDriverException e) {
            e.printStackTrace();
        }
    }


    @Override
    public long count(String db, String collection, Map<String, Object> query, ReadPreference rp) {
        MongoDatabase database = mongo.getDatabase(db);
        MongoCollection<Document> coll = database.getCollection(collection);
        //TODO: Read Preference handling
        return coll.count(new BasicDBObject(query));

    }

    @Override
    public void insert(String db, String collection, List<Map<String, Object>> objs, de.caluga.morphium.driver.WriteConcern wc) {

        MongoCollection c = mongo.getDatabase(db).getCollection(collection);
        if (objs.size() == 1) {
            c.insertOne(new BasicDBObject(objs.get(0)));
        } else {
            InsertManyOptions imo = new InsertManyOptions();
            imo.ordered(false);
            List<BasicDBObject> obj = new ArrayList<>();
            for (Map<String, Object> o : objs) {
                obj.add(new BasicDBObject(o));
            }
            c.insertMany(obj, imo);
        }
    }

    @Override
    public Map<String, Object> udate(String db, String collection, Map<String, Object> query, Map<String, Object> op, boolean multiple, boolean upsert, de.caluga.morphium.driver.WriteConcern wc) {
        return null;
    }

    @Override
    public Map<String, Object> delete(String db, String collection, Map<String, Object> query, boolean multiple, de.caluga.morphium.driver.WriteConcern wc) {
        return null;
    }

    @Override
    public Map<String, Object> drop(String db, String collection, de.caluga.morphium.driver.WriteConcern wc) {
        return null;
    }

    @Override
    public Map<String, Object> drop(String db, de.caluga.morphium.driver.WriteConcern wc) {
        return null;
    }

    @Override
    public boolean exists(String db) {
        return false;
    }

    @Override
    public boolean exists(String db, String collection) {
        return false;
    }

    @Override
    public Map<String, Object> getIndexes(String db, String collection) {
        return null;
    }

    @Override
    public List<String> getCollectionNames(String db) {
        return null;
    }

    @Override
    public Map<String, Object> killCursors(String db, String collection, List<Long> cursorIds) {
        return null;
    }

    @Override
    public Map<String, Object> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, ReadPreference readPreference) {
        return null;
    }

    @Override
    public boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    @Override
    public void setSocketKeepAlive(boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
    }

    @Override
    public int getHeartbeatConnectTimeout() {
        return heartbeatConnectTimeout;
    }

    @Override
    public void setHeartbeatConnectTimeout(int heartbeatConnectTimeout) {
        this.heartbeatConnectTimeout = heartbeatConnectTimeout;
    }

    @Override
    public int getMaxWaitTime() {
        return maxWaitTime;
    }

    @Override
    public void setMaxWaitTime(int maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
    }

    @Override
    public int getRetriesOnNetworkError() {
        return retriesOnNetworkError;
    }

    @Override
    public void setRetriesOnNetworkError(int retriesOnNetworkError) {
        this.retriesOnNetworkError = retriesOnNetworkError;
    }

    public int getSleepBetweenErrorRetries() {
        return sleepBetweenErrorRetries;
    }

    public void setSleepBetweenErrorRetries(int sleepBetweenErrorRetries) {
        this.sleepBetweenErrorRetries = sleepBetweenErrorRetries;
    }
}
