package de.caluga.morphium.driver.mongodb;/**
 * Created by stephan on 05.11.15.
 */


import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import org.bson.*;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SuppressWarnings({"WeakerAccess", "deprecation"})
public class Driver implements MorphiumDriver {
    private final Logger log = LoggerFactory.getLogger(Driver.class);
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

    private ReadPreference defaultReadPreference;

    private Map<String, String[]> credentials = new HashMap<>();
    private MongoClient mongo;
    private Maximums maximums;

    private boolean replicaset;


    private ThreadLocal<MongoTransactionContext> currentTransaction = new ThreadLocal<>();

    @Override
    public boolean isReplicaset() {
        return replicaset;
    }

    @Override
    public void setCredentials(String db, String login, char[] pwd) {
        String[] cred = new String[2];
        cred[0] = login;
        cred[1] = new String(pwd);
        credentials.put(db, cred);
    }

    @Override
    public List<String> listDatabases() throws MorphiumDriverException {
        if (!isConnected()) {
            return null;
        }
        Map<String, Object> command = new HashMap<>();
        command.put("listDatabases", 1);
        Map<String, Object> res = runCommand("admin", command);
        List<String> ret = new ArrayList<>();
        if (res.get("databases") != null) {
            //noinspection unchecked
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
    }

    @Override
    public List<String> listCollections(String db, String pattern) throws MorphiumDriverException {
        if (!isConnected()) {
            return null;
        }
        Map<String, Object> command = new LinkedHashMap<>();
        command.put("listCollections", 1);
        if (pattern != null) {
            Map<String, Object> query = new HashMap<>();
            query.put("name", Pattern.compile(pattern));
            command.put("filter", query);
        }
        Map<String, Object> res = runCommand(db, command);
        List<Map<String, Object>> colList = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        addToListFromCursor(db, colList, res);

        for (Map<String, Object> col : colList) {
            colNames.add(col.get("name").toString());
        }
        return colNames;
    }

    @SuppressWarnings("unchecked")
    private void addToListFromCursor(String db, List<Map<String, Object>> data, Map<String, Object> res) throws MorphiumDriverException {
        boolean valid;
        //noinspection unchecked
        Map<String, Object> crs = (Map<String, Object>) res.get("cursor");
        do {
            if (crs.get("firstBatch") != null) {
                //noinspection unchecked
                data.addAll((List<Map<String, Object>>) crs.get("firstBatch"));
            } else if (crs.get("nextBatch") != null) {
                data.addAll((List<Map<String, Object>>) crs.get("firstBatch"));
            }
            //next iteration.
            Map<String, Object> doc = new LinkedHashMap<>();
            if (crs.get("id") != null && !crs.get("id").toString().equals("0")) {
                valid = true;
                doc.put("getMore", crs.get("id"));
                crs = runCommand(db, doc);
            } else {
                valid = false;
            }

        } while (valid);
    }

    public ReadPreference getDefaultReadPreference() {
        return defaultReadPreference;
    }

    @Override
    public void setDefaultReadPreference(ReadPreference defaultReadPreference) {
        this.defaultReadPreference = defaultReadPreference;
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
    public void setDefaultFsync(boolean j) {
        defaultFsync = j;
    }

    @Override
    public String[] getHostSeed() {
        return hostSeed;
    }

    @Override
    public void setHostSeed(String... host) {
        hostSeed = host;
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
        return socketTimeout;
    }

    @Override
    public void setSocketTimeout(int timeout) {
        socketTimeout = timeout;
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
        return maxBlockintThreadMultiplier;
    }

    @Override
    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    @Override
    public void setHeartbeatFrequency(int heartbeatFrequency) {
        this.heartbeatFrequency = heartbeatFrequency;
    }

    @Override
    public void setDefaultBatchSize(int defaultBatchSize) {
        this.defaultBatchSize = defaultBatchSize;
    }

    @Override
    public void setCredentials(Map<String, String[]> credentials) {
        this.credentials = credentials;
    }

    @SuppressWarnings("unused")
    public void setMongo(MongoClient mongo) {
        this.mongo = mongo;
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

    @Override
    public int getWriteTimeout() {
        return writeTimeout;
    }

    @Override
    public void setWriteTimeout(int writeTimeout) {
        this.writeTimeout = writeTimeout;
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
    public void connect() throws MorphiumDriverException {
        connect(null);
    }

    @Override
    public void connect(String replicasetName) throws MorphiumDriverException {
        try {
            MongoClientOptions.Builder o = MongoClientOptions.builder();
            com.mongodb.WriteConcern w = new com.mongodb.WriteConcern(getDefaultW(), getWriteTimeout(), isDefaultFsync(), isDefaultJ());
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
                MongoCredential cred = MongoCredential.createCredential(e.getKey(), e.getValue()[0], e.getValue()[1].toCharArray());
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
            try {
                Document res = mongo.getDatabase("local").runCommand(new BasicDBObject("isMaster", true));
                if (res.get("setName") != null) {
                    replicaset = true;
                }
            } catch (MongoCommandException mce) {
                if (mce.getCode() == 20) {
                    //most likely a connection to a mongos,
                    //swallow error!
                    replicaset = false;
                } else {
                    throw new MorphiumDriverException("Error getting replicaset status", mce);
                }
            }
        } catch (Exception e) {
            throw new MorphiumDriverException("Error creating connection to mongo", e);
        }
    }

    @Override
    public Maximums getMaximums() {
        if (maximums == null) {
            maximums = new Maximums();
            try {
                Map<String, Object> cmd = new HashMap<>();
                cmd.put("isMaster", 1);
                Map<String, Object> res = runCommand("admin", cmd);
                maximums.setMaxBsonSize((Integer) res.get("maxBsonObjectSize"));
                maximums.setMaxMessageSize((Integer) res.get("maxMessageSizeBytes"));
                maximums.setMaxWriteBatchSize((Integer) res.get("maxWriteBatchSize"));
            } catch (Exception e) {
                log.error("Error reading max avalues from DB", e);
                //            maxBsonSize = 0;
                //            maxMessageSize = 0;
                //            maxWriteBatchSize = 0;
            }
        }
        return maximums;
    }

    @Override
    public boolean isConnected() {
        return mongo != null;
    }

    @Override
    public int getDefaultWriteTimeout() {
        return writeTimeout;
    }

    @Override
    public void setDefaultWriteTimeout(int wt) {
        writeTimeout = wt;
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
    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        return DriverHelper.doCall(() -> {
            Document ret = mongo.getDatabase("admin").runCommand(new BasicDBObject("replSetGetStatus", 1));
            @SuppressWarnings("unchecked") List<Document> mem = (List) ret.get("members");
            if (mem == null) {
                return null;
            }
            //noinspection unchecked
            mem.stream().filter(d -> d.get("optime") instanceof Map).forEach(d -> d.put("optime", ((Map<String, Document>) d.get("optime")).get("ts")));
            return convertBSON(ret);
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
        //        return null;
    }

    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        return DriverHelper.doCall(() -> {
            Document ret = mongo.getDatabase(db).runCommand(new BasicDBObject("dbstats", 1));
            return convertBSON(ret);
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
        //        return null;
    }

    @Override
    public Map<String, Object> getOps(long threshold) {
        throw new RuntimeException("Not implemented yet, sorry...");
        //        return null;
    }

    @Override
    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(cmd);
        return DriverHelper.doCall(() -> {
            Document ret = mongo.getDatabase(db).runCommand(new BasicDBObject(cmd));
            return convertBSON(ret);
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    public MorphiumCursor initIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, final Map<String, Object> findMetaData) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        //noinspection ConstantConditions
        return (MorphiumCursor) DriverHelper.doCall(() -> {

            MongoDatabase database = mongo.getDatabase(db);
            //                MongoDatabase database = mongo.getDatabase(db);
//            DBCollection coll = getColl(database, collection, readPreference, null);
            MongoCollection<Document> c = getCollection(database, collection, readPreference, null);
            FindIterable<Document> it = currentTransaction.get() == null ? c.find(new BasicDBObject(query)) : c.find(currentTransaction.get().getSession(), new BasicDBObject(query));
            if (projection != null) {
                it.projection(new BasicDBObject(projection));
            }
            if (sort != null) {
                it.sort(new BasicDBObject(sort));
            }
            if (skip != 0) {
                it.skip(skip);
            }
            if (limit != 0) {
                it.limit(limit);
            }
            if (batchSize != 0) {
                it.batchSize(batchSize);
            } else {
                it.batchSize(defaultBatchSize);
            }
            MongoCursor<Document> ret = it.iterator();
//            DBCursor ret = coll.find(new BasicDBObject(query), projection != null ? new BasicDBObject(projection) : null);

            handleMetaData(findMetaData, ret);

            List<Map<String, Object>> values = new ArrayList<>();

            while (ret.hasNext()) {
                Document d = ret.next();
                Map<String, Object> obj = convertBSON(d);
                values.add(obj);
                int cnt = values.size();
                if ((cnt >= batchSize && batchSize != 0) || (cnt >= 1000 && batchSize == 0)) {
                    break;
                }
            }

            Map<String, Object> r = new HashMap<>();

            MorphiumCursor<MongoCursor<Document>> crs = new MorphiumCursor<>();

            if ((values.size() < batchSize && batchSize != 0) || (values.size() < 1000 && batchSize == 0)) {
                ret.close();
            } else {
                crs.setInternalCursorObject(ret);
            }
            if (ret.getServerCursor() != null) {
                crs.setCursorId(ret.getServerCursor().getId());
            }
            crs.setBatch(values);
            r.put("result", crs);

            return r;
        }, retriesOnNetworkError, sleepBetweenErrorRetries).get("result");
    }


    @Override
    public void tailableIteration(String db, String collection, final Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, int timeout, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        //noinspection ConstantConditions
        DriverHelper.doCall(() -> {
            MongoDatabase database = mongo.getDatabase(db);
            //                MongoDatabase database = mongo.getDatabase(db);
            MongoCollection<Document> coll = getCollection(database, collection, readPreference, null);
            FindIterable<Document> ret = currentTransaction.get() == null ? coll.find(new BasicDBObject(query)) : coll.find(currentTransaction.get().getSession(), new BasicDBObject(query));
            if (projection != null) {
                ret.projection(new BasicDBObject(projection));
            }

            if (sort != null) {
                ret = ret.sort(new BasicDBObject(sort));
            }
            if (skip != 0) {
                ret = ret.skip(skip);
            }
            if (limit != 0) {
                ret = ret.limit(limit);
            }
            if (batchSize != 0) {
                ret.batchSize(batchSize);
            } else {
                ret.batchSize(defaultBatchSize);
            }

            ret.cursorType(CursorType.TailableAwait);
            if (timeout == 0) {
                ret.noCursorTimeout(true);
            } else {
                ret.maxAwaitTime(timeout, TimeUnit.MILLISECONDS);
                ret.maxTime(timeout, TimeUnit.MILLISECONDS);
            }
            long start = System.currentTimeMillis();
            for (Document d : ret) {
                Map<String, Object> obj = convertBSON(d);
                if (!cb.incomingData(obj, System.currentTimeMillis() - start)) {
                    break;
                }
            }
            //no close possible
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    private void handleMetaData(Map<String, Object> findMetaData, MongoCursor<Document> ret) {
        if (findMetaData != null) {
            if (ret.getServerAddress() != null) {
                findMetaData.put("server", ret.getServerAddress().getHost() + ":" + ret.getServerAddress().getPort());
            }
            if (ret.getServerCursor() != null) {
                findMetaData.put("cursorId", ret.getServerCursor().getId());
            }
        }
    }

    @Override
    public MorphiumCursor nextIteration(final MorphiumCursor crs) throws MorphiumDriverException {
        //noinspection ConstantConditions
        return (MorphiumCursor) DriverHelper.doCall(() -> {
            List<Map<String, Object>> values = new ArrayList<>();
            @SuppressWarnings("unchecked") DBCursor ret = ((MorphiumCursor<DBCursor>) crs).getInternalCursorObject();
            if (ret == null) {
                return new HashMap<>(); //finished
            }
            int batchSize = ret.getBatchSize();
            while (ret.hasNext()) {
                DBObject d = ret.next();
                Map<String, Object> obj = convertBSON((BasicDBObject) d);
                values.add(obj);
                int cnt = values.size();
                if (cnt >= batchSize && batchSize != 0 || cnt >= 1000 && batchSize == 0) {
                    break;
                }
            }
            Map<String, Object> r = new HashMap<>();

            MorphiumCursor<DBCursor> crs1 = new MorphiumCursor<>();
            crs1.setCursorId(ret.getCursorId());
            if ((values.size() < batchSize && batchSize != 0) || (values.size() < 1000 && batchSize == 0)) {
                ret.close();
            } else {
                crs1.setInternalCursorObject(ret);
            }
            crs1.setBatch(values);
            r.put("result", crs1);
            return r;
        }, retriesOnNetworkError, sleepBetweenErrorRetries).get("result");
    }

    @Override
    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        DriverHelper.doCall(() -> {
            if (crs != null) {
                @SuppressWarnings("unchecked") DBCursor ret = ((MorphiumCursor<DBCursor>) crs).getInternalCursorObject();
                ret.close();
            }
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    @SuppressWarnings("ALL")
    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, final Map<String, Object> findMetaData) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        //noinspection unused
        return (List<Map<String, Object>>) DriverHelper.doCall(new MorphiumDriverOperation() {
            @Override
            public Map<String, Object> execute() {
                MongoDatabase database = mongo.getDatabase(db);
                MongoCollection<Document> coll = getCollection(database, collection, readPreference, null);

                FindIterable<Document> it = currentTransaction.get() == null ? coll.find(new BasicDBObject(query)) : coll.find(currentTransaction.get().session, new BasicDBObject(query));
                if (projection != null) {
                    it.projection(new BasicDBObject(projection));
                }
                if (sort != null) {
                    it.sort(new BasicDBObject(sort));
                }
                if (skip != 0) {
                    it.skip(skip);
                }
                if (limit != 0) {
                    it.limit(limit);
                }
                if (batchSize != 0) {
                    it.batchSize(batchSize);
                } else {
                    it.batchSize(defaultBatchSize);
                }
                MongoCursor<Document> ret = it.iterator();
                handleMetaData(findMetaData, ret);

                List<Map<String, Object>> values = new ArrayList<>();
                while (ret.hasNext()) {
                    Document d = ret.next();
                    Map<String, Object> obj = convertBSON(d);
                    values.add(obj);
                }
                Map<String, Object> r = new HashMap<String, Object>();
                r.put("result", values);
                ret.close();
                return r;
            }
        }, retriesOnNetworkError, sleepBetweenErrorRetries).get("result");
    }

    private Map<String, Object> convertBSON(Map d) {
        Map<String, Object> obj = new HashMap<>();

        for (Object k : d.keySet()) {
            Object value = d.get(k);
            if (value instanceof BsonTimestamp) {
                value = (((BsonTimestamp) value).getTime() * 1000);
            } else if (value instanceof BsonDocument) {
                value = convertBSON((Map) value);
            } else if (value instanceof BsonBoolean) {
                value = ((BsonBoolean) value).getValue();
            } else if (value instanceof BsonDateTime) {
                value = ((BsonDateTime) value).getValue();
            } else if (value instanceof BsonInt32) {
                value = ((BsonInt32) value).getValue();
            } else if (value instanceof BsonInt64) {
                value = ((BsonInt64) value).getValue();
            } else if (value instanceof BsonDouble) {
                value = ((BsonDouble) value).getValue();
            } else if (value instanceof ObjectId) {
                value = new MorphiumId(((ObjectId) value).toByteArray());
            } else if (value instanceof BasicDBList) {
                Map m = new HashMap<>();
                //noinspection unchecked,unchecked
                m.put("list", new ArrayList(((BasicDBList) value)));
                value = convertBSON(m).get("list");
            } else if (value instanceof BasicBSONObject
                    || value instanceof Document
                    || value instanceof BSONObject) {
                value = convertBSON((Map) value);
            } else if (value instanceof BsonString) {
                value = value.toString();
            } else if (value instanceof List) {
                List v = new ArrayList<>();

                for (Object o : (List) value) {
                    if (o instanceof BSON || o instanceof BsonValue || o instanceof Map)
                    //noinspection unchecked
                    {
                        //noinspection unchecked
                        v.add(convertBSON((Map) o));
                    } else
                    //noinspection unchecked
                    {
                        //noinspection unchecked
                        v.add(o);
                    }
                }
                value = v;
            } else //noinspection ConstantConditions
                if (value instanceof BsonArray) {
                    Map m = new HashMap<>();
                    //noinspection unchecked,unchecked
                    m.put("list", new ArrayList(((BsonArray) value).getValues()));
                    value = convertBSON(m).get("list");
                } else //noinspection ConstantConditions
                    if (value instanceof Document) {
                        value = convertBSON((Map) value);
                    } else //noinspection ConstantConditions
                        if (value instanceof BSONObject) {
                            value = convertBSON((Map) value);
                        }
            obj.put(k.toString(), value);
        }
        return obj;
    }


    @SuppressWarnings("Duplicates")
    public DBCollection getColl(DB database, String collection, ReadPreference readPreference, de.caluga.morphium.driver.WriteConcern wc) {
        DBCollection coll = database.getCollection(collection);
        com.mongodb.ReadPreference prf;

        if (readPreference == null) {
            readPreference = defaultReadPreference;
        }
        if (readPreference != null) {
            TagSet tags = null;

            //Thanks to the poor downward compatibility, a more or less complete copy of this code was necessary!
            //great!
            if (readPreference.getTagSet() != null) {
                List<Tag> tagList = readPreference.getTagSet().entrySet().stream().map(e -> new Tag(e.getKey(), e.getValue())).collect(Collectors.toList());
                tags = new TagSet(tagList);

            }
            switch (readPreference.getType()) {
                case NEAREST:
                    if (tags != null) {
                        prf = com.mongodb.ReadPreference.nearest(tags);
                    } else {
                        prf = com.mongodb.ReadPreference.nearest();
                    }
                    break;
                case PRIMARY:
                    prf = com.mongodb.ReadPreference.primary();
                    if (tags != null) {
                        log.warn("Cannot use tags with primary only read preference!");
                    }
                    break;
                case PRIMARY_PREFERRED:
                    if (tags != null) {
                        prf = com.mongodb.ReadPreference.primaryPreferred(tags);
                    } else {
                        prf = com.mongodb.ReadPreference.primaryPreferred();
                    }
                    break;
                case SECONDARY:
                    if (tags != null) {
                        prf = com.mongodb.ReadPreference.secondary(tags);
                    } else {
                        prf = com.mongodb.ReadPreference.secondary();
                    }
                    break;
                case SECONDARY_PREFERRED:
                    if (tags != null) {
                        prf = com.mongodb.ReadPreference.secondaryPreferred(tags);
                    } else {
                        prf = com.mongodb.ReadPreference.secondary();
                    }
                    break;
                default:
                    log.error("Unhandeled read preference: " + readPreference.toString());
                    prf = null;

            }
            if (prf != null) {
                coll.setReadPreference(prf);
            }
        }

        if (wc != null) {
            com.mongodb.WriteConcern writeConcern;
            if (wc.getW() < 0) {
                //majority
                writeConcern = com.mongodb.WriteConcern.MAJORITY;
                writeConcern = writeConcern.withFsync(wc.isFsync());
                writeConcern = writeConcern.withJ(wc.isJ());
            } else {
                writeConcern = new com.mongodb.WriteConcern(wc.getW(), wc.getWtimeout(), wc.isFsync(), wc.isJ());
            }
            coll.setWriteConcern(writeConcern);
        }
        return coll;
    }


    @SuppressWarnings("Duplicates")
    public MongoCollection<Document> getCollection(MongoDatabase database, String collection, ReadPreference readPreference, de.caluga.morphium.driver.WriteConcern wc) {
        MongoCollection<Document> coll = database.getCollection(collection);
        com.mongodb.ReadPreference prf;

        if (readPreference == null) {
            readPreference = defaultReadPreference;
        }
        if (readPreference != null) {
            TagSet tags = null;
            if (readPreference.getTagSet() != null) {
                List<Tag> tagList = readPreference.getTagSet().entrySet().stream().map(e -> new Tag(e.getKey(), e.getValue())).collect(Collectors.toList());
                tags = new TagSet(tagList);

            }
            switch (readPreference.getType()) {
                case NEAREST:
                    if (tags != null) {
                        prf = com.mongodb.ReadPreference.nearest(tags);
                    } else {
                        prf = com.mongodb.ReadPreference.nearest();
                    }
                    break;
                case PRIMARY:
                    prf = com.mongodb.ReadPreference.primary();
                    if (tags != null) {
                        log.warn("Cannot use tags with primary only read preference!");
                    }
                    break;
                case PRIMARY_PREFERRED:
                    if (tags != null) {
                        prf = com.mongodb.ReadPreference.primaryPreferred(tags);
                    } else {
                        prf = com.mongodb.ReadPreference.primaryPreferred();
                    }
                    break;
                case SECONDARY:
                    if (tags != null) {
                        prf = com.mongodb.ReadPreference.secondary(tags);
                    } else {
                        prf = com.mongodb.ReadPreference.secondary();
                    }
                    break;
                case SECONDARY_PREFERRED:
                    if (tags != null) {
                        prf = com.mongodb.ReadPreference.secondaryPreferred(tags);
                    } else {
                        prf = com.mongodb.ReadPreference.secondary();
                    }
                    break;
                default:
                    log.error("Unhandeled read preference: " + readPreference.toString());
                    prf = null;

            }
            if (prf != null) {
                coll = coll.withReadPreference(prf);
            }
        }

        if (wc != null) {
            com.mongodb.WriteConcern writeConcern;
            if (wc.getW() < 0) {
                //majority
                writeConcern = com.mongodb.WriteConcern.MAJORITY;
                writeConcern = writeConcern.withFsync(wc.isFsync());
                writeConcern = writeConcern.withJ(wc.isJ());
            } else {
                writeConcern = new com.mongodb.WriteConcern(wc.getW(), wc.getWtimeout() >= 0 ? wc.getWtimeout() : 0, wc.isFsync(), wc.isJ());
            }
            coll = coll.withWriteConcern(writeConcern);
        }
        return coll;
    }


    @Override
    public long count(String db, String collection, Map<String, Object> query, ReadPreference rp) {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        MongoDatabase database = mongo.getDatabase(db);
        MongoCollection<Document> coll = getCollection(database, collection, rp, null);
        return coll.count(new BasicDBObject(query));

    }

    @Override
    public void store(String db, String collection, List<Map<String, Object>> objs, de.caluga.morphium.driver.WriteConcern wc) throws MorphiumDriverException {
        List<Map<String, Object>> isnew = new ArrayList<>();
        final List<Map<String, Object>> notnew = new ArrayList<>();
        for (Map<String, Object> o : objs) {
            if (o.get("_id") == null) {
                //isnew!!!
                isnew.add(o);
            } else {
                notnew.add(o);
            }
        }
        insert(db, collection, isnew, wc);
        //        for (Map<String,Object> o:isnew){
        //            Object id=o.get("_id");
        //            if (id instanceof ObjectId) o.put("_id",new MorphiumId(((ObjectId)id).toHexString()));
        //        }
        DriverHelper.doCall(() -> {
            DriverHelper.replaceMorphiumIdByObjectId(notnew);
            MongoCollection c = mongo.getDatabase(db).getCollection(collection);

            //                mongo.getDB(db).getCollection(collection).save()
            for (Map<String, Object> toUpdate : notnew) {

                UpdateOptions o = new UpdateOptions();
                o.upsert(true);
                Document filter = new Document();

                Object id = toUpdate.get("_id");
                if (id instanceof MorphiumId) {
                    id = new ObjectId(id.toString());
                }
                filter.put("_id", id);
                //                    toUpdate.remove("_id");
                //                    Document update = new Document("$set", toUpdate);
                Document tDocument = new Document(toUpdate);
                for (String k : tDocument.keySet()) {
                    if (tDocument.get(k) instanceof byte[]) {
                        BsonBinary b = new BsonBinary((byte[]) tDocument.get(k));
                        tDocument.put(k, b);
                    }
                }
                tDocument.remove("_id"); //not needed
                //noinspection unchecked
                c.replaceOne(filter, tDocument, o);


                id = toUpdate.get("_id");
                if (id instanceof ObjectId) {
                    toUpdate.put("_id", new MorphiumId(((ObjectId) id).toHexString()));
                }
            }

            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    public void insert(String db, String collection, List<Map<String, Object>> objs, de.caluga.morphium.driver.WriteConcern wc) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(objs);
        if (objs == null || objs.isEmpty()) {
            return;
        }
        final List<Document> lst = objs.stream().map(Document::new).collect(Collectors.toList());

        for (Document d : lst) {
            for (String k : d.keySet()) {
                if (d.get(k) instanceof byte[]) {
                    BsonBinary b = new BsonBinary((byte[]) d.get(k));
                    d.put(k, b);
                }
            }
        }
        DriverHelper.doCall(() -> {
            MongoCollection c = mongo.getDatabase(db).getCollection(collection);
            if (lst.size() == 1) {
                //noinspection unchecked
                if (currentTransaction.get() == null) {
                    c.insertOne(lst.get(0));
                } else {
                    c.insertOne(currentTransaction.get().getSession(), lst.get(0));
                }
            } else {
                InsertManyOptions imo = new InsertManyOptions();
                imo.ordered(false);

                //noinspection unchecked
                if (currentTransaction.get() == null) {
                    c.insertMany(lst, imo);
                } else {
                    c.insertMany(currentTransaction.get().getSession(), lst, imo);
                }
            }

            for (int i = 0; i < lst.size(); i++) {
                Object id = lst.get(i).get("_id");
                if (id instanceof ObjectId) {
                    id = new MorphiumId(((ObjectId) id).toHexString());
                }
                objs.get(i).put("_id", id);
            }
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);

    }

    @Override
    public Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> op, boolean multiple, boolean upsert, de.caluga.morphium.driver.WriteConcern wc) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        DriverHelper.replaceMorphiumIdByObjectId(op);
        return DriverHelper.doCall(() -> {
            UpdateOptions opts = new UpdateOptions();
            opts.upsert(upsert);
            UpdateResult res;
            if (multiple) {
                if (currentTransaction.get() == null) {
                    res = mongo.getDatabase(db).getCollection(collection).updateMany(new BasicDBObject(query), new BasicDBObject(op), opts);
                } else {
                    res = mongo.getDatabase(db).getCollection(collection).updateMany(currentTransaction.get().getSession(), new BasicDBObject(query), new BasicDBObject(op), opts);
                }
            } else {
                if (currentTransaction.get() == null) {
                    res = mongo.getDatabase(db).getCollection(collection).updateOne(new BasicDBObject(query), new BasicDBObject(op), opts);
                } else {
                    res = mongo.getDatabase(db).getCollection(collection).updateOne(currentTransaction.get().getSession(), new BasicDBObject(query), new BasicDBObject(op), opts);

                }
            }

            Map<String, Object> ret = new HashMap<>();
            ret.put("matched", res.getMatchedCount());
            ret.put("modified", res.getModifiedCount());
            ret.put("acc", res.wasAcknowledged());
            return ret;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);

    }

    @Override
    public Map<String, Object> delete(String db, String collection, Map<String, Object> query, boolean multiple, de.caluga.morphium.driver.WriteConcern wc) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        return DriverHelper.doCall(() -> {
            MongoDatabase database = mongo.getDatabase(db);
            MongoCollection<Document> coll = database.getCollection(collection);
            DeleteResult res;
            if (multiple) {
                if (currentTransaction.get() == null) {
                    res = coll.deleteMany(new BasicDBObject(query));
                } else {
                    res = coll.deleteMany(currentTransaction.get().getSession(), new BasicDBObject(query));

                }
            } else {
                if (currentTransaction.get() == null) {
                    res = coll.deleteOne(new BasicDBObject(query));
                } else {
                    res = coll.deleteOne(currentTransaction.get().getSession(), new BasicDBObject(query));
                }
            }
            Map<String, Object> r = new HashMap<>();
            r.put("deleted", res.getDeletedCount());
            r.put("acc", res.wasAcknowledged());

            return r;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    public void drop(String db, String collection, de.caluga.morphium.driver.WriteConcern wc) throws MorphiumDriverException {
        DriverHelper.doCall(() -> {
            MongoDatabase database = mongo.getDatabase(db);
            MongoCollection<Document> coll = database.getCollection(collection);
                coll.drop();
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    public void drop(String db, de.caluga.morphium.driver.WriteConcern wc) throws
            MorphiumDriverException {
        DriverHelper.doCall(() -> {
            MongoDatabase database = mongo.getDatabase(db);
            if (wc != null) {
                com.mongodb.WriteConcern writeConcern = new com.mongodb.WriteConcern(wc.getW(), wc.getWtimeout(), wc.isFsync(), wc.isJ());
                database = database.withWriteConcern(writeConcern);
            }
                database.drop();
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    public boolean exists(String db) {
        for (String dbName : mongo.getDatabaseNames()) {
            if (dbName.equals(db)) {
                return true;
            }
        }
        return false;
    }


    @Override
    public List<Object> distinct(String db, String collection, String field, final Map<String, Object> filter, ReadPreference rp) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(filter);
        final List<Object> ret = new ArrayList<>();
        DriverHelper.doCall(() -> {
            DistinctIterable<Document> it = null;
            if (currentTransaction.get() == null) {
                List<Object> lst = getColl(mongo.getDB(db), collection, getDefaultReadPreference(), null).distinct(field, new BasicDBObject(filter));
                for (Object o : lst) ret.add(o);
            } else {
                it = getCollection(mongo.getDatabase(db), collection, getDefaultReadPreference(), null).distinct(currentTransaction.get().getSession(), field, new BasicDBObject(filter), Document.class);
                for (Document d : it) {
                    ret.add(d);
                }
            }
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);

        return ret;
    }

    @Override
    public boolean exists(String db, String collection) throws MorphiumDriverException {
        Map<String, Object> found = DriverHelper.doCall(() -> {
            final Map<String, Object> ret = new HashMap<>();

            Document result = mongo.getDatabase(db).runCommand(new Document("listCollections", 1));
            @SuppressWarnings("unchecked") ArrayList<Document> batch = (ArrayList<Document>) (((Map) result.get("cursor")).get("firstBatch"));
            for (Document d : batch) {
                if (d.get("name").equals(collection)) {
                    return d;
                }
            }
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);


        return found != null && !found.isEmpty();
    }

    @Override
    public List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException {
        //noinspection unchecked,ConstantConditions
        return (List<Map<String, Object>>) DriverHelper.doCall(() -> {
            List<Map<String, Object>> values = new ArrayList<>();
            for (Document d : mongo.getDatabase(db).getCollection(collection).listIndexes()) {
                values.add(new HashMap<>(d));
            }
            HashMap<String, Object> ret = new HashMap<>();
            ret.put("values", values);
            return ret;
        }, retriesOnNetworkError, sleepBetweenErrorRetries).get("values");

    }

    @Override
    public List<String> getCollectionNames(String db) throws MorphiumDriverException {
        final List<String> ret = new ArrayList<>();
        DriverHelper.doCall(() -> {
            if (currentTransaction.get() == null) {
                for (String c : mongo.getDatabase(db).listCollectionNames()) {
                    ret.add(c);
                }
            } else {
                for (String c : mongo.getDatabase(db).listCollectionNames(currentTransaction.get().getSession())) {
                    ret.add(c);
                }
            }
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
        return ret;
    }


    @Override
    public Map<String, Object> group(String db, String coll, Map<String, Object> query, Map<String, Object> initial, String jsReduce, String jsFinalize, ReadPreference rp, String... keys) {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        DriverHelper.replaceMorphiumIdByObjectId(initial);
        BasicDBObject k = new BasicDBObject();
        BasicDBObject ini = new BasicDBObject();
        ini.putAll(initial);
        for (String ks : keys) {
            if (ks.startsWith("-")) {
                k.put(ks.substring(1), "false");
            } else if (ks.startsWith("+")) {
                k.put(ks.substring(1), "true");
            } else {
                k.put(ks, "true");
            }
        }
        if (!jsReduce.trim().startsWith("function(")) {
            jsReduce = "function (obj,data) { " + jsReduce + " }";
        }
        if (jsFinalize == null) {
            jsFinalize = "";
        }
        if (!jsFinalize.trim().startsWith("function(")) {
            jsFinalize = "function (data) {" + jsFinalize + "}";
        }
        DBCollection collection = mongo.getDB(db).getCollection(coll);
        GroupCommand cmd = new GroupCommand(collection,
                k, new BasicDBObject(query), ini, jsReduce, jsFinalize);

        //noinspection unchecked
        return convertBSON((Map<? extends String, ?>) cmd.toDBObject());
    }


    @Override
    public List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline,
                                               boolean explain, boolean allowDiskUse, ReadPreference readPreference) {
        DriverHelper.replaceMorphiumIdByObjectId(pipeline);
        //noinspection unchecked
        List list = new ArrayList<>(pipeline.stream().map(BasicDBObject::new).collect(Collectors.toList()));


        if (explain) {
            @SuppressWarnings("unchecked") CommandResult ret = getColl(mongo.getDB(db), collection, getDefaultReadPreference(), null).explainAggregate(list, null);
            List<Map<String, Object>> o = new ArrayList<>();
            o.add(new HashMap<>(ret));
            return o;
        } else {
            MongoCollection<Document> c = getCollection(mongo.getDatabase(db), collection, getDefaultReadPreference(), null);
            AggregateIterable<Document> it = null;
            if (currentTransaction.get() == null) {
                it = c.aggregate(list, Document.class);
            } else {
                it = c.aggregate(currentTransaction.get().getSession(), list, Document.class);
            }
//            @SuppressWarnings("unchecked") Cursor ret = getColl(mongo.getDB(db), collection, getDefaultReadPreference(), null).aggregate(list, opts);
            List<Map<String, Object>> result = new ArrayList<>();

            for (Document doc : it) {
                //noinspection unchecked
                result.add(convertBSON(new HashMap<>((Map) doc)));
            }
            return result;

        }

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

    public Map<String, Object> getCollectionStats(String db, String coll, int scale, boolean verbose) throws MorphiumDriverException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("collStats", coll);
        cmd.put("scale", scale);
        cmd.put("verbose", verbose);
        cmd = runCommand(db, cmd);
        return cmd;
    }

    @Override
    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        Object capped = getCollectionStats(db, coll, 1024, false).get("capped");
        if (capped instanceof String) {
            return capped.equals("true");
        }
        return capped.equals(Boolean.TRUE) || capped.equals(1) || capped.equals(true);
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, de.caluga.morphium.driver.WriteConcern wc) {
        return new MongodbBulkContext(m, db, collection, this, ordered, defaultBatchSize, wc);
    }


    public MongoDatabase getDb(String db) {
        return mongo.getDatabase(db);
    }

    @SuppressWarnings("unused")
    public MongoCollection getCollection(String db, String coll) {

        return mongo.getDatabase(db).getCollection(coll);
    }


    @Override
    public void createIndex(String db, String collection, Map<String, Object> index, Map<String, Object> options) throws MorphiumDriverException {
        DriverHelper.doCall(() -> {
            BasicDBObject options1 = options == null ? new BasicDBObject() : new BasicDBObject(options);
            //use IndexOptions and new API
            mongo.getDB(db).getCollection(collection).createIndex(new BasicDBObject(index), options1);
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);

    }


    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing) {
        return mapReduce(db, collection, mapping, reducing, null, null);
    }

    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query) {
        return mapReduce(db, collection, mapping, reducing, query, null);
    }

    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query, Map<String, Object> sorting) {
        MapReduceIterable<Document> res = null;
        if (currentTransaction.get() == null) {
            res = mongo.getDatabase(db).getCollection(collection).mapReduce(mapping, reducing);
        } else {
            res = mongo.getDatabase(db).getCollection(collection).mapReduce(currentTransaction.get().getSession(), mapping, reducing);
        }
        if (query != null) {
            BasicDBObject v = new BasicDBObject(query);
            res.filter(v);
        }
        if (sorting != null) {
            res.sort(new BasicDBObject(sorting));
        }
        ArrayList<Map<String, Object>> ret = new ArrayList<>();
        for (Document d : res) {
            //noinspection unchecked
            Map<String, Object> value = (Map) d.get("value");
            for (Map.Entry<String, Object> s : value.entrySet()) {
                if (s.getValue() instanceof ObjectId) {
                    value.put(s.getKey(), new MorphiumId(((ObjectId) s.getValue()).toHexString()));
                }
            }
            ret.add(value);
        }

        return ret;
    }


    @Override
    public void startTransaction() {
        if (currentTransaction.get() != null) {
            throw new IllegalArgumentException("Transaction in progress");
        }

        ClientSessionOptions.Builder b = ClientSessionOptions.builder();
        b.causallyConsistent(true);
        b.defaultTransactionOptions(TransactionOptions.builder().readConcern(ReadConcern.DEFAULT).readPreference(com.mongodb.ReadPreference.primaryPreferred()).build());

        ClientSession ses = mongo.startSession(b.build());
        ses.startTransaction();
        MongoTransactionContext ctx = new MongoTransactionContext();
        ctx.setSession(ses);
        currentTransaction.set(ctx);
    }

    @Override
    public void commitTransaction() {
        if (currentTransaction.get() == null) {
            throw new IllegalArgumentException("No transaction in progress");
        }
        currentTransaction.get().getSession().abortTransaction();
        currentTransaction.set(null);
    }

    @Override
    public MorphiumTransactionContext getTransactionContext() {
        return currentTransaction.get();
    }

    @Override
    public void abortTransaction() {
        if (currentTransaction.get() == null) {
            throw new IllegalArgumentException("No transaction in progress");
        }
        currentTransaction.get().getSession().abortTransaction();
        currentTransaction.set(null);
    }

    @Override
    public void setTransactionContext(MorphiumTransactionContext ctx) {
        if (currentTransaction.get() != null) {
            throw new IllegalArgumentException("Transaction in progress!");
        }
        currentTransaction.set((MongoTransactionContext) ctx);
    }
}
