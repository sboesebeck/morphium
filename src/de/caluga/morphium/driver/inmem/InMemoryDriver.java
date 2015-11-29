package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.morphium.driver.bulk.*;
import de.caluga.morphium.driver.mongodb.Maximums;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Stephan Bösebeck
 * Date: 28.11.15
 * Time: 23:25
 * <p>
 * TODO: Add documentation here
 */
public class InMemoryDriver implements MorphiumDriver {
    private Logger log = new Logger(InMemoryDriver.class);
    // DBName => Collection => List of documents
    private Map<String, Map<String, List<Map<String, Object>>>> database = new ConcurrentHashMap<>();

    @Override
    public void setCredentials(String db, String login, char[] pwd) {

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
    public String[] getHostSeed() {
        return new String[0];
    }

    @Override
    public int getMaxConnectionsPerHost() {
        return 0;
    }

    @Override
    public int getMinConnectionsPerHost() {
        return 0;
    }

    @Override
    public int getMaxConnectionLifetime() {
        return 0;
    }

    @Override
    public int getMaxConnectionIdleTime() {
        return 0;
    }

    @Override
    public int getSocketTimeout() {
        return 0;
    }

    @Override
    public int getConnectionTimeout() {
        return 0;
    }

    @Override
    public int getDefaultW() {
        return 0;
    }

    @Override
    public int getMaxBlockintThreadMultiplier() {
        return 0;
    }

    @Override
    public int getHeartbeatFrequency() {
        return 0;
    }

    @Override
    public void setHeartbeatSocketTimeout(int heartbeatSocketTimeout) {

    }

    @Override
    public void setUseSSL(boolean useSSL) {

    }

    @Override
    public void setHeartbeatFrequency(int heartbeatFrequency) {

    }

    @Override
    public void setWriteTimeout(int writeTimeout) {

    }

    @Override
    public void setDefaultBatchSize(int defaultBatchSize) {

    }

    @Override
    public void setCredentials(Map<String, String[]> credentials) {

    }

    @Override
    public int getHeartbeatSocketTimeout() {
        return 0;
    }

    @Override
    public boolean isUseSSL() {
        return false;
    }

    @Override
    public boolean isDefaultJ() {
        return false;
    }

    @Override
    public int getWriteTimeout() {
        return 0;
    }

    @Override
    public int getLocalThreshold() {
        return 0;
    }

    @Override
    public void setHostSeed(String... host) {

    }

    @Override
    public void setMaxConnectionsPerHost(int mx) {

    }

    @Override
    public void setMinConnectionsPerHost(int mx) {

    }

    @Override
    public void setMaxConnectionLifetime(int timeout) {

    }

    @Override
    public void setMaxConnectionIdleTime(int time) {

    }

    @Override
    public void setSocketTimeout(int timeout) {

    }

    @Override
    public void setConnectionTimeout(int timeout) {

    }

    @Override
    public void setDefaultW(int w) {

    }

    @Override
    public void setMaxBlockingThreadMultiplier(int m) {

    }

    @Override
    public void heartBeatFrequency(int t) {

    }

    @Override
    public void heartBeatSocketTimeout(int t) {

    }

    @Override
    public void useSsl(boolean ssl) {

    }

    @Override
    public void connect() throws MorphiumDriverException {

    }

    @Override
    public void setDefaultReadPreference(ReadPreference rp) {

    }

    @Override
    public void connect(String replicasetName) throws MorphiumDriverException {

    }

    @Override
    public Maximums getMaximums() {
        Maximums ret = new Maximums();
        ret.setMaxBsonSize(10000);
        ret.setMaxMessageSize(10000);
        ret.setMaxWriteBatchSize(100);
        return ret;
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public void setDefaultJ(boolean j) {

    }

    @Override
    public void setDefaultWriteTimeout(int wt) {

    }

    @Override
    public void setLocalThreshold(int thr) {

    }

    @Override
    public void setDefaultFsync(boolean j) {

    }

    @Override
    public void setRetriesOnNetworkError(int r) {

    }

    @Override
    public int getRetriesOnNetworkError() {
        return 0;
    }

    @Override
    public void setSleepBetweenErrorRetries(int s) {

    }

    @Override
    public int getSleepBetweenErrorRetries() {
        return 0;
    }

    @Override
    public void close() throws MorphiumDriverException {

    }

    @Override
    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        return new HashMap<>();
    }

    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        Map<String, Object> ret = new HashMap<>();
        ret.put("collections", getDB(db).size());


        return ret;
    }

    @Override
    public Map<String, Object> getOps(long threshold) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        return null;
    }


    private boolean matchesQuery(Map<String, Object> query, Map<String, Object> toCheck) {
        boolean matches = false;
        for (String key : query.keySet()) {
            if (key.equals("$and")) {
                //list of field queries
                List<Map<String, Object>> lst = ((List<Map<String, Object>>) query.get(key));
                for (Map<String, Object> q : lst) {
                    if (!matchesQuery(toCheck, q)) return false;
                }
                return true;
            } else if (key.equals("$or")) {
                //list of or queries
                List<Map<String, Object>> lst = ((List<Map<String, Object>>) query.get(key));
                for (Map<String, Object> q : lst) {
                    if (matchesQuery(toCheck, q)) return true;
                }
                return false;

            } else {
                //field check
                if (query.get(key) instanceof Map) {
                    //probably a query operand
                    Map<String, Object> q = (Map<String, Object>) query.get(key);
                    assert (q.size() == 1);
                    for (String k : q.keySet()) {
                        switch (k) {
                            case "$lt":
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) < 0;
                            case "$lte":
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) <= 0;
                            case "$gt":
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) > 0;
                            case "$gte":
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) >= 0;
                            case "$in":
                                for (Object v : (List) q.get(k)) {
                                    if (toCheck.get(key).equals(v)) return true;
                                }
                                return false;
                            default:
                                throw new RuntimeException("Unknown Operator " + k);
                        }

                    }
                } else {
                    //value comparison - should only be one here
                    assert (query.size() == 1);
                    return toCheck.get(key).equals(query.get(key));
                }
            }
        }
        return matches;
    }

    @Override
    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference rp, Map<String, Object> findMetaData) throws MorphiumDriverException {
        List<Map<String, Object>> data = getCollection(db, collection);
        List<Map<String, Object>> ret = new ArrayList<>();
        int count = 0;
        for (Map<String, Object> o : data) {
            count++;
            if (count < skip) {
                continue;
            }
            if (matchesQuery(query, o)) ret.add(o);
            if (limit > 0 && ret.size() >= limit) break;

            //todo add projection
            //todo add sort
        }

        return ret;
    }

    @Override
    public long count(String db, String collection, Map<String, Object> query, ReadPreference rp) throws MorphiumDriverException {
        List<Map<String, Object>> data = getCollection(db, collection);
        if (query.size() == 0) {
            return data.size();
        }
        long cnt = 0;
        for (Map<String, Object> o : data) {
            if (matchesQuery(query, o)) cnt++;
        }

        return cnt;
    }

    public List<Map<String, Object>> findByFieldValue(String db, String coll, String field, Object value) {
        List<Map<String, Object>> ret = new ArrayList<>();
        for (Map<String, Object> obj : getCollection(db, coll)) {
            if (obj.get(field).equals(value)) {
                ret.add(obj);
            }
        }
        return ret;
    }

    @Override
    public void insert(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        for (Map<String, Object> o : objs) {
            if (findByFieldValue(db, collection, "_id", o.get("_id")).size() != 0)
                throw new MorphiumDriverException("Duplicate _id!", null);
        }
        getCollection(db, collection).addAll(objs);
    }

    @Override
    public void store(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        for (Map<String, Object> o : objs) {
            if (o.get("_id") == null) {
                o.put("_id", new MorphiumId());
                getCollection(db, collection).add(o);
                continue;
            }
            List<Map<String, Object>> srch = findByFieldValue(db, collection, "_id", o.get("_id"));
            if (srch.size() != 0) {
                getCollection(db, collection).remove(srch.get(0));
            }
            getCollection(db, collection).add(o);
        }
    }

    private Map<String, List<Map<String, Object>>> getDB(String db) {
        if (database.get(db) == null) {
            database.put(db, new ConcurrentHashMap<>());
        }
        return database.get(db);
    }

    @Override
    public Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> op, boolean multiple, boolean upsert, WriteConcern wc) throws MorphiumDriverException {
        List<Map<String, Object>> lst = find(db, collection, query, null, null, 0, multiple ? 0 : 1, 1000, null, null);
        if (upsert && (lst == null || lst.size() == 0)) {
            //TODO upserting
            throw new RuntimeException("Upsert not implemented yet!");
        }
        for (Map<String, Object> obj : lst) {
            for (String operand : op.keySet()) {
                Map<String, Object> cmd = (Map<String, Object>) op.get(operand);
                switch (operand) {
                    case "$set":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            obj.put(entry.getKey(), entry.getValue());
                        }
                        break;
                    case "$unset":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            obj.remove(entry.getKey());
                        }
                        break;
                    case "$inc":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            Object value = obj.get(entry.getKey());
                            if (value instanceof Integer) {
                                value = (Integer) value + ((Integer) entry.getValue());
                            } else if (value instanceof Double) {
                                value = (Double) value + ((Double) entry.getValue());
                            } else if (value instanceof Float) {
                                value = (Float) value + ((Float) entry.getValue());
                            } else if (value instanceof Long) {
                                value = (Long) value + ((Long) entry.getValue());

                            }
                            obj.put(entry.getKey(), value);
                        }
                        break;
                    case "$mul":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            Object value = obj.get(entry.getKey());
                            if (value instanceof Integer) {
                                value = (Integer) value * ((Integer) entry.getValue());
                            } else if (value instanceof Double) {
                                value = (Double) value * ((Double) entry.getValue());
                            } else if (value instanceof Float) {
                                value = (Float) value * ((Float) entry.getValue());
                            } else if (value instanceof Long) {
                                value = (Long) value * ((Long) entry.getValue());
                            }
                            obj.put(entry.getKey(), value);
                        }
                        break;
                    case "$rename":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            obj.put((String) entry.getValue(), obj.get(entry.getKey()));
                            obj.remove(entry.getKey());
                        }
                        break;
                    case "$min":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            Comparable value = (Comparable) obj.get(entry.getKey());
                            if (value.compareTo(entry.getValue()) > 0) {
                                obj.put(entry.getKey(), entry.getValue());
                            }
                        }
                        break;
                    case "$max":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            Comparable value = (Comparable) obj.get(entry.getKey());
                            if (value.compareTo(entry.getValue()) < 0) {
                                obj.put(entry.getKey(), entry.getValue());
                            }
                        }
                        break;
                    default:
                        throw new RuntimeException("unknown operand " + operand);
                }
            }
        }
        return new HashMap<>();
    }

    @Override
    public Map<String, Object> delete(String db, String collection, Map<String, Object> query, boolean multiple, WriteConcern wc) throws MorphiumDriverException {
        List<Map<String, Object>> toDel = find(db, collection, query, null, null, 0, multiple ? 0 : 1, 10000, null, null);
        for (Map<String, Object> o : toDel) {
            getCollection(db, collection).remove(o);
        }
        return new HashMap<>();
    }

    private List<Map<String, Object>> getCollection(String db, String collection) {
        if (getDB(db).get(collection) == null) {
            getDB(db).put(collection, new Vector<>());
        }
        return getDB(db).get(collection);
    }

    @Override
    public void drop(String db, String collection, WriteConcern wc) throws MorphiumDriverException {
        getDB(db).remove(collection);
    }

    @Override
    public void drop(String db, WriteConcern wc) throws MorphiumDriverException {
        database.remove(db);
    }

    @Override
    public boolean exists(String db) throws MorphiumDriverException {
        return database.containsKey(db);
    }

    @Override
    public List<Object> distinct(String db, String collection, String field, Map<String, Object> filter) throws MorphiumDriverException {
        return null;
    }

    @Override
    public boolean exists(String db, String collection) throws MorphiumDriverException {
        return getDB(db).containsKey(collection);
    }

    @Override
    public List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException {
        return new ArrayList<>();
    }

    @Override
    public List<String> getCollectionNames(String db) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> group(String db, String coll, Map<String, Object> query, Map<String, Object> initial, String jsReduce, String jsFinalize, ReadPreference rp, String... keys) {
        return null;
    }

    @Override
    public List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, ReadPreference readPreference) throws MorphiumDriverException {
        return null;
    }

    @Override
    public boolean isSocketKeepAlive() {
        return false;
    }

    @Override
    public void setSocketKeepAlive(boolean socketKeepAlive) {

    }

    @Override
    public int getHeartbeatConnectTimeout() {
        return 0;
    }

    @Override
    public void setHeartbeatConnectTimeout(int heartbeatConnectTimeout) {

    }

    @Override
    public int getMaxWaitTime() {
        return 0;
    }

    @Override
    public void setMaxWaitTime(int maxWaitTime) {

    }

    @Override
    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        return false;
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return new BulkRequestContext(m) {
            List<BulkRequest> requests = new ArrayList<>();

            @Override
            public Map<String, Object> execute() {
                try {
                    for (BulkRequest r : requests) {
                        if (r instanceof InsertBulkRequest) {
                            insert(db, collection, ((InsertBulkRequest) r).getToInsert(), null);
                        } else if (r instanceof StoreBulkRequest) {
                            store(db, collection, ((StoreBulkRequest) r).getToInsert(), null);
                        } else if (r instanceof UpdateBulkRequest) {
                            UpdateBulkRequest up = (UpdateBulkRequest) r;
                            update(db, collection, up.getQuery(), up.getCmd(), up.isMultiple(), up.isUpsert(), null);
                        } else if (r instanceof DeleteBulkRequest) {
                            delete(db, collection, ((DeleteBulkRequest) r).getQuery(), ((DeleteBulkRequest) r).isMultiple(), null);
                        } else {
                            throw new RuntimeException("Unknown operation " + r.getClass().getName());
                        }
                    }
                } catch (MorphiumDriverException e) {
                    log.error("Got exception: ", e);
                }
                return new HashMap<>();

            }

            @Override
            public UpdateBulkRequest addUpdateBulkRequest() {
                UpdateBulkRequest up = new UpdateBulkRequest();
                requests.add(up);
                return up;
            }

            @Override
            public StoreBulkRequest addStoreBulkRequest(List<Map<String, Object>> toStore) {
                StoreBulkRequest store = new StoreBulkRequest(toStore);
                requests.add(store);
                return store;

            }

            @Override
            public InsertBulkRequest addInsertBulkReqpest(List<Map<String, Object>> toInsert) {
                InsertBulkRequest in = new InsertBulkRequest(toInsert);
                requests.add(in);
                return in;
            }

            @Override
            public DeleteBulkRequest addDeleteBulkRequest() {
                DeleteBulkRequest del = new DeleteBulkRequest();
                requests.add(del);
                return del;
            }
        };
    }

    @Override
    public void createIndex(String db, String collection, Map<String, Object> index, Map<String, Object> options) throws MorphiumDriverException {
        //TODO: implement hashmap access for this

    }
}
