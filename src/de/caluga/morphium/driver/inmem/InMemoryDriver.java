package de.caluga.morphium.driver.inmem;

import com.rits.cloning.Cloner;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.*;
import de.caluga.morphium.driver.mongodb.Maximums;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: Stephan Bösebeck
 * Date: 28.11.15
 * Time: 23:25
 * <p>
 * InMemory implementation of the MorphiumDriver interface. can be used for testing or caching. Does not cover all
 * functionality yet.
 */
@SuppressWarnings("WeakerAccess")
public class InMemoryDriver implements MorphiumDriver {
    private final Logger log = LoggerFactory.getLogger(InMemoryDriver.class);
    // DBName => Collection => List of documents
    private final Map<String, Map<String, List<Map<String, Object>>>> database = new ConcurrentHashMap<>();
    private final ThreadLocal<InMemTransactionContext> currentTransaction = new ThreadLocal<>();
    private final AtomicLong txn = new AtomicLong();
    private final Map<String, List<DriverTailableIterationCallback>> watchersByDb = new ConcurrentHashMap<>();

    @Override
    public List<String> listDatabases() {
        return new Vector<>(database.keySet());
    }

    @Override
    public List<String> listCollections(String db, String pattern) {

        Set<String> collections = database.get(db).keySet();
        List<String> ret = new Vector<>();
        if (pattern == null) {
            ret.addAll(collections);
        } else {
            for (String col : collections) {
                if (col.matches(pattern)) {
                    ret.add(col);
                }
            }
        }
        return ret;
    }

    public void resetData() {
        database.clear();
        currentTransaction.remove();
    }

    @Override
    public void setCredentials(String db, String login, char[] pwd) {

    }

    @Override
    public int getDefaultWriteTimeout() {
        return 0;
    }

    @Override
    public void setDefaultWriteTimeout(int wt) {

    }

    @Override
    public boolean isReplicaset() {
        return false;
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
        return new String[0];
    }

    @Override
    public void setHostSeed(String... host) {

    }

    @Override
    public int getMaxConnectionsPerHost() {
        return 0;
    }

    @Override
    public void setMaxConnectionsPerHost(int mx) {

    }

    @Override
    public int getMinConnectionsPerHost() {
        return 0;
    }

    @Override
    public void setMinConnectionsPerHost(int mx) {

    }

    @Override
    public int getMaxConnectionLifetime() {
        return 0;
    }

    @Override
    public void setMaxConnectionLifetime(int timeout) {

    }

    @Override
    public int getMaxConnectionIdleTime() {
        return 0;
    }

    @Override
    public void setMaxConnectionIdleTime(int time) {

    }

    @Override
    public int getSocketTimeout() {
        return 0;
    }

    @Override
    public void setSocketTimeout(int timeout) {

    }

    @Override
    public int getConnectionTimeout() {
        return 0;
    }

    @Override
    public void setConnectionTimeout(int timeout) {

    }

    @Override
    public int getDefaultW() {
        return 0;
    }

    @Override
    public void setDefaultW(int w) {

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
    public void setHeartbeatFrequency(int heartbeatFrequency) {

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
    public void setHeartbeatSocketTimeout(int heartbeatSocketTimeout) {

    }

    @Override
    public boolean isUseSSL() {
        return false;
    }

    @Override
    public void setUseSSL(boolean useSSL) {

    }

    @Override
    public boolean isDefaultJ() {
        return false;
    }

    @Override
    public void setDefaultJ(boolean j) {

    }

    @Override
    public int getWriteTimeout() {
        return 0;
    }

    @Override
    public void setWriteTimeout(int writeTimeout) {

    }

    @Override
    public int getLocalThreshold() {
        return 0;
    }

    @Override
    public void setLocalThreshold(int thr) {

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
    public void connect() {

    }

    @Override
    public void setDefaultReadPreference(ReadPreference rp) {

    }

    @Override
    public void connect(String replicasetName) {

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
    public int getRetriesOnNetworkError() {
        return 0;
    }

    @Override
    public void setRetriesOnNetworkError(int r) {

    }

    @Override
    public int getSleepBetweenErrorRetries() {
        return 0;
    }

    @Override
    public void setSleepBetweenErrorRetries(int s) {

    }

    @Override
    public void close() {

    }

    @Override
    public Map<String, Object> getReplsetStatus() {
        return new HashMap<>();
    }

    @Override
    public Map<String, Object> getDBStats(String db) {
        Map<String, Object> ret = new HashMap<>();
        ret.put("collections", getDB(db).size());


        return ret;
    }

    @Override
    public Map<String, Object> getOps(long threshold) {
        throw new RuntimeException("not working on memory");
    }

    @Override
    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) {
        throw new RuntimeException("not working on memory");
    }


    private boolean matchesQuery(Map<String, Object> query, Map<String, Object> toCheck) {
        if (query.isEmpty()) {
            return true;
        }
        if (query.containsKey("$where")) {
            ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
            //engine.eval("print('Hello World!');");
            engine.getContext().setAttribute("obj", toCheck, ScriptContext.ENGINE_SCOPE);
            engine.getContext().setAttribute("this", toCheck, ScriptContext.ENGINE_SCOPE);
            try {
                Object result = engine.eval((String) query.get("$where"));
                if (result == null || result.equals(Boolean.FALSE)) return false;
            } catch (ScriptException e) {
                throw new RuntimeException("Scripting error", e);
            }
        }
        //noinspection LoopStatementThatDoesntLoop
        for (String key : query.keySet()) {
            switch (key) {
                case "$and": {
                    //list of field queries
                    @SuppressWarnings("unchecked") List<Map<String, Object>> lst = ((List<Map<String, Object>>) query.get(key));
                    for (Map<String, Object> q : lst) {
                        if (!matchesQuery(q, toCheck)) {
                            return false;
                        }
                    }
                    return true;
                }
                case "$or": {
                    //list of or queries
                    @SuppressWarnings("unchecked") List<Map<String, Object>> lst = ((List<Map<String, Object>>) query.get(key));
                    for (Map<String, Object> q : lst) {
                        if (matchesQuery(q, toCheck)) {
                            return true;
                        }
                    }
                    return false;

                }
                default:
                    //field check
                    if (query.get(key) instanceof Map) {
                        //probably a query operand
                        @SuppressWarnings("unchecked") Map<String, Object> q = (Map<String, Object>) query.get(key);
                        assert (q.size() == 1);
                        String k = q.keySet().iterator().next();
                        switch (k) {
                            case "$lt":
                                //noinspection unchecked
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) < 0;
                            case "$lte":
                                //noinspection unchecked
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) <= 0;
                            case "$gt":
                                //noinspection unchecked
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) > 0;
                            case "$gte":
                                //noinspection unchecked
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) >= 0;
                            case "$mod":
                                Number n = (Number) toCheck.get(key);
                                List arr = (List) q.get(k);
                                int div = ((Integer) arr.get(0));
                                int rem = ((Integer) arr.get(1));
                                return n.intValue() % div == rem;
                            case "$ne":
                                //noinspection unchecked
                                boolean contains = false;
                                if (toCheck.get(key) instanceof List) {
                                    for (Object o : (List) toCheck.get(key)) {
                                        if (o != null && q.get(k) != null && o.equals(q.get(k))) {
                                            contains = true;
                                            break;
                                        }
                                    }
                                    return !contains;
                                }
                                if (toCheck.get(key) == null && q.get(k) != null) return true;
                                if (toCheck.get(key) == null && q.get(k) == null) return false;
                                //noinspection unchecked
                                return ((Comparable) toCheck.get(key)).compareTo(q.get(k)) != 0;
                            case "$exists":
                                boolean exists = (toCheck.containsKey(key));
                                if (q.get(k).equals(Boolean.TRUE) || q.get(k).equals("true") || q.get(k).equals(1)) {
                                    return exists;
                                } else {
                                    return !exists;
                                }
                            case "$nin":
                                boolean found = false;
                                if (toCheck.containsKey(key)) {
                                    for (Object v : (List) q.get(k)) {
                                        if (v instanceof MorphiumId) {
                                            v = new ObjectId(v.toString());
                                        }
                                        if (toCheck.get(key).equals(v)) {
                                            found = true;
                                        }
                                    }
                                }
                                return !found;
                            case "$in":
                                if (toCheck.containsKey(key)) {
                                    for (Object v : (List) q.get(k)) {
                                        if (v instanceof MorphiumId) {
                                            v = new ObjectId(v.toString());
                                        }
                                        if (toCheck.get(key).equals(v)) {
                                            return true;
                                        }
                                    }
                                }
                                return false;
                            default:
                                throw new RuntimeException("Unknown Operator " + k);
                        }


                    } else {
                        //value comparison - should only be one here
                        assert (query.size() == 1);
//                        if (toCheck.get(key)!=null) {
                        if (toCheck.get(key) instanceof MorphiumId || toCheck.get(key) instanceof ObjectId) {
                            return toCheck.get(key).toString().equals(query.get(key).toString());
                        }
                        if (toCheck.get(key) == null && query.get(key) != null) return false;
                        if (toCheck.get(key) == null && query.get(key) == null) return true;
                        return toCheck.get(key).equals(query.get(key));
//                        }
                    }
            }
        }
        return false;
    }

    @Override
    public MorphiumCursor initIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, Map<String, Object> findMetaData) throws MorphiumDriverException {
        MorphiumCursor crs = new MorphiumCursor();
        crs.setBatchSize(batchSize);
        crs.setCursorId(System.currentTimeMillis());
        InMemoryCursor inCrs = new InMemoryCursor();
        inCrs.skip = skip;
        inCrs.limit = limit;
        inCrs.batchSize = batchSize;
        if (batchSize == 0) {
            inCrs.batchSize = 1000;
        }

        inCrs.setCollection(collection);
        inCrs.setDb(db);
        inCrs.setProjection(projection);
        inCrs.setQuery(query);
        inCrs.setFindMetaData(findMetaData);
        inCrs.setReadPreference(readPreference);
        inCrs.setSort(sort);
        //noinspection unchecked
        crs.setInternalCursorObject(inCrs);
        int l = batchSize;
        if (limit != 0 && limit < batchSize) {
            l = limit;
        }
        List<Map<String, Object>> res = find(db, collection, query, sort, projection, skip, l, batchSize, readPreference, findMetaData);
        crs.setBatch(res);

        if (res.size() < batchSize) {
            //noinspection unchecked
            crs.setInternalCursorObject(null); //cursor ended - no more data
        } else {
            inCrs.dataRead = res.size();
        }
        return crs;
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void watch(String db, int timeout, boolean fullDocumentOnUpdate, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        watchersByDb.putIfAbsent(db, new Vector<>());
        watchersByDb.get(db).add(cb);

        //simulate blocking
        while (isConnected()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                //swallow
            }
        }
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void watch(String db, String collection, int timeout, boolean fullDocumentOnUpdate, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        String key = db + "." + collection;
        watchersByDb.putIfAbsent(key, new Vector<>());
        watchersByDb.get(key).add(cb);
        //simulate blocking
        while (isConnected()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public MorphiumCursor nextIteration(MorphiumCursor crs) throws MorphiumDriverException {
        MorphiumCursor next = new MorphiumCursor();
        next.setCursorId(crs.getCursorId());

        InMemoryCursor oldCrs = (InMemoryCursor) crs.getInternalCursorObject();
        if (oldCrs == null) {
            return null;
        }

        InMemoryCursor inCrs = new InMemoryCursor();
        inCrs.setReadPreference(oldCrs.getReadPreference());
        inCrs.setFindMetaData(oldCrs.getFindMetaData());
        inCrs.setDb(oldCrs.getDb());
        inCrs.setQuery(oldCrs.getQuery());
        inCrs.setCollection(oldCrs.getCollection());
        inCrs.setProjection(oldCrs.getProjection());
        inCrs.setBatchSize(oldCrs.getBatchSize());

        inCrs.setLimit(oldCrs.getLimit());

        inCrs.setSort(oldCrs.getSort());
        inCrs.skip = oldCrs.getDataRead() + 1;
        int limit = oldCrs.getBatchSize();
        if (oldCrs.getLimit() != 0) {
            if (oldCrs.getDataRead() + oldCrs.getBatchSize() > oldCrs.getLimit()) {
                limit = oldCrs.getLimit() - oldCrs.getDataRead();
            }
        }
        List<Map<String, Object>> res = find(inCrs.getDb(), inCrs.getCollection(), inCrs.getQuery(), inCrs.getSort(), inCrs.getProjection(), inCrs.getSkip(), limit, inCrs.getBatchSize(), inCrs.getReadPreference(), inCrs.getFindMetaData());
        next.setBatch(res);
        if (res.size() < inCrs.getBatchSize()) {
            //finished!
            //noinspection unchecked
            next.setInternalCursorObject(null);
        } else {
            inCrs.setDataRead(oldCrs.getDataRead() + res.size());
            //noinspection unchecked
            next.setInternalCursorObject(inCrs);
        }
        return next;
    }

    @Override
    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference rp, Map<String, Object> findMetaData) throws MorphiumDriverException {
        return find(db, collection, query, sort, projection, skip, limit, false);
    }


    @SuppressWarnings({"RedundantThrows", "UnusedParameters"})
    private List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, boolean internal) throws MorphiumDriverException {
        List<Map<String, Object>> data = new Vector<>(getCollection(db, collection));
        List<Map<String, Object>> ret = new Vector<>();
        int count = 0;

        if (sort != null) {
            data.sort((o1, o2) -> {
                for (String f : sort.keySet()) {
                    if (o1.get(f).equals(o2.get(f))) {
                        continue;
                    }
                    //noinspection unchecked
                    return ((Comparable) o1.get(f)).compareTo(o2.get(f)) * sort.get(f);
                }
                return 0;
            });
        }
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < data.size(); i++) {
            Map<String, Object> o = data.get(i);
            count++;
            if (count < skip) {
                continue;
            }
            if (matchesQuery(query, o)) {
                ret.add(internal ? o : new HashMap<>(o));
            }
            if (limit > 0 && ret.size() >= limit) {
                break;
            }

            //todo add projection
        }

        return new ArrayList<>(ret);
    }

    @Override
    public long count(String db, String collection, Map<String, Object> query, ReadPreference rp) {
        List<Map<String, Object>> data = getCollection(db, collection);
        if (query.isEmpty()) {
            return data.size();
        }
        long cnt = 0;
        for (Map<String, Object> o : data) {
            if (matchesQuery(query, o)) {
                cnt++;
            }
        }

        return cnt;
    }

    public List<Map<String, Object>> findByFieldValue(String db, String coll, String field, Object value) {
        List<Map<String, Object>> ret = new Vector<>();

        List<Map<String, Object>> data = getCollection(db, coll);
        for (Map<String, Object> obj : data) {
            if (obj.get(field) == null && value != null) {
                continue;
            }
            if ((obj.get(field) == null && value == null)
                    || obj.get(field).equals(value)) {
                ret.add(new HashMap<>(obj));
            }
        }
        return new ArrayList<>(ret);
    }

    @Override
    public void insert(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        for (Map<String, Object> o : objs) {
            if (o.get("_id") != null && !findByFieldValue(db, collection, "_id", o.get("_id")).isEmpty()) {
                throw new MorphiumDriverException("Duplicate _id! " + o.get("_id"), null);
            }
            o.putIfAbsent("_id", new MorphiumId());
        }
        getCollection(db, collection).addAll(objs);
        for (Map<String, Object> o : objs) {
            notifyWatchers(db, collection, "insert", o);
        }
    }

    @Override
    public Map<String, Object> store(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) {
        Map<String, Object> ret = new HashMap<>();
        int upd = 0;
        int total = objs.size();
        for (Map<String, Object> o : objs) {

            if (o.get("_id") == null) {
                o.put("_id", new MorphiumId());
                getCollection(db, collection).add(o);
                continue;
            }
            List<Map<String, Object>> srch = findByFieldValue(db, collection, "_id", o.get("_id"));

            if (!srch.isEmpty()) {
                getCollection(db, collection).remove(srch.get(0));
                upd++;
                notifyWatchers(db, collection, "replace", o);
            } else {
                notifyWatchers(db, collection, "insert", o);
            }
            getCollection(db, collection).add(o);
        }
        ret.put("matched", upd);
        ret.put("updated", upd);
        return ret;
    }


    private Map<String, List<Map<String, Object>>> getDB(String db) {
        if (currentTransaction.get() == null) {

            database.putIfAbsent(db, new ConcurrentHashMap<>());
            return database.get(db);
        } else {
            //noinspection unchecked
            currentTransaction.get().getDatabase().putIfAbsent(db, new ConcurrentHashMap<>());
            //noinspection unchecked
            return (Map<String, List<Map<String, Object>>>) currentTransaction.get().getDatabase().get(db);
        }
    }

    @Override
    public void closeIteration(MorphiumCursor crs) {

    }

    @Override
    public Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> op, boolean multiple, boolean upsert, WriteConcern wc) throws MorphiumDriverException {
        List<Map<String, Object>> lst = find(db, collection, query, null, null, 0, multiple ? 0 : 1, true);
        boolean insert = false;
        if (lst == null) lst = new Vector<>();
        if (upsert && lst.isEmpty()) {
            lst.add(new HashMap<>());
            for (String k : query.keySet()) {
                if (k.startsWith("$")) continue;
                lst.get(0).put(k, query.get(k));
            }
            insert = true;
        }
        for (Map<String, Object> obj : lst) {
            for (String operand : op.keySet()) {
                @SuppressWarnings("unchecked") Map<String, Object> cmd = (Map<String, Object>) op.get(operand);
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
                                if (entry.getValue() instanceof Integer) {
                                    value = (Integer) value + ((Integer) entry.getValue());
                                } else if (entry.getValue() instanceof Float) {
                                    value = (Integer) value + ((Float) entry.getValue());
                                } else if (entry.getValue() instanceof Double) {
                                    value = (Integer) value + ((Double) entry.getValue());
                                } else if (entry.getValue() instanceof Long) {
                                    value = (Integer) value + ((Long) entry.getValue());
                                }
                            } else if (value instanceof Double) {
                                if (entry.getValue() instanceof Integer) {
                                    value = (Double) value + ((Integer) entry.getValue());
                                } else if (entry.getValue() instanceof Float) {
                                    value = (Double) value + ((Float) entry.getValue());
                                } else if (entry.getValue() instanceof Double) {
                                    value = (Double) value + ((Double) entry.getValue());
                                } else if (entry.getValue() instanceof Long) {
                                    value = (Double) value + ((Long) entry.getValue());
                                }
                            } else if (value instanceof Float) {
                                if (entry.getValue() instanceof Integer) {
                                    value = (Float) value + ((Integer) entry.getValue());
                                } else if (entry.getValue() instanceof Float) {
                                    value = (Float) value + ((Float) entry.getValue());
                                } else if (entry.getValue() instanceof Double) {
                                    value = (Float) value + ((Double) entry.getValue());
                                } else if (entry.getValue() instanceof Long) {
                                    value = (Float) value + ((Long) entry.getValue());
                                }
                            } else if (value instanceof Long) {
                                if (entry.getValue() instanceof Integer) {
                                    value = (Long) value + ((Integer) entry.getValue());
                                } else if (entry.getValue() instanceof Float) {
                                    value = (Long) value + ((Float) entry.getValue());
                                } else if (entry.getValue() instanceof Double) {
                                    value = (Long) value + ((Double) entry.getValue());
                                } else if (entry.getValue() instanceof Long) {
                                    value = (Long) value + ((Long) entry.getValue());
                                }

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
                            //noinspection unchecked
                            if (value.compareTo(entry.getValue()) > 0) {
                                obj.put(entry.getKey(), entry.getValue());
                            }
                        }
                        break;
                    case "$max":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            Comparable value = (Comparable) obj.get(entry.getKey());
                            //noinspection unchecked
                            if (value.compareTo(entry.getValue()) < 0) {
                                obj.put(entry.getKey(), entry.getValue());
                            }
                        }
                        break;
                    case "$push":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            List v = (List) obj.get(entry.getKey());
                            if (v == null) {
                                v = new Vector();
                                obj.put(entry.getKey(), v);
                            }
                            if (entry.getValue() instanceof Map) {
                                if (((Map) entry.getValue()).get("$each") != null) {
                                    //noinspection unchecked
                                    v.addAll((List) ((Map) entry.getValue()).get("$each"));
                                } else {
                                    //noinspection unchecked
                                    v.add(entry.getValue());
                                }
                            } else {
                                //noinspection unchecked
                                v.add(entry.getValue());
                            }
                        }
                        break;
                    default:
                        throw new RuntimeException("unknown operand " + operand);
                }
            }
            notifyWatchers(db, collection, "update", obj);
        }
        if (insert) {
            store(db, collection, lst, wc);

        }
        return new HashMap<>();
    }


    /**
     * {
     * _id : { <BSON Object> },
     * "operationType" : "<operation>",
     * "fullDocument" : { <document> },
     * "ns" : {
     * "db" : "<database>",
     * "coll" : "<collection"
     * },
     * "to" : {
     * "db" : "<database>",
     * "coll" : "<collection"
     * },
     * "documentKey" : { "_id" : <ObjectId> },
     * "updateDescription" : {
     * "updatedFields" : { <document> },
     * "removedFields" : [ "<field>", ... ]
     * }
     * "clusterTime" : <Timestamp>,
     * "txnNumber" : <NumberLong>,
     * "lsid" : {
     * "id" : <UUID>,
     * "uid" : <BinData>
     * }
     * }
     * <p>
     * The type of operation that occurred. Can be any of the following values:
     * <p>
     * insert
     * delete
     * replace
     * update
     * drop
     * rename
     * dropDatabase
     * invalidate
     */
    private void notifyWatchers(String db, String collection, String op, Map doc) {
        List<DriverTailableIterationCallback> w = null;
        if (watchersByDb.containsKey(db)) {
            w = new ArrayList<>(watchersByDb.get(db));
        } else if (collection != null && watchersByDb.containsKey(db + "." + collection)) {
            w = new ArrayList<>(watchersByDb.get(db + "." + collection));
        }
        if (w == null) {
            return;
        }

        long tx = txn.incrementAndGet();
        for (DriverTailableIterationCallback cb : w) {
            Map<String, Object> data = new HashMap<>();
            data.put("fullDocument", doc);
            data.put("operationType", op);
            Map m = Utils.getMap("db", db);
            //noinspection unchecked
            m.put("coll", collection);
            data.put("ns", m);
            data.put("txnNumber", tx);
            data.put("clusterTime", System.currentTimeMillis());
            if (doc != null) {
                data.put("documentKey", Utils.getMap("_id", doc.get("_id")));
            }

            try {
                cb.incomingData(data, System.currentTimeMillis());
            } catch (Exception e) {
                log.error("Error calling watcher", e);
            }
        }


    }

    @Override
    public Map<String, Object> delete(String db, String collection, Map<String, Object> query, boolean multiple, WriteConcern wc) throws MorphiumDriverException {
        List<Map<String, Object>> toDel = find(db, collection, query, null, null, 0, multiple ? 0 : 1, 10000, null, null);
        for (Map<String, Object> o : toDel) {
            getCollection(db, collection).remove(o);
            notifyWatchers(db, collection, "delete", o);
        }
        return new HashMap<>();
    }

    private List<Map<String, Object>> getCollection(String db, String collection) {
        getDB(db).putIfAbsent(collection, Collections.synchronizedList(new ArrayList<>()));
        return getDB(db).get(collection);
    }

    @Override
    public void drop(String db, String collection, WriteConcern wc) {
        getDB(db).remove(collection);
        notifyWatchers(db, collection, "drop", null);
    }

    @Override
    public void drop(String db, WriteConcern wc) {
        database.remove(db);
        notifyWatchers(db, null, "drop", null);
    }

    @Override
    public boolean exists(String db) {
        return database.containsKey(db);
    }

    @Override
    public List<Object> distinct(String db, String collection, String field, Map<String, Object> filter, ReadPreference rp) {
        List<Map<String, Object>> list = getDB(db).get(collection);
        Set<Object> distinctValues = new HashSet<>();

        if (list != null && !list.isEmpty()) {
            for (Map<String, Object> doc : list) {
                if (doc != null && !doc.isEmpty() && doc.get(field) != null) {
                    distinctValues.add(doc.get(field));
                }
            }
        }

        return new ArrayList<>(distinctValues);
    }

    @Override
    public boolean exists(String db, String collection) {
        return getDB(db) != null && getDB(db).containsKey(collection);
    }

    @Override
    public List<Map<String, Object>> getIndexes(String db, String collection) {
        return new Vector<>();
    }

    @Override
    public List<String> getCollectionNames(String db) {
        return null;
    }

    @Override
    public Map<String, Object> group(String db, String coll, Map<String, Object> query, Map<String, Object> initial, String jsReduce, String jsFinalize, ReadPreference rp, String... keys) {
        return null;
    }

    @Override
    public List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, ReadPreference readPreference) {
        throw new RuntimeException("Aggregate not possible in memory!");
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
    public void tailableIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, int timeout, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        throw new FunctionNotSupportedException("not possible in Mem yet");
    }

    @Override
    public int getMaxWaitTime() {
        return 0;
    }

    @Override
    public void setMaxWaitTime(int maxWaitTime) {

    }

    @Override
    public boolean isCapped(String db, String coll) {
        return false;
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return new BulkRequestContext(m) {
            private final List<BulkRequest> requests = new Vector<>();

            @Override
            public Map<String, Object> execute() {
                try {
                    for (BulkRequest r : requests) {
                        if (r instanceof InsertBulkRequest) {
                            insert(db, collection, ((InsertBulkRequest) r).getToInsert(), null);
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
            public InsertBulkRequest addInsertBulkRequest(List<Map<String, Object>> toInsert) {
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
    public void createIndex(String db, String collection, Map<String, Object> index, Map<String, Object> options) {

    }

    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing) throws MorphiumDriverException {
        throw new FunctionNotSupportedException("no map reduce in memory");
    }

    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query) throws MorphiumDriverException {
        throw new FunctionNotSupportedException("no map reduce in memory");
    }

    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query, Map<String, Object> sorting) throws MorphiumDriverException {
        throw new FunctionNotSupportedException("no map reduce in memory");
    }

    @Override
    public void startTransaction() {
        if (currentTransaction.get() != null) throw new IllegalArgumentException("transaction in progress");
        InMemTransactionContext ctx = new InMemTransactionContext();
        Cloner cloner = new Cloner();
        ctx.setDatabase(cloner.deepClone(database));
        currentTransaction.set(ctx);
    }


    @Override
    public void commitTransaction() {
        if (currentTransaction.get() == null) throw new IllegalArgumentException("No transaction in progress");
        InMemTransactionContext ctx = currentTransaction.get();
        //noinspection unchecked
        database.putAll(ctx.getDatabase());
        currentTransaction.set(null);
    }

    @Override
    public MorphiumTransactionContext getTransactionContext() {
        return currentTransaction.get();
    }

    @Override
    public void abortTransaction() {
        currentTransaction.set(null);
    }

    @Override
    public void setTransactionContext(MorphiumTransactionContext ctx) {
        currentTransaction.set((InMemTransactionContext) ctx);
    }

    private class InMemoryCursor {
        private int skip;
        private int limit;
        private int batchSize;
        private int dataRead = 0;

        private String db;
        private String collection;
        private Map<String, Object> query;
        private Map<String, Integer> sort;
        private Map<String, Object> projection;
        private ReadPreference readPreference;
        private Map<String, Object> findMetaData;

        public String getDb() {
            return db;
        }

        public void setDb(String db) {
            this.db = db;
        }

        public String getCollection() {
            return collection;
        }

        public void setCollection(String collection) {
            this.collection = collection;
        }

        public Map<String, Object> getQuery() {
            return query;
        }

        public void setQuery(Map<String, Object> query) {
            this.query = query;
        }

        public Map<String, Integer> getSort() {
            return sort;
        }

        public void setSort(Map<String, Integer> sort) {
            this.sort = sort;
        }

        public Map<String, Object> getProjection() {
            return projection;
        }

        public void setProjection(Map<String, Object> projection) {
            this.projection = projection;
        }

        public ReadPreference getReadPreference() {
            return readPreference;
        }

        public void setReadPreference(ReadPreference readPreference) {
            this.readPreference = readPreference;
        }

        public Map<String, Object> getFindMetaData() {
            return findMetaData;
        }

        public void setFindMetaData(Map<String, Object> findMetaData) {
            this.findMetaData = findMetaData;
        }

        public int getDataRead() {
            return dataRead;
        }

        public void setDataRead(int dataRead) {
            this.dataRead = dataRead;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getSkip() {
            return skip;
        }

        @SuppressWarnings("unused")
        public void setSkip(int skip) {
            this.skip = skip;
        }

        public int getLimit() {
            return limit;
        }

        public void setLimit(int limit) {
            this.limit = limit;
        }
    }
}