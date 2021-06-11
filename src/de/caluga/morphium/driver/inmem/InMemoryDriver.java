package de.caluga.morphium.driver.inmem;

import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionPoolListener;
import com.rits.cloning.Cloner;
import de.caluga.morphium.*;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.*;
import de.caluga.morphium.driver.mongodb.Maximums;
import de.caluga.morphium.mapping.MorphiumTypeMapper;
import org.bson.types.ObjectId;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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
    private ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
    private final List<Object> monitors = new CopyOnWriteArrayList<>();
    private List<Runnable> eventQueue = new CopyOnWriteArrayList<>();

    public Map<String, List<Map<String, Object>>> getDatabase(String dbn) {
        return database.get(dbn);
    }

    public void setDatabase(String dbn, Map<String, List<Map<String, Object>>> db) {
        if (db != null) database.put(dbn, db);
    }


    public void restore(InputStream in) throws IOException, ParseException {
        GZIPInputStream gzin = new GZIPInputStream(in);
        BufferedInputStream bin = new BufferedInputStream(gzin);
        BufferedReader br = new BufferedReader(new InputStreamReader(bin));
        String l = null;
        StringBuilder b = new StringBuilder();
        while ((l = br.readLine()) != null) {
            b.append(l);
        }
        br.close();
        ObjectMapperImpl mapper = new ObjectMapperImpl();

        MorphiumTypeMapper<ObjectId> typeMapper = getObjectIdTypeMapper();
        mapper.registerCustomMapperFor(ObjectId.class, typeMapper);
        log.info("Read in json: " + b);
        InMemDumpContainer cnt = mapper.deserialize(InMemDumpContainer.class, b.toString());
        log.info("Restoring DB " + cnt.getDb() + " dump from " + new Date(cnt.getCreated()));
        setDatabase(cnt.getDb(), cnt.getData());
    }

    public void restoreFromFile(File f) throws IOException, ParseException {
        restore(new FileInputStream(f));
    }

    public void dumpToFile(Morphium m, String db, File f) throws IOException {
        dump(m, db, new FileOutputStream(f));
    }

    public void dump(Morphium m, String db, OutputStream out) throws IOException {
        MorphiumObjectMapper mapper = m.getMapper();

        MorphiumTypeMapper<ObjectId> typeMapper = getObjectIdTypeMapper();
        mapper.registerCustomMapperFor(ObjectId.class, typeMapper);
        GZIPOutputStream gzip = new GZIPOutputStream(out);

        InMemDumpContainer d = new InMemDumpContainer();
        d.setCreated(System.currentTimeMillis());
        d.setData(getDatabase(db));
        d.setDb(db);

        Map<String, Object> ser = mapper.serialize(d);
        OutputStreamWriter wr = new OutputStreamWriter(gzip);
        Utils.writeJson(ser, wr);
        wr.flush();
        gzip.finish();
        gzip.flush();
        out.flush();
        gzip.close();

    }

    private MorphiumTypeMapper<ObjectId> getObjectIdTypeMapper() {
        return new MorphiumTypeMapper<ObjectId>() {
            @Override
            public Object marshall(ObjectId o) {
                Map<String, String> m = new ConcurrentHashMap<>();
                m.put("value", o.toHexString());
                m.put("class_name", o.getClass().getName());
                return m;

            }

            @Override
            public ObjectId unmarshall(Object d) {
                return new ObjectId(((Map) d).get("value").toString());
            }
        };
    }


    @Override
    public List<String> listDatabases() {
        return new CopyOnWriteArrayList<>(database.keySet());
    }

    @Override
    public List<String> listCollections(String db, String pattern) {

        Set<String> collections = database.get(db).keySet();
        List<String> ret = new CopyOnWriteArrayList<>();
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
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public void setMaxConnections(int maxConnections) {

    }

    @Override
    public int getMinConnections() {
        return 0;
    }

    @Override
    public void setMinConnections(int minConnections) {

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
    public void addCommandListener(CommandListener cmd) {

    }

    @Override
    public void removeCommandListener(CommandListener cmd) {

    }

    @Override
    public void addClusterListener(ClusterListener cl) {

    }

    @Override
    public void removeClusterListener(ClusterListener cl) {

    }

    @Override
    public void addConnectionPoolListener(ConnectionPoolListener cpl) {

    }

    @Override
    public void removeConnectionPoolListener(ConnectionPoolListener cpl) {

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
    public boolean isRetryReads() {
        return false;
    }

    @Override
    public void setRetryReads(boolean retryReads) {

    }

    @Override
    public boolean isRetryWrites() {
        return false;
    }

    @Override
    public void setRetryWrites(boolean retryWrites) {

    }

    @Override
    public String getUuidRepresentation() {
        return null;
    }

    @Override
    public void setUuidRepresentation(String uuidRepresentation) {
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
    public int getReadTimeout() {
        return 0;
    }

    @Override
    public void setReadTimeout(int readTimeout) {

    }

    @Override
    public int getLocalThreshold() {
        return 0;
    }

    @Override
    public void setLocalThreshold(int thr) {

    }

    @Override
    public void heartBeatFrequency(int t) {

    }

    @Override
    public void useSsl(boolean ssl) {

    }

    @Override
    public void connect() {
        if (exec.isShutdown()) {
            exec = new ScheduledThreadPoolExecutor(1);
        }
        Runnable r = () -> {

            List<Runnable> current = eventQueue;
            eventQueue = new CopyOnWriteArrayList<>();
            Collections.shuffle(current);
            for (Runnable r1 : current) {
                try {
                    r1.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        };
        exec.scheduleWithFixedDelay(r, 100, 500, TimeUnit.MILLISECONDS); //check for events every 100ms
    }

    @Override
    public void setDefaultReadPreference(ReadPreference rp) {

    }

    @Override
    public void connect(String replicasetName) {
        connect();
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
        exec.shutdownNow();
        for (Object m : monitors) {
            synchronized (m) {
                m.notifyAll();
            }
        }
        database.clear();
    }

    @Override
    public Map<String, Object> getReplsetStatus() {
        return new ConcurrentHashMap<>();
    }

    @Override
    public Map<String, Object> getDBStats(String db) {
        Map<String, Object> ret = new ConcurrentHashMap<>();
        ret.put("collections", getDB(db).size());
        return ret;
    }

    @Override
    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        Map<String, Object> ret = new ConcurrentHashMap<>();
        ret.put("entries", getDB(db).get(coll).size());
        return null;
    }

    @Override
    public Map<String, Object> getOps(long threshold) {
        log.warn("getOpts not working on memory");
        return new ConcurrentHashMap<>();
    }

    @Override
    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) {
        log.warn("Runcommand not working on memory");
        return new ConcurrentHashMap<>();
    }

    @Override
    public MorphiumCursor initAggregationIteration(String db, String collection, List<Map<String, Object>> aggregationPipeline, ReadPreference readPreference, Collation collation, int batchSize, Map<String, Object> findMetaData) throws MorphiumDriverException {
        log.warn("aggregation not possible in mem");
        return new MorphiumCursor();
    }


    @Override
    public MorphiumCursor initIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, Collation coll, Map<String, Object> findMetaData) throws MorphiumDriverException {
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
        List<Map<String, Object>> res = find(db, collection, query, sort, projection, skip, l, batchSize, readPreference, coll, findMetaData);
        crs.setBatch(Collections.synchronizedList(new CopyOnWriteArrayList<>(res)));

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
    public void watch(String db, int timeout, boolean fullDocumentOnUpdate, List<Map<String, Object>> pipeline, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        watch(db, null, timeout, fullDocumentOnUpdate, pipeline, cb);
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public void watch(String db, String collection, int timeout, boolean fullDocumentOnUpdate, List<Map<String, Object>> pipeline, DriverTailableIterationCallback cb) throws MorphiumDriverException {

        Object monitor = new Object();
        monitors.add(monitor);

        DriverTailableIterationCallback cback = new DriverTailableIterationCallback() {
            @Override
            public void incomingData(Map<String, Object> data, long dur) {
                cb.incomingData(data, dur);
                if (!cb.isContinued()) {
                    synchronized (monitor) {
                        monitor.notifyAll();
                    }
                }
            }

            @Override
            public boolean isContinued() {
                return cb.isContinued();
            }
        };
        if (collection != null) {
            String key = db + "." + collection;
            watchersByDb.putIfAbsent(key, new CopyOnWriteArrayList<>());
            watchersByDb.get(key).add(cback);
        } else {
            watchersByDb.putIfAbsent(db, new CopyOnWriteArrayList<>());
            watchersByDb.get(db).add(cback);
        }


        //simulate blocking
        try {
            synchronized (monitor) {
                monitor.wait();
                monitors.remove(monitor);
            }
        } catch (InterruptedException e) {
        }

        watchersByDb.remove(db);
        log.debug("Exiting");
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
        inCrs.setCollation(oldCrs.getCollation());

        inCrs.setLimit(oldCrs.getLimit());

        inCrs.setSort(oldCrs.getSort());
        inCrs.skip = oldCrs.getDataRead() + 1;
        int limit = oldCrs.getBatchSize();
        if (oldCrs.getLimit() != 0) {
            if (oldCrs.getDataRead() + oldCrs.getBatchSize() > oldCrs.getLimit()) {
                limit = oldCrs.getLimit() - oldCrs.getDataRead();
            }
        }
        List<Map<String, Object>> res = find(inCrs.getDb(), inCrs.getCollection(), inCrs.getQuery(), inCrs.getSort(), inCrs.getProjection(), inCrs.getSkip(), limit, inCrs.getBatchSize(), inCrs.getReadPreference(), inCrs.getCollation(), inCrs.getFindMetaData());
        next.setBatch(Collections.synchronizedList(new CopyOnWriteArrayList<>(res)));
        if (res.size() < inCrs.getBatchSize() || (oldCrs.limit != 0 && res.size() + oldCrs.getDataRead() > oldCrs.limit)) {
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
    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference rp, Collation col, Map<String, Object> findMetaData) throws MorphiumDriverException {
        return find(db, collection, query, sort, projection, skip, limit, false);
    }


    @SuppressWarnings({"RedundantThrows", "UnusedParameters"})
    private List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, boolean internal) throws MorphiumDriverException {
        List<Map<String, Object>> data = new CopyOnWriteArrayList<>(getCollection(db, collection));
        List<Map<String, Object>> ret = new CopyOnWriteArrayList<>();
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
            if (QueryHelper.matchesQuery(query, o)) {
                if (o == null) o = new HashMap<>();
                synchronized (this) {
                    while (true) {
                        try {
                            ret.add(internal ? o : new HashMap<>(o));
                            break;
                        } catch (ConcurrentModificationException e) {
                            try {
                                Thread.sleep(5);
                            } catch (InterruptedException interruptedException) {
                                //ignore
                            }
                        }
                    }
                }
            }
            if (limit > 0 && ret.size() >= limit) {
                break;
            }

            //todo add projection
        }

        return Collections.synchronizedList(new CopyOnWriteArrayList<>(ret));
    }

    @Override
    public long count(String db, String collection, Map<String, Object> query, Collation collation, ReadPreference rp) {
        List<Map<String, Object>> d = getCollection(db, collection);
        List<Map<String, Object>> data = new CopyOnWriteArrayList<>(d);
        if (query.isEmpty()) {
            return data.size();
        }
        long cnt = 0;

        for (Map<String, Object> o : data) {
            if (QueryHelper.matchesQuery(query, o)) {
                cnt++;
            }
        }
        return cnt;
    }

    @Override
    public long estimatedDocumentCount(String db, String collection, ReadPreference rp) {
        return getCollection(db, collection).size();
    }

    public List<Map<String, Object>> findByFieldValue(String db, String coll, String field, Object value) {
        List<Map<String, Object>> ret = new CopyOnWriteArrayList<>();

        List<Map<String, Object>> data = new CopyOnWriteArrayList<>(getCollection(db, coll));
        for (Map<String, Object> obj : data) {
            if (obj.get(field) == null && value != null) {
                continue;
            }
            if ((obj.get(field) == null && value == null)
                    || obj.get(field).equals(value)) {
                ret.add(new ConcurrentHashMap<>(obj));
            }
        }
        return Collections.synchronizedList(new CopyOnWriteArrayList<>(ret));
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
    public Map<String, Integer> store(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) {
        Map<String, Integer> ret = new ConcurrentHashMap<>();
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
    public Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> op, boolean multiple, boolean upsert, Collation collation, WriteConcern wc) throws MorphiumDriverException {
        List<Map<String, Object>> lst = find(db, collection, query, null, null, 0, multiple ? 0 : 1, true);
        boolean insert = false;
        if (lst == null) lst = new CopyOnWriteArrayList<>();
        if (upsert && lst.isEmpty()) {
            lst.add(new ConcurrentHashMap<>());
            for (String k : query.keySet()) {
                if (k.startsWith("$")) continue;
                if (query.get(k) != null)
                    lst.get(0).put(k, query.get(k));
                else
                    lst.get(0).remove(k);
            }
            insert = true;
        }
        for (Map<String, Object> obj : lst) {
            for (String operand : op.keySet()) {
                @SuppressWarnings("unchecked") Map<String, Object> cmd = (Map<String, Object>) op.get(operand);
                switch (operand) {
                    case "$set":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            if (entry.getValue() != null)
                                obj.put(entry.getKey(), entry.getValue());
                            else
                                obj.remove(entry.getKey());
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
                            if (value != null)
                                obj.put(entry.getKey(), value);
                            else
                                obj.remove(entry.getKey());
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
                            if (value != null)
                                obj.put(entry.getKey(), value);
                            else
                                obj.remove(entry.getKey());
                        }
                        break;
                    case "$rename":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            if (obj.get(entry.getKey()) != null)
                                obj.put((String) entry.getValue(), obj.get(entry.getKey()));
                            else
                                obj.remove(entry.getValue());
                            obj.remove(entry.getKey());
                        }
                        break;
                    case "$min":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            Comparable value = (Comparable) obj.get(entry.getKey());
                            //noinspection unchecked
                            if (value.compareTo(entry.getValue()) > 0 && entry.getValue() != null) {
                                obj.put(entry.getKey(), entry.getValue());
                            }
                        }
                        break;
                    case "$max":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            Comparable value = (Comparable) obj.get(entry.getKey());
                            //noinspection unchecked
                            if (value.compareTo(entry.getValue()) < 0 && entry.getValue() != null) {
                                obj.put(entry.getKey(), entry.getValue());
                            }
                        }
                        break;
                    case "$push":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            List v = (List) obj.get(entry.getKey());
                            if (v == null) {
                                v = new CopyOnWriteArrayList();
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
        return new ConcurrentHashMap<>();
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
        Runnable r = () -> {
            List<DriverTailableIterationCallback> w = null;
            if (watchersByDb.containsKey(db)) {
                w = Collections.synchronizedList(new CopyOnWriteArrayList<>(watchersByDb.get(db)));
            } else if (collection != null && watchersByDb.containsKey(db + "." + collection)) {
                w = Collections.synchronizedList(new CopyOnWriteArrayList<>(watchersByDb.get(db + "." + collection)));
            }
            if (w == null || w.isEmpty()) {
                return;
            }

            long tx = txn.incrementAndGet();
            Collections.shuffle(w);
            for (DriverTailableIterationCallback cb : w) {
                Map<String, Object> data = new ConcurrentHashMap<>();
                if (doc != null)
                    data.put("fullDocument", doc);
                if (op != null)
                    data.put("operationType", op);
                Map m = Collections.synchronizedMap(Utils.getMap("db", db));
                //noinspection unchecked
                m.put("coll", collection);
                data.put("ns", m);
                data.put("txnNumber", tx);
                data.put("clusterTime", System.currentTimeMillis());
                if (doc != null) {
                    if (doc.get("_id") != null)
                        data.put("documentKey", doc.get("_id"));
                }

                try {
                    cb.incomingData(data, System.currentTimeMillis());
                } catch (Exception e) {
                    log.error("Error calling watcher", e);
                }
            }

        };

        eventQueue.add(r);


    }

    @Override
    public Map<String, Object> delete(String db, String collection, Map<String, Object> query, boolean multiple, Collation collation, WriteConcern wc) throws MorphiumDriverException {
        List<Map<String, Object>> toDel = find(db, collection, query, null, null, 0, multiple ? 0 : 1, 10000, null, collation, null);
        for (Map<String, Object> o : toDel) {
            getCollection(db, collection).remove(o);
            notifyWatchers(db, collection, "delete", o);
        }
        return new ConcurrentHashMap<>();
    }

    private List<Map<String, Object>> getCollection(String db, String collection) {
        getDB(db).putIfAbsent(collection, Collections.synchronizedList(new CopyOnWriteArrayList<>()));
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
    public List<Object> distinct(String db, String collection, String field, Map<String, Object> filter, Collation collation, ReadPreference rp) {
        List<Map<String, Object>> list = getDB(db).get(collection);
        Set<Object> distinctValues = new HashSet<>();

        if (list != null && !list.isEmpty()) {
            for (Map<String, Object> doc : list) {
                if (doc != null && !doc.isEmpty() && doc.get(field) != null) {
                    distinctValues.add(doc.get(field));
                }
            }
        }

        return Collections.synchronizedList(new CopyOnWriteArrayList<>(distinctValues));
    }

    @Override
    public boolean exists(String db, String collection) {
        return getDB(db) != null && getDB(db).containsKey(collection);
    }

    @Override
    public List<Map<String, Object>> getIndexes(String db, String collection) {
        return new CopyOnWriteArrayList<>();
    }

    @Override
    public List<String> getCollectionNames(String db) {
        return null;
    }

    @Override
    public Map<String, Object> findAndOneAndDelete(String db, String col, Map<String, Object> query, Map<String, Integer> sort, Collation collation) throws MorphiumDriverException {
        List<Map<String, Object>> r = find(db, col, query, sort, null, 0, 1, 1000, null, collation, null);
        if (r.size() == 0) {
            return null;
        }
        delete(db, col, Utils.getMap("_id", r.get(0).get("_id")), false, collation, null);
        return r.get(0);
    }

    @Override
    public Map<String, Object> findAndOneAndUpdate(String db, String col, Map<String, Object> query, Map<String, Object> update, Map<String, Integer> sort, Collation collation) throws MorphiumDriverException {
        List<Map<String, Object>> ret = find(db, col, query, sort, null, 0, 1, 1, null, collation, new ConcurrentHashMap<>());
        update(db, col, query, update, false, false, collation, null);
        return ret.get(0);
    }

    @Override
    public Map<String, Object> findAndOneAndReplace(String db, String col, Map<String, Object> query, Map<String, Object> replacement, Map<String, Integer> sort, Collation collation) throws MorphiumDriverException {
        List<Map<String, Object>> ret = find(db, col, query, sort, null, 0, 1, 1, null, collation, new ConcurrentHashMap<>());
        if (ret.get(0).get("_id") != null)
            replacement.put("_id", ret.get(0).get("_id"));
        else
            replacement.remove("_id");
        store(db, col, Arrays.asList(replacement), null);
        return replacement;
    }


    @Override
    public List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, Collation collation, ReadPreference readPreference) throws MorphiumDriverException {
        log.warn("Aggregate not possible yet in memory!");
        return new CopyOnWriteArrayList<>();
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
    public int getServerSelectionTimeout() {
        return 0;
    }

    @Override
    public void setServerSelectionTimeout(int serverSelectionTimeout) {
    }

    @Override
    public boolean isCapped(String db, String coll) {
        return false;
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return new BulkRequestContext(m) {
            private final List<BulkRequest> requests = new CopyOnWriteArrayList<>();

            @Override
            public Map<String, Object> execute() {
                try {
                    for (BulkRequest r : requests) {
                        if (r instanceof InsertBulkRequest) {
                            insert(db, collection, ((InsertBulkRequest) r).getToInsert(), null);
                        } else if (r instanceof UpdateBulkRequest) {
                            UpdateBulkRequest up = (UpdateBulkRequest) r;
                            update(db, collection, up.getQuery(), up.getCmd(), up.isMultiple(), up.isUpsert(), null, null);
                        } else if (r instanceof DeleteBulkRequest) {
                            delete(db, collection, ((DeleteBulkRequest) r).getQuery(), ((DeleteBulkRequest) r).isMultiple(), null, null);
                        } else {
                            throw new RuntimeException("Unknown operation " + r.getClass().getName());
                        }
                    }
                } catch (MorphiumDriverException e) {
                    log.error("Got exception: ", e);
                }
                return new ConcurrentHashMap<>();

            }

            @Override
            public UpdateBulkRequest addUpdateBulkRequest() {
                UpdateBulkRequest up = new UpdateBulkRequest();
                requests.add(up);
                return up;
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
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query, Map<String, Object> sorting, Collation collation) throws MorphiumDriverException {
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

    // creates a zipped json stream containing all data.
    public void writeDump(File f) {

    }

    private static class InMemoryCursor {
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
        private Collation collation;

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

        public Collation getCollation() {
            return collation;
        }

        public void setCollation(Collation collation) {
            this.collation = collation;
        }
    }

    @Override
    public SSLContext getSslContext() {
        return null;
    }

    @Override
    public void setSslContext(SSLContext sslContext) {

    }

    @Override
    public boolean isSslInvalidHostNameAllowed() {
        return false;
    }

    @Override
    public void setSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {

    }
}