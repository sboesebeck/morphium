package de.caluga.morphium.driver.inmem;

import com.rits.cloning.Cloner;
import de.caluga.morphium.*;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.*;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.commands.result.CursorResult;
import de.caluga.morphium.driver.commands.result.RunCommandResult;
import de.caluga.morphium.driver.commands.result.SingleElementResult;
import de.caluga.morphium.objectmapping.MorphiumTypeMapper;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.objectmapping.ObjectMapperImpl;
import org.bson.types.ObjectId;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
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
@SuppressWarnings({"WeakerAccess", "EmptyMethod", "BusyWait"})
public class InMemoryDriver implements MorphiumDriver {
    private final Logger log = LoggerFactory.getLogger(InMemoryDriver.class);
    // DBName => Collection => List of documents
    private final Map<String, Map<String, List<Map<String, Object>>>> database = new ConcurrentHashMap<>();

    /**
     * index definitions by db and collection name
     * DB -> Collection -> List of Map Index defintion (field -> 1/-1/hashed)
     */
    private final Map<String, Map<String, List<Map<String, Object>>>> indicesByDbCollection = new ConcurrentHashMap<>();

    /**
     * Map DB->Collection->FieldNames->Keys....
     */
//    private final Map<String, Map<String, Map<String, Map<IndexKey, List<Map<String,Object>>>>>> indexDataByDBCollection = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Map<String, Map<Integer, List<Map<String, Object>>>>>> indexDataByDBCollection = new ConcurrentHashMap<>();
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

        Doc ser = Doc.of(mapper.serialize(d));
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

            public Object marshall(ObjectId o) {
                Map<String, String> m = new ConcurrentHashMap<>();
                m.put("value", o.toHexString());
                m.put("class_name", o.getClass().getName());
                return m;

            }


            public ObjectId unmarshall(Object d) {
                return new ObjectId(((Map) d).get("value").toString());
            }
        };
    }


    public String getAtlasUrl() {
        return null;
    }


    public void setAtlasUrl(String atlasUrl) {
        if (atlasUrl != null && !atlasUrl.isEmpty())
            log.warn("InMemoryDriver does not work with atlas - ignoring URL!");
    }


    public List<String> listDatabases() {
        return new ArrayList<>(database.keySet());
    }

    @Override
    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        return null;
    }


    public List<String> listCollections(String db, String pattern) {

        Set<String> collections = database.get(db).keySet();
        List<String> ret = new ArrayList<>();
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

    @Override
    public String getReplicaSetName() {
        return null;
    }

    @Override
    public void setReplicaSetName(String replicaSetName) {

    }

    @Override
    public Map<String, String[]> getCredentials() {
        return null;
    }

    @Override
    public void setCredentials(Map<String, String[]> credentials) {

    }

    @Override
    public void setCredentialsFor(String db, String user, String password) {

    }

    public void resetData() {
        database.clear();
        currentTransaction.remove();
    }


    public void setCredentials(String db, String login, char[] pwd) {

    }



    @Override
    public void watch(WatchSettings settings) {

    }

    @Override
    public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
        return null;
    }

    @Override
    public List<Map<String, Object>> readAnswerFor(int queryId) throws MorphiumDriverException {
        return null;
    }

    @Override
    public MorphiumCursor getAnswerFor(int queryId) throws MorphiumDriverException {
        return null;
    }

    @Override
    public List<Map<String, Object>> readAnswerFor(MorphiumCursor crs) throws MorphiumDriverException {
        return null;
    }


    @Override
    public String getName() {
        return null;
    }

    @Override
    public int getMaxBsonObjectSize() {
        return 0;
    }

    @Override
    public void setMaxBsonObjectSize(int maxBsonObjectSize) {

    }

    @Override
    public int getMaxMessageSize() {
        return 0;
    }

    @Override
    public void setMaxMessageSize(int maxMessageSize) {

    }

    @Override
    public int getMaxWriteBatchSize() {
        return 0;
    }

    @Override
    public void setMaxWriteBatchSize(int maxWriteBatchSize) {

    }

    @Override
    public boolean isReplicaSet() {
        return false;
    }

    @Override
    public void setReplicaSet(boolean replicaSet) {

    }

    @Override
    public boolean getDefaultJ() {
        return false;
    }

    @Override
    public int getDefaultWriteTimeout() {
        return 0;
    }

    @Override
    public void setDefaultWriteTimeout(int wt) {

    }


    @Override
    public void setHostSeed(String... host) {

    }

    @Override
    public void setConnectionUrl(String connectionUrl) {

    }

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


    public void connect(String replicasetName) {
        connect();
    }


    public boolean isConnected() {
        return true;
    }


    public int getRetriesOnNetworkError() {
        return 0;
    }


    public MorphiumDriver setRetriesOnNetworkError(int r) {
        return this;
    }


    public int getSleepBetweenErrorRetries() {
        return 0;
    }


    public MorphiumDriver setSleepBetweenErrorRetries(int s) {
        return this;

    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public MorphiumDriver setMaxConnections(int maxConnections) {
        return this;

    }

    @Override
    public int getMinConnections() {
        return 0;
    }

    @Override
    public MorphiumDriver setMinConnections(int minConnections) {
        return this;

    }

    @Override
    public boolean isRetryReads() {
        return false;
    }

    @Override
    public MorphiumDriver setRetryReads(boolean retryReads) {
        return this;

    }

    @Override
    public boolean isRetryWrites() {
        return false;
    }

    @Override
    public MorphiumDriver setRetryWrites(boolean retryWrites) {
        return this;

    }

    @Override
    public int getReadTimeout() {
        return 0;
    }

    @Override
    public void setReadTimeout(int readTimeout) {

    }

    @Override
    public int getMinConnectionsPerHost() {
        return 0;
    }

    @Override
    public void setMinConnectionsPerHost(int minConnectionsPerHost) {

    }

    @Override
    public int getMaxConnectionsPerHost() {
        return 0;
    }

    @Override
    public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {

    }

    @Override
    public MorphiumDriver setCredentials(String db, String login, String pwd) {
        return this;

    }

    @Override
    public MorphiumTransactionContext startTransaction(boolean autoCommit) {
        return null;
    }

    @Override
    public boolean isTransactionInProgress() {
        return false;
    }


    public void disconnect() {
        exec.shutdownNow();
        for (Object m : monitors) {
            synchronized (m) {
                m.notifyAll();
            }
        }
        database.clear();
    }

    @Override
    public boolean isReplicaset() {
        return false;
    }
//

    public Map<String, Object> getReplsetStatus() {
        return new ConcurrentHashMap<>();
    }

    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        return null;
    }

//    
//    public Doc getDBStats(String db) {
//        Doc ret = new ConcurrentHashMap<>();
//        ret.put("collections", getDB(db).size());
//        return ret;
//    }
//
//    @SuppressWarnings("RedundantThrows")
//    
//    public Doc getCollStats(String db, String coll) throws MorphiumDriverException {
//        Doc ret = new ConcurrentHashMap<>();
//        ret.put("entries", getDB(db).get(coll).size());
//        return null;
//    }


    public Doc getOps(long threshold) {
        log.warn("getOpts not working on memory");
        return new Doc();
    }


    public Doc runCommand(String db, Doc cmd) {
        log.warn("Runcommand not working on memory");
        return new Doc();
    }

    @SuppressWarnings("RedundantThrows")

    public MorphiumCursor initAggregationIteration(String db, String collection, List<Map<String, Object>> aggregationPipeline, ReadPreference readPreference, Collation collation, int batchSize, Doc findMetaData) throws MorphiumDriverException {
        log.warn("aggregation not possible in mem");
        return new MorphiumCursor() {
            @Override
            public Iterator<Map<String, Object>> iterator() {
                return this;
            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Map<String, Object> next() {
                return null;
            }

            @Override
            public void close() {

            }

            @Override
            public int available() {
                return 0;
            }

            @Override
            public List<Map<String, Object>> getAll() throws MorphiumDriverException {
                return null;
            }

            @Override
            public void ahead(int skip) throws MorphiumDriverException {

            }

            @Override
            public void back(int jump) throws MorphiumDriverException {

            }

            @Override
            public int getCursor() {
                return 0;
            }
        };
    }


    public MorphiumCursor initIteration(String db, String collection, Doc query, Map<String, Integer> sort, Doc projection, int skip, int limit, int batchSize, ReadPreference readPreference, Collation coll, Doc findMetaData) throws MorphiumDriverException {
        MorphiumCursor crs = new MorphiumCursor() {
            @Override
            public Iterator<Map<String, Object>> iterator() {
                return this;
            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Map<String, Object> next() {
                return null;
            }

            @Override
            public void close() {

            }

            @Override
            public int available() {
                return 0;
            }

            @Override
            public List<Map<String, Object>> getAll() throws MorphiumDriverException {
                return null;
            }

            @Override
            public void ahead(int skip) throws MorphiumDriverException {

            }

            @Override
            public void back(int jump) throws MorphiumDriverException {

            }

            @Override
            public int getCursor() {
                return 0;
            }
        };
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
//        crs.setInternalCursorObject(inCrs);
//        int l = batchSize;
//        if (limit != 0 && limit < batchSize) {
//            l = limit;
//        }
//        List<Map<String,Object>> res = find(db, collection, query, sort, projection, skip, l, batchSize, readPreference, coll, findMetaData);
//        crs.setBatch(new CopyOnWriteArrayList<>(res));
//
//        if (res.size() < batchSize) {
//            //noinspection unchecked
//            crs.setInternalCursorObject(null); //cursor ended - no more data
//        } else {
//            inCrs.dataRead = res.size();
//        }
        return crs;
    }

    @SuppressWarnings("RedundantThrows")

    public void watch(String db, int timeout, boolean fullDocumentOnUpdate, List<Map<String, Object>> pipeline, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        watch(db, null, timeout, fullDocumentOnUpdate, pipeline, cb);
    }

    @SuppressWarnings({"RedundantThrows", "CatchMayIgnoreException"})

    public void watch(String db, String collection, int timeout, boolean fullDocumentOnUpdate, List<Map<String, Object>> pipeline, DriverTailableIterationCallback cb) throws MorphiumDriverException {

        Object monitor = new Object();
        monitors.add(monitor);

        DriverTailableIterationCallback cback = new DriverTailableIterationCallback() {

            public void incomingData(Map<String, Object> data, long dur) {
                cb.incomingData(data, dur);
                if (!cb.isContinued()) {
                    synchronized (monitor) {
                        monitor.notifyAll();
                    }
                }
            }


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


    public MorphiumCursor nextIteration(MorphiumCursor crs) throws MorphiumDriverException {
        MorphiumCursor next = new MorphiumCursor() {
            @Override
            public Iterator<Map<String, Object>> iterator() {
                return this;
            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Map<String, Object> next() {
                return null;
            }

            @Override
            public void close() {

            }

            @Override
            public int available() {
                return 0;
            }

            @Override
            public List<Map<String, Object>> getAll() throws MorphiumDriverException {
                return null;
            }

            @Override
            public void ahead(int skip) throws MorphiumDriverException {

            }

            @Override
            public void back(int jump) throws MorphiumDriverException {

            }

            @Override
            public int getCursor() {
                return 0;
            }
        };
        next.setCursorId(crs.getCursorId());

//        InMemoryCursor oldCrs = (InMemoryCursor) crs.getInternalCursorObject();
//        if (oldCrs == null) {
//            return null;
//        }
//
//        InMemoryCursor inCrs = new InMemoryCursor();
//        inCrs.setReadPreference(oldCrs.getReadPreference());
//        inCrs.setFindMetaData(oldCrs.getFindMetaData());
//        inCrs.setDb(oldCrs.getDb());
//        inCrs.setQuery(oldCrs.getQuery());
//        inCrs.setCollection(oldCrs.getCollection());
//        inCrs.setProjection(oldCrs.getProjection());
//        inCrs.setBatchSize(oldCrs.getBatchSize());
//        inCrs.setCollation(oldCrs.getCollation());
//
//        inCrs.setLimit(oldCrs.getLimit());
//
//        inCrs.setSort(oldCrs.getSort());
//        inCrs.skip = oldCrs.getDataRead() + 1;
//        int limit = oldCrs.getBatchSize();
//        if (oldCrs.getLimit() != 0) {
//            if (oldCrs.getDataRead() + oldCrs.getBatchSize() > oldCrs.getLimit()) {
//                limit = oldCrs.getLimit() - oldCrs.getDataRead();
//            }
//        }
//        List<Map<String,Object>> res = find(inCrs.getDb(), inCrs.getCollection(), inCrs.getQuery(), inCrs.getSort(), inCrs.getProjection(), inCrs.getSkip(), limit, inCrs.getBatchSize(), inCrs.getReadPreference(), inCrs.getCollation(), inCrs.getFindMetaData());
//        next.setBatch(new CopyOnWriteArrayList<>(res));
//        if (res.size() < inCrs.getBatchSize() || (oldCrs.limit != 0 && res.size() + oldCrs.getDataRead() > oldCrs.limit)) {
//            //finished!
//            //noinspection unchecked
//            next.setInternalCursorObject(null);
//        } else {
//            inCrs.setDataRead(oldCrs.getDataRead() + res.size());
//            //noinspection unchecked
//            next.setInternalCursorObject(inCrs);
//        }
        return next;
    }


    public List<Map<String, Object>> find(String db, String collection, Doc query, Map<String, Integer> sort, Doc projection, int skip, int limit, int batchSize, ReadPreference rp, Collation col, Doc findMetaData) throws MorphiumDriverException {
        return find(db, collection, query, sort, projection, skip, limit, false);
    }


    @SuppressWarnings({"RedundantThrows", "UnusedParameters"})
    private List<Map<String, Object>> find(String db, String collection, Doc query, Map<String, Integer> sort, Doc projection, int skip, int limit, boolean internal) throws MorphiumDriverException {
        List<Map<String, Object>> partialHitData = new ArrayList<>();
        if (query.containsKey("$and")) {
            //and complex query handling ?!?!?
            List<Map<String, Object>> m = (List<Map<String, Object>>) query.get("$and");

            if (m != null && !m.isEmpty()) {
                for (Map<String, Object> subquery : m) {
                    List<Map<String, Object>> dataFromIndex = getDataFromIndex(db, collection, subquery);
                    //one and-query result is enough to find candidates!
                    if (dataFromIndex != null) {
                        partialHitData = dataFromIndex;
                        break;
                    }
                }
            }

        } else if (query.containsKey("$or")) {
            List<Map<String, Object>> m = (List<Map<String, Object>>) query.get("$or");

            if (m != null) {
                for (Map<String, Object> subquery : m) {
                    List<Map<String, Object>> dataFromIndex = getDataFromIndex(db, collection, subquery);
                    if (dataFromIndex != null) {
                        partialHitData.addAll(dataFromIndex);
                    }
                }

            }
        } else {

            partialHitData = getDataFromIndex(db, collection, query);
        }

        List<Map<String, Object>> data;

        if (partialHitData == null || partialHitData.isEmpty()) {
            data = new ArrayList<>(getCollection(db, collection));
        } else {
            data = partialHitData;
        }
        List<Map<String, Object>> ret = new ArrayList<>();
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
            if (!internal) {
                while (true) {
                    try {
                        o = Doc.of(o);
                        if (o.get("_id") instanceof ObjectId) {
                            o.put("_id", new MorphiumId((ObjectId) o.get("_id")));
                        }
                        break;
                    } catch (ConcurrentModificationException c) {
                        //retry until it works
                    }

                }
            }
            if (QueryHelper.matchesQuery(query, o)) {
                if (o == null) o = new Doc();

                ret.add(o);
            }
            if (limit > 0 && ret.size() >= limit) {
                break;
            }

            //todo add projection
        }

        return new ArrayList<>(ret);
    }

    private List<Map<String, Object>> getDataFromIndex(String db, String collection, Map<String, Object> query) {
        List<Map<String, Object>> ret = null;
        int bucketId = 0;
        StringBuilder fieldList = new StringBuilder();
        for (Map<String, Object> idx : getIndexes(db, collection)) {
            if (idx.size() > query.size()) continue; //index has more fields

            boolean found = true;
//            values.clear();
            bucketId = 0;
            fieldList.setLength(0);
            for (String k : query.keySet()) {
                if (!idx.containsKey(k)) {
                    found = false;
                    break;
                }
                Object value = query.get(k);
                bucketId = iterateBucketId(bucketId, value);
                fieldList.append(k);
            }
            if (found) {
                //all keys in this index are found in query
                //log.debug("Index hit");
                String fields = fieldList.toString();
                Map<Integer, List<Map<String, Object>>> indexDataForCollection = getIndexDataForCollection(db, collection, fields);
                ret = indexDataForCollection.get(bucketId);
                if (ret == null || ret.size() == 0) {
                    //no direkt hit, need to use index data
                    //
//                    log.debug("indirect Index hit - fields: "+fields+" Index size: "+indexDataForCollection.size());
                    ret = new ArrayList<>();
////                    long start = System.currentTimeMillis();
                    for (Map.Entry<Integer, List<Map<String, Object>>> k : indexDataForCollection.entrySet()) {

                        for (Map<String, Object> o : k.getValue()) {
                            if (QueryHelper.matchesQuery(query, o)) {
                                ret.add(o);
                            }
                        }
                        //ret.addAll(indexDataForCollection.get(k.getKey()));
                    }
////                    long dur = System.currentTimeMillis() - start;
////                    log.info("Duration index assembling: " + dur + "ms - "+ret.size());
                    if (ret.size() == 0) {
//                        //log.error("Index miss although index exists");
                        ret = null;
                    }
                }
                break;
            }

        }
        return ret;
    }


    public long count(String db, String collection, Doc query, Collation collation, ReadPreference rp) {
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


    public long estimatedDocumentCount(String db, String collection, ReadPreference rp) {
        return getCollection(db, collection).size();
    }

    public List<Map<String, Object>> findByFieldValue(String db, String coll, String field, Object value) {
        List<Map<String, Object>> ret = new ArrayList<>();

        List<Map<String, Object>> data = new CopyOnWriteArrayList<>(getCollection(db, coll));
        for (Map<String, Object> obj : data) {
            if (obj.get(field) == null && value != null) {
                continue;
            }
            if ((obj.get(field) == null && value == null)
                    || obj.get(field).equals(value)) {
                Doc add = new Doc(obj);
                if (add.get("_id") instanceof ObjectId) {
                    add.put("_id", new MorphiumId((ObjectId) add.get("_id")));
                }
                ret.add(add);
            }
        }
        return ret;
    }

    //    public Map<IndexKey, List<Map<String,Object>>> getIndexDataForCollection(String db, String collection, String fields) {
    public Map<Integer, List<Map<String, Object>>> getIndexDataForCollection(String db, String collection, String fields) {
        indexDataByDBCollection.putIfAbsent(db, new ConcurrentHashMap<>());
        indexDataByDBCollection.get(db).putIfAbsent(collection, new ConcurrentHashMap<>());
        indexDataByDBCollection.get(db).get(collection).putIfAbsent(fields, new ConcurrentHashMap<>());
        return indexDataByDBCollection.get(db).get(collection).get(fields);
    }


    public void insert(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        for (Map<String, Object> o : objs) {
            if (o.get("_id") != null) {
                Map<Integer, List<Map<String, Object>>> id = getIndexDataForCollection(db, collection, "_id");
//                Map<IndexKey, List<Map<String,Object>>> id = getIndexDataForCollection(db, collection, "_id");
                if (id != null && id.containsKey(Integer.valueOf(o.get("_id").hashCode()))) {
                    throw new MorphiumDriverException("Duplicate _id! " + o.get("_id"), null);
                }
            }
            o.putIfAbsent("_id", new ObjectId());
        }

        getCollection(db, collection).addAll(objs);
        for (int i = 0; i < objs.size(); i++) {
            Map<String, Object> o = objs.get(i);

            List<Map<String, Object>> idx = getIndexes(db, collection);
            for (Map<String, Object> ix : idx) {
//                Doc indexValues = new HashMap<>();
                int bucketId = 0;
                StringBuilder fieldNames = new StringBuilder();
                for (String k : ix.keySet()) {
//                    indexValues.put(k, o.get(k));
                    bucketId = iterateBucketId(bucketId, o.get(k));
                    fieldNames.append(k);
                }
//                IndexKey key = new IndexKey(indexValues);
                String fn = fieldNames.toString();
//                Map<String, Map<IndexKey, List<Map<String,Object>>>> indexData = indexDataByDBCollection.get(db).get(collection);
                Map<String, Map<Integer, List<Map<String, Object>>>> indexData = indexDataByDBCollection.get(db).get(collection);
                indexData.putIfAbsent(fn, new HashMap<>());
                indexData.get(fn).putIfAbsent(bucketId, new ArrayList<>());
                indexData.get(fn).get(bucketId).add(o);

            }

            notifyWatchers(db, collection, "insert", o);
        }
    }

    private Integer iterateBucketId(int bucketId, Object o) {
        if (o == null) {
            return bucketId + 1;
        }
        return (bucketId + o.hashCode());
    }


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
            List<Map<String, Object>> idx = getIndexes(db, collection);
//            Doc indexValues = new HashMap<>();
            int bucketId = 0;
            StringBuilder fields = new StringBuilder();
            for (Map<String, Object> i : idx) {
                for (String k : i.keySet()) {
//                    indexValues.put(k, o.get(k));
                    bucketId = iterateBucketId(bucketId, o.get(k));
                    fields.append(k);
                }
//                IndexKey key = new IndexKey(indexValues);
                indexDataByDBCollection.get(db).get(collection).putIfAbsent(fields.toString(), new HashMap<>());
                indexDataByDBCollection.get(db).get(collection).get(fields.toString()).putIfAbsent(bucketId, new ArrayList<>());
                indexDataByDBCollection.get(db).get(collection).get(fields.toString()).get(bucketId).add(o);
            }
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


    public void closeIteration(MorphiumCursor crs) {

    }

    @Override
    public SingleElementResult runCommandSingleResult(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        return null;
    }

    @SuppressWarnings("ConstantConditions")

    public Doc update(String db, String collection, Doc query, Map<String, Integer> sort, Doc op, boolean multiple, boolean upsert, Collation collation, WriteConcern wc) throws MorphiumDriverException {
        List<Map<String, Object>> lst = find(db, collection, query, sort, null, 0, multiple ? 0 : 1, true);
        boolean insert = false;
        int count = 0;
        if (lst == null) lst = new ArrayList<>();
        if (upsert && lst.isEmpty()) {
            lst.add(new Doc());
            for (String k : query.keySet()) {
                if (k.startsWith("$")) continue;
                if (query.get(k) != null)
                    lst.get(0).put(k, query.get(k));
                else
                    lst.get(0).remove(k);
            }
            insert = true;
        }
        Set<Object> modified = new HashSet<>();
        for (Map<String, Object> obj : lst) {
            for (String operand : op.keySet()) {
                @SuppressWarnings("unchecked") Doc cmd = (Doc) op.get(operand);
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
                            if (!obj.get(entry.getKey()).equals(value)) {
                                modified.add(obj.get("_id"));
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
                            if (!obj.get(entry.getKey()).equals(value)) {
                                modified.add(obj.get("_id"));
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
                            modified.add(obj.get("_id"));

                        }
                        break;
                    case "$min":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            Comparable value = (Comparable) obj.get(entry.getKey());
                            //noinspection unchecked
                            if (value.compareTo(entry.getValue()) > 0 && entry.getValue() != null) {
                                modified.add(obj.get("_id"));
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
                                modified.add(obj.get("_id"));
                            }
                        }
                        break;
                    case "$push":
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            List v = (List) obj.get(entry.getKey());
                            if (v == null) {
                                v = new ArrayList();
                                obj.put(entry.getKey(), v);
                                modified.add(obj.get("_id"));
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
        return Doc.of("matched", (Object) lst.size(), "inserted", insert ? 1 : 0, "modified", count);
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
                Doc data = new Doc();
                if (doc != null)
                    data.put("fullDocument", doc);
                if (op != null)
                    data.put("operationType", op);
                Map m = Collections.synchronizedMap(new HashMap<>(UtilsMap.of("db", db)));
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


    public Doc delete(String db, String collection, Doc query, Map<String, Integer> sort, boolean multiple, Collation collation, WriteConcern wc) throws MorphiumDriverException {
        List<Map<String, Object>> toDel = find(db, collection, query, null, Doc.of("_id", 1), 0, multiple ? 0 : 1, 10000, null, collation, null);
        for (Map<String, Object> o : toDel) {
            for (Map<String, Object> dat : getCollection(db, collection)) {
                if (dat.get("_id") instanceof ObjectId || dat.get("_id") instanceof MorphiumId) {
                    if (dat.get("_id").toString().equals(o.get("_id").toString())) {
                        getCollection(db, collection).remove(dat);
                    }
                } else {
                    if (dat.get("_id").equals(o.get("_id"))) {
                        getCollection(db, collection).remove(dat);
                    }
                }
            }
            notifyWatchers(db, collection, "delete", o);
        }
        return new Doc();
    }

    private List<Map<String, Object>> getCollection(String db, String collection) {
        if (!getDB(db).containsKey(collection)) {
            getDB(db).put(collection, new CopyOnWriteArrayList<>());
            createIndex(db, collection, Doc.of("_id", 1), Doc.of());
        }
        return getDB(db).get(collection);
    }


    public void drop(String db, String collection, WriteConcern wc) {
        getDB(db).remove(collection);
        notifyWatchers(db, collection, "drop", null);
    }


    public void drop(String db, WriteConcern wc) {
        database.remove(db);
        notifyWatchers(db, null, "drop", null);
    }


    public boolean exists(String db) {
        return database.containsKey(db);
    }


    @Override
    public CursorResult runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        return null;
    }

    @Override
    public RunCommandResult sendCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        return null;
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
    public ReadPreference getDefaultReadPreference() {
        return null;
    }

    @Override
    public void setDefaultReadPreference(ReadPreference rp) {

    }

    @Override
    public int getDefaultBatchSize() {
        return 0;
    }

    @Override
    public void setDefaultBatchSize(int defaultBatchSize) {

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


    public List<Object> distinct(String db, String collection, String field, Doc filter, Collation collation, ReadPreference rp) {
        List<Map<String, Object>> list = getDB(db).get(collection);
        Set<Object> distinctValues = new HashSet<>();

        if (list != null && !list.isEmpty()) {
            for (Map<String, Object> doc : list) {
                if (doc != null && !doc.isEmpty() && doc.get(field) != null) {
                    distinctValues.add(doc.get(field));
                }
            }
        }

        return Collections.synchronizedList(new ArrayList<>(distinctValues));
    }


    public boolean exists(String db, String collection) {
        return getDB(db) != null && getDB(db).containsKey(collection);
    }

    private Map<String, List<Map<String, Object>>> getIndexesForDB(String db) {
        indicesByDbCollection.putIfAbsent(db, new ConcurrentHashMap<>());
        return indicesByDbCollection.get(db);
    }


    public List<Map<String, Object>> getIndexes(String db, String collection) {
        if (!getIndexesForDB(db).containsKey(collection)) {
            //new collection, create default index for _id
            ArrayList<Map<String, Object>> value = new ArrayList<>();
            getIndexesForDB(db).put(collection, value);
            value.add(Doc.of("_id", 1));
        }
        return getIndexesForDB(db).get(collection);
    }


    public List<String> getCollectionNames(String db) {
        return null;
    }


    public Map<String, Object> findAndOneAndDelete(String db, String col, Doc query, Map<String, Integer> sort, Collation collation) throws MorphiumDriverException {
        List<Map<String, Object>> r = find(db, col, query, sort, null, 0, 1, 1000, null, collation, null);
        if (r.size() == 0) {
            return null;
        }
        delete(db, col, Doc.of("_id", r.get(0).get("_id")), null, false, collation, null);
        return r.get(0);
    }


    public Map<String, Object> findAndOneAndUpdate(String db, String col, Doc query, Doc update, Map<String, Integer> sort, Collation collation) throws MorphiumDriverException {
        List<Map<String, Object>> ret = find(db, col, query, sort, null, 0, 1, 1, null, collation, new Doc());
        update(db, col, query, null, update, false, false, collation, null);
        return ret.get(0);
    }


    public Doc findAndOneAndReplace(String db, String col, Doc query, Doc replacement, Map<String, Integer> sort, Collation collation) throws MorphiumDriverException {
        List<Map<String, Object>> ret = find(db, col, query, sort, null, 0, 1, 1, null, collation, new Doc());
        if (ret.get(0).get("_id") != null)
            replacement.put("_id", ret.get(0).get("_id"));
        else
            replacement.remove("_id");
        store(db, col, Collections.singletonList(replacement), null);
        return replacement;
    }


    @SuppressWarnings("RedundantThrows")

    public List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, Collation collation, ReadPreference readPreference) throws MorphiumDriverException {
        log.warn("Aggregate not possible yet in memory!");
        return new CopyOnWriteArrayList<>();
    }


    public void tailableIteration(String db, String collection, Doc query, Map<String, Integer> sort, Doc projection, int skip, int limit, int batchSize, ReadPreference readPreference, int timeout, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        throw new FunctionNotSupportedException("not possible in Mem yet");
    }


    public int getMaxWaitTime() {
        return 0;
    }


    public void setMaxWaitTime(int maxWaitTime) {

    }

    @Override
    public String[] getCredentials(String db) {
        return new String[0];
    }

    @Override
    public List<String> getHostSeed() {
        return new ArrayList<>();
    }

    @Override
    public void setHostSeed(List<String> hosts) {

    }


    public int getServerSelectionTimeout() {
        return 0;
    }


    public void setServerSelectionTimeout(int serverSelectionTimeout) {
    }


    public boolean isCapped(String db, String coll) {
        return false;
    }

    @Override
    public Map<String, Integer> getNumConnectionsByHost() {
        return null;
    }


    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return new BulkRequestContext(m) {
            private final List<BulkRequest> requests = new ArrayList<>();


            public Map<String, Object> execute() {
                try {
                    for (BulkRequest r : requests) {
                        if (r instanceof InsertBulkRequest) {
                            insert(db, collection, ((InsertBulkRequest) r).getToInsert(), null);
                        } else if (r instanceof UpdateBulkRequest) {
                            UpdateBulkRequest up = (UpdateBulkRequest) r;
                            update(db, collection, up.getQuery(), null, up.getCmd(), up.isMultiple(), up.isUpsert(), null, null);
                        } else if (r instanceof DeleteBulkRequest) {
                            delete(db, collection, ((DeleteBulkRequest) r).getQuery(), null, ((DeleteBulkRequest) r).isMultiple(), null, null);
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


    public void createIndex(String db, String collection, Doc index, Doc options) {

        List<Map<String, Object>> indexes = getIndexes(db, collection);
        if (!indexes.contains(index)) {
            indexes.add(index);
        }
        updateIndexData(db, collection);
    }

    private void updateIndexData(String db, String collection) {
        StringBuilder b = new StringBuilder();
        indexDataByDBCollection.putIfAbsent(db, new ConcurrentHashMap<>());
        indexDataByDBCollection.get(db).putIfAbsent(collection, new ConcurrentHashMap<>());
        indexDataByDBCollection.get(db).get(collection).clear();
        for (Map<String, Object> doc : getCollection(db, collection)) {

            for (Map<String, Object> idx : getIndexes(db, collection)) {
//                Doc indexValues = new HashMap<>();
                b.setLength(0);
                int bucketId = 0;
                for (String k : idx.keySet()) {
//                    indexValues.put(k, doc.get(k));
                    bucketId = iterateBucketId(bucketId, doc.get(k));
                    b.append(k);
                }
//                IndexKey key = new IndexKey(indexValues);
                Map<Integer, List<Map<String, Object>>> index = getIndexDataForCollection(db, collection, b.toString());
//                Map<IndexKey, List<Map<String,Object>>> index = getIndexDataForCollection(db, collection, b.toString());
                index.putIfAbsent(bucketId, new CopyOnWriteArrayList<>());
                index.get(bucketId).add(doc);
            }
        }
    }

//    private class IndexKey {
//        private Doc values;
//
//        public IndexKey(Doc values) {
//            this.values = values;
//        }
//
//        public IndexKey() {
//            values = new HashMap<>();
//        }
//
//        
//        public int hashCode() {
//            int ret = 17+values.keySet().hashCode();
////            for (Map.Entry<String, Object> o : values.entrySet()) {
////                ret += o.getKey().hashCode();
////                if (o.getValue()!=null)
////                    o.getValue().hashCode();
////            }
//
//
//            return ret;
//        }
//
//        public boolean equals(Object o) {
//            if (!(o instanceof IndexKey)) {
//                return false;
//            }
//            IndexKey other = (IndexKey) o;
//            Set<String> keys = this.values.keySet();
//            if (other.values.size()>keys.size()) {
//                keys=other.values.keySet();
//            }
//
//            boolean ret = true;
//            for (String k : keys) {
//                if (this.values.get(k)!=null && other.values.get(k)!=null) {
//                    if (!this.values.get(k).equals(other.values.get(k))) {
//                        ret = false;
//                        break;
//                    }
//                } else if(this.values.get(k)!=other.values.get(k)) {
//                    ret=false;
//                    break;
//                }
//            }
//            return ret;
//        }
//
//        public Doc getValues() {
//            return values;
//        }
//    }


    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing) throws MorphiumDriverException {
        throw new FunctionNotSupportedException("no map reduce in memory");
    }


    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Doc query) throws MorphiumDriverException {
        throw new FunctionNotSupportedException("no map reduce in memory");
    }


    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Doc query, Doc sorting, Collation collation) throws MorphiumDriverException {
        throw new FunctionNotSupportedException("no map reduce in memory");
    }


    public void startTransaction() {
        if (currentTransaction.get() != null) throw new IllegalArgumentException("transaction in progress");
        InMemTransactionContext ctx = new InMemTransactionContext();
        Cloner cloner = new Cloner();
        ctx.setDatabase(cloner.deepClone(database));
        currentTransaction.set(ctx);
    }


    public void commitTransaction() {
        if (currentTransaction.get() == null) throw new IllegalArgumentException("No transaction in progress");
        InMemTransactionContext ctx = currentTransaction.get();
        //noinspection unchecked
        database.putAll(ctx.getDatabase());
        currentTransaction.set(null);
    }


    public MorphiumTransactionContext getTransactionContext() {
        return currentTransaction.get();
    }


    public void abortTransaction() {
        currentTransaction.set(null);
    }


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
        private Doc query;
        private Map<String, Integer> sort;
        private Doc projection;
        private ReadPreference readPreference;
        private Doc findMetaData;
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

        public Doc getQuery() {
            return query;
        }

        public void setQuery(Doc query) {
            this.query = query;
        }

        public Map<String, Integer> getSort() {
            return sort;
        }

        public void setSort(Map<String, Integer> sort) {
            this.sort = sort;
        }

        public Doc getProjection() {
            return projection;
        }

        public void setProjection(Doc projection) {
            this.projection = projection;
        }

        public ReadPreference getReadPreference() {
            return readPreference;
        }

        public void setReadPreference(ReadPreference readPreference) {
            this.readPreference = readPreference;
        }

        public Doc getFindMetaData() {
            return findMetaData;
        }

        public void setFindMetaData(Doc findMetaData) {
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


    public SSLContext getSslContext() {
        return null;
    }


    public void setSslContext(SSLContext sslContext) {

    }


    public boolean isSslInvalidHostNameAllowed() {
        return false;
    }


    public void setSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {

    }
}