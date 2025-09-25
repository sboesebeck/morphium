package de.caluga.morphium.driver.inmem;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.Collator;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.net.ssl.SSLContext;

import org.bson.types.ObjectId;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.openjdk.jol.vm.VM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Collation;
import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.Driver;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.FunctionNotSupportedException;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.MorphiumTransactionContext;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.SingleBatchCursor;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.bulk.BulkRequest;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.bulk.DeleteBulkRequest;
import de.caluga.morphium.driver.bulk.InsertBulkRequest;
import de.caluga.morphium.driver.bulk.UpdateBulkRequest;
import de.caluga.morphium.driver.commands.AbortTransactionCommand;
import de.caluga.morphium.driver.commands.AggregateMongoCommand;
import de.caluga.morphium.driver.commands.ClearCollectionCommand;
import de.caluga.morphium.driver.commands.CollStatsCommand;
import de.caluga.morphium.driver.commands.CommitTransactionCommand;
import de.caluga.morphium.driver.commands.CountMongoCommand;
import de.caluga.morphium.driver.commands.CreateCommand;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.CurrentOpCommand;
import de.caluga.morphium.driver.commands.DbStatsCommand;
import de.caluga.morphium.driver.commands.DeleteMongoCommand;
import de.caluga.morphium.driver.commands.DistinctMongoCommand;
import de.caluga.morphium.driver.commands.DropDatabaseMongoCommand;
import de.caluga.morphium.driver.commands.DropMongoCommand;
import de.caluga.morphium.driver.commands.ExplainCommand;
import de.caluga.morphium.driver.commands.FindAndModifyMongoCommand;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.GetMoreMongoCommand;
import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.KillCursorsCommand;
import de.caluga.morphium.driver.commands.ListCollectionsCommand;
import de.caluga.morphium.driver.commands.ListDatabasesCommand;
import de.caluga.morphium.driver.commands.ListIndexesCommand;
import de.caluga.morphium.driver.commands.MapReduceCommand;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.commands.RenameCollectionCommand;
import de.caluga.morphium.driver.commands.ReplicastStatusCommand;
import de.caluga.morphium.driver.commands.ShutdownCommand;
import de.caluga.morphium.driver.commands.StepDownCommand;
import de.caluga.morphium.driver.commands.StoreMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.commands.auth.CreateRoleAdminCommand;
import de.caluga.morphium.driver.commands.auth.CreateUserAdminCommand;
import de.caluga.morphium.driver.commands.auth.SaslAuthCommand;
import de.caluga.morphium.driver.wire.AtomicDecimal;
import de.caluga.morphium.driver.wire.HelloResult;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import de.caluga.morphium.objectmapping.MorphiumTypeMapper;
import de.caluga.morphium.ObjectMapperImpl;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

/**
 * User: Stephan Bösebeck
 * Date: 28.11.15
 * Time: 23:25
 * <p>
 * InMemory implementation of the MorphiumDriver interface. can be used for
 * testing or caching. Does not cover all
 * functionality yet.
 */
@SuppressWarnings({"WeakerAccess", "EmptyMethod", "BusyWait"})
@Driver(name = "InMemDriver", description = "in memory driver for simple test")
public class InMemoryDriver implements MorphiumDriver, MongoConnection {
    private final Logger log = LoggerFactory.getLogger(InMemoryDriver.class);
    public final static String driverName = "InMemDriver";
    @Override
    public int getLocalThreshold() {
        return 0;
    }

    @Override
    public void setLocalThreshold(int threshold) {
    }

    // DBName => Collection => List of documents
    private static final Map<String, Map<String, List<Map<String, Object >> >> GLOBAL_DATABASE = new ConcurrentHashMap<>();
    private final Map<String, Map<String, List<Map<String, Object >> >> database = GLOBAL_DATABASE;
    private int idleSleepTime = 20;
    /**
     * index definitions by db and collection name
     * DB -> Collection -> List of Map Index defintion (field -> 1/-1/hashed)
     */
    private static final Map<String, Map<String, List<Map<String, Object >> >> GLOBAL_INDICES_BY_DB_COLLECTION = new ConcurrentHashMap<>();
    private final Map<String, Map<String, List<Map<String, Object >> >> indicesByDbCollection = GLOBAL_INDICES_BY_DB_COLLECTION;

    /**
     * Map DB->Collection->FieldNames->Keys....
     */
    // private final Map<String, Map<String, Map<String, Map<IndexKey,
    // List<Map<String, Object>>>>>> indexDataByDBCollection = new
    // ConcurrentHashMap<>();
    private final Map<String, Map<String, Map<String, Map<Integer, List<Map<String, Object >> >> >> indexDataByDBCollection = new ConcurrentHashMap<>();
    private final ThreadLocal<InMemTransactionContext> currentTransaction = new ThreadLocal<>();
    private final AtomicLong txn = new AtomicLong();
    private final AtomicLong changeStreamSequence = new AtomicLong();
    private final Map<String, CopyOnWriteArrayList<ChangeStreamSubscription>> changeStreamSubscribers = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<ChangeStreamEventInfo> changeStreamHistory = new ConcurrentLinkedDeque<>();
    private final int CHANGE_STREAM_HISTORY_LIMIT = 1024;
    private final Map<String, Map<String, Map<String, Integer >>> cappedCollections = new ConcurrentHashMap<>();
    // size/max
    private final List<Object> monitors = new CopyOnWriteArrayList<>();
    private BlockingQueue<Runnable> eventQueue =  new LinkedBlockingDeque<>();
    private final List<Map<String, Object >> commandResults = new Vector<>();
    private final Map < String, Class <? extends MongoCommand >> commandsCache = new HashMap<>();
    private final AtomicInteger commandNumber = new AtomicInteger(0);
    private final AtomicInteger connectionId = new AtomicInteger(0);
    private final Map<DriverStatsKey, AtomicDecimal> stats = new ConcurrentHashMap<>();
    private final Map<Long, FindCommand> cursors = new ConcurrentHashMap<>();
    // state for MorphiumCursor-based iterations
    private final Map<Long, InMemoryCursor> iterationCursors = new ConcurrentHashMap<>();
    // command-level cursor buffers (simulate MongoDB batches)
    private final Map<Long, CursorResultBuffer> activeQueryCursors = new ConcurrentHashMap<>();
    private final AtomicLong cursorIdSequence = new AtomicLong(1);
    private ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(2);
    private boolean running = true;
    private int expireCheck = 10000;
    private ScheduledFuture<?> expire;
    private String replicaSetName;

    public Map<String, List<Map<String, Object >>> getDatabase(String dbn) {
        return database.get(dbn);
    }

    public InMemoryDriver() {
    }

    @Override
    public <T, R> Aggregator<T, R> createAggregator(Morphium morphium, Class<? extends T > type, Class <? extends R > resultType) {
        return new InMemAggregator<>(morphium, type, resultType);
    }

    public void setDatabase(String dbn, Map<String, List<Map<String, Object >>> db) {
        if (db != null) {
            database.put(dbn, db);
        }
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
                return new ObjectId(((Map <?, ? >) d).get("value").toString());
            }
        };
    }

    @Override
    public List<String> listDatabases() {
        return new ArrayList<>(database.keySet());
    }

    public List<String> listCollections(String db, String pattern) {
        Set<String> collections = new HashSet<>();

        if (db.equals("1")) {
            for (String k : database.keySet()) {
                collections.addAll(database.get(k).keySet());
            }
        } else if (database.containsKey(db)) {
            collections.addAll(database.get(db).keySet());
        } else {
            return new ArrayList<>();
        }

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
        return replicaSetName;
    }

    @Override
    public void setReplicaSetName(String replicaSetName) {
        this.replicaSetName = replicaSetName;
    }

    public void resetData() {
        database.clear();
        indexDataByDBCollection.clear();
        indicesByDbCollection.clear();
        cappedCollections.clear();

        for (var o : monitors) {
            o.notifyAll();
        }

        monitors.clear();
        changeStreamSubscribers.clear();
        changeStreamHistory.clear();
        changeStreamSequence.set(0);
        eventQueue.clear();
        cursors.clear();
        commandResults.clear();
        currentTransaction.remove();
    }

    public void setCredentials(String db, String login, char[] pwd) {
        // ignored for now
    }

    // public Map<String, Object> getAnswer(int queryId) {
    //     stats.get(DriverStatsKey.REPLY_PROCESSED).incrementAndGet();
    //     var data = commandResults.remove(queryId);
    //     return data;

    @Override
    public void watch(WatchCommand settings) throws MorphiumDriverException {
        if (settings == null || settings.getCb() == null) {
            return;
        }

        String db = settings.getDb();

        if (db == null || db.isBlank()) {
            db = "admin";
        }

        String collection = settings.getColl();
        Object monitor = new Object();
        monitors.add(monitor);

        ChangeStreamSubscription subscription = new ChangeStreamSubscription(
                        db,
                        collection,
                        settings.getCb(),
                        settings.getPipeline(),
                        settings.getFullDocument() == null ? WatchCommand.FullDocumentEnum.defaultValue : settings.getFullDocument(),
                        settings.getFullDocumentBeforeChange() == null ? WatchCommand.FullDocumentBeforeChangeEnum.off : settings.getFullDocumentBeforeChange(),
                        Boolean.TRUE.equals(settings.getShowExpandedEvents()),
                        monitor
        );

        registerSubscription(subscription);

        try {
            Long resumeAfterToken = extractResumeToken(settings.getResumeAfter());
            Long startAfterToken = resumeAfterToken == null ? extractResumeToken(settings.getStartAfter()) : null;
            Long startingToken = resumeAfterToken != null ? resumeAfterToken : startAfterToken;

            if (startingToken != null) {
                replayHistory(subscription, startingToken);
            }

            if (!subscription.isActive()) {
                return;
            }

            synchronized (monitor) {
                while (subscription.isActive()) {
                    Integer maxTime = settings.getMaxTimeMS();

                    if (maxTime != null && maxTime > 0) {
                        monitor.wait(maxTime);
                    } else {
                        monitor.wait();
                    }
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            unregisterSubscription(subscription);
            monitors.remove(monitor);
        }
    }

    @Override
    public List<Map<String, Object >> readAnswerFor(int queryId) throws MorphiumDriverException {
        // log.info("Reading answer for id " + queryId);
        stats.get(DriverStatsKey.REPLY_PROCESSED).incrementAndGet();
        var data = commandResults.remove(0);

        if (data.containsKey("results")) {
            return ((List<Map<String, Object >>) data.get("results"));
        } else if (data.containsKey("cursor")) {
            var cursor = (Map<String, Object>) data.get("cursor");

            if (cursor.containsKey("firstBatch")) {
                return (List<Map<String, Object >>) cursor.get("firstBatch");
            }

            else if (cursor.containsKey("nextBatch")) {
                return (List<Map<String, Object >>) cursor.get("nextBatch");
            }
        }

        return null;
    }

    @Override
    public MorphiumCursor getAnswerFor(int queryId, int batchsize) throws MorphiumDriverException {
        stats.get(DriverStatsKey.REPLY_PROCESSED).incrementAndGet();
        var data = commandResults.remove(0);

        if (data.containsKey("cursor")) {
            Map<String, Object> cursorDoc = (Map<String, Object>) data.get("cursor");

            if (cursorDoc.containsKey("firstBatch")) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> firstBatch = cursorDoc.get("firstBatch") != null ?
                    new ArrayList<>((List<Map<String, Object>>) cursorDoc.get("firstBatch")) : new ArrayList<>();
                long cursorId = cursorDoc.get("id") instanceof Number ? ((Number) cursorDoc.get("id")).longValue() : 0L;
                String namespace = cursorDoc.get("ns") instanceof String ? (String) cursorDoc.get("ns") : "";

                int effectiveBatchSize = batchsize > 0 ? batchsize : firstBatch.size();

                if (cursorId != 0) {
                    CursorResultBuffer buffer = activeQueryCursors.get(cursorId);

                    if ((effectiveBatchSize <= 0) && buffer != null && buffer.defaultBatchSize > 0) {
                        effectiveBatchSize = buffer.defaultBatchSize;
                    }
                }

                if (effectiveBatchSize <= 0) {
                    effectiveBatchSize = firstBatch.size() > 0 ? firstBatch.size() : 101;
                }

                return new InMemoryFindCursor(cursorId, namespace, firstBatch, effectiveBatchSize);
            }

            if (cursorDoc.containsKey("nextBatch")) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> nextBatch = cursorDoc.get("nextBatch") != null ?
                                                      (List<Map<String, Object>>) cursorDoc.get("nextBatch") : List.of();
                return new SingleBatchCursor(nextBatch);
            }
        }

        List<Map<String, Object>> batch = new ArrayList<>();

        if (data.containsKey("results")) {
            batch = (List<Map<String, Object>>) data.get("results");
        }

        return new SingleBatchCursor(batch);
    }

    @Override
    public List<Map<String, Object >> readAnswerFor(MorphiumCursor crs) throws MorphiumDriverException {
        return crs.getBatch();
    }

    @Override
    public int sendCommand(MongoCommand cmd) throws MorphiumDriverException {
        stats.get(DriverStatsKey.MSG_SENT).incrementAndGet();

        if (cmd.asMap().get("$db") == null) {
            throw new IllegalArgumentException("DB cannot be null");
        }

        try {
            try {
                Method method = this.getClass().getDeclaredMethod("runCommand", cmd.getClass());
                var o = method.invoke(this, cmd);
                stats.get(DriverStatsKey.REPLY_RECEIVED);

                if (o instanceof Integer) {
                    return (Integer) o;
                }
            } catch (NoSuchMethodException ex) {
                log.error("No method for command " + cmd.getClass().getSimpleName() + " - " + cmd.getCommandName());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return 0;
    }

    public int runCommand(CreateUserAdminCommand cmd) {
        return 0;
    }

    public int runCommand(CreateRoleAdminCommand cmd) {
        return 0;
    }

    public int runCommand(SaslAuthCommand cmd) {
        return 0;
    }

    public int runCommand(ExplainCommand cmd) {
        int ret = commandNumber.incrementAndGet();

        // Create a basic explain response compatible with MongoDB
        Map<String, Object> winningPlan = Doc.of(
                "stage", "COLLSCAN",
                "direction", "forward"
                                          );

        Map<String, Object> queryPlanner = new HashMap<>();
        queryPlanner.put("namespace", cmd.getDb() + "." + cmd.getColl());
        queryPlanner.put("indexFilterSet", false);
        // Try to get the filter from the command, default to empty query if not available
        Object parsedQuery = Doc.of();
        try {
            Object findCommand = cmd.getCommand().get("find");
            if (findCommand instanceof Map) {
                Object filter = ((Map<String, Object>) findCommand).get("filter");
                if (filter != null) {
                    parsedQuery = filter;
                }
            }
        } catch (Exception e) {
            // Use empty query as fallback
            parsedQuery = Doc.of();
        }
        queryPlanner.put("parsedQuery", parsedQuery);
        queryPlanner.put("queryHash", "InMemoryDriver");
        queryPlanner.put("planCacheKey", "InMemoryDriver");
        queryPlanner.put("maxIndexedOrSolutionsReached", false);
        queryPlanner.put("maxIndexedAndSolutionsReached", false);
        queryPlanner.put("maxScansToExplodeReached", false);
        queryPlanner.put("winningPlan", winningPlan);
        queryPlanner.put("rejectedPlans", new ArrayList<>());

        Map<String, Object> explainResult = Doc.of(
                "explainVersion", "1",
                "queryPlanner", queryPlanner,
                "ok", 1.0
                                            );

        commandResults.add(prepareResult(explainResult));
        return ret;
    }

    public int runCommand(GenericCommand cmd) {
        // log.info("Trying to handle generic Command");
        Map<String, Object> cmdMap = cmd.asMap();
        var commandName = cmdMap.keySet().stream().findFirst().get();
        Class <? extends MongoCommand > commandClass = commandsCache.get(commandName);

        if (commandName.equals("aggreagate") && cmdMap.containsKey("pipeline") && ((Map)((List)cmdMap.get("pipeline")).get(0)).containsKey("$changeStream")) {
            commandClass = WatchCommand.class;
        } else if (commandName.equals("aggregate")) {
            commandClass = AggregateMongoCommand.class;
        }

        if (commandClass == null) {
            throw new IllegalArgumentException("Unknown command " + commandName);
        }

        try {
            Constructor declaredConstructor = commandClass.getDeclaredConstructor(MongoConnection.class);
            declaredConstructor.setAccessible(true);
            var mongoCommand = (MongoCommand<MongoCommand>) declaredConstructor.newInstance(this);
            mongoCommand.fromMap(cmdMap);

            try {
                Method method = this.getClass().getDeclaredMethod("runCommand", commandClass);
                var o = method.invoke(this, mongoCommand);

                if (o instanceof Integer) {
                    return (int) o;
                } else {
                    log.error("THIS CANNOT HAPPEN!");
                    return 0; // executed, but did not return int?!?!?
                }
            } catch (NoSuchMethodException ex) {
                log.error("No method for command " + commandClass.getSimpleName() + " - " + mongoCommand.getCommandName());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        int ret = commandNumber.incrementAndGet();
        commandResults.add(prepareResult(Doc.of("ok", 0, "errmsg", "could not execute command inMemory")));
        return ret;
    }

    private int runCommand(StepDownCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        commandResults.add(prepareResult(Doc.of("ok", 0, "errmsg", "no replicaset - in Memory!")));
        return ret;
    }

    private int runCommand(AbortTransactionCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        abortTransaction();
        int ret = commandNumber.incrementAndGet();
        commandResults.add(prepareResult(Doc.of("ok", 1.0, "msg", "aborted")));
        return ret;
    }

    private int runCommand(CommitTransactionCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        commitTransaction();
        int ret = commandNumber.incrementAndGet();
        commandResults.add(prepareResult(Doc.of("ok", 1.0, "msg", "committed")));
        return ret;
    }

    private int runCommand(UpdateMongoCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        Map<String, Object> stats = new HashMap<>();
        int totalMatched = 0;
        int totalModified = 0;
        int updateIndex = 0;
        List<Map<String, Object>> upserted = new ArrayList<>();

        for (var update : cmd.getUpdates()) {
            // Doc.of("q", query, "u", update, "upsert", upsert, "multi", multi);
            boolean multi = false;

            if (update.containsKey("multi")) {
                multi = Boolean.TRUE.equals(update.get("multi"));
            }

            boolean upsert = false;

            if (update.containsKey("upsert")) {
                upsert = Boolean.TRUE.equals(update.get("upsert"));
            }

            Map<String, Object> collation = null;
            if (update.containsKey("collation") && update.get("collation") instanceof Map) {
                collation = (Map<String, Object>) update.get("collation");
            }
            var res = update(cmd.getDb(), cmd.getColl(), (Map<String, Object>) update.get("q"), null, (Map<String, Object>) update.get("u"), multi, upsert, collation, cmd.getWriteConcern());

            // accumulate matched and modified
            Object m = res.get("matched");
            if (m instanceof Number) totalMatched += ((Number) m).intValue();
            Object nm = res.get("nModified");
            if (nm instanceof Number) totalModified += ((Number) nm).intValue();

            // convert upsertedIds to wire-format upserted entries with index
            if (res.containsKey("upsertedIds") && res.get("upsertedIds") instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> ids = (List<Object>) res.get("upsertedIds");
                for (Object id : ids) {
                    upserted.add(Doc.of("index", updateIndex, "_id", id));
                }
            }
            updateIndex++;
        }

        // MongoDB compatibility: For upserts, "n" should include both matched and upserted documents
        int n = totalMatched;
        if (!upserted.isEmpty()) {
            n += upserted.size();
        }

        stats.put("n", n);
        stats.put("nModified", totalModified);
        if (!upserted.isEmpty()) {
            stats.put("upserted", upserted);
        }

        // System.out.println("[DEBUG_LOG] UpdateMongoCommand result: " + stats);
        Map<String, Object> preparedResult = prepareResult(stats);
        // System.out.println("[DEBUG_LOG] UpdateMongoCommand result after prepareResult: " + preparedResult);
        commandResults.add(preparedResult);
        return ret;
    }

    public int runCommand(WatchCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        watch(cmd);
        return ret;
    }

    public int runCommand(StoreMongoCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        var stats = store(cmd.getDb(), cmd.getColl(), cmd.getDocs(), null);
        Map<String, Object> result = new HashMap<>();
        result.put("ok", 1.0);
        result.putAll(stats);

        // Ensure n is set correctly for compatibility with MongoDB
        if (!result.containsKey("n") && (result.containsKey("matched") || result.containsKey("inserted"))) {
            int matched = result.containsKey("matched") ? (Integer)result.get("matched") : 0;
            int inserted = result.containsKey("inserted") ? (Integer)result.get("inserted") : 0;
            result.put("n", matched + inserted);
        }

        // System.out.println("[DEBUG_LOG] InMemoryDriver runCommand(StoreMongoCommand) result before prepareResult: " + result);
        Map<String, Object> preparedResult = prepareResult(result);
        // System.out.println("[DEBUG_LOG] InMemoryDriver runCommand(StoreMongoCommand) result after prepareResult: " + preparedResult);
        commandResults.add(preparedResult);
        return ret;
    }

    public int runCommand(ShutdownCommand cmd) {
        int ret = commandNumber.incrementAndGet();
        commandResults.add(prepareResult(Doc.of("ok", 0.0, "errmsg", "shutdown in memory not supported")));
        return ret;
    }

    public int runCommand(ReplicastStatusCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        commandResults.add(prepareResult(Doc.of("ok", 0.0, "errmsg", "no replicaset")));
        return ret;
    }

    public int runCommand(RenameCollectionCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        String target = cmd.getTo();
        String origin = cmd.getColl();
        var col = database.get(cmd.getDb()).remove(origin);
        database.get(cmd.getDb()).put(target, col);
        int ret = commandNumber.incrementAndGet();
        commandResults.add(prepareResult(Doc.of("ok", 1.0, "msg", "renamed " + origin + " to " + target)));
        return ret;
    }

    public int runCommand(MapReduceCommand cmd) {
        log.info("MapReduce command received: db={}, collection={}, map={}, reduce={}",
                 cmd.getDb(), cmd.getColl(), cmd.getMap(), cmd.getReduce());
        int ret = commandNumber.incrementAndGet();

        try {
            // Execute MapReduce using our implementation
            List<Map<String, Object>> results = mapReduceInternal(
                    cmd.getDb(),
                    cmd.getColl(),
                    cmd.getMap(),
                    cmd.getReduce(),
                    cmd.getQuery(),
                    cmd.getSort(),
                    null, // TODO: Implement proper collation support
                    cmd.getFinalize()
                                                );

            log.info("MapReduce completed with {} results", results.size());

            // Format results according to MongoDB MapReduce response format
            Map<String, Object> response = new HashMap<>();
            response.put("results", results);
            response.put("timeMillis", 0); // Could be enhanced to track actual time
            response.put("counts", Map.of(
                                         "input", results.size(),
                                         "emit", results.size(),
                                         "reduce", 0, // Could be enhanced to track actual reduce operations
                                         "output", results.size()
                         ));
            response.put("ok", 1.0);

            commandResults.add(prepareResult(response));

        } catch (Exception e) {
            log.error("MapReduce error: " + e.getMessage(), e);
            commandResults.add(prepareResult(Doc.of("ok", 0.0, "errmsg", "MapReduce error: " + e.getMessage())));
        }

        return ret;
    }

    public int runCommand(ListIndexesCommand cmd) {
        int ret = commandNumber.incrementAndGet();
        Map<String, List<Map<String, Object >>> indexesForDB = indicesByDbCollection.get(cmd.getDb());
        if (indexesForDB == null) {
            commandResults.add(prepareResult(Doc.of("cursor", Doc.of("firstBatch", List.of(), "id", 0L, "ns", cmd.getDb() + "." + cmd.getColl()), "ok", 1.0, "ns", cmd.getDb() + "." + cmd.getColl(), "id", 0)));
            return ret;
        }
        var idx = indexesForDB.get(cmd.getColl());
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        var indices = new ArrayList<Map<String, Object >> ();
        if (idx != null)
            for (var i : idx) {
                Map<String, Object> index = new HashMap<>();
                index.put("v", 2.0);

                for (var e : i.entrySet()) {
                    if (e.getKey().startsWith("$")) {
                        continue;
                    }

                    index.putIfAbsent("key", Doc.of());
                    ((Doc) index.get("key")).add(e.getKey(), e.getValue());
                }

                Map opt = (Map) i.get("$options");

                if (opt != null && opt.get("name") != null) {
                    index.put("name", opt.get("name"));
                } else {
                    index.put("name", "unknown");
                }

                if (opt != null && opt.get("unique") != null) {
                    index.put("unique", opt.get("unique"));
                }

                if (opt != null && opt.get("sparse") != null) {
                    index.put("sparse", opt.get("sparse"));
                }

                if (opt != null && opt.get("expireAfterSeconds") != null) {
                    index.put("expireAfterSeconds", opt.get("expireAfterSeconds"));
                }

                if (opt != null && opt.get("bachground") != null) {
                    index.put("background", opt.get("background"));
                }

                if (opt != null && opt.get("background") != null) {
                    index.put("background", opt.get("background"));
                }

                if (opt != null && opt.get("hidden") != null) {
                    index.put("hidden", opt.get("hidden"));
                }

                // todo: add index features
                indices.add(index);
            }
        commandResults.add(prepareResult(Doc.of("cursor", Doc.of("firstBatch", indices, "id", 0L, "ns", cmd.getDb() + "." + cmd.getColl()), "ok", 1.0, "ns", cmd.getDb() + "." + cmd.getColl(), "id", 1)));
        return ret;
    }

    public int runCommand(ListDatabasesCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object >> dbList = new ArrayList<>();
        data.put("databases", dbList);
        int sum = 0;

        for (var k : database.keySet()) {
            sum += database.get(k).size();
            Map<String, Object> db = Doc.of("name", k, "sizeOnDisk", 0, "entries", database.get(k).size(), "empty", database.get(k).isEmpty());
            dbList.add(db);
        }
        data.put("ok", 1.0);
        data.put("totalSize", 0);
        data.put("totalSizeMb", 0);
        data.put("totalEntries", sum);
        log.info("Storing listDb Result for id: " + ret);
        commandResults.add(prepareResult(data));
        return ret;
    }

    public int runCommand(ListCollectionsCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        var m = prepareResult();
        var cursorData = new ArrayList<Map<String, Object >> ();
        if (!database.containsKey(cmd.getDb())) {
            //            m.put("ok", 0.0);
            //            m.put("errmsg", "no such database");
            //            commandResults.add(m);
            //            return ret;
            database.putIfAbsent(cmd.getDb(), new HashMap<>());
        }

        for (String coll : database.get(cmd.getDb()).keySet()) {
            cursorData.add(Doc.of("name", coll, "type", "collection", "options", new Doc(), "info", Doc.of("readonly", false, "UUID", UUID.randomUUID())).add("idIndex",
                           Doc.of("v", 2.0, "key", Doc.of("_id", 1), "name", "_id_1", "ns", cmd.getDb() + "." + coll)));
        }
        addCursor(cmd.getDb(), "$cmd.listCollections", m, cursorData);
        commandResults.add(m);
        return ret;
    }

    public int runCommand(KillCursorsCommand cmd) {
        // log.error("Should not happen: " + cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        log.info("Killing cursors");
        int ret = commandNumber.incrementAndGet();

        for (Long id : cmd.getCursors()) {
            cursors.remove(id);
        }

        commandResults.add(prepareResult());
        return ret;
    }

    public int runCommand(InsertMongoCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        List<Map<String, Object >> writeErrors = insert(cmd.getDb(), cmd.getColl(), cmd.getDocuments(), cmd.getWriteConcern());
        var m = prepareResult();
        m.put("n", cmd.getDocuments().size() - writeErrors.size());
        if (writeErrors.size() != 0) {
            m.put("writeErrors", writeErrors);
        }
        commandResults.add(m);
        return ret;
    }

    public int runCommand(FindCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        List<Map<String, Object >> result = runFind(cmd);

        if (result == null) {
            result = new ArrayList<>();
        }

        if (cmd.isTailable() != null && cmd.isTailable()) {
            long cursorId = ret;
            cursors.put(cursorId, cmd);
            final List<Map<String, Object>> tailableResult = result;

            if (result.isEmpty()) {
                WatchCommand watchCmd = new WatchCommand(this)
                .setDb(cmd.getDb())
                .setColl(cmd.getColl())
                .setMaxTimeMS(cmd.getMaxTimeMS())
                .setFullDocument(WatchCommand.FullDocumentEnum.updateLookup)
                .setPipeline(Arrays.asList(Doc.of("$match", cmd.getFilter())))
                .setCb(new DriverTailableIterationCallback() {
                    @Override
                    public void incomingData(Map<String, Object> data, long dur) {
                        tailableResult.add(data);
                    }

                    @Override
                    public boolean isContinued() {
                        return false;
                    }
                });

                watch(watchCmd);
                watchCmd.releaseConnection();
            }

            Map<String, Object> response = prepareResult();
            addCursor(cmd.getDb(), cmd.getColl(), response, result);
            ((Map) response.get("cursor")).put("id", cursorId);
            commandResults.add(response);
            return ret;
        }

        int requestedBatchSize = cmd.getBatchSize() != null ? cmd.getBatchSize() : 0;

        if (requestedBatchSize <= 0) {
            requestedBatchSize = getDefaultBatchSize() > 0 ? getDefaultBatchSize() : Math.min(result.size(), 101);
        }

        if (cmd.isSingleBatch() != null && cmd.isSingleBatch()) {
            requestedBatchSize = result.size();
        }

        List<Map<String, Object>> firstBatch = new ArrayList<>();

        if (!result.isEmpty()) {
            int end = Math.min(requestedBatchSize, result.size());
            firstBatch.addAll(result.subList(0, end));
        }

        String namespace = cmd.getDb() + "." + cmd.getColl();
        long cursorId = registerCursorBuffer(namespace, result, requestedBatchSize);

        Map<String, Object> cursorDoc = new HashMap<>();
        cursorDoc.put("firstBatch", firstBatch);
        cursorDoc.put("ns", namespace);
        cursorDoc.put("id", cursorId);

        Map<String, Object> response = prepareResult();
        response.put("cursor", cursorDoc);
        commandResults.add(response);
        return ret;
    }

    private List<Map<String, Object >> runFind(FindCommand cmd) throws MorphiumDriverException {
        int limit = 0;

        if (cmd.getLimit() != null) {
            limit = cmd.getLimit();
        }

        int skip = 0;

        if (cmd.getSkip() != null) {
            skip = cmd.getSkip();
        }

        var filter = cmd.getFilter();

        if (filter == null) {
            filter = Doc.of();
        }

        var result = find(cmd.getDb(), cmd.getColl(), filter, cmd.getSort(), cmd.getProjection(), cmd.getCollation(), skip, limit, false);
        return result;
    }

    private int runCommand(GetMoreMongoCommand cmd) throws MorphiumDriverException {
        // log.error(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ") - should not be used inMem!");
        int ret = commandNumber.incrementAndGet();

        if (cursors.containsKey(cmd.getCursorId())) {
            FindCommand fnd = cursors.get(cmd.getCursorId());
            List<Map<String, Object >> result = new ArrayList<>();
            final List<Map<String, Object>> tailableResult = result;

            if (result.isEmpty()) {
                WatchCommand watchCmd = new WatchCommand(this)
                .setDb(fnd.getDb())
                .setColl(fnd.getColl())
                .setMaxTimeMS(fnd.getMaxTimeMS())
                .setFullDocument(WatchCommand.FullDocumentEnum.updateLookup)
                .setPipeline(Arrays.asList(Doc.of("$match", fnd.getFilter())))
                .setCb(new DriverTailableIterationCallback() {
                    @Override
                    public void incomingData(Map<String, Object> data, long dur) {
                        tailableResult.add((Map<String, Object>) data.get("fullDocument"));
                    }

                    @Override
                    public boolean isContinued() {
                        return false;
                    }
                });

                watch(watchCmd);
                watchCmd.releaseConnection();
            }

            Map<String, Object> response = prepareResult();
            response.put("cursor", Doc.of("nextBatch", result, "ns", cmd.getDb() + "." + cmd.getColl(), "id", cmd.getCursorId()));
            commandResults.add(response);
            return ret;
        }

        long cursorId = cmd.getCursorId();
        int requestedBatchSize = cmd.getBatchSize() != null ? cmd.getBatchSize() : 0;
        List<Map<String, Object>> nextBatch = drainNextBatch(cursorId, requestedBatchSize);
        String namespace = namespaceForCursor(cursorId);

        if (namespace == null) {
            namespace = cmd.getDb() + "." + cmd.getColl();
        }

        long nextId = activeQueryCursors.containsKey(cursorId) ? cursorId : 0L;

        Map<String, Object> cursorDoc = new HashMap<>();
        cursorDoc.put("nextBatch", nextBatch);
        cursorDoc.put("ns", namespace);
        cursorDoc.put("id", nextId);

        Map<String, Object> response = prepareResult();
        response.put("cursor", cursorDoc);
        commandResults.add(response);
        return ret;
    }

    private int runCommand(FindAndModifyMongoCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();

        if (cmd.isRemove()) {
            var list = find(cmd.getDb(), cmd.getColl(), cmd.getQuery(), cmd.getSort(), null, 0, 1);
            var res = delete (cmd.getDb(), cmd.getColl(), Doc.of("_id", list.get(0).get("_id")), null, false, null, null);
            commandResults.add(prepareResult(Doc.of("value", list.get(0))));
        } else {
            var res = findAndOneAndUpdate(cmd.getDb(), cmd.getColl(), cmd.getQuery(), cmd.getUpdate(), cmd.getSort(), cmd.getCollation());
            commandResults.add(prepareResult(Doc.of("value", res)));
        }

        return ret;
    }

    private int runCommand(DropMongoCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        drop(cmd.getDb(), cmd.getColl(), null);
        commandResults.add(prepareResult(Doc.of("ok", 1.0, "msg", "dropped collection " + cmd.getColl())));

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
        }

        return ret;
    }

    private int runCommand(DropDatabaseMongoCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        drop(cmd.getDb(), null);
        commandResults.add(prepareResult(Doc.of("ok", 1.0, "msg", "dropped database " + cmd.getDb())));
        return ret;
    }

    private int runCommand(DistinctMongoCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        var distinctResult = distinct(cmd.getDb(), cmd.getColl(), cmd.getKey(), cmd.getQuery(), cmd.getCollation());
        var m = prepareResult();
        m.put("values", distinctResult);
        commandResults.add(m);
        return ret;
    }

    private int runCommand(DeleteMongoCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();

        // del.addDelete(Doc.of("q", new HashMap<>(), "limit", 0));
        int totalDeleted = 0;

        for (var del : cmd.getDeletes()) {
            boolean multi = true;
            if (del.get("limit") != null) {
                multi = ((int) del.get("limit")) == 0;
            }
            Map<String, Object> collation = null;
            if (del.get("collation") instanceof Map) {
                collation = (Map<String, Object>) del.get("collation");
            }
            Map<String, Object> deleteStats = delete (cmd.getDb(), cmd.getColl(), (Map) del.get("q"), null, multi, collation, null);
            if (deleteStats.get("n") instanceof Number n) {
                totalDeleted += n.intValue();
            }
        }

        Map<String, Object> result = prepareResult();
        result.put("ok", 1.0);
        result.put("n", totalDeleted);
        commandResults.add(result);
        return ret;
    }

    private int runCommand(DbStatsCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        var m = prepareResult();
        m.put("databases", database.size());
        commandResults.add(m);
        return ret;
    }

    private int runCommand(CurrentOpCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        var m = prepareResult();
        m.put("ok", 0.0);
        m.put("errmsg", "no running ops in memory");
        commandResults.add(m);
        return ret;
    }

    private int runCommand(CreateIndexesCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();

        for (var idx : cmd.getIndexes()) {
            IndexDescription descr = IndexDescription.fromMap(idx);
            createIndex(cmd.getDb(), cmd.getColl(), (Map<String, Object>) idx.get("key"), idx);
        }

        commandResults.add(prepareResult());
        return ret;
    }

    private int runCommand(CreateCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();

        if (cmd.getCapped() != null && cmd.getCapped()) {
            cappedCollections.putIfAbsent(cmd.getDb(), new HashMap<>());
            cappedCollections.get(cmd.getDb()).putIfAbsent(cmd.getColl(), new HashMap<>());
            cappedCollections.get(cmd.getDb()).get(cmd.getColl()).put("size", cmd.getSize());
            cappedCollections.get(cmd.getDb()).get(cmd.getColl()).put("max", cmd.getMax());
        }

        if (cmd.getTimeseries() != null) {
            log.warn("Timeseries collections not supported in memory");
        }

        if (cmd.getPipeline() != null) {
            log.warn("pipeline not supported in memory");
        }

        database.putIfAbsent(cmd.getDb(), new HashMap<>());

        if (database.get(cmd.getDb()).containsKey(cmd.getColl())) {
            // throw new IllegalArgumentException("Collection exists");
            log.warn("Collection already exists...");
        } else {
            database.get(cmd.getDb()).put(cmd.getColl(), new ArrayList<>());
        }

        var m = prepareResult();
        addCursor(cmd.getDb(), cmd.getColl(), m, Arrays.asList(Doc.of()));
        commandResults.add(m);
        return ret;
    }

    private int runCommand(CountMongoCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        var m = prepareResult();
        var cnt = find(cmd.getDb(), cmd.getColl(), cmd.getQuery(), null, null, cmd.getCollation(), 0, 0, false).size();
        m.put("n", cnt);
        m.put("count", cnt);
        commandResults.add(m);
        return ret;
    }

    private int runCommand(CollStatsCommand cmd) {
        /**
         * "ns" : "admin.admin",
         * "size" : 0.0,
         * "count" : 0.0,
         * "numOrphanDocs" : 0.0,
         * "storageSize" : 0.0,
         * "totalSize" : 0.0,
         * "nindexes" : 0.0,
         * "totalIndexSize" : 0.0,
         * "indexDetails" : {
         *
         * },
         * "indexSizes" : {
         *
         * },
         */
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        var m = prepareResult();
        m.put("ns", cmd.getDb() + "." + cmd.getColl());
        var size = VM.current().sizeOf(database.get(cmd.getDb()).get(cmd.getColl()));
        m.put("size", size);
        m.put("storageSize", 0);
        List<Map<String, Object >> indexes = getIndexes(cmd.getDb(), cmd.getColl());
        m.put("nindexes", indexes.size());
        var indexDetails = Doc.of();
        var indexSizes = Doc.of();
        long totalSize = size;
        for (var idx : indexes) {
            String idxName = (String)((Map) idx.get("$options")).get("name");
            indexDetails.put(idxName, idx);
            long sz = VM.current().sizeOf(indexDataByDBCollection.get(cmd.getDb()).get(cmd.getColl())) + VM.current().sizeOf(idx);
            indexSizes.put(idxName, sz);
            totalSize += sz;
        }
        m.put("totalSize", totalSize);
        m.put("indexDetails", indexDetails);
        m.put("indexSizes", indexSizes);
        commandResults.add(m);
        return ret;
    }

    private int runCommand(ClearCollectionCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        database.get(cmd.getDb()).get(cmd.getColl()).clear();
        int ret = commandNumber.incrementAndGet();
        commandResults.add(prepareResult(Doc.of("ok", 1.0)));
        return ret;
    }

    private int runCommand(AggregateMongoCommand cmd) {
        if (cmd.getDb().equals("admin") && cmd.getColl().equals("atlascli")) {
            int ret = commandNumber.incrementAndGet();
            commandResults.add(prepareResult(Doc.of("ok", 0.0, "msg", "not found")));
            return ret;
        }
        throw new IllegalArgumentException("please use morphium for aggregation in Memory!");
    }

    private int runCommand(MongoCommand cmd) {
        throw new IllegalArgumentException("Unhandled command " + cmd.getCommandName() + " class: " + cmd.getClass().getSimpleName());
    }

    private int runCommand(HelloCommand cmd) {
        log.info("Hello Command incoming");
        int ret = commandNumber.incrementAndGet();
        // { "ok" : 1.0, "$clusterTime" : { "clusterTime" : 7129872915129958401,
        // "signature" : { "hash" : [ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 0], "keyId" : 0 } } , "operationTime" : 7129872915129958401 }
        Map<String, Object> helloResponse = new HashMap<>();
        helloResponse.put("helloOk", true);
        helloResponse.put("isWritablePrimary", true);
        helloResponse.put("ismaster", true);  // Critical for driver compatibility
        helloResponse.put("secondary", false);
        helloResponse.put("maxBsonObjectSize", 128 * 1024 * 1024);
        helloResponse.put("maxWriteBatchSize", 100000);
        helloResponse.put("maxWireVersion", 21);
        helloResponse.put("minWireVersion", 0);
        helloResponse.put("localTime", new Date());
        helloResponse.put("connectionId", connectionId.incrementAndGet());
        helloResponse.put("msg", driverName + " - ok");

        var m = addCursor(cmd.getDb(), cmd.getColl(), prepareResult(),
                          Arrays.asList(helloResponse));
        commandResults.add(m);
        return ret;
    }

    private Map<String, Object> addCursor(String db, String coll, Map<String, Object> result, List<Map<String, Object >> data) {
        result.put("cursor", Doc.of("firstBatch", data, "ns", db + "." + coll, "id", 0L));
        return result;
    }

    private long registerCursorBuffer(String namespace, List<Map<String, Object>> allResults, int batchSize) {
        if (allResults == null || allResults.isEmpty()) {
            return 0L;
        }

        if (batchSize <= 0) {
            batchSize = Math.min(allResults.size(), 101);
        }

        if (allResults.size() <= batchSize) {
            return 0L;
        }

        Deque<Map<String, Object>> remaining = new ArrayDeque<>(allResults.subList(batchSize, allResults.size()));
        long cursorId = cursorIdSequence.getAndIncrement();
        activeQueryCursors.put(cursorId, new CursorResultBuffer(remaining, namespace, batchSize));
        return cursorId;
    }

    private List<Map<String, Object>> drainNextBatch(long cursorId, int requestedBatchSize) {
        CursorResultBuffer buffer = activeQueryCursors.get(cursorId);

        if (buffer == null) {
            return Collections.emptyList();
        }

        synchronized (buffer) {
            int size = requestedBatchSize > 0 ? requestedBatchSize : buffer.defaultBatchSize;

            if (size <= 0) {
                size = buffer.defaultBatchSize > 0 ? buffer.defaultBatchSize : 101;
            }

            List<Map<String, Object>> batch = new ArrayList<>(Math.min(size, buffer.remaining.size()));

            for (int i = 0; i < size && !buffer.remaining.isEmpty(); i++) {
                batch.add(buffer.remaining.pollFirst());
            }

            if (buffer.remaining.isEmpty()) {
                activeQueryCursors.remove(cursorId);
            }

            return batch;
        }
    }

    private String namespaceForCursor(long cursorId) {
        CursorResultBuffer buffer = activeQueryCursors.get(cursorId);
        return buffer == null ? null : buffer.namespace;
    }

    private void closeCursor(long cursorId) {
        activeQueryCursors.remove(cursorId);
    }

    private Map<String, Object> prepareResult() {
        return prepareResult(Doc.of());
    }

    private Map<String, Object> prepareResult(Map<String, Object> result) {
        if (!result.containsKey("ok")) {
            result.put("ok", 1.0);
        }

        result.put("$clusterTime", Doc.of("clusterTime", System.currentTimeMillis(), "signature", Doc.of("hash", new byte[20], "keyId", 0)));
        result.put("operationTime", System.currentTimeMillis());
        return result;
    }

    @Override
    public MongoConnection getReadConnection(ReadPreference rp) {
        stats.get(DriverStatsKey.CONNECTIONS_BORROWED).incrementAndGet();
        return this;
    }

    @Override
    public MongoConnection getPrimaryConnection(WriteConcern wc) {
        stats.get(DriverStatsKey.CONNECTIONS_BORROWED).incrementAndGet();
        return this;
    }

    @Override
    public void releaseConnection(MongoConnection con) {
    }

    @Override
    public void closeConnection(MongoConnection con) {
    }

    @Override
    public String getName() {
        return driverName;
    }

    @Override
    public int getMaxBsonObjectSize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void setMaxBsonObjectSize(int maxBsonObjectSize) {
    }

    @Override
    public int getMaxMessageSize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void setMaxMessageSize(int maxMessageSize) {
    }

    @Override
    public int getMaxWriteBatchSize() {
        return Integer.MAX_VALUE;
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
        try (ScanResult scanResult = new ClassGraph()
            // .verbose() // Enable verbose logging
            .enableAnnotationInfo()
            // .enableAllInfo() // Scan classes, methods, fields, annotations
            .scan()) {
            ClassInfoList entities = scanResult.getSubclasses(MongoCommand.class.getName());

            for (ClassInfo info : entities) {
                try {
                    String n = info.getName();
                    Class cls = Class.forName(n);

                    if (Modifier.isAbstract(cls.getModifiers())) {
                        continue;
                    }

                    if (cls.isInterface()) {
                        continue;
                    }

                    Constructor declaredConstructor = cls.getDeclaredConstructor(MongoConnection.class);
                    declaredConstructor.setAccessible(true);
                    var mongoCommand = (MongoCommand<MongoCommand>) declaredConstructor.newInstance(this);
                    commandsCache.put(mongoCommand.getCommandName(), cls);

                    // checking method
                    try {
                        Method m = InMemoryDriver.class.getDeclaredMethod("runCommand", mongoCommand.getClass());
                    } catch (NoSuchMethodException e) {
                        log.error("No runcommand-Method for Command " + mongoCommand.getCommandName() + " / " + mongoCommand.getClass().getSimpleName());
                    } catch (SecurityException e) {
                        log.error("runcommand-Method for Command " + mongoCommand.getCommandName() + " / " + mongoCommand.getClass().getSimpleName() + " not accessible!");
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        for (DriverStatsKey k : DriverStatsKey.values()) {
            stats.put(k, new AtomicDecimal(0));
        }

        database.put("local", new ConcurrentHashMap<>());
        database.put("admin", new ConcurrentHashMap<>());
        database.put("test", new ConcurrentHashMap<>());

        if (exec.isShutdown()) {
            exec = new ScheduledThreadPoolExecutor(2);
        }

        Runnable r = ()-> {
            // Notification of watchers.
            try {
                while (true) {
                    Runnable r1 = eventQueue.poll();

                    if (r1 == null) return;

                    try {
                        r1.run();
                    } catch (Exception e) {
                        //swallow
                    }
                }
            } catch (Throwable e) {
                log.error("Error", e);
            }
        };
        exec.scheduleWithFixedDelay(r, 100, 1, TimeUnit.MILLISECONDS); // check for events every 500ms
        scheduleExpire();
    }

    private void scheduleExpire() {
        expire = exec.scheduleWithFixedDelay(()-> {
            // checking indexes for expire options
            // log.info("Checking expire indices");

            try {
                for (String db : database.keySet()) {
                    for (String coll : database.get(db).keySet()) {
                        // log.info("Checking collection {} - size {}", coll, getCollection(db, coll).size());
                        if (getCollection(db, coll).isEmpty()) {
                            continue;
                        }
                        var idx = getIndexes(db, coll);

                        for (var i : idx) {
                            Map<String, Object> options = (Map<String, Object>) i.get("$options");

                            if (options != null && options.containsKey("expireAfterSeconds")) {
                                // log.info("Found collection candidate for expire...{}.{}", db, coll);

                                var k = new HashMap<>(i);
                                k.remove("$options");
                                var keys = k.keySet().toArray(new String[] {});


                                if (keys.length > 1) {
                                    log.error("Too many keys for expire-index!!!");
                                } else {
                                    try {
                                        Date threshold = new Date(System.currentTimeMillis() - ((int) options.get("expireAfterSeconds")) * 1000);
                                        List<Map<String, Object>> snapshot = new ArrayList<>(getCollection(db, coll));
                                        List<Map<String, Object>> toRemove = new ArrayList<>();
                                        // log.info("Checking {} candidates for key {}", snapshot.size(), keys[0]);

                                        for (Map<String, Object> existing : snapshot) {
                                            Object val = existing.get(keys[0]);

                                            if (val == null) {
                                                // log.info("Objects value for {} is null", keys[0]);
                                                continue;
                                            }
                                            // log.info("Objects value for {} is {}", keys[0], val);

                                            boolean expired;

                                            if (val instanceof Date) {
                                                // log.info("value is of type date: {}", val);
                                                expired = !((Date) val).after(threshold);
                                            } else if (val instanceof Number) {
                                                // log.info("value is of type number: {}", val);
                                                expired = ((Number) val).longValue() <= threshold.getTime();
                                            } else {
                                                log.warn("expireAfterSeconds value is not Number or date: {} of type {}", val, val.getClass().getName());
                                                expired = QueryHelper.matchesQuery(Doc.of(keys[0], Doc.of("$lte", threshold)), existing, null);
                                            }
                                            // log.info("expired: {}", expired);

                                            if (expired) {
                                                toRemove.add(existing);
                                            }
                                        }

                                        if (!toRemove.isEmpty()) {
                                            for (Map<String, Object> o : toRemove) {
                                                getCollection(db, coll).remove(o);
                                            }

                                            updateIndexData(db, coll, null);
                                        }
                                    } catch (Exception e) {
                                        log.error("Error", e);
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Error", e);
            }
        }, 100, expireCheck, TimeUnit.MILLISECONDS);
    }

    public int getExpireCheck() {
        return expireCheck;
    }

    public InMemoryDriver setExpireCheck(int expireCheck) {
        this.expireCheck = expireCheck;

        if (expire != null) {
            expire.cancel(true);
        }

        scheduleExpire();
        return this;
    }

    public void connect(String replicasetName) {
        connect();
    }

    public boolean isConnected() {
        return true;
    }

    @Override
    public String getConnectedTo() {
        return "inMem:0000";
    }

    @Override
    public String getConnectedToHost() {
        return "inMem";
    }

    @Override
    public int getConnectedToPort() {
        return 6666;
    }

    public int getRetriesOnNetworkError() {
        return 1;
    }

    public MorphiumDriver setRetriesOnNetworkError(int r) {
        return this;
    }

    public int getSleepBetweenErrorRetries() {
        return 100;
    }

    public MorphiumDriver setSleepBetweenErrorRetries(int s) {
        return this;
    }

    @Override
    public int getMaxConnections() {
        return 1;
    }

    @Override
    public MorphiumDriver setMaxConnections(int maxConnections) {
        return this;
    }

    @Override
    public int getMinConnections() {
        return 1;
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
    public void setCredentials(String db, String login, String pwd) {
    }

    private <T> T deepClone(T object) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            return (T) ois.readObject();
        } catch (Exception e) {
            log.error("Error", e);
            return null;
        }
    }

    @Override
    public MorphiumTransactionContext startTransaction(boolean autoCommit) {
        if (currentTransaction.get() != null) {
            throw new IllegalArgumentException("transaction in progress");
        }

        InMemTransactionContext ctx = new InMemTransactionContext();
        ctx.setDatabase(deepClone(database));
        currentTransaction.set(ctx);
        return currentTransaction.get();
    }

    @Override
    public boolean isTransactionInProgress() {
        return currentTransaction.get() != null;
    }

    @Override
    public MorphiumTransactionContext getTransactionContext() {
        return currentTransaction.get();
    }

    @Override
    public HelloResult connect(MorphiumDriver drv, String host, int port) throws IOException, MorphiumDriverException {
        return new HelloResult().setHosts(Arrays.asList("inMem")).setHelloOk(true).setLocalTime(new Date()).setMaxBsonObjectSize(Integer.MAX_VALUE).setMe("inMem").setWritablePrimary(true);
    }

    @Override
    public MorphiumDriver getDriver() {
        return this;
    }

    @Override
    public int getSourcePort() {
        return 0;
    }

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
    public boolean isReplicaset() {
        return false;
    }

    //

    public Map<String, Object> getReplsetStatus() {
        return prepareResult(Doc.of("ok", 0.0, "errmsg", "no replicaset"));
    }

    @Override
    public Map<String, Object> getDBStats(String db) {
        Map<String, Object> ret = new ConcurrentHashMap<>();
        ret.put("collections", getDB(db).size());
        return ret;
    }

    //
    // public Map<String,Object> getDBStats(String db) {
    // Map<String,Object> ret = new ConcurrentHashMap<>();
    // ret.put("collections", getDB(db).size());
    // return ret;
    // }
    //
    // @SuppressWarnings("RedundantThrows")
    //
    // public Map<String,Object> getCollStats(String db, String coll) throws
    // MorphiumDriverException {
    // Map<String,Object> ret = new ConcurrentHashMap<>();
    // ret.put("entries", getDB(db).get(coll).size());
    // return null;
    // }

    @SuppressWarnings("RedundantThrows")
    @Override
    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        Map<String, Object> ret = new ConcurrentHashMap<>();
        ret.put("entries", getDB(db).get(coll).size());
        return ret;
    }

    public MorphiumCursor initIteration(String db, String collection, Map<String, Object> query, Map<String, Object> sort, Map<String, Object> projection, int skip, int limit, int batchSize,
                                        ReadPreference readPreference, Collation coll, Map<String, Object> findMetaData) throws MorphiumDriverException {
        final InMemoryCursor inCrs = new InMemoryCursor();
        inCrs.skip = Math.max(0, skip);
        inCrs.limit = Math.max(0, limit);
        inCrs.batchSize = (batchSize == 0 ? 1000 : batchSize);
        inCrs.setCollection(collection);
        inCrs.setDb(db);
        inCrs.setProjection(projection);
        inCrs.setQuery(query == null ? Doc.of() : query);
        inCrs.setFindMetaData(findMetaData);
        inCrs.setReadPreference(readPreference);
        inCrs.setSort(sort);
        inCrs.setCollation(coll);

        int l = inCrs.batchSize;
        if (inCrs.limit != 0) {
            l = Math.min(l, inCrs.limit);
        }
        List<Map<String, Object>> res = find(db, collection, inCrs.getQuery(), sort, projection, coll == null ? null : coll.toQueryObject(), inCrs.skip, l, false);
        inCrs.dataRead = res.size();

        final List<Map<String, Object>> batch = new CopyOnWriteArrayList<>(res);
        final long cursorId = System.currentTimeMillis();
        iterationCursors.put(cursorId, inCrs);

        MorphiumCursor crs = new MorphiumCursor() {
            private int idx = 0;
            @Override
            public Iterator<Map<String, Object>> iterator() { return this; }
            @Override
            public boolean hasNext() { return idx < batch.size(); }
            @Override
            public Map<String, Object> next() { return batch.get(idx++); }
            @Override
            public void close() { iterationCursors.remove(getCursorId()); }
            @Override
            public int available() { return Math.max(0, batch.size() - idx); }
            @Override
            public List<Map<String, Object>> getAll() { return new ArrayList<>(batch.subList(idx, batch.size())); }
            @Override
            public void ahead(int skip) { idx = Math.min(batch.size(), Math.max(0, idx + skip)); }
            @Override
            public void back(int jump) { idx = Math.max(0, idx - Math.max(0, jump)); }
            @Override
            public int getCursor() { return idx; }
            @Override
            public MongoConnection getConnection() { return InMemoryDriver.this; }
        };
        crs.setBatchSize(inCrs.batchSize);
        crs.setCursorId(cursorId);
        crs.setDb(db);
        crs.setCollection(collection);
        crs.setBatch(batch);
        return crs;
    }

    public MorphiumCursor nextIteration(MorphiumCursor crs) throws MorphiumDriverException {
        InMemoryCursor st = iterationCursors.get(crs.getCursorId());
        if (st == null) return null;
        int remainingLimit = (st.limit == 0) ? Integer.MAX_VALUE : (st.limit - st.dataRead);
        if (remainingLimit <= 0) {
            iterationCursors.remove(crs.getCursorId());
            return null;
        }
        int l = Math.min(st.batchSize, remainingLimit);
        int nextSkip = st.skip + st.dataRead;
        List<Map<String, Object>> res = find(st.getDb(), st.getCollection(), st.getQuery(), st.getSort(), st.getProjection(), st.getCollation() == null ? null : st.getCollation().toQueryObject(), nextSkip, l, false);
        if (res == null || res.isEmpty()) {
            iterationCursors.remove(crs.getCursorId());
            return null;
        }
        st.dataRead += res.size();
        final List<Map<String, Object>> batch = new CopyOnWriteArrayList<>(res);
        MorphiumCursor next = new MorphiumCursor() {
            private int idx = 0;
            @Override
            public Iterator<Map<String, Object>> iterator() { return this; }
            @Override
            public boolean hasNext() { return idx < batch.size(); }
            @Override
            public Map<String, Object> next() { return batch.get(idx++); }
            @Override
            public void close() { iterationCursors.remove(getCursorId()); }
            @Override
            public int available() { return Math.max(0, batch.size() - idx); }
            @Override
            public List<Map<String, Object>> getAll() { return new ArrayList<>(batch.subList(idx, batch.size())); }
            @Override
            public void ahead(int skip) { idx = Math.min(batch.size(), Math.max(0, idx + skip)); }
            @Override
            public void back(int jump) { idx = Math.max(0, idx - Math.max(0, jump)); }
            @Override
            public int getCursor() { return idx; }
            @Override
            public MongoConnection getConnection() { return InMemoryDriver.this; }
        };
        next.setCursorId(crs.getCursorId());
        next.setBatchSize(st.batchSize);
        next.setDb(st.getDb());
        next.setCollection(st.getCollection());
        next.setBatch(batch);
        return next;
    }

    public List<Map<String, Object >> find(String db, String collection, Map<String, Object> query, Map<String, Object> sort, Map<String, Object> projection, int skip, int limit)
    throws MorphiumDriverException {
        return find(db, collection, query, sort, projection, null, skip, limit, false);
    }

    @SuppressWarnings({"RedundantThrows", "UnusedParameters"})
    private List<Map<String, Object >> find(String db, String collection, Map<String, Object> query, Map<String, Object> sort, Map<String, Object> projection, Map<String, Object> collation, int skip,
                                            int limit, boolean internal) throws MorphiumDriverException {
        List<Map<String, Object >> partialHitData = new ArrayList<>();

        if (query == null) {
            query = Doc.of();
        }

        // Special handling for map field queries like "stringMap.key1"
        // System.out.println("[DEBUG_LOG] Original query: " + query);
        Map<String, Object> renamedQuery = new LinkedHashMap<>(query);
        for (String key : new ArrayList<>(query.keySet())) {
            if (!key.startsWith("$") && key.contains(".")) {
                String[] parts = key.split("\\.", 2);

                if (parts.length == 2) {
                    String translatedField = camelToSnakeCase(parts[0]);
                    String newKey = translatedField + "." + parts[1];

                    if (!newKey.equals(key)) {
                        Object value = renamedQuery.remove(key);
                        renamedQuery.put(newKey, value);
                    }
                }
            }
        }
        query = renamedQuery;
        // System.out.println("[DEBUG_LOG] Modified query: " + query);
        if (query.containsKey("$and")) {
            // and complex query handling ?!?!?
            List<Map<String, Object >> m = (List<Map<String, Object >> ) query.get("$and");

            if (m != null && !m.isEmpty()) {
                for (Map<String, Object> subquery : m) {
                    List<Map<String, Object >> dataFromIndex = getDataFromIndex(db, collection, subquery);

                    // one and-query result is enough to find candidates!
                    if (dataFromIndex != null) {
                        partialHitData = dataFromIndex;
                        break;
                    }
                }
            }
        }
        else if (query.containsKey("$or")) {
            List<Map<String, Object >> m = (List<Map<String, Object >> ) query.get("$or");
            if (m != null) {
                for (Map<String, Object> subquery : m) {
                    List<Map<String, Object >> dataFromIndex = getDataFromIndex(db, collection, subquery);
                    if (dataFromIndex != null) {
                        partialHitData.addAll(dataFromIndex);
                    }
                }
            }
        }
        else {
            partialHitData = getDataFromIndex(db, collection, query);
        }
        List<Map<String, Object >> data;

        if (partialHitData == null || partialHitData.isEmpty()) {
            data = new ArrayList<>(getCollection(db, collection));
        } else {
            data = partialHitData;
        }
        List<Map<String, Object >> ret = new ArrayList<>();
        int count = 0;

        if (sort != null) {
            Collator coll = QueryHelper.getCollator(collation);
            data.sort((o1, o2)-> {
                for (String f : sort.keySet()) {
                    if (o1.get(f) == null && o2.get(f) == null) {
                        continue;
                    }

                    if (o1.get(f) == null && o2.get(f) != null) {
                        return -1;
                    }

                    if (o1.get(f) != null && o2.get(f) == null) {
                        return 1;
                    }

                    // noinspection unchecked
                    if (sort.get(f) instanceof Integer) {
                        if (coll != null) {
                            var r = (coll.compare(o1.get(f).toString(), o2.get(f).toString())) * ((Integer) sort.get(f));

                            if (r == 0) {
                                continue;
                            }

                            return r;
                        }

                        var r = ((Comparable) o1.get(f)).compareTo(o2.get(f)) * ((Integer) sort.get(f));

                        if (r == 0) {
                            continue;
                        }

                        return r;
                    } else {
                        var r = (coll.compare(o1.toString(), o2.toString()));

                        if (r == 0) {
                            continue;
                        }

                        return r;
                    }
                }
                return 0;
            });
        }

        // noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < data.size(); i++) {
            Map<String, Object> o = data.get(i);
            count++;

            if (count < skip) {
                continue;
            }

            if (!internal) {
                while (true) {
                    try {
                        o = new HashMap<>(o);

                        if (o.get("_id") instanceof ObjectId) {
                            o.put("_id", new MorphiumId((ObjectId) o.get("_id")));
                        }

                        break;
                    } catch (ConcurrentModificationException c) {
                        // retry until it works
                    }
                }
            }

            if (QueryHelper.matchesQuery(query, o, collation)) {
                if (o == null) {
                    o = new HashMap<>();
                }

                // apply projection if requested
                if (projection != null && !projection.isEmpty()) {
                    Map<String, Object> projected = new HashMap<>();
                    boolean hasInclude = projection.values().stream().anyMatch(v -> {
                        if (v instanceof Number) return ((Number) v).intValue() == 1;
                        if (v instanceof Boolean) return (Boolean) v;
                        if (v instanceof Map && (((Map) v).containsKey("$slice") || ((Map) v).containsKey("$elemMatch") || ((Map) v).containsKey("$meta"))) return true;
                        return false;
                    });
                    boolean hasExclude = projection.values().stream().anyMatch(v -> {
                        if (v instanceof Number) return ((Number) v).intValue() == 0;
                        if (v instanceof Boolean) return !(Boolean) v;
                        return false;
                    });

                    if (hasInclude && !hasExclude) {
                        // include only specified fields; _id included by default unless explicitly set to 0
                        for (var e : projection.entrySet()) {
                            var k = e.getKey();
                            var v = e.getValue();
                            boolean include = (v instanceof Number && ((Number) v).intValue() == 1) || (v instanceof Boolean && (Boolean) v) || (v instanceof Map);
                            if (!include) continue;

                            if (v instanceof Map && ((Map) v).containsKey("$slice")) {
                                Object arr = getByPath(o, k);
                                Object sliced = applySlice(arr, ((Map) v).get("$slice"));
                                if (sliced != null) setByPath(projected, k, sliced);
                                continue;
                            }
                            if (v instanceof Map && ((Map) v).containsKey("$elemMatch")) {
                                Object arr = getByPath(o, k);
                                Object em = applyElemMatchProjection(k, arr, (Map) ((Map) v).get("$elemMatch"));
                                if (em != null) setByPath(projected, k, em);
                                continue;
                            }
                            Object val = getByPath(o, k);
                            boolean fieldExists = containsByPath(o, k);
                            if (fieldExists) setByPath(projected, k, val);
                        }
                        if (!projection.containsKey("_id") || truthy(projection.get("_id"))) {
                            if (o.containsKey("_id")) projected.put("_id", o.get("_id"));
                        }
                        o = projected;
                    } else {
                        // exclusion style: start with full doc and remove excluded fields
                        Map<String, Object> copy = deepCopyDoc(o);
                        for (var e : projection.entrySet()) {
                            var k = e.getKey();
                            var v = e.getValue();
                            boolean exclude = (v instanceof Number && ((Number) v).intValue() == 0) || (v instanceof Boolean && !(Boolean) v);
                            if (exclude) {
                                removeByPath(copy, k);
                            }
                        }
                        o = copy;
                    }
                }

                ret.add(o);
            }

            if (limit > 0 && ret.size() >= limit) {
                break;
            }

            // todo add projection
        }
        return new ArrayList<>(ret);
    }

    // ---------- Projection helpers (dot-path include/exclude, $slice, $elemMatch) ----------
    private static Object getByPath(Map<String, Object> doc, String path) {
        if (doc == null || path == null) return null;
        String[] parts = path.split("\\.");
        Object cur = doc;
        for (String p : parts) {
            if (!(cur instanceof Map)) {
                return null;
            }
            cur = ((Map) cur).get(p);
            if (cur == null) return null;
        }
        return cur;
    }

    private static boolean containsByPath(Map<String, Object> doc, String path) {
        if (doc == null || path == null) return false;
        String[] parts = path.split("\\.");
        Map<String, Object> cur = doc;
        for (int i = 0; i < parts.length - 1; i++) {
            String p = parts[i];
            if (!cur.containsKey(p) || !(cur.get(p) instanceof Map)) {
                return false;
            }
            cur = (Map<String, Object>) cur.get(p);
        }
        // Check if the final field exists (even if it's null)
        return cur.containsKey(parts[parts.length - 1]);
    }

    private static void setByPath(Map<String, Object> target, String path, Object value) {
        String[] parts = path.split("\\.");
        Map cur = target;
        for (int i = 0; i < parts.length - 1; i++) {
            Object n = cur.get(parts[i]);
            if (!(n instanceof Map)) {
                n = new HashMap<>();
                cur.put(parts[i], n);
            }
            cur = (Map) n;
        }
        cur.put(parts[parts.length - 1], value);
    }

    private static void removeByPath(Map<String, Object> target, String path) {
        String[] parts = path.split("\\.");
        Map cur = target;
        for (int i = 0; i < parts.length - 1; i++) {
            Object n = cur.get(parts[i]);
            if (!(n instanceof Map)) {
                return;
            }
            cur = (Map) n;
        }
        cur.remove(parts[parts.length - 1]);
    }

    private static Map<String, Object> deepCopyDoc(Map<String, Object> src) {
        Map<String, Object> out = new HashMap<>();
        for (var e : src.entrySet()) {
            Object v = e.getValue();
            if (v instanceof Map) {
                v = deepCopyDoc((Map<String, Object>) v);
            } else if (v instanceof List) {
                v = new ArrayList<>((List) v);
            }
            out.put(e.getKey(), v);
        }
        return out;
    }

    private static Object applySlice(Object arrayVal, Object sliceSpec) {
        if (!(arrayVal instanceof List)) return null;
        List lst = (List) arrayVal;
        if (sliceSpec instanceof Number) {
            int n = ((Number) sliceSpec).intValue();
            if (n >= 0) return new ArrayList(lst.subList(0, Math.min(n, lst.size())));
            int from = Math.max(0, lst.size() + n);
            return new ArrayList(lst.subList(from, lst.size()));
        } else if (sliceSpec instanceof List && ((List) sliceSpec).size() == 2) {
            Object s0 = ((List) sliceSpec).get(0);
            Object s1 = ((List) sliceSpec).get(1);
            if (s0 instanceof Number && s1 instanceof Number) {
                int skip = ((Number) s0).intValue();
                int lim = ((Number) s1).intValue();
                int from = Math.max(0, Math.min(skip, lst.size()));
                int to = Math.max(from, Math.min(from + lim, lst.size()));
                return new ArrayList(lst.subList(from, to));
            }
        }
        return null;
    }

    private static Object applyElemMatchProjection(String field, Object arrayVal, Map<String, Object> cond) {
        if (!(arrayVal instanceof List)) return null;
        List lst = (List) arrayVal;
        for (Object el : lst) {
            boolean matches;
            if (el instanceof Map) {
                matches = QueryHelper.matchesQuery(cond, (Map<String, Object>) el, null);
            } else {
                Map<String, Object> wrapper = Doc.of("value", el);
                matches = QueryHelper.matchesQuery(Doc.of("value", cond), wrapper, null);
            }
            if (matches) {
                return new ArrayList<>(List.of(el));
            }
        }
        return new ArrayList<>();
    }

    private List<Map<String, Object >> getDataFromIndex(String db, String collection, Map<String, Object> query) {
        List<Map<String, Object >> ret = null;
        int bucketId = 0;
        StringBuilder fieldList = new StringBuilder();
        for (Map<String, Object> idx : getIndexes(db, collection)) {
            if (idx.size() > query.size()) {
                continue;
            } // index has more fields

            boolean found = true;
            // values.clear();
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
                // all keys in this index are found in query
                // log.debug("Index hit");
                String fields = fieldList.toString();
                Map<Integer, List<Map<String, Object >>> indexDataForCollection = getIndexDataForCollection(db, collection, fields);
                ret = indexDataForCollection.get(bucketId);
                if (ret == null || ret.size() == 0) {
                    // no direkt hit, need to use index data
                    //
                    // log.debug("indirect Index hit - fields: "+fields+" Index size:
                    // "+indexDataForCollection.size());
                    ret = new ArrayList<>();

                    //// long start = System.currentTimeMillis();
                    for (Map.Entry<Integer, List<Map<String, Object >>> k : indexDataForCollection.entrySet()) {
                        for (Map<String, Object> o : k.getValue()) {
                            if (QueryHelper.matchesQuery(query, o, null)) {
                                ret.add(o);
                            }
                        }

                        // ret.addAll(indexDataForCollection.get(k.getKey()));
                    }

                    //// long dur = System.currentTimeMillis() - start;
                    //// log.info("Duration index assembling: " + dur + "ms - "+ret.size());
                    if (ret.size() == 0) {
                        // //log.error("Index miss although index exists");
                        ret = null;
                    }
                }
                break;
            }
        }
        return ret;
    }

    public long count(String db, String collection, Map<String, Object> query, Collation collation, ReadPreference rp) throws MorphiumDriverException {
        List<Map<String, Object >> d = getCollection(db, collection);
        List<Map<String, Object >> data = new CopyOnWriteArrayList<>(d);

        if (query.isEmpty()) {
            return data.size();
        }
        long cnt = 0;

        for (Map<String, Object> o : data) {
            if (QueryHelper.matchesQuery(query, o, collation == null ? null : collation.toQueryObject())) {
                cnt++;
            }
        }
        return cnt;
    }

    public long estimatedDocumentCount(String db, String collection, ReadPreference rp) throws MorphiumDriverException {
        return getCollection(db, collection).size();
    }

    public List<Map<String, Object >> findByFieldValue(String db, String coll, String field, Object value) throws MorphiumDriverException {
        List<Map<String, Object >> ret = new ArrayList<>();
        List<Map<String, Object >> data = new CopyOnWriteArrayList<>(getCollection(db, coll));

        for (Map<String, Object> obj : data) {
            // MongoDB behavior: when searching for null, only match documents where field exists and is null
            // Missing fields should not match null queries
            if (value == null) {
                // For null value queries, field must exist and be null
                if (obj.containsKey(field) && obj.get(field) == null) {
                    Map<String, Object> add = new HashMap<>(obj);

                    if (add.get("_id") instanceof ObjectId) {
                        add.put("_id", new MorphiumId((ObjectId) add.get("_id")));
                    }

                    ret.add(add);
                }
            } else {
                // For non-null value queries, field must exist and equal the value
                if (obj.containsKey(field) && Objects.equals(obj.get(field), value)) {
                    Map<String, Object> add = new HashMap<>(obj);

                    if (add.get("_id") instanceof ObjectId) {
                        add.put("_id", new MorphiumId((ObjectId) add.get("_id")));
                    }

                    ret.add(add);
                }
            }
        }

        return ret;
    }

    // public Map<IndexKey, List<Map<String, Object>>>
    // getIndexDataForCollection(String db, String collection, String fields) {
    public Map<Integer, List<Map<String, Object >>> getIndexDataForCollection(String db, String collection, String fields) {
        indexDataByDBCollection.putIfAbsent(db, new ConcurrentHashMap<>());
        indexDataByDBCollection.get(db).putIfAbsent(collection, new ConcurrentHashMap<>());
        indexDataByDBCollection.get(db).get(collection).putIfAbsent(fields, new HashMap<>());
        return indexDataByDBCollection.get(db).get(collection).get(fields);
    }

    public synchronized List<Map<String, Object >> insert(String db, String collection, List<Map<String, Object >> objs, Map<String, Object> wc) throws MorphiumDriverException {
        int errors = 0;
        objs = new ArrayList<>(objs);
        List<Map<String, Object >> writeErrors = new ArrayList<>();
        // check for unique index
        List<Map<String, Object >> indexes = getIndexes(db, collection);
        if (indexes != null && !indexes.isEmpty()) {
            for (var idx : indexes) {
                if (idx.containsKey("$options")) {
                    Map<String, Object> options = (Map<String, Object>) idx.get("$options");

                    if (options.containsKey("unique") && (options.get("unique").equals("true") || options.get("unique").equals(true))) {
                        // checking fields
                        Map<String, Object> indexKey = new HashMap<>(idx);

                        for (var o : objs) {
                            var q = Doc.of();

                            for (String k : indexKey.keySet()) {
                                if (k.startsWith("$")) {
                                    continue;
                                }

                                q.put(k, o.get(k));
                            }

                            if (q.size() != 1) {
                                //need to add and query
                                List<Map<String, Object >> and = new ArrayList();
                                for (var e : q.entrySet()) {
                                    and .add(Doc.of(e.getKey(), e.getValue()));
                                }
                                q = Doc.of("$and", and);
                            }

                            if (find(db, collection, q, null, null, 0, 0).size() > 0) {
                                log.error("Cannot store - unique index!");
                                writeErrors.add(o);
                            }
                        }

                        errors = errors + writeErrors.size();
                        objs.removeAll(writeErrors);
                    }
                }
            }
        }

        for (Map<String, Object> o : objs) {
            if (o.get("_id") != null) {
                Map<Integer, List<Map<String, Object >>> id = getIndexDataForCollection(db, collection, "_id");
                int bucketId = iterateBucketId(0, o.get("_id"));
                if (id != null && id.containsKey(bucketId)) {
                    for (Map<String, Object> objectMap : id.get(bucketId)) {
                        if (objectMap.get("_id").equals(o.get("_id"))) {
                            throw new MorphiumDriverException("Duplicate _id! " + o.get("_id"), null);
                        }
                    }
                }
            }

            o.putIfAbsent("_id", new ObjectId());
        }
        var collectionData = getCollection(db, collection);
        if (cappedCollections.containsKey(db) && cappedCollections.get(db).containsKey(collection)) {
            while (!collectionData.isEmpty() && cappedCollections.get(db).get(collection).containsKey("max")
                    && cappedCollections.get(db).get(collection).get("max") < collectionData.size() + objs.size()) {
                collectionData.remove(0);
            }
            while (collectionData.size() > 0 && cappedCollections.get(db).get(collection).get("size") < VM.current().sizeOf(collectionData) + VM.current().sizeOf(objs)) {
                collectionData.remove(0);
            }

            while (objs.size() > 0 && cappedCollections.get(db).get(collection).containsKey("max") && collectionData.size() + objs.size() > cappedCollections.get(db).get(collection).get("max")) {
                objs.remove(0);
            }

            while (objs.size() > 0 && cappedCollections.get(db).get(collection).containsKey("size")
                    && VM.current().sizeOf(collectionData) + VM.current().sizeOf(objs.size()) > cappedCollections.get(db).get(collection).get("size")) {
                objs.remove(0);
            }
        }
        collectionData.addAll(objs);

        for (int i = 0; i < objs.size(); i++) {
            Map<String, Object> o = objs.get(i);
            List<Map<String, Object >> idx = indexes;
            Map<String, Map<Integer, List<Map<String, Object >> >> indexData = indexDataByDBCollection.get(db).get(collection);

            for (Map<String, Object> ix : idx) {
                int bucketId = 0;
                StringBuilder fieldNames = new StringBuilder();

                for (String k : ix.keySet()) {
                    bucketId = iterateBucketId(bucketId, o.get(k));
                    fieldNames.append(k);
                }

                String fn = fieldNames.toString();
                indexData.putIfAbsent(fn, new HashMap<>());
                indexData.get(fn).putIfAbsent(bucketId, new ArrayList<>());
                indexData.get(fn).get(bucketId).add(o);
            }
            // _id index
            int buckedId = iterateBucketId(0, o.get("_id"));
            indexData.putIfAbsent("_id", new HashMap<>());
            indexData.get("_id").putIfAbsent(buckedId, new ArrayList<>());
            indexData.get("_id").get(buckedId).add(o);
            notifyWatchers(db, collection, "insert", o);
        }
        return writeErrors;
    }

    private Integer iterateBucketId(int bucketId, Object o) {
        if (o == null) {
            return bucketId + 1;
        }

        return (bucketId + o.hashCode());
    }

    private static boolean truthy(Object v) {
        if (v == null) return true;
        if (v instanceof Number) return ((Number) v).intValue() != 0;
        if (v instanceof Boolean) return (Boolean) v;
        return true;
    }

    private static String camelToSnakeCase(String camelCase) {
        if (camelCase == null) return null;
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < camelCase.length(); i++) {
            char c = camelCase.charAt(i);
            if (Character.isUpperCase(c)) {
                result.append('_').append(Character.toLowerCase(c));
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    public synchronized Map<String, Integer> store(String db, String collection, List<Map<String, Object >> objs, Map<String, Object> wc) throws MorphiumDriverException {
        Map<String, Integer> ret = new HashMap<>();
        int upd = 0;
        int inserted = 0;
        int total = objs.size();
        // System.out.println("[DEBUG_LOG] store method called with db=" + db + ", collection=" + collection + ", objs=" + objs);

        for (Map<String, Object> o : objs) {
            if (o.get("_id") == null) {
                o.put("_id", new MorphiumId());
                // enforce unique indexes before insert
                enforceUniqueOrThrow(db, collection, o);
                getCollection(db, collection).add(o);
                inserted++;
                continue;
            }

            List<Map<String, Object >> srch = findByFieldValue(db, collection, "_id", o.get("_id"));
            if (!srch.isEmpty()) {
                Map<String, Object> previous = deepCopyDoc(srch.get(0));
                getCollection(db, collection).remove(srch.get(0));
                // enforce unique indexes before replacing
                enforceUniqueOrThrow(db, collection, o);
                upd++;
                notifyWatchers(db, collection, "replace", o, null, null, previous);
            } else {
                // unique check for insert
                enforceUniqueOrThrow(db, collection, o);
                notifyWatchers(db, collection, "insert", o);
                inserted++;
            }
            getCollection(db, collection).add(o);
            List<Map<String, Object >> idx = getIndexes(db, collection);
            // Map<String, Object> indexValues = new HashMap<>();
            int bucketId = 0;
            StringBuilder fields = new StringBuilder();

            for (Map<String, Object> i : idx) {
                for (String k : i.keySet()) {
                    // indexValues.put(k, o.get(k));
                    bucketId = iterateBucketId(bucketId, o.get(k));
                    fields.append(k);
                }

                // IndexKey key = new IndexKey(indexValues);
                indexDataByDBCollection.putIfAbsent(db, new ConcurrentHashMap<>());
                indexDataByDBCollection.get(db).putIfAbsent(collection, new ConcurrentHashMap<>());
                indexDataByDBCollection.get(db).get(collection).putIfAbsent(fields.toString(), new HashMap<>());
                indexDataByDBCollection.get(db).get(collection).get(fields.toString()).putIfAbsent(bucketId, new ArrayList<>());
                indexDataByDBCollection.get(db).get(collection).get(fields.toString()).get(bucketId).add(o);
            }
        }

        ret.put("matched", upd);
        ret.put("updated", upd);
        ret.put("inserted", inserted);
        ret.put("n", upd + inserted);
        // System.out.println("[DEBUG_LOG] store method returning stats: " + ret);
        return ret;
    }

    private Map<String, List<Map<String, Object >>> getDB(String db) {
        if (currentTransaction.get() == null) {
            database.putIfAbsent(db, new ConcurrentHashMap<>());
            return database.get(db);
        }
        else {
            // noinspection unchecked
            currentTransaction.get().getDatabase().putIfAbsent(db, new ConcurrentHashMap<>());
            // noinspection unchecked
            return (Map<String, List<Map<String, Object >>>) currentTransaction.get().getDatabase().get(db);
        }
    }

    public void closeIteration(MorphiumCursor crs) {
    }

    @Override
    public Map<String, Object> killCursors(String db, String coll, long... ids) throws MorphiumDriverException {
        for (long i : ids) {
            cursors.remove(i);
        }

        return prepareResult();
    }

    public OpMsg readNextMessage(int timeout) throws MorphiumDriverException {
        OpMsg msg = new OpMsg();
        msg.setMessageId(0);
        Map<String, Object> o = new HashMap<>(commandResults.remove(0));
        msg.setFirstDoc(o);
        return msg;
    }

    @Override
    public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
        if (commandResults.isEmpty()) return null;

        return commandResults.remove(0);
    }

    @SuppressWarnings("ConstantConditions")

    public synchronized Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> sort, Map<String, Object> op, boolean multiple, boolean upsert,
            Map<String, Object> collation, Map<String, Object> wc) throws MorphiumDriverException {
        List<Map<String, Object >> lst = find(db, collection, query, sort, null, collation, 0, multiple ? 0 : 1, true);
        int matchedCount = (lst == null) ? 0 : lst.size();
        boolean insert = false;
        int count = 0;
        if (lst == null) {
            lst = new ArrayList<>();
        }

        if (upsert && lst.isEmpty()) {
            lst.add(new HashMap<>());

            for (String k : query.keySet()) {
                if (k.startsWith("$")) {
                    continue;
                }

                if (query.get(k) != null) {
                    lst.get(0).put(k, query.get(k));
                } else {
                    lst.get(0).remove(k);
                }
            }

            insert = true;
        }
        // Track modifications per object and collect upsert ids
        Set<Object> modified = new HashSet<>();
        List<Object> upsertedIds = new ArrayList<>();
        int modifiedCount = 0;

        for (Map<String, Object> obj : lst) {
            // keep a deep copy to detect if the object actually changed
            Map<String, Object> original = deepClone(obj);
            if (original == null) {
                original = new HashMap<>(obj); // fallback
            }
            for (String operand : op.keySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> cmd = (Map<String, Object>) op.get(operand);

                switch (operand) {
                    case "$set":

                        // $set:{"field":"value", "other_field": 123}
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            // Process all entries, including null values (unlike $unset which removes fields)
                            var v = entry.getValue();

                            if (v instanceof Map) {
                                try {
                                    v = Expr.parse(v).evaluate(obj);
                                } catch (Exception e) {
                                    // swallow
                                }
                            }

                            if (entry.getKey().contains(".")) {
                                String[] path = entry.getKey().split("\\.");
                                var current = obj;
                                Map lastEl = null;

                                for (String p : path) {
                                    if (current.get(p) != null) {
                                        if (current.get(p) instanceof Map) {
                                            ((Map) current.get(p)).put(p, Doc.of());
                                        } else {
                                            log.error("could not set value! " + p);
                                            break;
                                        }
                                    } else {
                                        current.put(p, Doc.of());
                                    }

                                    lastEl = current;
                                    current = (Map) current.get(p);
                                }

                                lastEl.put(path[path.length - 1], v);
                            } else {
                                obj.put(entry.getKey(), v);
                            }
                            // } else {
                            //     if (entry.getKey().contains(".")) {
                            //         String[] path = entry.getKey().split("\\.");
                            //         var current = obj;
                            //         Map lastEl = null;

                            //         for (String p : path) {
                            //             lastEl = current;

                            //             if (current.get(p) != null) {
                            //                 current = (Map) current.get(p);
                            //             } else {
                            //                 break;
                            //             }
                            //         }

                            //         if (lastEl != null) {
                            //             lastEl.remove(path[path.length - 1]);
                            //         }
                            //     } else {
                            //         obj.remove(entry.getKey());
                            //     }
                            // }
                        }

                        break;

                    case "$unset":

                        // $unset: { <field1>: "", ... }
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            obj.remove(entry.getKey());
                        }

                        break;

                    case "$inc":

                        // $inc: { <field1>: <amount1>, <field2>: <amount2>, ... }
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            Object value = obj.get(entry.getKey());

                            if (value == null) value = 0;

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

                            Object currentValue = obj.get(entry.getKey());
                            if (!Objects.equals(currentValue, value)) {
                                modified.add(obj.get("_id"));
                            }

                            if (value != null) {
                                obj.put(entry.getKey(), value);
                            } else {
                                obj.remove(entry.getKey());
                            }
                        }

                        break;

                    case "$currentDate":
                        // TODO: Fix it
                        // $currentDate: { <field1>: <typeSpecification1>, ... }
                        // log.info("current date");
                        obj.put((String) cmd.keySet().toArray()[0], new Date());
                        break;

                    case "$mul":

                        // $mul: { <field1>: <number1>, ... }
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

                            Object currentValue = obj.get(entry.getKey());
                            if (!Objects.equals(currentValue, value)) {
                                modified.add(obj.get("_id"));
                            }

                            if (value != null) {
                                obj.put(entry.getKey(), value);
                            } else {
                                obj.remove(entry.getKey());
                            }
                        }

                        break;

                    case "$rename":

                        // $rename: { <field1>: <newName1>, <field2>: <newName2>, ... }
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            if (obj.get(entry.getKey()) != null) {
                                obj.put((String) entry.getValue(), obj.get(entry.getKey()));
                            } else {
                                obj.remove(entry.getValue());
                            }

                            obj.remove(entry.getKey());
                            modified.add(obj.get("_id"));
                        }

                        break;

                    case "$min":

                        // $min: { <field1>: <value1>, ... }
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            Comparable value = (Comparable) obj.get(entry.getKey());

                            // noinspection unchecked
                            if (value.compareTo(entry.getValue()) > 0 && entry.getValue() != null) {
                                modified.add(obj.get("_id"));
                                obj.put(entry.getKey(), entry.getValue());
                            }
                        }

                        break;

                    case "$max":

                        // $max: { <field1>: <value1>, ... }
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            Comparable value = (Comparable) obj.get(entry.getKey());

                            // noinspection unchecked
                            if (value.compareTo(entry.getValue()) < 0 && entry.getValue() != null) {
                                obj.put(entry.getKey(), entry.getValue());
                                modified.add(obj.get("_id"));
                            }
                        }

                        break;

                    case "$pull":

                        // $pull: { <field1>: <value|condition>, <field2>: <value|condition>, ... }
                        // Examples:
                        // $pull: { fruits: { $in: [ "apples", "oranges" ] }, vegetables: "carrots" }
                        // $pull: { votes: { $gte: 6 } }
                        // $pull: { results: {$elemMatch: { score: 8 , item: "B" } }}
                        // $pull: { results: { answers: { $elemMatch: { q: 2, a: { $gte: 8 } } } } }
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            List values = new ArrayList((List) obj.get(entry.getKey()));
                            Map<String, Object> subquery = Doc.of(entry.getKey(), entry.getValue());
                            List filteredValues = new ArrayList();

                            for (Object value : values) {
                                if (!QueryHelper.matchesQuery(subquery, Doc.of(entry.getKey(), value), null)) {
                                    filteredValues.add(value);
                                }
                            }

                            boolean valueIsChanged = !filteredValues.containsAll(values) || !values.containsAll(filteredValues);

                            if (valueIsChanged) {
                                modified.add(obj.get("_id"));
                            }

                            obj.put(entry.getKey(), filteredValues);
                        }

                        break;

                    case "$pullAll":

                        // $pullAll: { <field1>: [ <value1>, <value2> ... ], ... }
                        // Examples:
                        // $pullAll: { scores: [ 0, 5 ] }
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            List v = new ArrayList((List) obj.get(entry.getKey()));
                            List objectsToBeDeleted = (List) entry.getValue();
                            boolean valueIsChanged = objectsToBeDeleted.stream().anyMatch(object->v.contains(object));

                            if (valueIsChanged) {
                                modified.add(obj.get("_id"));
                            }

                            v.removeAll(objectsToBeDeleted);
                            obj.put(entry.getKey(), v);
                        }

                        break;

                    case "$addToSet":
                    case "$push":
                    case "$pushAll":

                        // $addToSet: { <field1>: <value1>, ... }
                        //$push:{"field":"value"}
                        //$pushAll:{"field":["value","value",...]}
                        for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                            String field = entry.getKey();
                            List v;
                            boolean created = false;

                            if (field.contains(".")) {
                                Object existing = getByPath(obj, field);

                                if (existing == null) {
                                    v = new ArrayList<>();
                                    setByPath(obj, field, v);
                                    created = true;
                                } else if (existing instanceof List) {
                                    v = (List) existing;
                                } else {
                                    throw new MorphiumDriverException("Cannot apply " + operand + " to non-array field '" + field + "'");
                                }
                            } else {
                                Object existing = obj.get(field);

                                if (existing == null) {
                                    v = new ArrayList<>();
                                    obj.put(field, v);
                                    created = true;
                                } else if (existing instanceof List) {
                                    v = (List) existing;
                                } else {
                                    throw new MorphiumDriverException("Cannot apply " + operand + " to non-array field '" + field + "'");
                                }
                            }

                            boolean changed = created;
                            Object rawValue = entry.getValue();

                            List<Object> valuesToAdd = new ArrayList<>();
                            Integer position = null;
                            Integer slice = null;

                            if (rawValue instanceof Map valueMap && valueMap.containsKey("$each")) {
                                Object eachVal = valueMap.get("$each");

                                if (!(eachVal instanceof List)) {
                                    throw new MorphiumDriverException("$each requires an array value");
                                }

                                valuesToAdd.addAll((List<Object>) eachVal);

                                if (valueMap.containsKey("$position") && valueMap.get("$position") instanceof Number) {
                                    position = ((Number) valueMap.get("$position")).intValue();
                                }

                                if (valueMap.containsKey("$slice") && valueMap.get("$slice") instanceof Number) {
                                    slice = ((Number) valueMap.get("$slice")).intValue();
                                }
                            } else if (operand.equals("$pushAll") && rawValue instanceof List) {
                                valuesToAdd.addAll((List<Object>) rawValue);
                            } else {
                                valuesToAdd.add(rawValue);
                            }

                            if (operand.equals("$addToSet")) {
                                for (Object elem : valuesToAdd) {
                                    if (!v.contains(elem)) {
                                        v.add(elem);
                                        changed = true;
                                    }
                                }
                            } else {
                                if (position != null) {
                                    int insertAt = Math.min(Math.max(position, 0), v.size());
                                    for (Object elem : valuesToAdd) {
                                        v.add(insertAt++, elem);
                                    }
                                } else {
                                    v.addAll(valuesToAdd);
                                }

                                if (!valuesToAdd.isEmpty()) {
                                    changed = true;
                                }

                                if (slice != null) {
                                    if (slice >= 0) {
                                        while (v.size() > slice) {
                                            v.remove(v.size() - 1);
                                        }
                                    } else {
                                        while (v.size() > Math.abs(slice)) {
                                            v.remove(0);
                                        }
                                    }
                                }
                            }

                            if (changed) {
                                modified.add(obj.get("_id"));
                            }
                        }

                        break;

                    default:
                        throw new RuntimeException("unknown operand " + operand);
                }
            }

            // determine if this document has actually changed
            boolean objChanged;
            try {
                objChanged = !Objects.equals(original, obj) || modified.contains(obj.get("_id"));
            } catch (Exception ignored) {
                objChanged = !modified.isEmpty();
            }

            if (!insert && objChanged) {
                modifiedCount++;
            }

            if (!insert) {
                // enforce uniqueness after modification; rollback if violation
                try {
                    enforceUniqueOrThrow(db, collection, obj);
                } catch (MorphiumDriverException ex) {
                    // rollback
                    obj.clear();
                    obj.putAll(original);
                    throw ex;
                }
                Map<String, Object> beforeImage = deepCopyDoc(original);
                Map<String, Object> updatedMap = computeUpdatedFields(original, obj);
                List<String> removedList = computeRemovedFields(original, obj);
                notifyWatchers(db, collection, "update", obj, updatedMap, removedList, beforeImage);
            }
        }
        if (insert) {
            store(db, collection, lst, wc);
            // collect upserted ids (single upsert typical)
            for (Map<String, Object> d : lst) {
                if (d.get("_id") != null) {
                    upsertedIds.add(d.get("_id"));
                }
            }
        }
        indexDataByDBCollection.get(db).remove(collection);
        updateIndexData(db, collection, null);
        Doc res = Doc.of("matched", (Object) matchedCount, "nModified", modifiedCount, "modified", modifiedCount);
        if (!upsertedIds.isEmpty()) {
            res.put("upsertedIds", upsertedIds);
        }
        return res;
    }

    private void notifyWatchers(String db, String collection, String op, Map doc) {
        notifyWatchers(db, collection, op, doc, null, null, null);
    }

    private void notifyWatchers(String db, String collection, String op, Map doc, Map<String, Object> updatedFields, List<String> removedFields) {
        notifyWatchers(db, collection, op, doc, updatedFields, removedFields, null);
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
     */
    private void notifyWatchers(String db, String collection, String op, Map doc, Map<String, Object> updatedFields, List<String> removedFields, Map<String, Object> beforeDocument) {
        Runnable r = ()-> {
            ChangeStreamEventInfo eventInfo = buildChangeStreamEvent(db, collection, op, doc, updatedFields, removedFields, beforeDocument);

            if (eventInfo == null) {
                return;
            }

            changeStreamHistory.addLast(eventInfo);

            while (changeStreamHistory.size() > CHANGE_STREAM_HISTORY_LIMIT) {
                changeStreamHistory.pollFirst();
            }

            dispatchEvent(eventInfo);
        };
        eventQueue.add(r);
    }

    private ChangeStreamEventInfo buildChangeStreamEvent(String db, String collection, String op, Map doc, Map<String, Object> updatedFields, List<String> removedFields, Map<String, Object> beforeDocument) {
        Map<String, Object> newDocument = cloneAndNormalizeDocument((Map<String, Object>) doc);
        Map<String, Object> previousDocument = cloneAndNormalizeDocument((Map<String, Object>) beforeDocument);

        Map<String, Object> event = new LinkedHashMap<>();
        long token = changeStreamSequence.incrementAndGet();
        event.put("_id", createResumeToken(token));
        event.put("operationType", op);
        Map<String, String> ns = new HashMap<>();
        ns.put("db", db);
        if (collection != null) {
            ns.put("coll", collection);
        }
        event.put("ns", ns);
        long clusterTime = System.currentTimeMillis();
        event.put("clusterTime", clusterTime);
        event.put("txnNumber", txn.incrementAndGet());

        if (newDocument != null) {
            event.put("fullDocument", newDocument);
        }

        if (previousDocument != null) {
            event.put("fullDocumentBeforeChange", previousDocument);
        }

        Object documentKey = extractDocumentKey(newDocument, previousDocument);

        if (documentKey != null) {
            event.put("documentKey", Doc.of("_id", documentKey));
        }

        if ("update".equals(op)) {
            Map<String, Object> updated = updatedFields != null ? updatedFields : computeUpdatedFields(previousDocument, newDocument);
            List<String> removed = removedFields != null ? removedFields : computeRemovedFields(previousDocument, newDocument);
            Map<String, Object> description = new LinkedHashMap<>();
            description.put("updatedFields", updated == null ? Map.of() : updated);
            description.put("removedFields", removed == null ? List.of() : removed);
            event.put("updateDescription", description);
        }

        return new ChangeStreamEventInfo(token, db, collection, Collections.unmodifiableMap(event), clusterTime);
    }

    private void dispatchEvent(ChangeStreamEventInfo eventInfo) {
        deliverToSubscribers(changeStreamSubscribers.get(eventInfo.db), eventInfo);

        if (eventInfo.collection != null) {
            deliverToSubscribers(changeStreamSubscribers.get(eventInfo.db + "." + eventInfo.collection), eventInfo);
        }
    }

    private void deliverToSubscribers(List<ChangeStreamSubscription> subscriptions, ChangeStreamEventInfo eventInfo) {
        if (subscriptions == null || subscriptions.isEmpty()) {
            return;
        }

        for (ChangeStreamSubscription subscription : subscriptions) {
            if (!subscription.isActive()) {
                continue;
            }

            if (!subscription.matches(eventInfo)) {
                continue;
            }

            subscription.deliver(eventInfo);

            if (!subscription.isActive()) {
                unregisterSubscription(subscription);
            }
        }
    }

    private void registerSubscription(ChangeStreamSubscription subscription) {
        String namespaceKey = subscription.collection == null ? subscription.db : subscription.db + "." + subscription.collection;
        subscription.namespaceKey = namespaceKey;
        changeStreamSubscribers.computeIfAbsent(namespaceKey, k -> new CopyOnWriteArrayList<>()).add(subscription);
    }

    private void unregisterSubscription(ChangeStreamSubscription subscription) {
        if (subscription.namespaceKey == null) {
            return;
        }

        List<ChangeStreamSubscription> subs = changeStreamSubscribers.get(subscription.namespaceKey);

        if (subs != null) {
            subs.remove(subscription);

            if (subs.isEmpty()) {
                changeStreamSubscribers.remove(subscription.namespaceKey);
            }
        }

        subscription.deactivate();
    }

    private void replayHistory(ChangeStreamSubscription subscription, long startingToken) {
        for (ChangeStreamEventInfo info : changeStreamHistory) {
            if (info.token <= startingToken) {
                continue;
            }

            if (!subscription.matches(info)) {
                continue;
            }

            subscription.deliver(info);

            if (!subscription.isActive()) {
                break;
            }
        }
    }

    private static Long extractResumeToken(Map<String, Object> tokenDocument) {
        if (tokenDocument == null) {
            return null;
        }

        Object data = tokenDocument.get("_data");

        if (data instanceof String str) {
            try {
                return Long.parseUnsignedLong(str, 16);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }

        if (data instanceof Number num) {
            return num.longValue();
        }

        return null;
    }

    private static Map<String, Object> createResumeToken(long token) {
        return Doc.of("_data", String.format(Locale.ROOT, "%016x", token));
    }

    private Map<String, Object> cloneAndNormalizeDocument(Map<String, Object> source) {
        if (source == null) {
            return null;
        }

        Map<String, Object> copy = deepCopyDoc(source);

        if (copy.containsKey("_id")) {
            copy.put("_id", normalizeId(copy.get("_id")));
        }

        return copy;
    }

    private Object extractDocumentKey(Map<String, Object> newDocument, Map<String, Object> previousDocument) {
        Object id = newDocument != null ? newDocument.get("_id") : null;

        if (id == null && previousDocument != null) {
            id = previousDocument.get("_id");
        }

        return normalizeId(id);
    }

    private Object normalizeId(Object value) {
        if (value instanceof MorphiumId || value == null) {
            return value;
        }

        if (value instanceof ObjectId objectId) {
            return new MorphiumId(objectId);
        }

        return value;
    }

    private Map<String, Object> computeUpdatedFields(Map<String, Object> previousDocument, Map<String, Object> newDocument) {
        if (newDocument == null) {
            return Map.of();
        }

        Map<String, Object> beforeFlat = flattenDocument(previousDocument);
        Map<String, Object> afterFlat = flattenDocument(newDocument);
        Map<String, Object> updated = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : afterFlat.entrySet()) {
            if ("_id".equals(entry.getKey())) {
                continue;
            }

            Object previous = beforeFlat.get(entry.getKey());

            if (!Objects.equals(previous, entry.getValue())) {
                updated.put(entry.getKey(), entry.getValue());
            }
        }

        return updated;
    }

    private List<String> computeRemovedFields(Map<String, Object> previousDocument, Map<String, Object> newDocument) {
        if (previousDocument == null) {
            return List.of();
        }

        Map<String, Object> beforeFlat = flattenDocument(previousDocument);
        Map<String, Object> afterFlat = flattenDocument(newDocument);
        List<String> removed = new ArrayList<>();

        for (String key : beforeFlat.keySet()) {
            if ("_id".equals(key)) {
                continue;
            }

            if (!afterFlat.containsKey(key)) {
                removed.add(key);
            }
        }

        return removed;
    }

    private Map<String, Object> flattenDocument(Map<String, Object> document) {
        Map<String, Object> flattened = new LinkedHashMap<>();

        if (document == null) {
            return flattened;
        }

        for (Map.Entry<String, Object> entry : document.entrySet()) {
            flattenValue(flattened, entry.getKey(), entry.getValue());
        }

        return flattened;
    }

    private void flattenValue(Map<String, Object> target, String prefix, Object value) {
        if (value instanceof Map) {
            Map <?, ? > map = (Map <?, ? >) value;

            if (map.isEmpty()) {
                target.put(prefix, Map.of());
            }

            for (Map.Entry <?, ? > entry : map.entrySet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                String newPrefix = prefix == null || prefix.isEmpty() ? entry.getKey().toString() : prefix + "." + entry.getKey();
                flattenValue(target, newPrefix, entry.getValue());
            }
            return;
        }

        if (value instanceof List) {
            List<?> list = (List<?>) value;

            if (list.isEmpty()) {
                target.put(prefix, List.of());
                return;
            }

            for (int i = 0; i < list.size(); i++) {
                String newPrefix = prefix + "." + i;
                flattenValue(target, newPrefix, list.get(i));
            }
            return;
        }

        target.put(prefix, value);
    }

    private static final class ChangeStreamEventInfo {
        private final long token;
        private final String db;
        private final String collection;
        private final Map<String, Object> event;
        private final long createdAt;

        private ChangeStreamEventInfo(long token, String db, String collection, Map<String, Object> event, long createdAt) {
            this.token = token;
            this.db = db;
            this.collection = collection;
            this.event = event;
            this.createdAt = createdAt;
        }
    }

    private class ChangeStreamSubscription {
        private final String db;
        private final String collection;
        private final DriverTailableIterationCallback callback;
        private final List<Map<String, Object>> pipeline;
        private final WatchCommand.FullDocumentEnum fullDocumentMode;
        private final WatchCommand.FullDocumentBeforeChangeEnum beforeChangeMode;
        private final boolean showExpandedEvents;
        private final Object monitor;
        private volatile boolean active = true;
        private String namespaceKey;

        private ChangeStreamSubscription(String db, String collection, DriverTailableIterationCallback callback, List<Map<String, Object>> pipeline,
                                         WatchCommand.FullDocumentEnum fullDocumentMode, WatchCommand.FullDocumentBeforeChangeEnum beforeChangeMode,
                                         boolean showExpandedEvents, Object monitor) {
            this.db = db;
            this.collection = collection;
            this.callback = callback;
            this.pipeline = pipeline;
            this.fullDocumentMode = fullDocumentMode;
            this.beforeChangeMode = beforeChangeMode;
            this.showExpandedEvents = showExpandedEvents;
            this.monitor = monitor;
        }

        private boolean matches(ChangeStreamEventInfo info) {
            if (!Objects.equals(db, info.db)) {
                return false;
            }

            if (collection == null) {
                return true;
            }

            return Objects.equals(collection, info.collection);
        }

        private boolean isActive() {
            return active;
        }

        private void deactivate() {
            if (!active) {
                return;
            }

            active = false;

            synchronized (monitor) {
                monitor.notifyAll();
            }
        }

        private void deliver(ChangeStreamEventInfo info) {
            if (!active) {
                return;
            }

            Map<String, Object> working = deepCopyDoc(info.event);
            adjustFullDocument(working);

            if (!applyFullDocumentBeforeChange(working)) {
                return;
            }

            Map<String, Object> processed = applyPipeline(working);

            if (processed == null) {
                return;
            }

            try {
                callback.incomingData(processed, System.currentTimeMillis() - info.createdAt);
            } catch (Exception e) {
                log.error("Error calling change-stream callback", e);
            }

            if (!callback.isContinued()) {
                deactivate();
            }
        }

        private void adjustFullDocument(Map<String, Object> working) {
            if (fullDocumentMode != WatchCommand.FullDocumentEnum.updateLookup && "update".equals(working.get("operationType"))) {
                working.remove("fullDocument");
            }
        }

        private boolean applyFullDocumentBeforeChange(Map<String, Object> working) {
            Map<String, Object> before = (Map<String, Object>) working.get("fullDocumentBeforeChange");

            switch (beforeChangeMode) {
                case off:
                    working.remove("fullDocumentBeforeChange");
                    return true;

                case whenAvailable:
                    if (before == null) {
                        working.remove("fullDocumentBeforeChange");
                    }

                    return true;

                case required:
                    if (before == null) {
                        return false;
                    }

                    return true;

                default:
                    return true;
            }
        }

        private Map<String, Object> applyPipeline(Map<String, Object> working) {
            if (pipeline == null || pipeline.isEmpty()) {
                return working;
            }

            InMemAggregator agg = new InMemAggregator(null, Map.class, Map.class);
            List<Map<String, Object>> current = new ArrayList<>();
            current.add(working);

            for (Map<String, Object> stage : pipeline) {
                current = agg.execStep(stage, current);

                if (current == null || current.isEmpty()) {
                    return null;
                }
            }

            Map<String, Object> result = current.get(0);

            if (result == working) {
                return working;
            }

            return deepCopyDoc(result);
        }
    }

    public synchronized Map<String, Object> delete (String db, String collection, Map<String, Object> query, Map<String, Object> sort, boolean multiple, Map<String, Object> collation, WriteConcern wc)
    throws MorphiumDriverException {
        List<Map<String, Object >> toDel = new ArrayList<>(find(db, collection, query, null, UtilsMap.of("_id", 1), collation, 0, multiple ? 0 : 1, true));

        int deleted = 0;

        for (Map<String, Object> o : toDel) {
            for (Map<String, Object> dat : new ArrayList<>(getCollection(db, collection))) {
                if (dat.get("_id") instanceof ObjectId || dat.get("_id") instanceof MorphiumId) {
                    if (dat.get("_id").toString().equals(o.get("_id").toString())) {
                        getCollection(db, collection).remove(dat);
                        deleted++;

                        // indexDataByDBCollection.get(db).remove(collection);
                        // updateIndexData(db,collection,null);
                        for (String keys : indexDataByDBCollection.get(db).get(collection).keySet()) {
                            Map<Integer, List<Map<String, Object >>> id = getIndexDataForCollection(db, collection, keys);
                            for (int bucketId : id.keySet()) {
                                var lst = new ArrayList<Map<String, Object >> (id.get(bucketId));
                                for (Map<String, Object> objectMap : lst) {
                                    if (objectMap.get("_id").toString().equals(o.get("_id").toString())) {
                                        id.get(bucketId).remove(objectMap);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    if (dat.get("_id").equals(o.get("_id"))) {
                        getCollection(db, collection).remove(dat);
                        deleted++;
                        // indexDataByDBCollection.get(db).remove(collection);
                        // updateIndexData(db,collection,null);

                        for (String keys : indexDataByDBCollection.get(db).get(collection).keySet()) {
                            Map<Integer, List<Map<String, Object >>> id = getIndexDataForCollection(db, collection, keys);
                            for (int bucketId : id.keySet()) {
                                var lst = new ArrayList<Map<String, Object >> (id.get(bucketId));
                                for (Map<String, Object> objectMap : lst) {
                                    if (objectMap.get("_id").equals(o.get("_id"))) {
                                        id.get(bucketId).remove(objectMap);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // updating index data
            notifyWatchers(db, collection, "delete", o, null, null, o);
        }

        return Doc.of("n", deleted, "ok", 1.0);
    }

    private List<Map<String, Object >> getCollection(String db, String collection) throws MorphiumDriverException {
        if (!getDB(db).containsKey(collection)) {
            getDB(db).put(collection, new CopyOnWriteArrayList<>());

            try {
                createIndex(db, collection, Doc.of("_id", 1), Doc.of("name", "_id_1"));
            } catch (MorphiumDriverException e) {
                // already exists
            }
        }

        return getDB(db).get(collection);
    }

    public synchronized void drop(String db, String collection, WriteConcern wc) {
        getDB(db).remove(collection);

        if (indexDataByDBCollection.containsKey(db)) {
            indexDataByDBCollection.get(db).remove(collection);
        }

        if (indicesByDbCollection.containsKey(db)) {
            indicesByDbCollection.get(db).remove(collection);
        }

        notifyWatchers(db, collection, "drop", null);
    }

    public synchronized void drop(String db, WriteConcern wc) {
        database.remove(db);

        if (indexDataByDBCollection.containsKey(db)) {
            indexDataByDBCollection.remove(db);
        }

        if (indicesByDbCollection.containsKey(db)) {
            indicesByDbCollection.remove(db);
        }

        notifyWatchers(db, null, "drop", null);
    }

    public boolean exists(String db) {
        return database.containsKey(db);
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

    public List<Object> distinct(String db, String collection, String field, Map<String, Object> filter, Map<String, Object> collation) throws MorphiumDriverException {
        List<Map<String, Object >> list = find(db, collection, filter, null, null, 0, 0);
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

    private Map<String, List<Map<String, Object >>> getIndexesForDB(String db) {
        indicesByDbCollection.putIfAbsent(db, new ConcurrentHashMap<>());
        return indicesByDbCollection.get(db);
    }

    public List<Map<String, Object >> getIndexes(String db, String collection) {
        if (!getIndexesForDB(db).containsKey(collection)) {
            // new collection, create default index for _id
            ArrayList<Map<String, Object >> value = new ArrayList<>();
            getIndexesForDB(db).put(collection, value);
            value.add(Doc.of("_id", 1, "$options", Doc.of("name", "_id_1")));
        }

        return getIndexesForDB(db).get(collection);
    }

    private void enforceUniqueOrThrow(String db, String collection, Map<String, Object> doc) throws MorphiumDriverException {
        List<Map<String, Object>> indexes = getIndexes(db, collection);
        if (indexes == null) return;
        for (var idx : indexes) {
            Object opt = idx.get("$options");
            if (!(opt instanceof Map)) continue;
            Map<String, Object> options = (Map<String, Object>) opt;
            if (!Boolean.TRUE.equals(options.get("unique")) && !(options.get("unique") instanceof String && "true".equalsIgnoreCase((String) options.get("unique")))) {
                continue;
            }
            // build equality query for all index keys present in doc
            Doc q = new Doc();
            boolean hasAll = true;
            for (var e : idx.entrySet()) {
                if (e.getKey().startsWith("$")) continue;
                Object v = doc.get(e.getKey());
                if (v == null) { hasAll = false; break; }
                q.put(e.getKey(), v);
            }
            if (!hasAll || q.isEmpty()) continue;
            List<Map<String, Object>> matches = find(db, collection, q, null, null, 0, 0);
            for (var m : matches) {
                Object mid = m.get("_id");
                Object did = doc.get("_id");
                if (did == null || mid == null || !mid.toString().equals(did.toString())) {
                    throw new MorphiumDriverException("Duplicate key for unique index: " + options.get("name"), null);
                }
            }
        }
    }

    public List<String> getCollectionNames(String db) {
        try {
            return new ArrayList<>(getDB(db).keySet());
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    public Map<String, Object> findAndOneAndDelete(String db, String col, Map<String, Object> query, Map<String, Object> sort, Map<String, Object> collation) throws MorphiumDriverException {
        List<Map<String, Object >> r = find(db, col, query, sort, null, 0, 1);
        if (r.size() == 0) {
            return null;
        }
        delete (db, col, Doc.of("_id", r.get(0).get("_id")), null, false, collation, null);
        return r.get(0);
    }

    public synchronized Map<String, Object> findAndOneAndUpdate(String db, String col, Map<String, Object> query, Map<String, Object> update, Map<String, Object> sort, Map<String, Object> collation)
    throws MorphiumDriverException {
        List<Map<String, Object >> ret = find(db, col, query, sort, null, 0, 1);
        update(db, col, query, null, update, false, false, collation, null);
        return ret.get(0);
    }

    public synchronized Map<String, Object> findAndOneAndReplace(String db, String col, Map<String, Object> query, Map<String, Object> replacement, Map<String, Object> sort,
            Map<String, Object> collation) throws MorphiumDriverException {
        List<Map<String, Object >> ret = find(db, col, query, sort, null, 0, 1);
        if (ret.get(0).get("_id") != null) {
            replacement.put("_id", ret.get(0).get("_id"));
        }
        else {
            replacement.remove("_id");
        }
        store(db, col, Collections.singletonList(replacement), null);
        return replacement;
    }

    public void tailableIteration(String db, String collection, Map<String, Object> query, Map<String, Object> sort, Map<String, Object> projection, int skip, int limit, int batchSize,
                                  ReadPreference readPreference, int timeout, DriverTailableIterationCallback cb) throws MorphiumDriverException {
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
        return cappedCollections.containsKey(db) && cappedCollections.get(db).containsKey(coll);
    }

    @Override
    public Map<String, Integer> getNumConnectionsByHost() {
        return Map.of("inMem", 1);
    }

    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return new BulkRequestContext(m) {
            private final List<BulkRequest> requests = new ArrayList<>();
            @Override
            public Map<String, Object> execute() {
                try {
                    for (BulkRequest r : requests) {
                        if (r instanceof InsertBulkRequest) {
                            insert(db, collection, ((InsertBulkRequest) r).getToInsert(), null);
                        } else if (r instanceof UpdateBulkRequest) {
                            UpdateBulkRequest up = (UpdateBulkRequest) r;
                            update(db, collection, up.getQuery(), null, up.getCmd(), up.isMultiple(), up.isUpsert(), null, null);
                        } else if (r instanceof DeleteBulkRequest) {
                            delete (db, collection, ((DeleteBulkRequest) r).getQuery(), null, ((DeleteBulkRequest) r).isMultiple(), null, null);
                        } else {
                            throw new RuntimeException("Unknown operation " + r.getClass().getName());
                        }
                    }
                } catch (MorphiumDriverException e) {
                    log.error("Got exception: ", e);
                }

                return new Doc();
            }
            @Override
            public UpdateBulkRequest addUpdateBulkRequest() {
                UpdateBulkRequest up = new UpdateBulkRequest();
                requests.add(up);
                return up;
            }
            public InsertBulkRequest addInsertBulkRequest(List<Map<String, Object >> toInsert) {
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
    public Map<DriverStatsKey, Double> getDriverStats() {
        Map<DriverStatsKey, Double> ret = new HashMap<>();

        for (var e : stats.entrySet()) {
            ret.put(e.getKey(), e.getValue().doubleValue());
        }

        ret.put(DriverStatsKey.REPLY_IN_MEM, (double) commandResults.size());
        return ret;
    }

    public void createIndex(String db, String collection, Map<String, Object> indexDef, Map<String, Object> options) throws MorphiumDriverException {
        Map<String, Object> index = new HashMap<>(indexDef);
        index.put("$options", options);

        if (!options.containsKey("name")) {
            StringBuilder name = new StringBuilder();

            for (String k : index.keySet()) {
                if (k.startsWith("$")) {
                    continue;
                }

                name.append(k + "_" + index.get(k).toString());
                name.append("_");
            }

            name.setLength(name.length() - 1);
            ((Map) index.get("$options")).put("name", name.toString());
        }

        List<Map<String, Object >> indexes = getIndexes(db, collection);
        // looking for keys...
        boolean found = true;
        for (var i : indexes) {
            found = true;
            if (i.size() - 1 != indexDef.size()) {
                // log.info("Index sizes differ - no match");
                found = false;
                continue;
            }
            for (var e : indexDef.entrySet()) {
                if (e.getKey().startsWith("$")) {
                    continue;
                }
                if (!i.containsKey(e.getKey()) || !i.get(e.getKey()).equals(e.getValue())) {
                    found = false;
                    break;
                }
            }
            if (found) {
                break;
            }
        }
        if (!found) {
            indexes.add(index);
        }
        else {
            if (index.size() == 2 && index.containsKey("_id") && index.containsKey("$options")) {
                // ignoring attempt to re-create_id index
            } else {

                // log.debug("Index with those keys already exists: " + Utils.toJsonString(index));
                // throw new MorphiumDriverException("Index with those keys already exists");
            }

            //
        }
        updateIndexData(db, collection, options);
    }

    private void updateIndexData(String db, String collection, Map<String, Object> options) throws MorphiumDriverException {
        //TODO: deal with options!
        StringBuilder b = new StringBuilder();
        indexDataByDBCollection.putIfAbsent(db, new ConcurrentHashMap<>());
        indexDataByDBCollection.get(db).putIfAbsent(collection, new ConcurrentHashMap<>());
        indexDataByDBCollection.get(db).get(collection).clear();

        for (Map<String, Object> doc : getCollection(db, collection)) {
            for (Map<String, Object> idx : getIndexes(db, collection)) {
                b.setLength(0);
                int bucketId = 0;

                for (String k : idx.keySet()) {
                    if (k.equals("$options")) {
                        continue;
                    }

                    bucketId = iterateBucketId(bucketId, doc.get(k));
                    b.append(k);
                }

                Map<Integer, List<Map<String, Object >>> index = getIndexDataForCollection(db, collection, b.toString());
                index.putIfAbsent(bucketId, new CopyOnWriteArrayList<>());
                index.get(bucketId).add(doc);
            }
        }
    }

    public List<Map<String, Object >> mapReduce(String db, String collection, String mapping, String reducing) throws MorphiumDriverException {
        return mapReduceInternal(db, collection, mapping, reducing, null, null, null, null);
    }

    public List<Map<String, Object >> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query) throws MorphiumDriverException {
        return mapReduceInternal(db, collection, mapping, reducing, query, null, null, null);
    }

    public List<Map<String, Object >> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query, Map<String, Object> sorting, Collation collation)
    throws MorphiumDriverException {
        return mapReduceInternal(db, collection, mapping, reducing, query, sorting, collation, null);
    }

    /**
     * Internal MapReduce implementation using JavaScript engine
     *
     * @param db Database name
     * @param collection Collection name
     * @param mapFunction JavaScript map function as string
     * @param reduceFunction JavaScript reduce function as string
     * @param query Optional query to filter documents
     * @param sort Optional sort specification
     * @param collation Optional collation
     * @param finalizeFunction Optional finalize function
     * @return List of result documents
     */
    private List<Map<String, Object>> mapReduceInternal(String db, String collection, String mapFunction, String reduceFunction,
            Object query, Object sort, Collation collation, String finalizeFunction) throws MorphiumDriverException {

        // Get documents from collection
        Map<String, Object> queryMap = query instanceof Map ? (Map<String, Object>) query : null;
        Map<String, Object> sortMap = sort instanceof Map ? (Map<String, Object>) sort : null;
        List<Map<String, Object>> documents = find(db, collection, queryMap, sortMap, null, collation == null ? null : collation.toQueryObject(), 0, 0, false);

        log.info("MapReduce internal: found {} documents to process", documents.size());

        // Initialize JavaScript engine
        System.setProperty("polyglot.engine.WarnInterpreterOnly", "false");
        javax.script.ScriptEngineManager mgr = new javax.script.ScriptEngineManager();
        javax.script.ScriptEngine engine = mgr.getEngineByExtension("js");

        if (engine == null) {
            engine = mgr.getEngineByName("js");
        }
        if (engine == null) {
            engine = mgr.getEngineByName("JavaScript");
        }
        if (engine == null) {
            throw new MorphiumDriverException("JavaScript engine not available for MapReduce");
        }

        log.info("JavaScript engine found: {}", engine.getClass().getName());

        try {
            // Prepare the JavaScript environment
            // Add emit function to collect map results
            List<Map<String, Object>> mapResults = new ArrayList<>();

            // Create a Java object to handle emit calls from JavaScript
            MapReduceEmitter emitter = new MapReduceEmitter(mapResults);

            // Create a simple approach using a List that GraalJS can access
            List<Map<String, Object>> emitResults = new ArrayList<>();
            engine.put("emitResults", emitResults);

            // Test basic JavaScript functionality
            Object basicTest = engine.eval("1 + 1");
            log.info("Basic JS test (1+1): {}", basicTest);

            // Define emit function in JavaScript that calls our Java emitter
            log.debug("Setting up emit function with emitter: {}", emitter);

            // Define emit function by putting it directly in the bindings
            engine.put("emit", new Object() {
                public void emit(Object key, Object value) {
                    log.info("Java emit function called with key={}, value={}", key, value);
                    emitter.emit(key, value);
                }
            });

            // Create a JavaScript array and define emit function to use it
            engine.eval("var jsEmitResults = [];");
            engine.eval("function emit(key, value) { " +
                        "  var result = { _id: key, value: value }; " +
                        "  jsEmitResults.push(result); " +
                        "}");

            // Provide an ObjectId() function compatible with MongoDB map/reduce scripts
            engine.eval("function ObjectId() { " +
                        "  var hex = '0123456789abcdef';" +
                        "  var id = '';" +
                        "  for (var i = 0; i < 24; i++) {" +
                        "    id += hex.charAt(Math.floor(Math.random() * 16));" +
                        "  }" +
                        "  return id;" +
                        "}");

            // Prepare the map function once
            engine.eval("var mapFunc = " + mapFunction + ";");

            // Phase 1: Map phase - execute map function for each document
            log.info("Starting map phase with {} documents", documents.size());
            for (Map<String, Object> doc : documents) {
                log.debug("Processing document: {}", doc);
                // Execute map function with document bound to `this`
                try {
                    String jsonDoc = Utils.toJsonString(doc);
                    String escapedJson = jsonDoc.replace("\\", "\\\\").replace("'", "\\'");
                    String executable = "var __mapDoc = JSON.parse('" + escapedJson + "'); mapFunc.call(__mapDoc);";
                    log.debug("Executing map function for document");
                    engine.eval(executable);
                } catch (Exception e) {
                    log.error("JavaScript execution error: {}", e.getMessage(), e);
                }
                Object jsArraySize = engine.eval("jsEmitResults.length");
                log.debug("Map results so far: {}", jsArraySize);
            }

            // Get the final JavaScript array size and convert to Java objects
            Object finalJsArraySize = engine.eval("jsEmitResults.length");
            log.info("Map phase completed with {} emissions", finalJsArraySize);

            // Convert JavaScript array to Java List using standard ScriptEngine approach
            JSONParser jsonParser = new JSONParser();
            Object jsArrayLength = engine.eval("jsEmitResults.length");
            if (jsArrayLength instanceof Number) {
                int arrayLength = ((Number) jsArrayLength).intValue();
                log.info("Converting {} JavaScript results to Java", arrayLength);

                for (int i = 0; i < arrayLength; i++) {
                    Object key = engine.eval("jsEmitResults[" + i + "]._id");
                    String valueJson = (String) engine.eval("JSON.stringify(jsEmitResults[" + i + "].value)");

                    Map<String, Object> javaResult = new HashMap<>();
                    javaResult.put("_id", key);
                    javaResult.put("valueJson", valueJson);
                    mapResults.add(javaResult);
                    log.debug("Converted result {}: key={}, valueJson={}", i, key, valueJson);
                }
            }

            // Phase 2: Group by key
            Map<Object, List<String>> groupedResults = new HashMap<>();
            for (Map<String, Object> mapResult : mapResults) {
                Object key = mapResult.get("_id");
                String valueJson = (String) mapResult.get("valueJson");

                groupedResults.computeIfAbsent(key, k -> new ArrayList<>()).add(valueJson);
            }

            if (reduceFunction != null && !reduceFunction.trim().isEmpty()) {
                engine.eval("var reduceFunc = " + reduceFunction + ";");
            }

            if (finalizeFunction != null && !finalizeFunction.trim().isEmpty()) {
                engine.eval("var finalizeFunc = " + finalizeFunction + ";");
            }

            // Phase 3: Reduce phase - execute reduce function for each key
            List<Map<String, Object>> finalResults = new ArrayList<>();
            for (Map.Entry<Object, List<String>> entry : groupedResults.entrySet()) {
                Object key = entry.getKey();
                List<String> values = entry.getValue();

                Object reducedValue;
                if (reduceFunction == null || reduceFunction.trim().isEmpty()) {
                    reducedValue = values.size() == 1 ? values.get(0) : new ArrayList<>(values);
                } else {
                    engine.put("__javaKey__", key);
                    StringBuilder valuesArrayJson = new StringBuilder("[");
                    for (int i = 0; i < values.size(); i++) {
                        if (i > 0) {
                            valuesArrayJson.append(',');
                        }
                        valuesArrayJson.append(values.get(i));
                    }
                    valuesArrayJson.append(']');
                    engine.put("__valuesJson__", valuesArrayJson.toString());
                    reducedValue = engine.eval("reduceFunc(__javaKey__, JSON.parse(__valuesJson__))");
                }

                // Apply finalize function if provided
                if (finalizeFunction != null && !finalizeFunction.trim().isEmpty()) {
                    engine.put("__javaKey__", key);
                    engine.put("__reducedValue__", reducedValue);
                    reducedValue = engine.eval("finalizeFunc(__javaKey__, __reducedValue__)");
                }

                // Create result document
                Map<String, Object> result = new HashMap<>();
                result.put("_id", key);
                if (reducedValue instanceof Map) {
                    result.put("value", reducedValue);
                } else {
                    try {
                        engine.put("__reducedJson__", reducedValue);
                        String reducedJson = (String) engine.eval("JSON.stringify(__reducedJson__)");
                        Object parsed = jsonParser.parse(reducedJson);
                        result.put("value", parsed);
                    } catch (ParseException pe) {
                        throw new MorphiumDriverException("Failed to parse reduce result", pe);
                    }
                }
                finalResults.add(result);
            }

            return finalResults;

        } catch (javax.script.ScriptException e) {
            throw new MorphiumDriverException("JavaScript error in MapReduce: " + e.getMessage(), e);
        }
    }

    public void commitTransaction() {
        if (currentTransaction.get() == null) {
            throw new IllegalArgumentException("No transaction in progress");
        }

        InMemTransactionContext ctx = currentTransaction.get();
        // replace full database state with transaction snapshot
        database.clear();
        // noinspection unchecked
        database.putAll(ctx.getDatabase());
        // rebuild index data to reflect committed dataset
        indexDataByDBCollection.clear();
        for (var dbName : database.keySet()) {
            for (var collName : database.get(dbName).keySet()) {
                try {
                    updateIndexData(dbName, collName, null);
                } catch (Exception e) {
                    // swallow to avoid breaking commit; indexes will be lazily rebuilt
                }
            }
        }
        currentTransaction.set(null);
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

    private static class CursorResultBuffer {
        private final Deque<Map<String, Object>> remaining;
        private final String namespace;
        private final int defaultBatchSize;

        CursorResultBuffer(Deque<Map<String, Object>> remaining, String namespace, int defaultBatchSize) {
            this.remaining = remaining;
            this.namespace = namespace;
            this.defaultBatchSize = defaultBatchSize;
        }
    }

    private class InMemoryFindCursor extends MorphiumCursor {
        private int internalIndex = 0;
        private int index = 0;

        InMemoryFindCursor(long cursorId, String namespace, List<Map<String, Object>> firstBatch, int batchSize) {
            setCursorId(cursorId);
            setBatchSize(batchSize > 0 ? batchSize : (firstBatch.size() > 0 ? firstBatch.size() : 101));
            setBatch(firstBatch != null ? new ArrayList<>(firstBatch) : new ArrayList<>());
            applyNamespace(namespace);
        }

        private void applyNamespace(String namespace) {
            if (namespace == null || namespace.isEmpty()) {
                setDb("");
                setCollection("");
                return;
            }

            int dotIdx = namespace.indexOf('.');

            if (dotIdx < 0) {
                setDb(namespace);
                setCollection("$cmd");
            } else {
                setDb(namespace.substring(0, dotIdx));
                setCollection(namespace.substring(dotIdx + 1));
            }
        }

        @Override
        public synchronized boolean hasNext() {
            if (getBatch() != null && internalIndex < getBatch().size()) {
                return true;
            }

            if (getCursorId() == 0) {
                return false;
            }

            loadNextBatch();
            return getBatch() != null && internalIndex < getBatch().size();
        }

        @Override
        public synchronized Map<String, Object> next() {
            if (!hasNext()) {
                return null;
            }

            Map<String, Object> doc = getBatch().get(internalIndex++);
            index++;
            return doc;
        }

        @Override
        public synchronized void close() {
            if (getCursorId() != 0) {
                closeCursor(getCursorId());
                setCursorId(0);
            }

            setBatch(Collections.emptyList());
            internalIndex = 0;
        }

        @Override
        public synchronized int available() {
            if (getBatch() == null) {
                return 0;
            }
            return Math.max(0, getBatch().size() - internalIndex);
        }

        @Override
        public synchronized List<Map<String, Object>> getAll() throws MorphiumDriverException {
            List<Map<String, Object>> res = new ArrayList<>();

            while (hasNext()) {
                res.add(next());
            }

            return res;
        }

        @Override
        public synchronized void ahead(int jump) throws MorphiumDriverException {
            if (jump < 0) {
                throw new IllegalArgumentException("jump must be >= 0");
            }

            internalIndex += jump;
            index += jump;

            while (getBatch() != null && internalIndex >= getBatch().size()) {
                int diff = internalIndex - getBatch().size();

                if (getCursorId() == 0) {
                    internalIndex = getBatch() != null ? getBatch().size() : 0;
                    break;
                }

                loadNextBatch();

                if (getBatch() == null || getBatch().isEmpty()) {
                    internalIndex = 0;
                    break;
                }

                internalIndex = diff;
            }
        }

        @Override
        public synchronized void back(int jump) throws MorphiumDriverException {
            if (jump < 0) {
                throw new IllegalArgumentException("jump must be >= 0");
            }

            internalIndex -= jump;
            index -= jump;

            if (internalIndex < 0) {
                throw new IllegalArgumentException("cannot jump back over batch boundaries!");
            }
        }

        @Override
        public int getCursor() {
            return index;
        }

        @Override
        public MongoConnection getConnection() {
            return InMemoryDriver.this;
        }

        @Override
        public Iterator<Map<String, Object>> iterator() {
            return this;
        }

        private void loadNextBatch() {
            if (getCursorId() == 0) {
                setBatch(Collections.emptyList());
                internalIndex = 0;
                return;
            }

            List<Map<String, Object>> next = drainNextBatch(getCursorId(), getBatchSize());

            if (next.isEmpty()) {
                setCursorId(0);
                setBatch(Collections.emptyList());
            } else {
                setBatch(new ArrayList<>(next));
            }

            internalIndex = 0;
        }
    }

    private static class InMemoryCursor {
        private int skip;
        private int limit;
        private int batchSize;
        private int dataRead = 0;

        private String db;
        private String collection;
        private Map<String, Object> query;
        private Map<String, Object> sort;
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

        public Map<String, Object> getSort() {
            return sort;
        }

        public void setSort(Map<String, Object> sort) {
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

    @Override
    public int getIdleSleepTime() {
        return 0;
    }

    @Override
    public void setIdleSleepTime(int sl) {
        // TODO Auto-generated method stub
    }

    @Override
    public int getCompression() {
        return 0;
    }

    @Override
    public MorphiumDriver setCompression(int type) {
        log.warn("Cannot set compression on inMemDriver");
        return this;
    }

    /**
     * Helper class to handle emit() calls from JavaScript MapReduce functions
     */
    public static class MapReduceEmitter {
        private final List<Map<String, Object>> results;

        public MapReduceEmitter(List<Map<String, Object>> results) {
            this.results = results;
        }

        public void emit(Object key, Object value) {
            // System.out.println("EMIT called with key=" + key + ", value=" + value);
            Map<String, Object> result = new HashMap<>();
            result.put("_id", key);
            result.put("value", value);
            results.add(result);
            // System.out.println("Total emit results now: " + results.size());
        }
    }

}
