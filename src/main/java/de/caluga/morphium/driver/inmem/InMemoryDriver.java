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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.net.ssl.SSLContext;

import org.bson.types.ObjectId;
import org.json.simple.parser.ParseException;
import org.openjdk.jol.vm.VM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Collation;
import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.Expr;
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
public class InMemoryDriver implements MorphiumDriver, MongoConnection {
    private final Logger log = LoggerFactory.getLogger(InMemoryDriver.class);
    public final static String driverName = "InMemDriver";
    // DBName => Collection => List of documents
    private final Map<String, Map<String, List<Map<String, Object>> >> database = new ConcurrentHashMap<>();
    private int idleSleepTime = 20;
    /**
     * index definitions by db and collection name
     * DB -> Collection -> List of Map Index defintion (field -> 1/-1/hashed)
     */
    private final Map<String, Map<String, List<Map<String, Object>> >> indicesByDbCollection = new ConcurrentHashMap<>();

    /**
     * Map DB->Collection->FieldNames->Keys....
     */
    // private final Map<String, Map<String, Map<String, Map<IndexKey,
    // List<Map<String, Object>>>>>> indexDataByDBCollection = new
    // ConcurrentHashMap<>();
    private final Map<String, Map<String, Map<String, Map<Integer, List<Map<String, Object>> >> >> indexDataByDBCollection = new ConcurrentHashMap<>();
    private final ThreadLocal<InMemTransactionContext> currentTransaction = new ThreadLocal<>();
    private final AtomicLong txn = new AtomicLong();
    private final Map<String, List<DriverTailableIterationCallback>> watchersByDb = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Map<String, Integer>>> cappedCollections = new ConcurrentHashMap<>();// db->coll->Settings
    // size/max
    private final List<Object> monitors = new CopyOnWriteArrayList<>();
    private List<Runnable> eventQueue = new CopyOnWriteArrayList<>();
    private final Map<Integer, Map<String, Object>> commandResults = new ConcurrentHashMap<>();
    private final Map<String, Class<? extends MongoCommand>> commandsCache = new HashMap<>();
    private final AtomicInteger commandNumber = new AtomicInteger(0);
    private final Map<DriverStatsKey, AtomicDecimal> stats = new ConcurrentHashMap<>();
    private final Map<Long, FindCommand> cursors = new ConcurrentHashMap<>();
    private ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(2);
    private boolean running = true;
    private int expireCheck = 45000;
    private ScheduledFuture<?> expire;
    private String replicaSetName;

    public Map<String, List<Map<String, Object>>> getDatabase(String dbn) {
        return database.get(dbn);
    }

    public InMemoryDriver() {
    }

    @Override
    public <T, R> Aggregator<T, R> createAggregator(Morphium morphium, Class<? extends T> type, Class<? extends R> resultType) {
        return new InMemAggregator<>(morphium, type, resultType);
    }

    public void setDatabase(String dbn, Map<String, List<Map<String, Object>>> db) {
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
                       return new ObjectId(((Map<?, ?>) d).get("value").toString());
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
        watchersByDb.clear();
        eventQueue.clear();
        cursors.clear();
        commandResults.clear();
        currentTransaction.remove();
    }

    public void setCredentials(String db, String login, char[] pwd) {
        // ignored for now
    }

    public Map<String, Object> getAnswer(int queryId) {
        stats.get(DriverStatsKey.REPLY_PROCESSED).incrementAndGet();
        var data = commandResults.remove(queryId);
        return data;
    }

    @Override
    public void watch(WatchCommand settings) throws MorphiumDriverException {
        watch(settings.getDb(), settings.getColl(), settings.getMaxTimeMS(), settings.getFullDocument().equals(WatchCommand.FullDocumentEnum.updateLookup), settings.getPipeline(), settings.getCb());
    }

    @Override
    public List<Map<String, Object>> readAnswerFor(int queryId) throws MorphiumDriverException {
        log.info("Reading answer for id " + queryId);
        stats.get(DriverStatsKey.REPLY_PROCESSED).incrementAndGet();
        var data = commandResults.remove(queryId);

        if (data.containsKey("results")) {
            return ((List<Map<String, Object>>) data.get("results"));
        } else if (data.containsKey("cursor")) {
            var cursor = (Map<String, Object>) data.get("cursor");

            if (cursor.containsKey("firstBatch")) {
                return (List<Map<String, Object>>) cursor.get("firstBatch");
            } else if (cursor.containsKey("nextBatch")) {
                return (List<Map<String, Object>>) cursor.get("nextBatch");
            }
        }

        return null;
    }

    @Override
    public MorphiumCursor getAnswerFor(int queryId, int batchsize) throws MorphiumDriverException {
        stats.get(DriverStatsKey.REPLY_PROCESSED).incrementAndGet();
        var data = commandResults.remove(queryId);
        List<Map<String, Object>> batch = new ArrayList<>();

        if (data.containsKey("results")) {
            batch = ((List<Map<String, Object>>) data.get("results"));
        } else if (data.containsKey("cursor")) {
            var cursor = (Map<String, Object>) data.get("cursor");

            if (cursor.containsKey("firstBatch")) {
                batch = (List<Map<String, Object>>) cursor.get("firstBatch");
            } else if (cursor.containsKey("nextBatch")) {
                batch = (List<Map<String, Object>>) cursor.get("nextBatch");
            }
        }

        return new SingleBatchCursor(batch);
    }

    @Override
    public List<Map<String, Object>> readAnswerFor(MorphiumCursor crs) throws MorphiumDriverException {
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
        commandResults.put(ret, prepareResult(Doc.of("ok", 0, "errmsg", "no explain possible yet - in Memory!")));
        return ret;
    }

    public int runCommand(GenericCommand cmd) {
        log.info("Trying to handle generic Command");
        Map<String, Object> cmdMap = cmd.asMap();
        var commandName = cmdMap.keySet().stream().findFirst().get();
        Class<? extends MongoCommand> commandClass = commandsCache.get(commandName);

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
        commandResults.put(ret, prepareResult(Doc.of("ok", 0, "errmsg", "could not execute command inMemory")));
        return ret;
    }

    private int runCommand(StepDownCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        commandResults.put(ret, prepareResult(Doc.of("ok", 0, "errmsg", "no replicaset - in Memory!")));
        return ret;
    }

    private int runCommand(AbortTransactionCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        abortTransaction();
        int ret = commandNumber.incrementAndGet();
        commandResults.put(ret, prepareResult(Doc.of("ok", 1.0, "msg", "aborted")));
        return ret;
    }

    private int runCommand(CommitTransactionCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        commitTransaction();
        int ret = commandNumber.incrementAndGet();
        commandResults.put(ret, prepareResult(Doc.of("ok", 1.0, "msg", "committed")));
        return ret;
    }

    private int runCommand(UpdateMongoCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        Map<String, Object> stats = new HashMap<>();

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

            var res = update(cmd.getDb(), cmd.getColl(), (Map<String, Object>) update.get("q"), null, (Map<String, Object>) update.get("u"), multi, upsert, null, cmd.getWriteConcern());

            for (var e : res.entrySet()) {
                if (!stats.containsKey(e.getKey())) {
                    stats.put(e.getKey(), (Integer) e.getValue());
                } else {
                    stats.put(e.getKey(), (Integer) e.getValue() + (Integer) stats.get(e.getKey()));
                }
            }
        }

        stats.put("n", stats.get("inserted"));
        commandResults.put(ret, prepareResult(stats));
        return ret;
    }

    private int runCommand(WatchCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        watch(cmd);
        return ret;
    }

    private int runCommand(StoreMongoCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        var stats = store(cmd.getDb(), cmd.getColl(), cmd.getDocs(), null);
        commandResults.put(ret, prepareResult(Doc.of("ok", 1.0, "stats", stats)));
        return ret;
    }

    private int runCommand(ShutdownCommand cmd) {
        int ret = commandNumber.incrementAndGet();
        commandResults.put(ret, prepareResult(Doc.of("ok", 0.0, "errmsg", "shutdown in memory not supported")));
        return ret;
    }

    private int runCommand(ReplicastStatusCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        commandResults.put(ret, prepareResult(Doc.of("ok", 0.0, "errmsg", "no replicaset")));
        return ret;
    }

    private int runCommand(RenameCollectionCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        String target = cmd.getTo();
        String origin = cmd.getColl();
        var col = database.get(cmd.getDb()).remove(origin);
        database.get(cmd.getDb()).put(target, col);
        int ret = commandNumber.incrementAndGet();
        commandResults.put(ret, prepareResult(Doc.of("ok", 1.0, "msg", "renamed " + origin + " to " + target)));
        return ret;
    }

    private int runCommand(MapReduceCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        commandResults.put(ret, prepareResult(Doc.of("ok", 0.0, "errmsg", "MapReduce not possible inMem (yet)")));
        return ret;
    }

    private int runCommand(ListIndexesCommand cmd) {
        int ret = commandNumber.incrementAndGet();
        Map<String, List<Map<String, Object>>> indexesForDB = indicesByDbCollection.get(cmd.getDb());

        if (indexesForDB == null) {
            commandResults.put(ret, prepareResult(Doc.of("cursor", Doc.of("firstBatch", List.of()), "ok", 1.0, "ns", cmd.getDb() + "." + cmd.getColl(), "id", 0)));
            return ret;
        }

        var idx = indexesForDB.get(cmd.getColl());
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        var indices = new ArrayList<Map<String, Object>>();

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

        commandResults.put(ret, prepareResult(Doc.of("cursor", Doc.of("firstBatch", indices), "ok", 1.0, "ns", cmd.getDb() + "." + cmd.getColl(), "id", 0)));
        return ret;
    }

    private int runCommand(ListDatabasesCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        Map<String, Object> data = new HashMap<>();
        List<Map<String, Object>> dbList = new ArrayList<>();
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
        commandResults.put(ret, prepareResult(data));
        return ret;
    }

    private int runCommand(ListCollectionsCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        var m = prepareResult();
        var cursorData = new ArrayList<Map<String, Object>>();

        if (!database.containsKey(cmd.getDb())) {
            m.put("ok", 0.0);
            m.put("errmsg", "no such database");
            commandResults.put(ret, m);
            return ret;
        }

        for (String coll : database.get(cmd.getDb()).keySet()) {
            cursorData.add(Doc.of("name", coll, "type", "collection", "options", new Doc(), "info", Doc.of("readonly", false, "UUID", UUID.randomUUID())).add("idIndex",
             Doc.of("v", 2.0, "key", Doc.of("_id", 1), "name", "_id_1", "ns", cmd.getDb() + "." + coll)));
        }

        addCursor(cmd.getDb(), "$cmd.listCollections", m, cursorData);
        commandResults.put(ret, m);
        return ret;
    }

    private int runCommand(KillCursorsCommand cmd) {
        // log.error("Should not happen: " + cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        log.info("Killing cursors");
        int ret = commandNumber.incrementAndGet();

        for (Long id : cmd.getCursors()) {
            cursors.remove(id);
        }

        commandResults.put(ret, prepareResult());
        return ret;
    }

    private int runCommand(InsertMongoCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        List<Map<String, Object>> writeErrors = insert(cmd.getDb(), cmd.getColl(), cmd.getDocuments(), cmd.getWriteConcern());
        var m = prepareResult();
        m.put("n", cmd.getDocuments().size() - writeErrors.size());

        if (writeErrors.size() != 0) {
            m.put("writeErrors", writeErrors);
        }

        commandResults.put(ret, m);
        return ret;
    }

    private int runCommand(FindCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        List<Map<String, Object>> result = runFind(cmd);
        long cursorId = (long) 0;

        if (cmd.isTailable() != null && cmd.isTailable()) {
            // tailable cursor
            cursorId = (long) ret;
            cursors.put(cursorId, cmd);

            if (result == null || result.isEmpty()) {
                // waiting for some changes
                watch(cmd.getDb(), cmd.getColl(), cmd.getMaxTimeMS(), true, Arrays.asList(Doc.of("$match", cmd.getFilter())), new DriverTailableIterationCallback() {
                    @Override
                    public void incomingData(Map<String, Object> data, long dur) {
                        result.add(data);
                    }
                    @Override
                    public boolean isContinued() {
                        return false;
                    }
                });
            }
        }

        var m = prepareResult();
        addCursor(cmd.getDb(), cmd.getColl(), m, result);

        if (cursorId != 0) {
            ((Map) m.get("cursor")).put("id", cursorId);
        }

        commandResults.put(ret, m);
        return ret;
    }

    private List<Map<String, Object>> runFind(FindCommand cmd) throws MorphiumDriverException {
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
        var m = prepareResult();
        m.put("ok", 0);

        if (cursors.containsKey(cmd.getCursorId())) {
            FindCommand fnd = cursors.get(cmd.getCursorId());
            List<Map<String, Object>> result = new ArrayList<>();

            if (result == null || result.isEmpty()) {
                // waiting for some changes
                watch(fnd.getDb(), fnd.getColl(), fnd.getMaxTimeMS(), true, Arrays.asList(Doc.of("$match", fnd.getFilter())), new DriverTailableIterationCallback() {
                    @Override
                    public void incomingData(Map<String, Object> data, long dur) {
                        result.add((Map<String, Object>) data.get("fullDocument"));
                    }
                    @Override
                    public boolean isContinued() {
                        return false;
                    }
                });
            }

            // fnd.setLimit(fnd.getLimit()-result.size());
            // long cursorId = cmd.getCursorId();
            // if (fnd.getLimit()<=0){
            // cursorId=0;
            // cursors.remove(cmd.getCursorId());
            // }
            m.put("cursor", Doc.of("nextBatch", result, "ns", cmd.getDb() + "." + cmd.getColl(), "id", cmd.getCursorId()));
        }

        commandResults.put(ret, m);
        return ret;
    }

    private int runCommand(FindAndModifyMongoCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();

        if (cmd.isRemove()) {
            var list = find(cmd.getDb(), cmd.getColl(), cmd.getQuery(), cmd.getSort(), null, 0, 1);
            var res = delete (cmd.getDb(), cmd.getColl(), Doc.of("_id", list.get(0).get("_id")), null, false, null, null);
            commandResults.put(ret, prepareResult(Doc.of("value", list.get(0))));
        } else {
            var res = findAndOneAndUpdate(cmd.getDb(), cmd.getColl(), cmd.getQuery(), cmd.getUpdate(), cmd.getSort(), cmd.getCollation());
            commandResults.put(ret, prepareResult(Doc.of("value", res)));
        }

        return ret;
    }

    private int runCommand(DropMongoCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        drop(cmd.getDb(), cmd.getColl(), null);
        commandResults.put(ret, prepareResult(Doc.of("ok", 1.0, "msg", "dropped collection " + cmd.getColl())));

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
        commandResults.put(ret, prepareResult(Doc.of("ok", 1.0, "msg", "dropped database " + cmd.getDb())));
        return ret;
    }

    private int runCommand(DistinctMongoCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        var distinctResult = distinct(cmd.getDb(), cmd.getColl(), cmd.getKey(), cmd.getQuery(), cmd.getCollation());
        var m = prepareResult();
        m.put("values", distinctResult);
        commandResults.put(ret, m);
        return ret;
    }

    private int runCommand(DeleteMongoCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();

        // del.addDelete(Doc.of("q", new HashMap<>(), "limit", 0));
        for (var del : cmd.getDeletes()) {
            delete (cmd.getDb(), cmd.getColl(), (Map) del.get("q"), null, true, null, null);
        }

        commandResults.put(ret, prepareResult());
        return ret;
    }

    private int runCommand(DbStatsCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        var m = prepareResult();
        m.put("databases", database.size());
        commandResults.put(ret, m);
        return ret;
    }

    private int runCommand(CurrentOpCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        var m = prepareResult();
        m.put("ok", 0.0);
        m.put("errmsg", "no running ops in memory");
        commandResults.put(ret, m);
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

        commandResults.put(ret, prepareResult());
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

        database.putIfAbsent(cmd.getDb(), new ConcurrentHashMap<>());

        if (database.get(cmd.getDb()).containsKey(cmd.getColl())) {
            throw new IllegalArgumentException("Collection exists");
        }

        database.get(cmd.getDb()).put(cmd.getColl(), new ArrayList<>());
        var m = prepareResult();
        addCursor(cmd.getDb(), cmd.getColl(), m, Arrays.asList(Doc.of()));
        commandResults.put(ret, m);
        return ret;
    }

    private int runCommand(CountMongoCommand cmd) throws MorphiumDriverException {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        int ret = commandNumber.incrementAndGet();
        var m = prepareResult();
        var cnt = find(cmd.getDb(), cmd.getColl(), cmd.getQuery(), null, null, 0, 0).size();
        m.put("n", cnt);
        m.put("count", cnt);
        commandResults.put(ret, m);
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
        List<Map<String, Object>> indexes = getIndexes(cmd.getDb(), cmd.getColl());
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
        commandResults.put(ret, m);
        return ret;
    }

    private int runCommand(ClearCollectionCommand cmd) {
        // log.info(cmd.getCommandName() + " - incoming (" +
        // cmd.getClass().getSimpleName() + ")");
        database.get(cmd.getDb()).get(cmd.getColl()).clear();
        int ret = commandNumber.incrementAndGet();
        commandResults.put(ret, prepareResult(Doc.of("ok", 1.0)));
        return ret;
    }

    private int runCommand(AggregateMongoCommand cmd) {
        throw new IllegalArgumentException("pleas use morphium for aggregation in Memory!");
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
        var m = addCursor(cmd.getDb(), cmd.getColl(), prepareResult(),
          Arrays.asList(Doc.of("helloOk", true, "isWritablePrimary", true, "maxBsonObjectSize", 128 * 1024 * 1024, "msg", driverName + " - ok")));
        commandResults.put(ret, m);
        return ret;
    }

    private Map<String, Object> addCursor(String db, String coll, Map<String, Object> result, List<Map<String, Object>> data) {
        result.put("cursor", Doc.of("firstBatch", data, "ns", db + "." + coll, "id", 0));
        return result;
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

        Runnable r = ()->{
            // Notification of watchers.
            try {
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
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        exec.scheduleWithFixedDelay(r, 100, 100, TimeUnit.MILLISECONDS); // check for events every 500ms
        scheduleExpire();
    }

    private void scheduleExpire() {
        expire = exec.scheduleWithFixedDelay(()->{
            // checking indexes for expire options
            try {
                for (String db : database.keySet()) {
                    for (String coll : database.get(db).keySet()) {
                        var idx = getIndexes(db, coll);

                        for (var i : idx) {
                            Map<String, Object> options = (Map<String, Object>) i.get("$options");

                            if (options != null && options.containsKey("expireAfterSeconds")) {
                                // log.info("Found collection candidate for expire..." + db + "." + coll);
                                var k = new HashMap<>(i);
                                k.remove("$options");
                                var keys = k.keySet().toArray(new String[] {});

                                if (keys.length > 1) {
                                    log.error("Too many keys for expire-index!!!");
                                } else {
                                    try {
                                        var candidates = find(db, coll, Doc.of(keys[0], Doc.of("$lte", new Date(System.currentTimeMillis() - ((int) options.get("expireAfterSeconds")) * 1000))), null,
                                        null, null, 0, 0, true);

                                        for (Map<String, Object> o : candidates) {
                                            if (!o.containsKey(keys[0])) {
                                                continue;
                                            }

                                            getCollection(db, coll).remove(o);
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
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
            e.printStackTrace();
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
    public void release() {
        stats.get(DriverStatsKey.CONNECTIONS_RELEASED).incrementAndGet();
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
            @Override
            public MongoConnection getConnection() {
                return null;
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
        // noinspection unchecked
        // crs.setInternalCursorObject(inCrs);
        // int l = batchSize;
        // if (limit != 0 && limit < batchSize) {
        // l = limit;
        // }
        // List<Map<String,Object>> res = find(db, collection, query, sort, projection,
        // skip, l, batchSize, readPreference, coll, findMetaData);
        // crs.setBatch(new CopyOnWriteArrayList<>(res));
        //
        // if (res.size() < batchSize) {
        // //noinspection unchecked
        // crs.setInternalCursorObject(null); //cursor ended - no more data
        // } else {
        // inCrs.dataRead = res.size();
        // }
        return crs;
    }

    @SuppressWarnings({"RedundantThrows", "CatchMayIgnoreException"})

    public void watch(String db, String collection, int timeout, boolean fullDocumentOnUpdate, List<Map<String, Object>> pipeline, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        Object monitor = new Object();
        monitors.add(monitor);
        DriverTailableIterationCallback cback = new DriverTailableIterationCallback() {
            public void incomingData(Map<String, Object> data, long dur) {
                if (pipeline != null && !pipeline.isEmpty()) {
                    InMemAggregator agg = new InMemAggregator(null, Map.class, Map.class);

                    for (var step : pipeline) {
                        List<Map<String, Object>> lst = agg.execStep(step, Arrays.asList(data));

                        if (lst == null || lst.isEmpty()) {
                            return;
                        }

                        data = lst.get(0);
                    }
                }

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
            db = db + "." + collection;
        }

        watchersByDb.putIfAbsent(db, new CopyOnWriteArrayList<>());
        watchersByDb.get(db).add(cback);

        // simulate blocking
        try {
            synchronized (monitor) {
                monitor.wait();
                monitors.remove(monitor);
            }
        } catch (InterruptedException e) {
        }

        watchersByDb.get(db).remove(cb);
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
            @Override
            public MongoConnection getConnection() {
                return null;
            }
        };
        next.setCursorId(crs.getCursorId());
        // InMemoryCursor oldCrs = (InMemoryCursor) crs.getInternalCursorObject();
        // if (oldCrs == null) {
        // return null;
        // }
        //
        // InMemoryCursor inCrs = new InMemoryCursor();
        // inCrs.setReadPreference(oldCrs.getReadPreference());
        // inCrs.setFindMetaData(oldCrs.getFindMetaData());
        // inCrs.setDb(oldCrs.getDb());
        // inCrs.setQuery(oldCrs.getQuery());
        // inCrs.setCollection(oldCrs.getCollection());
        // inCrs.setProjection(oldCrs.getProjection());
        // inCrs.setBatchSize(oldCrs.getBatchSize());
        // inCrs.setCollation(oldCrs.getCollation());
        //
        // inCrs.setLimit(oldCrs.getLimit());
        //
        // inCrs.setSort(oldCrs.getSort());
        // inCrs.skip = oldCrs.getDataRead() + 1;
        // int limit = oldCrs.getBatchSize();
        // if (oldCrs.getLimit() != 0) {
        // if (oldCrs.getDataRead() + oldCrs.getBatchSize() > oldCrs.getLimit()) {
        // limit = oldCrs.getLimit() - oldCrs.getDataRead();
        // }
        // }
        // List<Map<String,Object>> res = find(inCrs.getDb(), inCrs.getCollection(),
        // inCrs.getQuery(), inCrs.getSort(), inCrs.getProjection(), inCrs.getSkip(),
        // limit, inCrs.getBatchSize(), inCrs.getReadPreference(), inCrs.getCollation(),
        // inCrs.getFindMetaData());
        // next.setBatch(new CopyOnWriteArrayList<>(res));
        // if (res.size() < inCrs.getBatchSize() || (oldCrs.limit != 0 && res.size() +
        // oldCrs.getDataRead() > oldCrs.limit)) {
        // //finished!
        // //noinspection unchecked
        // next.setInternalCursorObject(null);
        // } else {
        // inCrs.setDataRead(oldCrs.getDataRead() + res.size());
        // //noinspection unchecked
        // next.setInternalCursorObject(inCrs);
        // }
        return next;
    }

    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Object> sort, Map<String, Object> projection, int skip, int limit)
    throws MorphiumDriverException {
        return find(db, collection, query, sort, projection, null, skip, limit, false);
    }

    @SuppressWarnings({"RedundantThrows", "UnusedParameters"})
    private List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Object> sort, Map<String, Object> projection, Map<String, Object> collation, int skip,
     int limit, boolean internal) throws MorphiumDriverException {
        List<Map<String, Object>> partialHitData = new ArrayList<>();

        if (query == null) {
            query = Doc.of();
        }

        if (query.containsKey("$and")) {
            // and complex query handling ?!?!?
            List<Map<String, Object>> m = (List<Map<String, Object>>) query.get("$and");

            if (m != null && !m.isEmpty()) {
                for (Map<String, Object> subquery : m) {
                    List<Map<String, Object>> dataFromIndex = getDataFromIndex(db, collection, subquery);

                    // one and-query result is enough to find candidates!
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
            Collator coll = QueryHelper.getCollator(collation);
            data.sort((o1, o2)->{
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

                ret.add(o);
            }

            if (limit > 0 && ret.size() >= limit) {
                break;
            }

            // todo add projection
        }

        return new ArrayList<>(ret);
    }

    private List<Map<String, Object>> getDataFromIndex(String db, String collection, Map<String, Object> query) {
        List<Map<String, Object>> ret = null;
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
                Map<Integer, List<Map<String, Object>>> indexDataForCollection = getIndexDataForCollection(db, collection, fields);
                ret = indexDataForCollection.get(bucketId);

                if (ret == null || ret.size() == 0) {
                    // no direkt hit, need to use index data
                    //
                    // log.debug("indirect Index hit - fields: "+fields+" Index size:
                    // "+indexDataForCollection.size());
                    ret = new ArrayList<>();

                    //// long start = System.currentTimeMillis();
                    for (Map.Entry<Integer, List<Map<String, Object>>> k : indexDataForCollection.entrySet()) {
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
        List<Map<String, Object>> d = getCollection(db, collection);
        List<Map<String, Object>> data = new CopyOnWriteArrayList<>(d);

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

    public List<Map<String, Object>> findByFieldValue(String db, String coll, String field, Object value) throws MorphiumDriverException {
        List<Map<String, Object>> ret = new ArrayList<>();
        List<Map<String, Object>> data = new CopyOnWriteArrayList<>(getCollection(db, coll));

        for (Map<String, Object> obj : data) {
            if (obj.get(field) == null && value != null) {
                continue;
            }

            if ((obj.get(field) == null && value == null) || obj.get(field).equals(value)) {
                ConcurrentHashMap<String, Object> add = new ConcurrentHashMap<>(obj);

                if (add.get("_id") instanceof ObjectId) {
                    add.put("_id", new MorphiumId((ObjectId) add.get("_id")));
                }

                ret.add(add);
            }
        }

        return ret;
    }

    // public Map<IndexKey, List<Map<String, Object>>>
    // getIndexDataForCollection(String db, String collection, String fields) {
    public Map<Integer, List<Map<String, Object>>> getIndexDataForCollection(String db, String collection, String fields) {
        indexDataByDBCollection.putIfAbsent(db, new ConcurrentHashMap<>());
        indexDataByDBCollection.get(db).putIfAbsent(collection, new ConcurrentHashMap<>());
        indexDataByDBCollection.get(db).get(collection).putIfAbsent(fields, new HashMap<>());
        return indexDataByDBCollection.get(db).get(collection).get(fields);
    }

    public synchronized List<Map<String, Object>> insert(String db, String collection, List<Map<String, Object>> objs, Map<String, Object> wc) throws MorphiumDriverException {
        int errors = 0;
        objs = new ArrayList<>(objs);
        List<Map<String, Object>> writeErrors = new ArrayList<>();
        // check for unique index
        List<Map<String, Object>> indexes = getIndexes(db, collection);

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
                                List<Map<String, Object>> and = new ArrayList();

                                for (var e : q.entrySet()) {
                                    and.add(Doc.of(e.getKey(), e.getValue()));
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
                Map<Integer, List<Map<String, Object>>> id = getIndexDataForCollection(db, collection, "_id");
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
            List<Map<String, Object>> idx = indexes;
            Map<String, Map<Integer, List<Map<String, Object>> >> indexData = indexDataByDBCollection.get(db).get(collection);

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

    public synchronized Map<String, Integer> store(String db, String collection, List<Map<String, Object>> objs, Map<String, Object> wc) throws MorphiumDriverException {
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
        return ret;
    }

    private Map<String, List<Map<String, Object>>> getDB(String db) {
        if (currentTransaction.get() == null) {
            database.putIfAbsent(db, new ConcurrentHashMap<>());
            return database.get(db);
        } else {
            // noinspection unchecked
            currentTransaction.get().getDatabase().putIfAbsent(db, new ConcurrentHashMap<>());
            // noinspection unchecked
            return (Map<String, List<Map<String, Object>>>) currentTransaction.get().getDatabase().get(db);
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

    @Override
    public boolean replyAvailableFor(int msgId) {
        return commandResults.containsKey(msgId);
    }

    @Override
    public OpMsg getReplyFor(int msgid, long timeout) throws MorphiumDriverException {
        OpMsg msg = new OpMsg();
        msg.setResponseTo(msgid);
        msg.setMessageId(0);
        Map<String, Object> o = new HashMap<>(commandResults.get(msgid));
        msg.setFirstDoc(o);
        return msg;
    }

    @Override
    public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
        return commandResults.remove(id);
    }

    @SuppressWarnings("ConstantConditions")

    public synchronized Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> sort, Map<String, Object> op, boolean multiple, boolean upsert,
     Map<String, Object> collation, Map<String, Object> wc) throws MorphiumDriverException {
        List<Map<String, Object>> lst = find(db, collection, query, sort, null, collation, 0, multiple ? 0 : 1, true);
        boolean insert = false;
        int count = 0;

        if (lst == null) {
            lst = new ArrayList<>();
        }

        if (upsert && lst.isEmpty()) {
            lst.add(new ConcurrentHashMap<>());

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

        Set<Object> modified = new HashSet<>();

        for (Map<String, Object> obj : lst) {
            for (String operand : op.keySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> cmd = (Map<String, Object>) op.get(operand);

                switch (operand) {
                case "$set":
                    // $set:{"field":"value", "other_field": 123}
                    for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                        if (entry.getValue() != null) {
                            var v = entry.getValue();

                            if (v instanceof Map) {
                                try {
                                    v = Expr.parse(v).evaluate(obj);
                                } catch (Exception e) {
                                    // swallow
                                }
                            }

                            obj.put(entry.getKey(), v);
                        } else {
                            obj.remove(entry.getKey());
                        }
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

                        if (value == null) {
                            value = new Integer(0);
                        }

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

                        if (!obj.get(entry.getKey()).equals(value)) {
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
                    // $pull: { results: { score: 8 , item: "B" } }
                    // $pull: { results: { answers: { $elemMatch: { q: 2, a: { $gte: 8 } } } } }

                    // Case 1:   $pull: { fruits: { $in: [ "apples", "oranges" ] }, vegetables: "carrots" }
                    for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                        Set<String> objectsToBeDeleted = new HashSet<>();
                        List<String> v = new ArrayList<>((List) obj.get(entry.getKey()));
                        Object entryValue = entry.getValue();
                        if(entryValue instanceof Map) {
                            Map<String, Object> entryValueAsMap = (Map<String, Object>) entryValue;
                            for (Map.Entry<String, Object> submapEntry : entryValueAsMap.entrySet()) {
                                if(submapEntry.getKey().startsWith("$")) {
                                    Expr entryValueParsed = Expr.parse(entryValue);
                                    String jsonString = Utils.toJsonString(entryValueParsed.toQueryObject());
                                    System.out.println("jsonString=" + jsonString);
                                    var exprValue = entryValueParsed.evaluate(obj);
                                    List<Expr.ValueExpr> exprValueAsList = (List<Expr.ValueExpr>) exprValue;
                                    objectsToBeDeleted.addAll(exprValueAsList.stream().map(o -> o.toQueryObject().toString()).collect(Collectors.toList()));
                                    break;
                                }
                            }
                        } else if(entryValue instanceof String) {
                            String entryValueAsString = (String) entryValue;
                            objectsToBeDeleted.add(entryValueAsString);
                        }

                        boolean valueIsChanged = objectsToBeDeleted.stream().anyMatch(object -> v.contains( object));
                        if(valueIsChanged) {
                            modified.add(obj.get("_id"));
                        }
                        v.removeAll(objectsToBeDeleted);
                        obj.put(entry.getKey(), v);
                    }

                    break;

                    case "$pullAll":
                    // $pullAll: { <field1>: [ <value1>, <value2> ... ], ... }
                    // Examples:
                    // $pullAll: { scores: [ 0, 5 ] }

                    for (Map.Entry<String, Object> entry : cmd.entrySet()) {
                        if (obj.get(entry.getKey())==null){
                            break;
                        }
                        List v = new ArrayList((List) obj.get(entry.getKey()));
                        List objectsToBeDeleted = (List) entry.getValue();

                        boolean valueIsChanged = objectsToBeDeleted.stream().anyMatch(object -> v.contains(object));
                        if(valueIsChanged) {
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
                        List v = (List) obj.get(entry.getKey());

                        if (v == null) {
                            v = new ArrayList();
                            obj.put(entry.getKey(), v);
                            modified.add(obj.get("_id"));
                        }

                        if (entry.getValue() instanceof Map) {
                            if (((Map) entry.getValue()).get("$each") != null) {
                                // noinspection unchecked
                                if (operand.equals("$addToSet")) {
                                    for (Object elem : (List)((Map) entry.getValue()).get("$each")) {
                                        if (!v.contains(elem)) {
                                            v.add(elem);
                                        }
                                    }
                                } else {
                                    v.addAll((List)((Map) entry.getValue()).get("$each"));
                                }
                            } else {
                                // noinspection unchecked
                                if (operand.equals("$addToSet") && v.contains(entry.getValue())) {
                                    break;
                                }

                                if (operand.equals("$pushAll") && entry.getValue() instanceof List) {
                                    v.addAll(((List) entry.getValue()));
                                } else {
                                    v.add(entry.getValue());
                                }
                            }
                        } else {
                            // noinspection unchecked
                            if (operand.equals("$addToSet") && v.contains(entry.getValue())) {
                                break;
                            }

                            if (operand.equals("$pushAll") && entry.getValue() instanceof List) {
                                v.addAll(((List) entry.getValue()));
                            } else {
                                v.add(entry.getValue());
                            }
                        }
                    }

                    break;

                default:
                    throw new RuntimeException("unknown operand " + operand);
                }
            }

            count++;

            if (!insert) {
                notifyWatchers(db, collection, "update", obj);
            }
        }

        if (insert) {
            store(db, collection, lst, wc);
        }

        indexDataByDBCollection.get(db).remove(collection);
        updateIndexData(db, collection, null);
        return Doc.of("matched", (Object) lst.size(), "inserted", insert ? 1 : 0, "nModified", count, "modified", count);
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
        Runnable r = ()->{
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

                if (doc != null) {
                    data.put("fullDocument", doc);
                }

                if (op != null) {
                    data.put("operationType", op);
                }

                Map m = Collections.synchronizedMap(new HashMap<>(UtilsMap.of("db", db)));
                // noinspection unchecked
                m.put("coll", collection);
                data.put("ns", m);
                data.put("txnNumber", tx);
                data.put("clusterTime", System.currentTimeMillis());

                if (doc != null) {
                    if (doc.get("_id") != null) {
                        data.put("documentKey", doc.get("_id"));
                    }
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

    public synchronized Map<String, Object> delete (String db, String collection, Map<String, Object> query, Map<String, Object> sort, boolean multiple, Map<String, Object> collation, WriteConcern wc)
    throws MorphiumDriverException {
        List<Map<String, Object>> toDel = new ArrayList<>(find(db, collection, query, null, UtilsMap.of("_id", 1), 0, multiple ? 0 : 1));

        for (Map<String, Object> o : toDel) {
            for (Map<String, Object> dat : new ArrayList<>(getCollection(db, collection))) {
                if (dat.get("_id") instanceof ObjectId || dat.get("_id") instanceof MorphiumId) {
                    if (dat.get("_id").toString().equals(o.get("_id").toString())) {
                        getCollection(db, collection).remove(dat);

                        // indexDataByDBCollection.get(db).remove(collection);
                        // updateIndexData(db,collection,null);
                        for (String keys : indexDataByDBCollection.get(db).get(collection).keySet()) {
                            Map<Integer, List<Map<String, Object>>> id = getIndexDataForCollection(db, collection, keys);

                            for (int bucketId : id.keySet()) {
                                var lst = new ArrayList<Map<String, Object>>(id.get(bucketId));

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
                        // indexDataByDBCollection.get(db).remove(collection);
                        // updateIndexData(db,collection,null);

                        for (String keys : indexDataByDBCollection.get(db).get(collection).keySet()) {
                            Map<Integer, List<Map<String, Object>>> id = getIndexDataForCollection(db, collection, keys);

                            for (int bucketId : id.keySet()) {
                                var lst = new ArrayList<Map<String, Object>>(id.get(bucketId));

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
            notifyWatchers(db, collection, "delete", o);
        }

        return new ConcurrentHashMap<>();
    }

    private List<Map<String, Object>> getCollection(String db, String collection) throws MorphiumDriverException {
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
        List<Map<String, Object>> list = find(db, collection, filter, null, null, 0, 0);
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
            // new collection, create default index for _id
            ArrayList<Map<String, Object>> value = new ArrayList<>();
            getIndexesForDB(db).put(collection, value);
            value.add(Doc.of("_id", 1, "$options", Doc.of("name", "_id_1")));
        }

        return getIndexesForDB(db).get(collection);
    }

    public List<String> getCollectionNames(String db) {
        return null;
    }

    public Map<String, Object> findAndOneAndDelete(String db, String col, Map<String, Object> query, Map<String, Object> sort, Map<String, Object> collation) throws MorphiumDriverException {
        List<Map<String, Object>> r = find(db, col, query, sort, null, 0, 1);

        if (r.size() == 0) {
            return null;
        }

        delete (db, col, Doc.of("_id", r.get(0).get("_id")), null, false, collation, null);
        return r.get(0);
    }

    public synchronized Map<String, Object> findAndOneAndUpdate(String db, String col, Map<String, Object> query, Map<String, Object> update, Map<String, Object> sort, Map<String, Object> collation)
    throws MorphiumDriverException {
        List<Map<String, Object>> ret = find(db, col, query, sort, null, 0, 1);
        update(db, col, query, null, update, false, false, collation, null);
        return ret.get(0);
    }

    public synchronized Map<String, Object> findAndOneAndReplace(String db, String col, Map<String, Object> query, Map<String, Object> replacement, Map<String, Object> sort,
     Map<String, Object> collation) throws MorphiumDriverException {
        List<Map<String, Object>> ret = find(db, col, query, sort, null, 0, 1);

        if (ret.get(0).get("_id") != null) {
            replacement.put("_id", ret.get(0).get("_id"));
        } else {
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

        List<Map<String, Object>> indexes = getIndexes(db, collection);
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
        } else {
            if (index.size() == 2 && index.containsKey("_id") && index.containsKey("$options")) {
                // ignoring attempt to re-create_id index
            } else {
                log.warn("Index with those keys already exists: " + Utils.toJsonString(index));
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

                Map<Integer, List<Map<String, Object>>> index = getIndexDataForCollection(db, collection, b.toString());
                index.putIfAbsent(bucketId, new CopyOnWriteArrayList<>());
                index.get(bucketId).add(doc);
            }
        }
    }

    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing) throws MorphiumDriverException {
        throw new FunctionNotSupportedException("no map reduce in memory");
    }

    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query) throws MorphiumDriverException {
        throw new FunctionNotSupportedException("no map reduce in memory");
    }

    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query, Map<String, Object> sorting, Collation collation)
    throws MorphiumDriverException {
        throw new FunctionNotSupportedException("no map reduce in memory");
    }

    public void commitTransaction() {
        if (currentTransaction.get() == null) {
            throw new IllegalArgumentException("No transaction in progress");
        }

        InMemTransactionContext ctx = currentTransaction.get();
        // noinspection unchecked
        database.putAll(ctx.getDatabase());
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
}
