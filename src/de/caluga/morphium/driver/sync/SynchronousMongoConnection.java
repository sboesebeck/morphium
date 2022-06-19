package de.caluga.morphium.driver.sync;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.*;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;
import java.util.stream.Collectors;

/**
 * User: Stephan Bösebeck
 * Date: 02.12.15
 * Time: 23:47
 * <p>
 * connects to one node only!
 */
public class SynchronousMongoConnection extends DriverBase {

    private final Logger log = LoggerFactory.getLogger(SynchronousMongoConnection.class);
    private Socket s;
    private OutputStream out;
    private InputStream in;

    private ThreadLocal<MorphiumTransactionContextImpl> transactionContext = new ThreadLocal<>();

    //    private Vector<OpReply> replies = new Vector<>();


    private void reconnect() {
        try {
            if (out != null) {
                out.close();
            }
            if (in != null) {
                in.close();
            }
            if (s != null) {
                s.close();
            }
            connect();
        } catch (Exception e) {
            s = null;
            in = null;
            out = null;
            log.error("Could not reconnect!", e);
        }
    }

    @Override
    public void connect(String replSet) {
        try {
            String host = getHostSeed()[0];
            String h[] = host.split(":");
            int port = 27017;
            if (h.length > 1) {
                port = Integer.parseInt(h[1]);
            }
            s = new Socket(h[0], port);
            out = s.getOutputStream();
            in = s.getInputStream();


            try {
                var result = runCommand("local", Doc.of("hello", true, "helloOk", true,
                        "client", Doc.of("application", Doc.of("name", "Morphium"),
                                "driver", Doc.of("name", "MorphiumDriver", "version", "1.0"),
                                "os", Doc.of("type", "MacOs"))

                ));
                //log.info("Got result");
                if (!result.get("secondary").equals(false)) {
                    //TODO: search for master!
                    disconnect();
                    throw new RuntimeException("Cannot run with secondary connection only!");
                }
                if (replSet != null) {
                    setReplicaSetName((String) result.get("setName"));
                    if (replSet != null && !replSet.equals(getReplicaSetName())) {
                        throw new MorphiumDriverException("Replicaset name is wrong - connected to " + getReplicaSetName() + " should be " + replSet);
                    }
                }
                setMaxBsonObjectSize((Integer) result.get("maxBsonObjectSize"));
                setMaxMessageSize((Integer) result.get("maxMessageSizeBytes"));
                setMaxWriteBatchSize((Integer) result.get("maxWriteBatchSize"));

            } catch (MorphiumDriverException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            throw new RuntimeException("connection failed", e);
        }
    }


    protected OpMsg getReply(int waitingFor, int timeout) throws MorphiumDriverException {
        return getReply();
    }

    private synchronized OpMsg getReply() throws MorphiumDriverNetworkException {
        return (OpMsg) WireProtocolMessage.parseFromStream(in);
    }


    @Override
    public String getName() {
        return "SingleSyncConnection";
    }

    @Override
    public int getBuildNumber() {
        return 0;
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public void setConnectionUrl(String connectionUrl) {

    }

    @Override
    public void connect() {
        connect(null);
    }


    public boolean isConnected() {
        return s != null && s.isConnected();
    }

    @Override
    public void disconnect() {
        try {
            in.close();
            out.close();
            s.close();
        } catch (IOException e) {
            //throw new RuntimeException(e);
        }
    }

    @Override
    public MorphiumTransactionContext startTransaction(boolean autoCommit) {
        if (this.transactionContext.get() != null) throw new IllegalArgumentException("Transaction in progress");
        MorphiumTransactionContextImpl ctx = new MorphiumTransactionContextImpl();
        ctx.setLsid(UUID.randomUUID().toString());
        ctx.setTxnNumber((long) getNextId());
        this.transactionContext.set(ctx);
        return ctx;
    }

    @Override
    public MorphiumTransactionContext getTransactionContext() {
        return transactionContext.get();
    }

    @Override
    public void setTransactionContext(MorphiumTransactionContext ctx) {
        if (transactionContext.get() != null) throw new IllegalArgumentException("Transaction already in progress!");
        if (ctx instanceof MorphiumTransactionContextImpl)
            transactionContext.set((MorphiumTransactionContextImpl) ctx);
        else
            throw new IllegalArgumentException("Transaction context of wrong type!");
    }

    @Override
    public boolean isTransactionInProgress() {
        return transactionContext.get() != null;
    }

    @Override
    public void commitTransaction() throws MorphiumDriverException {
        if (transactionContext.get() == null)
            throw new IllegalArgumentException("No transaction in progress, cannot commit");
        MorphiumTransactionContextImpl ctx = transactionContext.get();
        runCommand("admin", Doc.of("commitTransaction", 1, "txnNumber", ctx.getTxnNumber(), "autocommit", false, "lsid", Doc.of("id", ctx.getLsid())));
        transactionContext.remove();
    }

    @Override
    public void abortTransaction() throws MorphiumDriverException {
        if (transactionContext.get() == null)
            throw new IllegalArgumentException("No transaction in progress, cannot abort");
        MorphiumTransactionContextImpl ctx = transactionContext.get();
        runCommand("admin", Doc.of("abortTransaction", 1, "txnNumber", ctx.getTxnNumber(), "autocommit", false, "lsid", Doc.of("id", ctx.getLsid())));
        transactionContext.remove();
    }

    public MorphiumCursor initAggregationIteration(AggregateCmdSettings settings) throws MorphiumDriverException {
        return (MorphiumCursor) new NetworkCallHelper().doCall(() -> {
            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());
            settings.setMetaData("server", getHostSeed()[0]);
            Doc doc = settings.asMap("aggregate");
            doc.putIfAbsent("cursor", new Doc());
            if (settings.getBatchSize() != null) {
                ((Map) doc.get("cursor")).put("batchSize", settings.getBatchSize());
                doc.remove("batchSize");
            } else {
                ((Map) doc.get("cursor")).put("batchSize", getDefaultBatchSize());
            }
            q.setFirstDoc(doc);
            OpMsg reply;
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);
                reply = waitForReply(settings, q);
            }

            MorphiumCursor crs = new MorphiumCursor();
            @SuppressWarnings("unchecked") Doc cursor = (Doc) reply.getFirstDoc().get("cursor");
            if (cursor != null && cursor.get("id") != null) {
                crs.setCursorId((Long) cursor.get("id"));
            }

            if (cursor != null) {
                if (cursor.get("firstBatch") != null) {
                    //noinspection unchecked
                    crs.setBatch((List) cursor.get("firstBatch"));
                } else if (cursor.get("nextBatch") != null) {
                    //noinspection unchecked
                    crs.setBatch((List) cursor.get("nextBatch"));
                }
            }

            SynchronousConnectCursor internalCursorData = new SynchronousConnectCursor(this);
            internalCursorData.setBatchSize(settings.getBatchSize() == null ? getDefaultBatchSize() : settings.getBatchSize());
            internalCursorData.setCollection(settings.getColl());
            internalCursorData.setDb(settings.getDb());
            //noinspection unchecked
            crs.setInternalCursorObject(internalCursorData);
            return Map.of("cursor", crs);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("cursor");

    }

    @Override
    public List<Map<String, Object>> aggregate(AggregateCmdSettings settings) throws MorphiumDriverException {
        //noinspection unchecked
        return (List<Map<String, Object>>) new NetworkCallHelper().doCall(() -> {
            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());

            Doc doc = settings.asMap("aggregate");
            doc.put("cursor", Doc.of("batchSize", getDefaultBatchSize()));

            q.setFirstDoc(doc);
            settings.setMetaData("server", getHostSeed()[0]);
            long start = System.currentTimeMillis();
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);
                List<Map<String, Object>> lst = readBatches(q.getMessageId(), settings.getDb(), settings.getColl(), getMaxWriteBatchSize());
                settings.setMetaData("duration", System.currentTimeMillis() - start);
                return Doc.of("result", lst);
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("result");
    }

    @Override
    public long count(CountCmdSettings settings) throws MorphiumDriverException {
        Map<String, Object> ret = new NetworkCallHelper().doCall(() -> {
            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());

            Doc doc = settings.asMap("count");
            q.setFirstDoc(doc);
            q.setFlags(0);
            q.setResponseTo(0);
            settings.setMetaData("server", getHostSeed()[0]);
            OpMsg rep;
            long start = System.currentTimeMillis();
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);
                rep = waitForReply(settings.getDb(), settings.getColl(), q.getMessageId());
                settings.setMetaData("duration", System.currentTimeMillis() - start);
            }
            Integer n = (Integer) rep.getFirstDoc().get("n");
            return Doc.of("count", n == null ? 0 : n);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
        return ((int) ret.get("count"));
    }

    @Override
    public void watch(WatchCmdSettings settings) throws MorphiumDriverException {
        int maxWait = 0;
        if (settings.getDb() == null) settings.setDb("1"); //watch all dbs!
        if (settings.getColl() == null) {
            //watch all collections
            settings.setColl("1");
        }
        if (settings.getMaxWaitTime() == null || settings.getMaxWaitTime() <= 0) maxWait = getReadTimeout();
        OpMsg startMsg = new OpMsg();
        int batchSize = settings.getBatchSize() == null ? getDefaultBatchSize() : settings.getBatchSize();
        startMsg.setMessageId(getNextId());
        ArrayList<Doc> localPipeline = new ArrayList<>();
        localPipeline.add(Doc.of("$changeStream", new HashMap<>()));
        if (settings.getPipeline() != null && !settings.getPipeline().isEmpty())
            localPipeline.addAll(settings.getPipeline());
        Doc cmd = Doc.of("aggregate", settings.getColl()).add("pipeline", localPipeline)
                .add("cursor", Doc.of("batchSize", batchSize))  //getDefaultBatchSize()
                .add("$db", settings.getDb());
        startMsg.setFirstDoc(cmd);
        long start = System.currentTimeMillis();
        sendQuery(startMsg);

        OpMsg msg = startMsg;
        settings.setMetaData("server", getHostSeed()[0]);
        long docsProcessed = 0;
        while (true) {
            OpMsg reply = waitForReply(settings.getDb(), settings.getColl(), msg.getMessageId());
            log.info("got answer for watch!");
            Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");
            if (cursor == null) throw new MorphiumDriverException("Could not watch - cursor is null");
            log.debug("CursorID:" + cursor.get("id").toString());

            long cursorId = Long.parseLong(cursor.get("id").toString());
            settings.setMetaData("cursor", cursorId);
            List<Map<String, Object>> result = (List<Map<String, Object>>) cursor.get("firstBatch");
            if (result == null) {
                result = (List<Map<String, Object>>) cursor.get("nextBatch");
            }
            if (result != null) {
                for (Map<String, Object> o : result) {
                    settings.getCb().incomingData(o, System.currentTimeMillis() - start);
                    docsProcessed++;
                }
            }
            if (!settings.getCb().isContinued()) {
                killCursors(settings.getDb(), settings.getColl(), cursorId);
                settings.setMetaData("duration", System.currentTimeMillis() - start);
                break;
            }
            if (cursorId != 0) {
                msg = new OpMsg();
                msg.setMessageId(getNextId());

                Doc doc = new Doc();
                doc.put("getMore", cursorId);
                doc.put("collection", settings.getColl());

                doc.put("batchSize", batchSize);
                doc.put("maxTimeMS", maxWait);
                doc.put("$db", settings.getDb());
                msg.setFirstDoc(doc);
                sendQuery(msg);
                log.debug("sent getmore....");
            } else {
                log.debug("Cursor exhausted, restarting");
                msg = startMsg;
                msg.setMessageId(getNextId());
                sendQuery(msg);

            }
        }
    }

    @Override
    public List<Object> distinct(DistinctCmdSettings settings) throws MorphiumDriverException {
        Map<String, Object> ret = new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setMessageId(getNextId());

            Doc cmd = settings.asMap("distinct");
            op.setFirstDoc(cmd);
            settings.setMetaData("server", getHostSeed()[0]);
            synchronized (SynchronousMongoConnection.this) {
                long start = System.currentTimeMillis();
                sendQuery(op);
                //noinspection EmptyCatchBlock
                OpMsg res = waitForReply(settings.getDb(), null, op.getMessageId());
                settings.setMetaData("duration", System.currentTimeMillis() - start);
                return res.getFirstDoc();
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
        //noinspection unchecked
        return (List<Object>) ret.get("values");
    }

    @Override
    public List<Map<String, Object>> mapReduce(MapReduceSettings settings) throws MorphiumDriverException {
        settings.setMetaData("server", getHostSeed()[0]);
        OpMsg msg = new OpMsg();
        msg.setMessageId(getNextId());
        msg.setFirstDoc(settings.asMap("mapReduce"));
        long start = System.currentTimeMillis();
        sendQuery(msg);
        List<Map<String, Object>> res = readBatches(msg.getMessageId(), settings.getDb(), settings.getColl(), getDefaultBatchSize());
        settings.setMetaData("duration", System.currentTimeMillis() - start);
        return res;
    }

    @Override
    public int delete(DeleteCmdSettings settings) throws MorphiumDriverException {
        Map<String, Object> d = new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setMessageId(getNextId());
            Doc o = settings.asMap("delete");
            op.setFirstDoc(o);
            synchronized (SynchronousMongoConnection.this) {
                settings.setMetaData("server", getHostSeed()[0]);
                long start = System.currentTimeMillis();
                sendQuery(op);

                int waitingfor = op.getMessageId();

                OpMsg reply = waitForReply(settings.getDb(), settings.getColl(), waitingfor);
                settings.setMetaData("duration", System.currentTimeMillis() - start);
                return reply.getFirstDoc();
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());

        if (d.containsKey("n")) {
            return (int) d.get("n");
        }
        return 0;
    }

    @Override
    public List<Map<String, Object>> find(FindCmdSettings settings) throws MorphiumDriverException {

        //noinspection unchecked
        return (List<Map<String, Object>>) new NetworkCallHelper().doCall(() -> {

            List<Map<String, Object>> ret;

            OpMsg q = new OpMsg();

            q.setFirstDoc(settings.asMap("find"));
            q.setMessageId(getNextId());
            q.setFlags(0);
            synchronized (SynchronousMongoConnection.this) {
                settings.setMetaData(Doc.of("server", getHostSeed()[0]));
                long start = System.currentTimeMillis();
                sendQuery(q);
                int waitingfor = q.getMessageId();
                ret = readBatches(waitingfor, settings.getDb(), settings.getColl(), settings.getBatchSize() == null ? 100 : settings.getBatchSize());
                long dur = System.currentTimeMillis() - start;
                settings.getMetaData().put("duration", dur);
            }
            return Doc.of("values", ret);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("values");
    }

    @Override
    public Map<String, Object> findAndModify(FindAndModifyCmdSettings settings) throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            OpMsg msg = new OpMsg();
            msg.setMessageId(getNextId());
            msg.setFirstDoc(settings.asMap("findAndModify"));
            settings.setMetaData("server", getHostSeed()[0]);
            long start = System.currentTimeMillis();
            sendQuery(msg);
            OpMsg reply = waitForReply(settings.getDb(), settings.getColl(), msg.getMessageId());
            settings.setMetaData("duration", System.currentTimeMillis() - start);
            settings.setMetaData("result", reply.getFirstDoc());
            return (Doc) (reply.getFirstDoc().get("value"));
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }
//
//    @Override
//    public void insert(InsertCmdSettings settings) throws MorphiumDriverException {
//        new NetworkCallHelper().doCall(() -> {
//            OpMsg msg = new OpMsg();
//            msg.setMessageId(getNextId());
//            msg.setFirstDoc(settings.asMap("insert"));
//            settings.setMetaData("server", getHostSeed()[0]);
//            long start = System.currentTimeMillis();
//            sendQuery(msg);
//            OpMsg reply = waitForReply(settings, msg);
//            settings.setMetaData("duration", System.currentTimeMillis() - start);
//            if (reply.getFirstDoc().get("ok").equals(1.0)) {
//                return null;
//            } else {
//                throw new MorphiumDriverException("Error: " + reply.getFirstDoc().get("code") + ": " + reply.getFirstDoc().get("errmsg"));
//            }
//        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
//    }

    @Override
    public Map<String, Object> update(UpdateCmdSettings settings) throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setResponseTo(0);
            op.setMessageId(getNextId());
            Doc map = settings.asMap("update");
            op.setFirstDoc(map);
            settings.setMetaData("server", getHostSeed()[0]);
//            WriteConcern lwc = wc;
//            if (lwc == null) lwc = WriteConcern.getWc(0, false, false, 0);
//            map.put("writeConcern", lwc.toMongoWriteConcern().asDocument());
            long start = System.currentTimeMillis();
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(op);
                OpMsg res = waitForReply(settings.getDb(), settings.getColl(), op.getMessageId());
                settings.setMetaData("duration", System.currentTimeMillis() - start);
                return res.getFirstDoc();
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public Doc drop(DropCmdSettings settings) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setResponseTo(0);
            op.setMessageId(getNextId());
            settings.setMetaData("server", getHostSeed()[0]);
            Doc map = new Doc();
            map.put("drop", settings.getColl());
            map.put("$db", settings.getDb());
            op.setFirstDoc(map);
            long start = System.currentTimeMillis();
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(op);
                try {
                    waitForReply(settings.getDb(), settings.getColl(), op.getMessageId());
                    settings.setMetaData("duration", System.currentTimeMillis() - start);
                } catch (Exception e) {
                    log.warn("Drop failed! " + e.getMessage());
                }
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
        return null;
    }

    @Override
    public Map<String, Object> dropDatabase(DropCmdSettings settings) throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setResponseTo(0);
            op.setMessageId(getNextId());
            if (settings.getColl() != null) {
                throw new IllegalArgumentException("Cannot drop collection with dropDatabaseCommand!");
            }
            settings.setMetaData("server", getHostSeed()[0]);
            Doc map = settings.asMap("dropDatabase");
            map.put("dropDatabase", 1);
            op.setFirstDoc(map);
            long start = System.currentTimeMillis();
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(op);
                try {
                    OpMsg reply = waitForReply(settings.getDb(), settings.getColl(), op.getMessageId());
                    settings.setMetaData("duration", System.currentTimeMillis() - start);
                    if (reply.getFirstDoc().get("ok").equals(1.0)) {
                        return reply.getFirstDoc();
                    } else {
                        throw new MorphiumDriverException("Drop Failed! " + reply.getFirstDoc().get("code") + ": " + reply.getFirstDoc().get("errmsg"));
                    }
                } catch (Exception e) {
                    throw new MorphiumDriverException("Drop failed!", e);
                }
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public int clearCollection(ClearCollectionSettings settings) throws MorphiumDriverException {
        DeleteCmdSettings del = new DeleteCmdSettings();
        del.addDelete(Doc.of("q", new HashMap<>(), "limit", 0));
        del.setDb(settings.getDb());
        del.setColl(settings.getColl());
        del.setOrdered(false);
        del.setComment(settings.getComment());
        int ret = delete(del);
        settings.setMetaData(del.getMetaData());
        return ret;
    }

    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            var ret = runCommand("admin", Doc.of("replSetGetStatus", 1));
            @SuppressWarnings("unchecked") List<Doc> mem = (List) ret.get("members");
            if (mem == null) {
                return null;
            }
            //noinspection unchecked
            mem.stream().filter(d -> d.get("optime") instanceof Map).forEach(d -> d.put("optime", ((Map<String, Doc>) d.get("optime")).get("ts")));
            return ret;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }


    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        return runCommand(db, Doc.of("dbstats", 1));
    }


    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        return runCommand(db, Doc.of("collStats", coll, "scale", 1024));
    }


    public Map<String, Object> currentOp(long threshold) throws MorphiumDriverException {
        return runCommand("admin", Doc.of("currentOp", 1, "secs_running", Doc.of("$gt", threshold)));
    }


    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            OpMsg q = new OpMsg();
            cmd.put("$db", db);
            q.setMessageId(getNextId());
            q.setFirstDoc(Doc.of(cmd));

            OpMsg rep = null;
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);
                try {
                    rep = waitForReply(db, null, q.getMessageId());
                } catch (MorphiumDriverException e) {
                    e.printStackTrace();
                }
            }
            if (rep == null || rep.getFirstDoc() == null) {
                return null;
            }
            return rep.getFirstDoc();
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());

    }


    public MorphiumCursor initIteration(FindCmdSettings settings) throws MorphiumDriverException {
        OpMsg q = new OpMsg();
        q.setMessageId(getNextId());
        q.setFirstDoc(settings.asMap("find"));
        OpMsg reply;
        synchronized (SynchronousMongoConnection.this) {
            sendQuery(q);
            int waitingfor = q.getMessageId();
            reply = getReply();
            if (reply.getResponseTo() != waitingfor) {
                throw new MorphiumDriverNetworkException("Got wrong answser. Request: " + waitingfor + " got answer for " + reply.getResponseTo());
            }

        }

        MorphiumCursor crs = new MorphiumCursor();
        @SuppressWarnings("unchecked") Doc cursor = (Doc) reply.getFirstDoc().get("cursor");
        if (cursor != null && cursor.get("id") != null) {
            crs.setCursorId((Long) cursor.get("id"));
        }

        if (cursor != null) {
            if (cursor.get("firstBatch") != null) {
                //noinspection unchecked
                crs.setBatch((List) cursor.get("firstBatch"));
            } else if (cursor.get("nextBatch") != null) {
                //noinspection unchecked
                crs.setBatch((List) cursor.get("nextBatch"));
            }
        }

        SynchronousConnectCursor internalCursorData = new SynchronousConnectCursor(this);
        internalCursorData.setBatchSize(settings.getBatchSize());
        internalCursorData.setCollection(settings.getColl());
        internalCursorData.setDb(settings.getDb());
        //noinspection unchecked
        crs.setInternalCursorObject(internalCursorData);
        return crs;


    }


    public MorphiumCursor nextIteration(MorphiumCursor crs) throws MorphiumDriverException {

        long cursorId = crs.getCursorId();
        SynchronousConnectCursor internalCursorData = (SynchronousConnectCursor) crs.getInternalCursorObject();

        if (cursorId == 0) {
            return null;
        }
        OpMsg reply;
        synchronized (SynchronousMongoConnection.this) {
            OpMsg q = new OpMsg();

            q.setFirstDoc(Doc.of("getMore", (Object) cursorId)
                    .add("$db", internalCursorData.getDb())
                    .add("collection", internalCursorData.getCollection())
                    .add("batchSize", internalCursorData.getBatchSize()
                    ));
            q.setMessageId(getNextId());
            sendQuery(q);
            reply = getReply();
        }
        crs = new MorphiumCursor();
        //noinspection unchecked
        crs.setInternalCursorObject(internalCursorData);
        @SuppressWarnings("unchecked") Doc cursor = (Doc) reply.getFirstDoc().get("cursor");
        if (cursor == null) {
            //cursor not found
            throw new MorphiumDriverException("Iteration failed! Error: " + reply.getFirstDoc().get("code") + "  Message: " + reply.getFirstDoc().get("errmsg"));
        }
        if (cursor.get("id") != null) {
            crs.setCursorId((Long) cursor.get("id"));
        }
        if (cursor.get("firstBatch") != null) {
            //noinspection unchecked
            crs.setBatch((List) cursor.get("firstBatch"));
        } else if (cursor.get("nextBatch") != null) {
            //noinspection unchecked
            crs.setBatch((List) cursor.get("nextBatch"));
        }

        return crs;
    }


    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        if (crs == null) {
            return;
        }
        SynchronousConnectCursor internalCursor = (SynchronousConnectCursor) crs.getInternalCursorObject();
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection") Doc m = new Doc();
        m.put("killCursors", internalCursor.getCollection());
        List<Long> cursors = new ArrayList<>();
        cursors.add(crs.getCursorId());
        m.put("cursors", cursors);

    }


    private List<Map<String, Object>> readBatches(int waitingfor, String db, String collection, int batchSize) throws MorphiumDriverException {
        List<Map<String, Object>> ret = new ArrayList<>();

        Map<String, Object> doc;
        synchronized (SynchronousMongoConnection.this) {
            while (true) {
                OpMsg reply = getReply();
                if (reply.getResponseTo() != waitingfor) {
                    log.error("Wrong answer - waiting for " + waitingfor + " but got " + reply.getResponseTo());
                    log.error("Document: " + Utils.toJsonString(reply.getFirstDoc()));
                    continue;
                }
                //                    replies.remove(i);
                @SuppressWarnings("unchecked") Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");
                if (cursor == null) {
                    //trying result
                    if (reply.getFirstDoc().get("result") != null) {
                        //noinspection unchecked
                        return (List<Map<String, Object>>) reply.getFirstDoc().get("result");
                    }
                    if (reply.getFirstDoc().containsKey("results")) {
                        return (List<Map<String, Object>>) reply.getFirstDoc().get("results");
                    }
                    throw new MorphiumDriverException("Mongo Error: " + reply.getFirstDoc().get("codeName") + " - " + reply.getFirstDoc().get("errmsg"));
                }
                if (cursor.get("firstBatch") != null) {
                    //noinspection unchecked
                    ret.addAll((List) cursor.get("firstBatch"));
                } else if (cursor.get("nextBatch") != null) {
                    //noinspection unchecked
                    ret.addAll((List) cursor.get("nextBatch"));
                }
                if (((Long) cursor.get("id")) != 0) {
                    //                        log.info("getting next batch for cursor " + cursor.get("id"));
                    //there is more! Sending getMore!

                    //there is more! Sending getMore!
                    OpMsg q = new OpMsg();
                    q.setFirstDoc(Doc.of("getMore", (Object) cursor.get("id"))
                            .add("$db", db)
                            .add("collection", collection)
                            .add("batchSize", batchSize)
                    );
                    q.setMessageId(getNextId());
                    waitingfor = q.getMessageId();
                    sendQuery(q);
                } else {
                    break;
                }
            }
        }
        return ret;
    }


    protected void sendQuery(OpMsg q) throws MorphiumDriverException {
        if (transactionContext.get() != null) {
            q.getFirstDoc().put("lsid", Doc.of("id", transactionContext.get().getLsid()));
            q.getFirstDoc().put("txnNumber", transactionContext.get().getTxnNumber());
            if (!transactionContext.get().isStarted()) {
                q.getFirstDoc().put("startTransaction", true);
                transactionContext.get().setStarted(true);
            }
            if (transactionContext.get().getAutoCommit() != null)
                q.getFirstDoc().putIfAbsent("autocommit", transactionContext.get().getAutoCommit());
        }

        boolean retry = true;
        long start = System.currentTimeMillis();
        while (retry) {
            try {
                if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                    throw new MorphiumDriverException("Could not send message! Timeout!");
                }
                //q.setFlags(4); //slave ok
                out.write(q.bytes());
                out.flush();
                retry = false;
            } catch (IOException e) {
                log.error("Error sending request - reconnecting", e);
                reconnect();

            }
        }
    }


    public OpMsg sendAndWaitForReply(OpMsg q) throws MorphiumDriverException {
        sendQuery(q);
        return getReply(q.getMessageId(), getMaxWaitTime());
    }


    public long estimatedDocumentCount(String db, String collection, ReadPreference rp) {

        return 0;
    }


    public void insert(InsertCmdSettings settings) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            int idx = 0;
            settings.getDocuments().forEach(o -> o.putIfAbsent("_id", new MorphiumId()));
            settings.setMetaData("server", getHostSeed()[0]);
            long start = System.currentTimeMillis();
            while (idx < settings.getDocuments().size()) {
                OpMsg op = new OpMsg();
                op.setResponseTo(0);
                op.setMessageId(getNextId());
                Doc map = new Doc();
                map.put("insert", settings.getColl());

                List<Doc> docs = new ArrayList<>();
                for (int i = idx; i < idx + getMaxWriteBatchSize() && i < settings.getDocuments().size(); i++) {
                    docs.add(settings.getDocuments().get(i));
                }
                idx += docs.size();
                map.put("documents", docs);
                map.put("$db", settings.getDb());
                map.put("ordered", true);
                if (settings.getWriteConcern() != null)
                    map.put("writeConcern", settings.getWriteConcern());
                op.setFirstDoc(map);

                synchronized (SynchronousMongoConnection.this) {
                    sendQuery(op);
                    OpMsg ret = waitForReply(settings, op);
                    if (ret.getFirstDoc().containsKey("writeErrors")) {
                        if (((List) ret.getFirstDoc().get("writeErrors")).size() != 0) {
                            StringBuilder b = new StringBuilder();
                            for (Doc d : ((List<Doc>) ret.getFirstDoc().get("writeErrors"))) {
                                b.append("Error: ");
                                b.append(d.get("code"));
                                b.append(" - ");
                                b.append(d.get("errmsg"));
                                b.append("\n");
                            }
                            throw new MorphiumDriverException("Exception while writing: " + b.toString());
                        }
                    }
                }
            }
            settings.setMetaData("duration", System.currentTimeMillis() - start);
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }


    public Map<String, Object> store(StoreCmdSettings settings) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            List<Map<String, Object>> opsLst = new ArrayList<>();
            for (Map<String, Object> o : settings.getDocs()) {
                o.putIfAbsent("_id", new ObjectId());
                Map<String, Object> up = new LinkedHashMap<>();
                up.put("q", Doc.of("_id", o.get("_id")));
                up.put("u", o);
                up.put("upsert", true);
                up.put("multi", false);
                up.put("collation", null);
                //up.put("arrayFilters",list of arrayfilters)
                //up.put("hint",indexInfo);
                //up.put("c",variablesDocument);
                opsLst.add(up);
            }
            UpdateCmdSettings updateSettings = new UpdateCmdSettings().setDb(settings.getDb()).setColl(settings.getColl())
                    .setUpdates(opsLst).setWriteConcern(settings.getWriteConcern());
            settings.setMetaData(updateSettings.getMetaData());
            return update(updateSettings);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
        return null;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private OpMsg waitForReply(CmdSettings settings, OpMsg query) throws MorphiumDriverException {
        return waitForReply(settings.getDb(), settings.getColl(), query.getMessageId());
    }

    private OpMsg waitForReply(String db, String collection, int waitingfor) throws MorphiumDriverException {
        OpMsg reply;
        reply = getReply();
        //                replies.remove(i);
        if (reply.getResponseTo() == waitingfor) {
            if (!reply.getFirstDoc().get("ok").equals(1) && !reply.getFirstDoc().get("ok").equals(1.0)) {
                Object code = reply.getFirstDoc().get("code");
                Object errmsg = reply.getFirstDoc().get("errmsg");
                //                throw new MorphiumDriverException("Operation failed - error: " + code + " - " + errmsg, null, collection, db, query);
                MorphiumDriverException mde = new MorphiumDriverException("Operation failed on " + getHostSeed()[0] + " - error: " + code + " - " + errmsg, null, collection, db, null);
                mde.setMongoCode(code);
                mde.setMongoReason(errmsg);

                throw mde;

            } else {
                //got OK message
                //                        log.info("ok");
            }
        }

        return reply;
    }


    public boolean exists(String db) throws MorphiumDriverException {
        //noinspection EmptyCatchBlock
        try {
            getDBStats(db);
            return true;
        } catch (MorphiumDriverException e) {
        }
        return false;
    }


    public boolean exists(String db, String collection) throws MorphiumDriverException {
        List<Map<String, Object>> ret = getCollectionInfo(db, collection);
        for (Map<String, Object> c : ret) {
            if (c.get("name").equals(collection)) {
                return true;
            }
        }
        return false;
    }


    public List<Doc> getIndexes(String db, String collection) throws MorphiumDriverException {
        //noinspection unchecked
        return (List<Doc>) new NetworkCallHelper().doCall(() -> {
            Doc cmd = new Doc();
            cmd.put("listIndexes", 1);
            cmd.put("$db", db);
            cmd.put("collection", collection);
            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());

            q.setFirstDoc(cmd);
            q.setFlags(0);
            q.setResponseTo(0);

            List<Map<String, Object>> ret;
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);
                ret = readBatches(q.getMessageId(), db, null, getMaxWriteBatchSize());
            }
            return Doc.of("result", ret);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("result");
    }


    public List<String> getCollectionNames(String db) throws MorphiumDriverException {
        List<Map<String, Object>> ret = getCollectionInfo(db, null);
        return ret.stream().map(c -> (String) c.get("name")).collect(Collectors.toList());
    }

    public Map<String, Object> getDbStats(String db) throws MorphiumDriverException {
        return getDbStats(db, false);
    }

    public Map<String, Object> getDbStats(String db, boolean withStorage) throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            OpMsg msg = new OpMsg();
            msg.setMessageId(getNextId());
            Map<String, Object> v = Doc.of("dbStats", 1, "scale", 1024);
            v.put("$db", db);
            if (withStorage) {
                v.put("freeStorage", 1);
            }
            msg.setFirstDoc(v);
            sendQuery(msg);
            OpMsg reply = waitForReply(db, null, msg.getMessageId());
            return reply.getFirstDoc();
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }


    private List<Map<String, Object>> getCollectionInfo(String db, String collection) throws MorphiumDriverException {
        //noinspection unchecked
        return (List<Map<String, Object>>) new NetworkCallHelper().doCall(() -> {
            Map<String, Object> cmd = new Doc();
            cmd.put("listCollections", 1);
            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());

            if (collection != null) {
                cmd.put("filter", Doc.of("name", collection));
            }
            cmd.put("$db", db);
            q.setFirstDoc(cmd);
            q.setFlags(0);
            q.setResponseTo(0);

            List<Map<String, Object>> ret;
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);
                ret = readBatches(q.getMessageId(), db, null, getMaxWriteBatchSize());
            }
            return Doc.of("result", ret);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("result");
    }


    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        List<Map<String, Object>> lst = getCollectionInfo(db, coll);
        try {
            if (!lst.isEmpty() && lst.get(0).get("name").equals(coll)) {
                Object capped = ((Map) lst.get(0).get("options")).get("capped");
                return capped != null && capped.equals(true);
            }
        } catch (Exception e) {
            log.error("Error", e);
        }
        return false;
    }


    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return new BulkRequestContext(m) {
            private final List<BulkRequest> requests = new ArrayList<>();


            public Doc execute() {
                try {
                    for (BulkRequest r : requests) {
                        if (r instanceof InsertBulkRequest) {
                            InsertCmdSettings settings = new InsertCmdSettings();
                            settings.setDb(db).setColl(collection)
                                    .setComment("Bulk insert")
                                    .setDocuments(((InsertBulkRequest) r).getToInsert());
                            insert(settings);
                        } else if (r instanceof UpdateBulkRequest) {
                            UpdateBulkRequest up = (UpdateBulkRequest) r;
                            UpdateCmdSettings upCmd = new UpdateCmdSettings();
                            upCmd.setColl(collection).setDb(db)
                                    .setUpdates(Arrays.asList(Doc.of("q", up.getQuery(), "u", up.getCmd(), "upsert", up.isUpsert(), "multi", up.isMultiple())));
                            update(upCmd);
                        } else if (r instanceof DeleteBulkRequest) {
                            DeleteBulkRequest dbr = ((DeleteBulkRequest) r);
                            DeleteCmdSettings del = new DeleteCmdSettings();
                            del.setColl(collection).setDb(db)
                                    .setDeletes(Arrays.asList(Doc.of("q", dbr.getQuery(), "limit", dbr.isMultiple() ? 0 : 1)));
                            delete(del);
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


            public InsertBulkRequest addInsertBulkRequest(List<Doc> toInsert) {
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

    public void createIndex(CreateIndexesCmd settings) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            runCommand(settings.getDb(), settings.asMap("createIndexes"));
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }
//    public void createIndex(String db, String collection, Doc index, Doc options) throws MorphiumDriverException {
//        new NetworkCallHelper().doCall(() -> {
//            Doc cmd = new Doc();
//            cmd.put("createIndexes", collection);
//            List<Doc> lst = new ArrayList<>();
//            Doc idx = new Doc();
//            idx.put("key", index);
//            StringBuilder stringBuilder = new StringBuilder();
//            for (String k : index.keySet()) {
//                stringBuilder.append(k);
//                stringBuilder.append("-");
//                stringBuilder.append(index.get(k));
//            }
//
//            idx.put("name", "idx_" + stringBuilder.toString());
//            if (options != null) {
//                idx.putAll(options);
//            }
//            lst.add(idx);
//            cmd.put("indexes", lst);
//            runCommand(db, cmd);
//            return null;
//        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
//
//    }
}