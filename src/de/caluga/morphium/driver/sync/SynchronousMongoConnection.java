package de.caluga.morphium.driver.sync;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.*;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
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

    //    private List<OpMsg> replies = Collections.synchronizedList(new ArrayList<>());
    private Thread readerThread = null;
    private Map<Integer, OpMsg> incoming = new HashMap<>();
    private Map<Integer, Long> incomingTimes = new HashMap<>();
    private boolean running = true;

    private int unsansweredMessageId = 0;

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
    public void connect(String replSet) throws MorphiumDriverException {
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


            var result = runCommand("local", Doc.of("hello", true, "helloOk", true,
                    "client", Doc.of("application", Doc.of("name", "Morphium"),
                            "driver", Doc.of("name", "MorphiumDriver", "version", "1.0"),
                            "os", Doc.of("type", "MacOs"))

            )).next();
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

            readerThread = new Thread(() -> {
                while (running) {
                    try {
                        if (in.available() > 0) {
                            synchronized (incoming) {
                                OpMsg msg = (OpMsg) WireProtocolMessage.parseFromStream(in);
                                incoming.put(msg.getResponseTo(), msg);
                                incomingTimes.put(msg.getResponseTo(), System.currentTimeMillis());
                                if (unsansweredMessageId != msg.getResponseTo()) {
                                    log.error("Received wrong answer - expected " + unsansweredMessageId + " but got " + msg.getResponseTo());
                                }
                                unsansweredMessageId = 0;
                                for (var k : incomingTimes.keySet()) {
                                    if (System.currentTimeMillis() - incomingTimes.get(k) > 10000) {
                                        log.warn("Discarding unused answer " + k);
                                        incoming.remove(k);
                                        incomingTimes.remove(k);
                                    }
                                }
                            }
                        }
                    } catch (IOException e) {
                        log.error("Reader-Thread error", e);
                    }
                    Thread.yield();
                }
            });

        } catch (IOException e) {
            throw new MorphiumDriverException("connection failed", e);
        }
    }

    public boolean isBusy() {
        return unsansweredMessageId != 0;
    }

    @Override
    public boolean replyForMsgAvailable(int msg) {
        return incoming.containsKey(msg);
    }

    @Override
    public OpMsg getReplyFor(int msgid, long timeout) {
        while (!incoming.containsKey(msgid)) {
            Thread.yield();
        }
        return incoming.remove(msgid);
    }


    @Override
    public String getName() {
        return "SingleSyncConnection";
    }


    @Override
    public void setConnectionUrl(String connectionUrl) {

    }

    @Override
    public void connect() throws MorphiumDriverException {
        connect(null);
    }


    public boolean isConnected() {
        return s != null && s.isConnected();
    }

    @Override
    public void disconnect() {
        try {
            running = false;
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
        ctx.setLsid(UUID.randomUUID());
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




    @Override
    public void watch(WatchMongoCommand settings) throws MorphiumDriverException {
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




//    @Override
//    public List<Map<String, Object>> find(FindCommand settings) throws MorphiumDriverException {
//
//        //noinspection unchecked
//        return (List<Map<String, Object>>) new NetworkCallHelper<Map<String,Object>>.doCall(() -> {
//
//            List<Map<String, Object>> ret;
//
//            OpMsg q = new OpMsg();
//
//            q.setFirstDoc(settings.asMap("find"));
//            q.setMessageId(getNextId());
//            q.setFlags(0);
//            synchronized (SynchronousMongoConnection.this) {
//                settings.setMetaData(Doc.of("server", getHostSeed()[0]));
//                long start = System.currentTimeMillis();
//                sendQuery(q);
//                int waitingfor = q.getMessageId();
//                ret = readBatches(waitingfor, settings.getDb(), settings.getColl(), settings.getBatchSize() == null ? 100 : settings.getBatchSize());
//                long dur = System.currentTimeMillis() - start;
//                settings.getMetaData().put("duration", dur);
//            }
//            return Doc.of("values", ret);
//        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("values");
//    }
//
//    @Override
//    public Map<String, Object> findAndModify(FindAndModifyMongoCommand settings) throws MorphiumDriverException {
//        return new NetworkCallHelper<Map<String, Object>>.doCall(() -> {
//            OpMsg msg = new OpMsg();
//            msg.setMessageId(getNextId());
//            msg.setFirstDoc(settings.asMap());
//            settings.setMetaData("server", getHostSeed()[0]);
//            long start = System.currentTimeMillis();
//            sendQuery(msg);
//            OpMsg reply = waitForReply(settings.getDb(), settings.getColl(), msg.getMessageId());
//            settings.setMetaData("duration", System.currentTimeMillis() - start);
//            settings.setMetaData("result", reply.getFirstDoc());
//            return (Doc) (reply.getFirstDoc().get("value"));
//        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
//    }
//
//    @Override
//    public void insert(InsertCmdSettings settings) throws MorphiumDriverException {
//        new NetworkCallHelper<Map<String,Object>>.doCall(() -> {
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

//
//    @Override
//    public Doc drop(DropMongoCommand settings) throws MorphiumDriverException {
//        new NetworkCallHelper<Map<String, Object>>.doCall(() -> {
//            OpMsg op = new OpMsg();
//            op.setResponseTo(0);
//            op.setMessageId(getNextId());
//            settings.setMetaData("server", getHostSeed()[0]);
//            Doc map = new Doc();
//            map.put("drop", settings.getColl());
//            map.put("$db", settings.getDb());
//            op.setFirstDoc(map);
//            long start = System.currentTimeMillis();
//            synchronized (SynchronousMongoConnection.this) {
//                sendQuery(op);
//                try {
//                    waitForReply(settings.getDb(), settings.getColl(), op.getMessageId());
//                    settings.setMetaData("duration", System.currentTimeMillis() - start);
//                } catch (Exception e) {
//                    log.warn("Drop failed! " + e.getMessage());
//                }
//            }
//            return null;
//        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
//        return null;
//    }
////
//    @Override
//    public Map<String, Object> dropDatabase(DropMongoCommand settings) throws MorphiumDriverException {
//        return new NetworkCallHelper<Map<String, Object>>.doCall(() -> {
//            OpMsg op = new OpMsg();
//            op.setResponseTo(0);
//            op.setMessageId(getNextId());
//            if (settings.getColl() != null) {
//                throw new IllegalArgumentException("Cannot drop collection with dropDatabaseCommand!");
//            }
//            settings.setMetaData("server", getHostSeed()[0]);
//            Doc map = settings.asMap("dropDatabase");
//            map.put("dropDatabase", 1);
//            op.setFirstDoc(map);
//            long start = System.currentTimeMillis();
//            synchronized (SynchronousMongoConnection.this) {
//                sendQuery(op);
//                try {
//                    OpMsg reply = waitForReply(settings.getDb(), settings.getColl(), op.getMessageId());
//                    settings.setMetaData("duration", System.currentTimeMillis() - start);
//                    if (reply.getFirstDoc().get("ok").equals(1.0)) {
//                        return reply.getFirstDoc();
//                    } else {
//                        throw new MorphiumDriverException("Drop Failed! " + reply.getFirstDoc().get("code") + ": " + reply.getFirstDoc().get("errmsg"));
//                    }
//                } catch (Exception e) {
//                    throw new MorphiumDriverException("Drop failed!", e);
//                }
//            }
//        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
//    }


    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        return new NetworkCallHelper<Map<String, Object>>().doCall(() -> {
            var ret = runCommand("admin", Doc.of("replSetGetStatus", 1)).next();
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
        return runCommand(db, Doc.of("dbstats", 1)).next();
    }


    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        return runCommand(db, Doc.of("collStats", coll, "scale", 1024)).next();
    }


    public Map<String, Object> currentOp(long threshold) throws MorphiumDriverException {
        return runCommand("admin", Doc.of("currentOp", 1, "secs_running", Doc.of("$gt", threshold))).next();
    }


    public int dataAvailable() {
        try {
            return in.available();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public MorphiumCursor runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {

        return (MorphiumCursor) new NetworkCallHelper<Map<String, Object>>().doCall(() -> {

            int id = sendCommand(db, cmd);

            OpMsg rep = null;
            synchronized (SynchronousMongoConnection.this) {
                rep = waitForReply(db, null, id);
            }
            if (rep == null || rep.getFirstDoc() == null) {
                return null;
            }
            if (rep.getFirstDoc().containsKey("cursor")) {
                return Doc.of("cursor", new SynchronousConnectCursor(this, db, getDefaultBatchSize(), false, rep));
            } else {
                final var msg = rep;
                //no cursor returned, create one
                MorphiumCursor ret = new MorphiumCursorAdapter() {
                    private int idx = 0;

                    @Override
                    public boolean hasNext() throws MorphiumDriverException {
                        return idx == 0;
                    }

                    @Override
                    public Map<String, Object> next() throws MorphiumDriverException {
                        idx++;
                        return msg.getFirstDoc();
                    }

                    @Override
                    public int available() {
                        return 1 - idx;
                    }

                    @Override
                    public List<Map<String, Object>> getAll() throws MorphiumDriverException {
                        return List.of(msg.getFirstDoc());
                    }

                    @Override
                    public int getCursor() {
                        return idx;
                    }
                };
                return Doc.of("cursor", ret);
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("cursor");

    }

//
//    public MorphiumCursor initIteration(FindCommand settings) throws MorphiumDriverException {
//        OpMsg q = new OpMsg();
//        q.setMessageId(getNextId());
//        q.setFirstDoc(settings.asMap());
//        OpMsg reply;
//        synchronized (SynchronousMongoConnection.this) {
//            sendQuery(q);
//            int waitingfor = q.getMessageId();
//            reply = getReplyFor(waitingfor, getMaxWaitTime());
//            if (reply.getResponseTo() != waitingfor) {
//                throw new MorphiumDriverNetworkException("Got wrong answser. Request: " + waitingfor + " got answer for " + reply.getResponseTo());
//            }
//
//        }
//        //noinspection unchecked
//        return new SynchronousConnectCursor(this, settings.getDb(), settings.getBatchSize(), false, reply);
//
//
//    }


    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        if (crs == null) {
            return;
        }
        killCursors(crs.getDb(), crs.getCollection(), crs.getCursorId());
        @SuppressWarnings("MismatchedQ ueryAndUpdateOfCollection") Doc m = new Doc();
        m.put("killCursors", crs.getCollection());
        List<Long> cursors = new ArrayList<>();
        cursors.add(crs.getCursorId());
        m.put("cursors", cursors);

    }


    @Override
    public int sendCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        OpMsg q = new OpMsg();
        cmd.put("$db", db);
        q.setMessageId(getNextId());
        q.setFirstDoc(Doc.of(cmd));

        OpMsg rep = null;
        synchronized (SynchronousMongoConnection.this) {
            sendQuery(q);
        }
        unsansweredMessageId = q.getMessageId();
        return unsansweredMessageId;
    }


    private List<Map<String, Object>> readBatches(int waitingfor, String db, String collection, int batchSize) throws MorphiumDriverException {
        List<Map<String, Object>> ret = new ArrayList<>();

        Map<String, Object> doc;
        synchronized (SynchronousMongoConnection.this) {
            while (true) {
                OpMsg reply = getReplyFor(waitingfor, getMaxWaitTime());
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


    public void sendQuery(OpMsg q) throws MorphiumDriverException {
        if (transactionContext.get() != null) {
            q.getFirstDoc().put("lsid", Doc.of("id", transactionContext.get().getLsid()));
            q.getFirstDoc().put("txnNumber", transactionContext.get().getTxnNumber());
            if (!transactionContext.get().isStarted()) {
                q.getFirstDoc().put("startTransaction", true);
                transactionContext.get().setStarted(true);
            }
            q.getFirstDoc().putIfAbsent("autocommit", transactionContext.get().getAutoCommit());
            q.getFirstDoc().remove("writeConcern");
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
        return getReplyFor(q.getMessageId(), getMaxWaitTime());
    }


    public long estimatedDocumentCount(String db, String collection, ReadPreference rp) {

        return 0;
    }

//
//    public void insert(InsertMongoCommand settings) throws MorphiumDriverException {
//        new NetworkCallHelper<Map<String, Object>>().doCall(() -> {
//            int idx = 0;
//            settings.getDocuments().forEach(o -> o.putIfAbsent("_id", new MorphiumId()));
//            settings.setMetaData("server", getHostSeed()[0]);
//            long start = System.currentTimeMillis();
//            while (idx < settings.getDocuments().size()) {
//                OpMsg op = new OpMsg();
//                op.setResponseTo(0);
//                op.setMessageId(getNextId());
//                Doc map = new Doc();
//                map.put("insert", settings.getColl());
//
//                List<Doc> docs = new ArrayList<>();
//                for (int i = idx; i < idx + getMaxWriteBatchSize() && i < settings.getDocuments().size(); i++) {
//                    docs.add(settings.getDocuments().get(i));
//                }
//                idx += docs.size();
//                map.put("documents", docs);
//                map.put("$db", settings.getDb());
//                map.put("ordered", true);
//                if (settings.getWriteConcern() != null)
//                    map.put("writeConcern", settings.getWriteConcern());
//                op.setFirstDoc(map);
//
//                synchronized (SynchronousMongoConnection.this) {
//                    sendQuery(op);
//                    OpMsg ret = waitForReply(settings, op);
//                    if (ret.getFirstDoc().containsKey("writeErrors")) {
//                        if (((List) ret.getFirstDoc().get("writeErrors")).size() != 0) {
//                            StringBuilder b = new StringBuilder();
//                            for (Doc d : ((List<Doc>) ret.getFirstDoc().get("writeErrors"))) {
//                                b.append("Error: ");
//                                b.append(d.get("code"));
//                                b.append(" - ");
//                                b.append(d.get("errmsg"));
//                                b.append("\n");
//                            }
//                            throw new MorphiumDriverException("Exception while writing: " + b.toString());
//                        }
//                    }
//                }
//            }
//            settings.setMetaData("duration", System.currentTimeMillis() - start);
//            return null;
//        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
//    }
//


    @SuppressWarnings("StatementWithEmptyBody")
    private OpMsg waitForReply(MongoCommand settings, OpMsg query) throws MorphiumDriverException {
        return waitForReply(settings.getDb(), settings.getColl(), query.getMessageId());
    }

    private OpMsg waitForReply(String db, String collection, int waitingfor) throws MorphiumDriverException {
        OpMsg reply;
        reply = getReplyFor(waitingfor, getMaxWaitTime());
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
        } else {
            throw new MorphiumDriverException("Did receive wrong answer!");
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


    public List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException {
        //noinspection unchecked
        return new NetworkCallHelper<List<Map<String, Object>>>().doCall(() -> {
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
            return ret;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }


    public List<String> getCollectionNames(String db) throws MorphiumDriverException {
        List<Map<String, Object>> ret = getCollectionInfo(db, null);
        return ret.stream().map(c -> (String) c.get("name")).collect(Collectors.toList());
    }

    public Map<String, Object> getDbStats(String db) throws MorphiumDriverException {
        return getDbStats(db, false);
    }

    public Map<String, Object> getDbStats(String db, boolean withStorage) throws MorphiumDriverException {
        return new NetworkCallHelper<Map<String, Object>>().doCall(() -> {
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
        return new NetworkCallHelper<List<Map<String, Object>>>().doCall(() -> {
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
            return ret;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
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
                            InsertMongoCommand settings = new InsertMongoCommand(SynchronousMongoConnection.this);
                            settings.setDb(db).setColl(collection)
                                    .setComment("Bulk insert")
                                    .setDocuments(((InsertBulkRequest) r).getToInsert());
                            settings.execute();
                        } else if (r instanceof UpdateBulkRequest) {
                            UpdateBulkRequest up = (UpdateBulkRequest) r;
                            UpdateMongoCommand upCmd = new UpdateMongoCommand(SynchronousMongoConnection.this);
                            upCmd.setColl(collection).setDb(db)
                                    .setUpdates(Arrays.asList(Doc.of("q", up.getQuery(), "u", up.getCmd(), "upsert", up.isUpsert(), "multi", up.isMultiple())));
                            upCmd.execute();
                        } else if (r instanceof DeleteBulkRequest) {
                            DeleteBulkRequest dbr = ((DeleteBulkRequest) r);
                            DeleteMongoCommand del = new DeleteMongoCommand(SynchronousMongoConnection.this);
                            del.setColl(collection).setDb(db)
                                    .setDeletes(Arrays.asList(Doc.of("q", dbr.getQuery(), "limit", dbr.isMultiple() ? 0 : 1)));
                            del.execute();
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

//    public void createIndex(String db, String collection, Doc index, Doc options) throws MorphiumDriverException {
//        new NetworkCallHelper<Map<String,Object>>.doCall(() -> {
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
