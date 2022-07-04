package de.caluga.morphium.driver.sync;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.*;
import de.caluga.morphium.driver.commands.DeleteMongoCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.commands.WatchSettings;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Stephan Bösebeck
 * Date: 02.12.15
 * Time: 23:47
 * <p>
 * connects to one node only!
 */
public class SingleMongoConnection extends DriverBase {

    private final Logger log = LoggerFactory.getLogger(SingleMongoConnection.class);
    private Socket s;
    private OutputStream out;
    private InputStream in;


    //    private List<OpMsg> replies = Collections.synchronizedList(new ArrayList<>());
    private Thread readerThread = null;
    private Map<Integer, OpMsg> incoming = new HashMap<>();
    private Map<Integer, Long> incomingTimes = new ConcurrentHashMap<>();
    private int heartbeatPause = 1000;
    private boolean running = true;

    //private int unsansweredMessageId = 0;

    private void reconnect() {
        try {
            stopHousekeeping();
            running = false;
            disconnect();
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
            running = true;
            String host = getHostSeed()[0];
            String h[] = host.split(":");
            int port = 27017;
            if (h.length > 1) {
                port = Integer.parseInt(h[1]);
            }
            log.info("Connecting to " + h[0] + ":" + port);
            s = new Socket(h[0], port);
            out = s.getOutputStream();
            in = s.getInputStream();

            readerThread = new Thread(() -> {
                while (running) {
                    try {
                        //reading in data
                        if (in.available() > 0) {
                            OpMsg msg = (OpMsg) WireProtocolMessage.parseFromStream(in);
                            incoming.put(msg.getResponseTo(), msg);
                            synchronized (incomingTimes) {
                                incomingTimes.put(msg.getResponseTo(), System.currentTimeMillis());
                            }
                            synchronized (incoming) {
                                incoming.notifyAll();
                            }
                            var s = new HashSet(incomingTimes.keySet());
                            for (var k : s) {
                                synchronized (incomingTimes) {
                                    if (incomingTimes.get(k) == null) continue;
                                    if (System.currentTimeMillis() - incomingTimes.get(k) > 10000) {
                                        log.warn("Discarding unused answer " + k);
                                        incoming.remove(k);
                                        incomingTimes.remove(k);
                                    }
                                }
                            }
                        }

                    } catch (Exception e) {
                        log.error("Reader-Thread error", e);
                    }
                    Thread.yield();
                }
                log.info("Reader Thread terminated");
                synchronized (incoming) {
                    incoming.notifyAll();
                }
            });
            readerThread.start();

            if (isUseCollectionNameCache()) {
                //Clear collection name cache
                startHousekeeping();
            }

            var result = runCommand("local", Doc.of("hello", true, "helloOk", true,
                    "client", Doc.of("application", Doc.of("name", "Morphium"),
                            "driver", Doc.of("name", "MorphiumDriver", "version", "1.0"),
                            "os", Doc.of("type", "MacOs"))

            )).next();
            //log.info("Got result");
            if (replSet != null) {
                if (replSet != null && !replSet.equals(getReplicaSetName())) {
                    throw new MorphiumDriverException("Replicaset name is wrong - connected to " + getReplicaSetName() + " should be " + replSet);
                }
                setReplicaSetName((String) result.get("setName"));
            }
            if (!result.get("secondary").equals(false) && getConnectionType().equals(ConnectionType.PRIMARY)) {
                disconnect();
                host = (String) result.get("primary");
                List<String> hostSeed = new ArrayList<>();
                if (host != null)
                    hostSeed.add(host);
                List<String> hosts = (List<String>) result.get("hosts");
                Collections.shuffle(hosts);
                for (String hst : hosts) {
                    if (hst.equals(host)) continue;
                    hostSeed.add(hst);
                }
                setHostSeed(hostSeed.toArray(new String[hostSeed.size()]));
                running = true;
                connect(getReplicaSetName());
                return;
            } else if (result.get("secondary").equals(false) && getConnectionType().equals(ConnectionType.SECONDARY)) {
                log.info("Connect to different host because connection type secondary");
                disconnect();
                host = (String) result.get("primary");
                List<String> hostSeed = new ArrayList<>();

                for (String hst : ((List<String>) result.get("hosts"))) {
                    if (hst.equals(host)) continue;
                    hostSeed.add(hst);
                }
                hostSeed.add(host);
                setHostSeed(hostSeed.toArray(new String[hostSeed.size()]));
                running = true;
                connect(getReplicaSetName());
                return;

            }

            setMaxBsonObjectSize((Integer) result.get("maxBsonObjectSize"));
            setMaxMessageSize((Integer) result.get("maxMessageSizeBytes"));
            setMaxWriteBatchSize((Integer) result.get("maxWriteBatchSize"));


        } catch (IOException e) {
            throw new MorphiumDriverException("connection failed", e);
        }
    }


    @Override
    public boolean replyForMsgAvailable(int msg) {
        return incoming.containsKey(msg);
    }

    @Override
    public OpMsg getReplyFor(int msgid, long timeout) throws MorphiumDriverException {
        long start = System.currentTimeMillis();
        while (!incoming.containsKey(msgid)) {
            try {
                synchronized (incoming) {
                    incoming.wait(timeout);
                }
            } catch (InterruptedException e) {
                //Swallow
            }
        }
        synchronized (incomingTimes) {
            incomingTimes.remove(msgid);
        }
        return incoming.remove(msgid);
    }


    @Override
    public String getName() {
        return "SingleMongoConnection";
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
            while (readerThread.isAlive()) {
                Thread.yield();
            }
            stopHousekeeping();
            //executor.shutdownNow();

        } catch (IOException e) {
            //throw new RuntimeException(e);
        }
    }

    @Override
    public void commitTransaction() throws MorphiumDriverException {
        if (getTransactionContext() == null)
            throw new IllegalArgumentException("No transaction in progress, cannot commit");
        MorphiumTransactionContext ctx = getTransactionContext();
        runCommand("admin", Doc.of("commitTransaction", 1, "txnNumber", ctx.getTxnNumber(), "autocommit", false, "lsid", Doc.of("id", ctx.getLsid())));
        clearTransactionContext();
    }

    @Override
    public void abortTransaction() throws MorphiumDriverException {
        if (getTransactionContext() == null)
            throw new IllegalArgumentException("No transaction in progress, cannot abort");
        MorphiumTransactionContext ctx = getTransactionContext();
        runCommand("admin", Doc.of("abortTransaction", 1, "txnNumber", ctx.getTxnNumber(), "autocommit", false, "lsid", Doc.of("id", ctx.getLsid())));
        clearTransactionContext();
    }


    @Override
    public void watch(WatchSettings settings) throws MorphiumDriverException {
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
        ArrayList<Map<String, Object>> localPipeline = new ArrayList<>();
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
            OpMsg reply = getReplyFor(msg.getMessageId(), getMaxWaitTime());
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
            } else {
                log.info("No/empty result");
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
                //log.debug("sent getmore....");
            } else {
                log.debug("Cursor exhausted, restarting");
                msg = startMsg;
                msg.setMessageId(getNextId());
                sendQuery(msg);

            }
        }
    }


    @Override
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


    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        return runCommand(db, Doc.of("dbstats", 1)).next();
    }


    @Override
    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        return runCommand(db, Doc.of("collStats", coll, "scale", 1024)).next();
    }


    public Map<String, Object> currentOp(long threshold) throws MorphiumDriverException {
        return runCommand("admin", Doc.of("currentOp", 1, "secs_running", Doc.of("$gt", threshold))).next();
    }

    @Override
    public MorphiumCursor runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {

        return new NetworkCallHelper<MorphiumCursor>().doCall(() -> {

            int id = sendCommand(db, cmd);
            int batchSize = getDefaultBatchSize();
            if (cmd.containsKey("batchSize")) {
                batchSize = (int) cmd.get("batchSize");
            }
            OpMsg rep = null;
            rep = getReplyFor(id, getMaxWaitTime());
            if (rep == null || rep.getFirstDoc() == null) {
                return null;
            }
            if (rep.getFirstDoc().containsKey("ok") && rep.getFirstDoc().get("ok").equals(Double.valueOf(0.0))) {
                throw new MorphiumDriverException("Error: " + rep.getFirstDoc().get("code") + ": " + rep.getFirstDoc().get("errmsg"));
            }
            if (rep.getFirstDoc().containsKey("cursor")) {
                return new SingleMongoConnectionCursor(this, batchSize, false, rep);
            } else if (rep.getFirstDoc().containsKey("results")) {
                return new SingleBatchCursor((List<Map<String, Object>>) rep.getFirstDoc().get("results"));
            } else {
                final var msg = rep;
                //no cursor returned, create one
                MorphiumCursor ret = new SingleElementCursor(msg.getFirstDoc());
                return ret;
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());

    }

    @Override
    public Map<String, Object> runCommandSingleResult(String db, Map<String, Object> cmd) throws MorphiumDriverException {

        return new NetworkCallHelper<Map<String, Object>>().doCall(() -> {

            int id = sendCommand(db, cmd);

            OpMsg rep = null;
            rep = getReplyFor(id, getMaxWaitTime());
            if (rep == null || rep.getFirstDoc() == null) {
                return null;
            }
            if (rep.getFirstDoc().containsKey("cursor")) {
                //should actually not happen!
                log.warn("Expecting single result, got Cursor instead!");

                Map cursor = (Map) rep.getFirstDoc().get("cursor");
                Map<String, Object> ret = null;
                if (cursor.containsKey("firstBatch")) {
                    ret = ((List<Map<String, Object>>) cursor.get("firstBatch")).get(0);
                } else if (cursor.containsKey("nextBatch")) {
                    ret = ((List<Map<String, Object>>) cursor.get("firstBatch")).get(0);
                }
                killCursors(db, ((String) cursor.get("ns")).split("\\.")[1], (Long) cursor.get("id"));
                return ret;
            } else {
                return rep.getFirstDoc();
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());

    }


    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        if (crs == null) {
            return;
        }
        killCursors(crs.getDb(), crs.getCollection(), crs.getCursorId());


    }


    @Override
    public int sendCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        OpMsg q = new OpMsg();
        cmd.put("$db", db);
        q.setMessageId(getNextId());
        q.setFirstDoc(Doc.of(cmd));

        OpMsg rep = null;
        sendQuery(q);
//        unsansweredMessageId = q.getMessageId();
        return q.getMessageId();//unsansweredMessageId;
    }

    @Override
    public boolean replyAvailableFor(int msgId) {
        return replyForMsgAvailable(msgId);
    }

    @Override
    public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
        OpMsg reply = getReplyFor(id, getMaxWaitTime());
        if (reply.hasCursor()) {
            return getSingleDocAndKillCursor(reply);
        }
        return reply.getFirstDoc();
    }

    @Override
    public List<Map<String, Object>> readAnswerFor(int queryId) throws MorphiumDriverException {
        return readBatches(queryId, getDefaultBatchSize());
    }

    @Override
    public MorphiumCursor getAnswerFor(int queryId) throws MorphiumDriverException {
        OpMsg reply = getReplyFor(queryId, getMaxWaitTime());
        if (reply.hasCursor()) {
            return new SingleMongoConnectionCursor(this, getDefaultBatchSize(), true, reply);
        }
        return null;
    }

    @Override
    public List<Map<String, Object>> readAnswerFor(MorphiumCursor crs) throws MorphiumDriverException {
        List<Map<String, Object>> ret = new ArrayList<>();
        while (crs.hasNext()) {
            ret.addAll(crs.getBatch());
            crs.ahead(crs.getBatch().size());
        }

        return ret;
    }

    private String getDBFromCursor(OpMsg msg) {
        if (msg.hasCursor()) {
            var crs = (Map<String, Object>) msg.getFirstDoc().get("cursor");
            String ns = crs.get("ns").toString();
            return ns.split("\\.")[0];
        }
        return null;
    }

    private Map<String, Object> getSingleDocAndKillCursor(OpMsg msg) throws MorphiumDriverException {
        if (!msg.hasCursor()) return null;
        Map<String, Object> cursor = (Map<String, Object>) msg.getFirstDoc().get("cursor");
        Map<String, Object> ret = null;
        if (cursor.containsKey("firstBatch")) {
            ret = (Map<String, Object>) cursor.get("firstBatch");
        } else {
            ret = (Map<String, Object>) cursor.get("nextBatch");
        }
        String[] namespace = cursor.get("ns").toString().split("\\.");
        killCursors(namespace[0], namespace[1], (Long) cursor.get("id"));
        return ret;
    }


    private List<Map<String, Object>> readBatches(int waitingfor, int batchSize) throws MorphiumDriverException {
        List<Map<String, Object>> ret = new ArrayList<>();

        Map<String, Object> doc;
        String db = null;
        String coll = null;
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
            if (db == null) {
                //getting ns
                String[] namespace = cursor.get("ns").toString().split("\\.");
                db = namespace[0];
                if (namespace.length > 1)
                    coll = namespace[1];
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
                        .add("batchSize", batchSize)
                );
                if (coll != null) q.getFirstDoc().put("collection", coll);
                q.setMessageId(getNextId());
                waitingfor = q.getMessageId();
                sendQuery(q);
            } else {
                break;
            }

        }
        return ret;
    }


    public void sendQuery(OpMsg q) throws MorphiumDriverException {
        if (getTransactionContext() != null) {
            q.getFirstDoc().put("lsid", Doc.of("id", getTransactionContext().getLsid()));
            q.getFirstDoc().put("txnNumber", getTransactionContext().getTxnNumber());
            if (!getTransactionContext().isStarted()) {
                q.getFirstDoc().put("startTransaction", true);
                getTransactionContext().setStarted(true);
            }
            q.getFirstDoc().putIfAbsent("autocommit", getTransactionContext().getAutoCommit());
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
//
//    private OpMsg waitForReply(MongoCommand settings, OpMsg query) throws MorphiumDriverException {
//        return waitForReply(settings.getDb(), settings.getColl(), query.getMessageId());
//    }
//
//    private OpMsg waitForReply(String db, String collection, int waitingfor) throws MorphiumDriverException {
//        OpMsg reply;
//        reply = getReplyFor(waitingfor, getMaxWaitTime());
//        //                replies.remove(i);
//        if (reply.getResponseTo() == waitingfor) {
//            if (!reply.getFirstDoc().get("ok").equals(1) && !reply.getFirstDoc().get("ok").equals(1.0)) {
//                Object code = reply.getFirstDoc().get("code");
//                Object errmsg = reply.getFirstDoc().get("errmsg");
//                //                throw new MorphiumDriverException("Operation failed - error: " + code + " - " + errmsg, null, collection, db, query);
//                MorphiumDriverException mde = new MorphiumDriverException("Operation failed on " + getHostSeed()[0] + " - error: " + code + " - " + errmsg, null, collection, db, null);
//                mde.setMongoCode(code);
//                mde.setMongoReason(errmsg);
//
//                throw mde;
//
//            } else {
//                //got OK message
//                //                        log.info("ok");
//            }
//        } else {
//            throw new MorphiumDriverException("Did receive wrong answer!");
//        }
//
//        return reply;
//    }


    public boolean exists(String db) throws MorphiumDriverException {
        //noinspection EmptyCatchBlock
        try {
            getDBStats(db);
            return true;
        } catch (MorphiumDriverException e) {
        }
        return false;
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
            OpMsg reply = getReplyFor(msg.getMessageId(), getMaxWaitTime());
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
            sendQuery(q);
            ret = readBatches(q.getMessageId(), getMaxWriteBatchSize());
            return ret;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }


    @Override
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


    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return new BulkRequestContext(m) {
            private final List<BulkRequest> requests = new ArrayList<>();


            public Doc execute() {
                try {
                    for (BulkRequest r : requests) {
                        if (r instanceof InsertBulkRequest) {
                            InsertMongoCommand settings = new InsertMongoCommand(SingleMongoConnection.this);
                            settings.setDb(db).setColl(collection)
                                    .setComment("Bulk insert")
                                    .setDocuments(((InsertBulkRequest) r).getToInsert());
                            settings.execute();
                        } else if (r instanceof UpdateBulkRequest) {
                            UpdateBulkRequest up = (UpdateBulkRequest) r;
                            UpdateMongoCommand upCmd = new UpdateMongoCommand(SingleMongoConnection.this);
                            upCmd.setColl(collection).setDb(db)
                                    .setUpdates(Arrays.asList(Doc.of("q", up.getQuery(), "u", up.getCmd(), "upsert", up.isUpsert(), "multi", up.isMultiple())));
                            upCmd.execute();
                        } else if (r instanceof DeleteBulkRequest) {
                            DeleteBulkRequest dbr = ((DeleteBulkRequest) r);
                            DeleteMongoCommand del = new DeleteMongoCommand(SingleMongoConnection.this);
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


}
