package de.caluga.morphium.driver.wire;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.morphium.driver.commands.KillCursorsCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.commands.result.CursorResult;
import de.caluga.morphium.driver.commands.result.SingleElementResult;
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
import java.util.concurrent.atomic.AtomicInteger;

public class SingleMongoConnection implements MongoConnection {

    private final Logger log = LoggerFactory.getLogger(SingleMongoConnection.class);
    private Socket s;
    private OutputStream out;
    private InputStream in;

    private AtomicInteger msgId = new AtomicInteger(1000);


    //    private List<OpMsg> replies = Collections.synchronizedList(new ArrayList<>());
    private Thread readerThread = null;
    private Map<Integer, OpMsg> incoming = new HashMap<>();
    private Map<Integer, Long> incomingTimes = new ConcurrentHashMap<>();
    private int heartbeatPause = 1000;
    private boolean running = true;

    private String connectedTo;
    private int connectedToPort;
    private boolean connected;
    private MorphiumDriver driver;

    @Override
    public HelloResult connect(MorphiumDriver drv, String host, int port) throws MorphiumDriverException {
        driver = drv;
        try {
            log.info("Connecting to " + host + ":" + port);
            s = new Socket(host, port);
            out = s.getOutputStream();
            in = s.getInputStream();
        } catch (IOException e) {
            throw new MorphiumDriverException("Connection failed", e);
        }
        startReaderThread();
        HelloCommand cmd = new HelloCommand(null);
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(cmd.asMap());
        var result = sendAndWaitForReply(msg);
        Map<String, Object> firstDoc = result.getFirstDoc();
        var hello = HelloResult.fromMsg(firstDoc);
        connectedTo = host;
        connectedToPort = port;


        connected = true;
        return hello;
    }

    @Override
    public MorphiumDriver getDriver() {
        return driver;
    }

    @Override
    public String getConnectedTo() {
        return connectedTo + ":" + connectedToPort;
    }

    public String getConnectedToHost() {
        return connectedTo;
    }

    @Override
    public int getConnectedToPort() {
        return connectedToPort;
    }

    @Override
    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        if (crs == null) {
            return;
        }
        killCursors(crs.getDb(), crs.getCollection(), crs.getCursorId());
    }

    @Override
    public Map<String, Object> killCursors(String db, String coll, long... ids) throws MorphiumDriverException {
        List<Long> cursorIds = new ArrayList<>();
        for (long l : ids) {
            if (l != 0) {
                cursorIds.add(l);
            }
        }
        if (cursorIds.isEmpty()) {
            return null;
        }

        KillCursorsCommand k = new KillCursorsCommand(this)
                .setCursorIds(cursorIds)
                .setDb(db)
                .setColl(coll);

        var ret = k.execute();
        log.info("killed cursor");
        return ret;
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    private void startReaderThread() {
        running = true;
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
//                log.info("Reader Thread terminated");
            synchronized (incoming) {
                incoming.notifyAll();
            }
        });
        readerThread.start();
    }


    @Override
    public void disconnect() {
        running = false;
        while (readerThread.isAlive()) {
            Thread.yield();
        }
        connected = false;
        try {
            in.close();
            out.close();
            s.close();
        } catch (IOException e) {
            //swallow
        }
        in = null;
        out = null;
        s = null;
    }

    @Override
    public boolean replyAvailableFor(int msgId) {
        return incoming.containsKey(msgId);
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
        if (!incoming.containsKey(msgid)) {
            log.warn("Did not get reply within " + timeout + "ms");
        }
        return incoming.remove(msgid);
    }

    @Override
    public void sendQuery(OpMsg q) throws MorphiumDriverException {
        if (driver.getTransactionContext() != null) {
            q.getFirstDoc().put("lsid", Doc.of("id", driver.getTransactionContext().getLsid()));
            q.getFirstDoc().put("txnNumber", driver.getTransactionContext().getTxnNumber());
            if (!driver.getTransactionContext().isStarted()) {
                q.getFirstDoc().put("startTransaction", true);
                driver.getTransactionContext().setStarted(true);
            }
            q.getFirstDoc().putIfAbsent("autocommit", driver.getTransactionContext().getAutoCommit());
            q.getFirstDoc().remove("writeConcern");
        }

        boolean retry = true;
        long start = System.currentTimeMillis();
        while (retry) {
            try {
                if (System.currentTimeMillis() - start > driver.getMaxWaitTime()) {
                    throw new MorphiumDriverException("Could not send message! Timeout!");
                }
                //q.setFlags(4); //slave ok
                out.write(q.bytes());
                out.flush();
                retry = false;
            } catch (IOException e) {
                log.error("Error sending request - reconnecting", e);

                disconnect();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    //swallow
                }
                connect(driver, connectedTo, connectedToPort);

            }
        }
    }

    @Override
    public OpMsg sendAndWaitForReply(OpMsg q) throws MorphiumDriverException {
        sendQuery(q);
        return getReplyFor(q.getMessageId(), driver.getMaxWaitTime());
    }

    @Override
    public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
        OpMsg reply = getReplyFor(id, driver.getMaxWaitTime());
        if (reply.hasCursor()) {
            return getSingleDocAndKillCursor(reply);
        }
        return reply.getFirstDoc();
    }

    @Override
    public int sendCommand(Map<String, Object> cmd) throws MorphiumDriverException {
        OpMsg q = new OpMsg();
        q.setMessageId(msgId.incrementAndGet());
        q.setFirstDoc(Doc.of(cmd));

        sendQuery(q);
        return q.getMessageId();
    }

    @Override
    public void watch(WatchCommand settings) throws MorphiumDriverException {
        int maxWait = 0;
        if (settings.getDb() == null) settings.setDb("1"); //watch all dbs!
        if (settings.getColl() == null) {
            //watch all collections
            settings.setColl("1");
        }
        if (settings.getMaxWaitTime() == null || settings.getMaxWaitTime() <= 0) maxWait = driver.getReadTimeout();
        OpMsg startMsg = new OpMsg();
        int batchSize = settings.getBatchSize() == null ? driver.getDefaultBatchSize() : settings.getBatchSize();
        startMsg.setMessageId(msgId.incrementAndGet());
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
        settings.setMetaData("server", getConnectedTo());
        long docsProcessed = 0;
        while (true) {
            OpMsg reply = getReplyFor(msg.getMessageId(), driver.getMaxWaitTime());
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
                msg.setMessageId(msgId.incrementAndGet());

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
                msg.setMessageId(msgId.incrementAndGet());
                sendQuery(msg);

            }
        }
    }

    @Override
    public List<Map<String, Object>> readAnswerFor(int queryId) throws MorphiumDriverException {
        return readBatches(queryId, driver.getDefaultBatchSize());
    }

    @Override
    public MorphiumCursor getAnswerFor(int queryId) throws MorphiumDriverException {
        OpMsg reply = getReplyFor(queryId, driver.getMaxWaitTime());
        checkForError(reply);
        if (reply.hasCursor()) {
            return new SingleMongoConnectionCursor(this, driver.getDefaultBatchSize(), true, reply).setServer(connectedTo);
        } else if (reply.getFirstDoc().containsKey("results")) {
            return new SingleBatchCursor((List<Map<String, Object>>) reply.getFirstDoc().get("results"));
        }
        return new SingleElementCursor(reply.getFirstDoc());
    }

    private void checkForError(OpMsg msg) throws MorphiumDriverException {
        if (msg.getFirstDoc().containsKey("ok") && !msg.getFirstDoc().get("ok").equals(1.0)) {
            throw new MorphiumDriverException("Error: " + msg.getFirstDoc().get("code") + " - " + msg.getFirstDoc().get("errmsg"));
        }
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

    @Override
    public List<Map<String, Object>> readBatches(int waitingfor, int batchSize) throws MorphiumDriverException {
        List<Map<String, Object>> ret = new ArrayList<>();

        Map<String, Object> doc;
        String db = null;
        String coll = null;
        while (true) {
            OpMsg reply = getReplyFor(waitingfor, driver.getMaxWaitTime());
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
                q.setMessageId(msgId.incrementAndGet());
                waitingfor = q.getMessageId();
                sendQuery(q);
            } else {
                break;
            }

        }
        return ret;
    }


    public SingleElementResult runCommandSingleResult(Map<String, Object> cmd) throws MorphiumDriverException {

        return new NetworkCallHelper<SingleElementResult>().doCall(() -> {
            long start = System.currentTimeMillis();
            int id = sendCommand(cmd);

            OpMsg rep = null;
            rep = getReplyFor(id, driver.getMaxWaitTime());
            if (rep == null || rep.getFirstDoc() == null) {
                return null;
            }
            if (rep.getFirstDoc().containsKey("cursor")) {
                //should actually not happen!
                log.warn("Expecting single result, got Cursor instead!");

                Map cursor = (Map) rep.getFirstDoc().get("cursor");
                String ns = (String) cursor.get("ns");
                String[] sp = ns.split("\\.");
                String db = sp[0];
                String col = sp[1];
                Map<String, Object> ret = null;
                if (cursor.containsKey("firstBatch")) {
                    ret = ((List<Map<String, Object>>) cursor.get("firstBatch")).get(0);
                } else if (cursor.containsKey("nextBatch")) {
                    ret = ((List<Map<String, Object>>) cursor.get("firstBatch")).get(0);
                }
                killCursors(db, col, (Long) cursor.get("id"));
                SingleElementResult result = new SingleElementResult().setResult(ret)
                        .setServer(getConnectedTo()).setDuration(System.currentTimeMillis() - start);
                return ret;
            } else {
                return rep.getFirstDoc();
            }
        }, driver.getRetriesOnNetworkError(), driver.getSleepBetweenErrorRetries());

    }

}
