package de.caluga.morphium.driver.wire;

import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.morphium.driver.commands.KillCursorsCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
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
            s.setKeepAlive(true);
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

    public SingleMongoConnection setDriver(MorphiumDriver d) {
        driver = d;
        return this;
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
                    if (!s.isConnected() || s.isClosed()){
                        log.error("Connection died!");
                        close();
                        return;
                    }
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
    public void close() {
        running = false;
        synchronized (incoming) {
            incoming.notifyAll();
        }
        while (readerThread.isAlive()) {
            Thread.yield();
        }
        connected = false;
        try {
            if (in!=null)
                in.close();
            if (out!=null)
                out.close();
            if (s!=null)
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
                //throw new RuntimeException("Interrupted", e);
                 //swallow
            }
            if (System.currentTimeMillis() - start > timeout) {
                throw new MorphiumDriverException("server did not answer in time: " + timeout + "ms");
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

        try {
            //q.setFlags(4); //slave ok
            if (out == null) {
                close(); //should be already
                throw new MorphiumDriverException("closed");
            }
            out.write(q.bytes());
            out.flush();
        } catch (MorphiumDriverException e) {
            close();
            throw (e);
        } catch (Exception e) {
//                log.error("Error sending request", e);

            close();
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException ex) {
//                    //swallow
//                }
//                connect(driver, connectedTo, connectedToPort);
            throw (new MorphiumDriverException("Error sending Request: ", e));


        }
    }

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
    public void watch(WatchCommand command) throws MorphiumDriverException {
        int maxWait = command.getMaxTimeMS();
        if (command.getDb() == null) command.setDb("1"); //watch all dbs!

        if (command.getMaxTimeMS() == null || command.getMaxTimeMS() <= 0) maxWait = driver.getReadTimeout();
        OpMsg startMsg = new OpMsg();
        int batchSize = command.getBatchSize() == null ? driver.getDefaultBatchSize() : command.getBatchSize();
        startMsg.setMessageId(msgId.incrementAndGet());
//        ArrayList<Map<String, Object>> localPipeline = new ArrayList<>();
//        localPipeline.add(Doc.of("$changeStream", new HashMap<>()));
//        if (command.getPipeline() != null && !command.getPipeline().isEmpty())
//            localPipeline.addAll(command.getPipeline());
//        Doc cmd = Doc.of("aggregate", command.getColl()).add("pipeline", localPipeline)
//                .add("cursor", Doc.of("batchSize", batchSize))  //getDefaultBatchSize()
//                .add("$db", command.getDb());
        startMsg.setFirstDoc(command.asMap());
        long start = System.currentTimeMillis();
        sendQuery(startMsg);

        OpMsg msg = startMsg;
        command.setMetaData("server", getConnectedTo());
        long docsProcessed = 0;
        while (true) {
            OpMsg reply = null;
            try {
                reply = getReplyFor(msg.getMessageId(), command.getMaxTimeMS());
            } catch (MorphiumDriverException e) {
                if (e.getMessage().contains("Did not receive OpMsg-Reply in time")) {
                    log.info("timout in watch - restarting");
                    msg.setMessageId(msgId.incrementAndGet());
                    sendQuery(msg);
                    continue;
                }
                throw (e);
            }
            //log.info("got answer for watch!");
            checkForError(reply);
            Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");
            if (cursor == null) throw new MorphiumDriverException("Could not watch - cursor is null");
            // log.debug("CursorID:" + cursor.get("id").toString());

            long cursorId = Long.parseLong(cursor.get("id").toString());
            command.setMetaData("cursor", cursorId);
            List<Map<String, Object>> result = (List<Map<String, Object>>) cursor.get("firstBatch");
            if (result == null) {
                result = (List<Map<String, Object>>) cursor.get("nextBatch");
            }
            if (result != null && !result.isEmpty()) {
                for (Map<String, Object> o : result) {
                    command.getCb().incomingData(o, System.currentTimeMillis() - start);
                    docsProcessed++;
                }
            } else {
                log.info("No/empty result");
            }
            if (!command.getCb().isContinued()) {
                killCursors(command.getDb(), command.getColl(), cursorId);
                command.setMetaData("duration", System.currentTimeMillis() - start);
                break;
            }
            if (cursorId != 0) {
                msg = new OpMsg();
                msg.setMessageId(msgId.incrementAndGet());
                String[] ns = cursor.get("ns").toString().split("\\.");
                var db = ns[0];
                var col = ns[1];
                if (ns.length > 2) {
                    for (int i = 2; i < ns.length; i++) {
                        col = col + "." + ns[i];
                    }
                }
                Doc doc = new Doc();
                doc.put("getMore", cursorId);
                doc.put("collection", col);

                doc.put("batchSize", batchSize);
                doc.put("maxTimeMS", maxWait);
                doc.put("$db", db);
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
        return readAnswerFor(getAnswerFor(queryId, driver.getDefaultBatchSize()));
        //return readBatches(queryId, driver.getDefaultBatchSize());
    }

    @Override
    public MorphiumCursor getAnswerFor(int queryId, int batchSize) throws MorphiumDriverException {
        OpMsg reply = getReplyFor(queryId, driver.getMaxWaitTime());
        checkForError(reply);
        if (reply.hasCursor()) {
            return new SingleMongoConnectionCursor(this, batchSize, true, reply).setServer(connectedTo);
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


}
