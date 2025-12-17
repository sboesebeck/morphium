package de.caluga.morphium.driver.wire;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.morphium.driver.commands.KillCursorsCommand;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.commands.auth.SaslAuthCommand;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage.OpCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static de.caluga.morphium.driver.MorphiumDriver.DriverStatsKey.*;

public class SingleMongoConnection implements MongoConnection {

    private final Logger log = LoggerFactory.getLogger(SingleMongoConnection.class);
    private Socket s;
    private OutputStream out;
    private InputStream in;

    private AtomicInteger msgId = new AtomicInteger(1000);

    //    private List<OpMsg> replies = Collections.synchronizedList(new ArrayList<>());
    // private Thread readerThread = null;
    // private Map<Integer, OpMsg> incoming = new HashMap<>();
    // private Map<Integer, Long> incomingTimes = new ConcurrentHashMap<>();
    private boolean running = true;

    private Map<MorphiumDriver.DriverStatsKey, AtomicDecimal> stats;
    private String connectedTo;
    private int connectedToPort;
    private boolean connected = false;
    private MorphiumDriver driver;

    private String authDb = null;
    private String user = null;
    private String password = null;

    public SingleMongoConnection() {
        stats = new HashMap<>();
        stats.put(MSG_SENT, new AtomicDecimal(0));
        stats.put(REPLY_PROCESSED, new AtomicDecimal(0));
        stats.put(REPLY_RECEIVED, new AtomicDecimal(0));
    }

    @Override
    public void setCredentials(String authDb, String userName, String password) {
        this.authDb = authDb;
        this.user = userName;
        this.password = password;
    }

    @Override
    public HelloResult connect(MorphiumDriver drv, String host, int port) throws MorphiumDriverException {
        driver = drv;

        try {
            //            log.info("Connecting to " + host + ":" + port);
            if (drv.isUseSSL()) {
                s = createSslSocket(drv, host, port);
            } else {
                s = new Socket();
                int timeout = drv.getConnectionTimeout();
                if (timeout <= 0) {
                    timeout = 1000;
                }
                s.connect(new InetSocketAddress(host, port), timeout);
            }
            s.setKeepAlive(true);
            int readTimeout = drv.getReadTimeout();
            if (readTimeout > 0) {
                s.setSoTimeout(readTimeout);
            }
            out = s.getOutputStream();
            in = s.getInputStream();
        } catch (IOException e) {
            throw new MorphiumDriverException("Connection failed: " + host + ":" + port, e);
        }

        // startReaderThread();
        var hello = getHelloResult(true);
        connectedTo = host;
        connectedToPort = port;
        //log.info("Connected to "+connectedTo+":"+port);
        connected = true;
        return hello;
    }

    private Socket createSslSocket(MorphiumDriver drv, String host, int port) throws IOException {
        javax.net.ssl.SSLContext sslContext = null;

        // Try to get custom SSLContext from driver if it's a DriverBase
        if (drv instanceof DriverBase) {
            sslContext = ((DriverBase) drv).getSslContext();
        }

        // Use default SSLContext if none provided
        if (sslContext == null) {
            try {
                sslContext = javax.net.ssl.SSLContext.getDefault();
            } catch (java.security.NoSuchAlgorithmException e) {
                throw new IOException("Failed to get default SSLContext", e);
            }
        }

        javax.net.ssl.SSLSocketFactory factory = sslContext.getSocketFactory();
        javax.net.ssl.SSLSocket sslSocket = (javax.net.ssl.SSLSocket) factory.createSocket();
        int timeout = drv.getConnectionTimeout();
        if (timeout <= 0) {
            timeout = 1000;
        }
        sslSocket.connect(new InetSocketAddress(host, port), timeout);

        // Configure SSL parameters
        javax.net.ssl.SSLParameters params = sslSocket.getSSLParameters();

        // Enable hostname verification unless explicitly disabled
        boolean allowInvalidHostname = false;
        if (drv instanceof DriverBase) {
            allowInvalidHostname = ((DriverBase) drv).isSslInvalidHostNameAllowed();
        }

        if (!allowInvalidHostname) {
            params.setEndpointIdentificationAlgorithm("HTTPS");
        }

        sslSocket.setSSLParameters(params);
        sslSocket.startHandshake();

        log.debug("SSL connection established to {}:{}", host, port);
        return sslSocket;
    }

    public HelloResult getHelloResult(boolean includeClient) throws MorphiumDriverException {
        OpMsg result = null;
        long start = System.currentTimeMillis();

        while (null == result) {
            HelloCommand cmd = new HelloCommand(null);

            if (authDb != null) {
                cmd.setUser(user);
                cmd.setSaslSupportedMechs(authDb + "." + user);
            }

            if (!includeClient) {
                cmd.setIncludeClient(false);
            }

            cmd.setLoadBalanced(false);
            OpMsg msg = new OpMsg();
            msg.setMessageId(msgId.incrementAndGet());
            msg.setFirstDoc(cmd.asMap());
            result = sendAndWaitForReply(msg);

            if (result == null && System.currentTimeMillis() - start > getDriver().getMaxWaitTime()) {
                throw new MorphiumDriverException("Hello result is null");
            }

            try {
                if (result == null) Thread.sleep(1000);
            } catch (InterruptedException e) {
                // swallow
            }
        }

        Map<String, Object> firstDoc = result.getFirstDoc();
        var hello = HelloResult.fromMsg(firstDoc);

        if (authDb != null) {
            SaslAuthCommand auth = new SaslAuthCommand(this);

            if (hello.getSaslSupportedMechs() == null || hello.getSaslSupportedMechs().isEmpty()) {
                throw new MorphiumDriverException("Authentication failed - no mechanisms offered!");
            }

            auth.setUser(user).setDb(authDb).setPassword(password);

            if (hello.getSaslSupportedMechs().contains("SCRAM-SHA-256")) {
                auth.setMechanism("SCRAM-SHA-256");
            } else {
                auth.setMechanism("SCRAM-SHA-1");
            }

            try {
                auth.execute();
            } catch (Exception e) {
                throw new MorphiumDriverException("Error Authenticating", e);
            }

            //            log.info("No error up to here - we should be authenticated!");
        }

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

    public Map<MorphiumDriver.DriverStatsKey, Double> getStats() {
        Map<MorphiumDriver.DriverStatsKey, Double> ret = new HashMap<>();
        var s = new HashMap<>(stats);

        for (var e : s.entrySet()) {
            ret.put(e.getKey(), e.getValue().get());
        }

        ret.put(THREADS_CREATED, 1.0);
        // ret.put(REPLY_IN_MEM, (double) incoming.size());
        return ret;
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

        KillCursorsCommand k = new KillCursorsCommand(this).setCursors(cursorIds).setDb(db).setColl(coll);
        var ret = k.execute();
        log.debug("killed cursor");
        return ret;
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    public boolean isDataAvailable() {

        if (in == null) {
            return false;
        }

        try {
            return in.available() != 0;
        } catch (Exception e) {
            //error?  close connection
            close();
        }

        return false;
    }

    public OpMsg readNextMessage(int timeout) throws MorphiumDriverException {
        if (s == null) {
            throw new MorphiumDriverException("Connection closed");
        }

        while (true) {
            try {
                s.setSoTimeout(timeout);
            } catch (SocketException e) {
                close();
                throw new MorphiumDriverException("socket error", e);
            }

            try {
                var incoming = WireProtocolMessage.parseFromStream(in);
                OpMsg msg = null;

                if (incoming instanceof OpCompressed) {
                    var opc = ((OpCompressed)incoming);
                    byte[] msgb = opc.getCompressedMessage();
                    OpMsg message = new OpMsg();
                    message.setMessageId(opc.getMessageId());
                    message.parsePayload(msgb, 0);
                    msg = message;
                } else {
                    msg = (OpMsg)incoming;
                }

                if (msg == null) {
                    return null;
                }

                stats.get(REPLY_RECEIVED).incrementAndGet();
                return msg;
            } catch (SocketTimeoutException ste) {
                log.debug("socket timeout - retrying");
            } catch (Exception e) {
                if (e.getCause() instanceof SocketTimeoutException) {
                    log.debug("socket timeout - retry");
                    continue;
                } else if (running) {
                    close();
                    throw new MorphiumDriverException("" + e.getMessage(), e);
                }
                return null;
            }
        }
    }

    @Override
    public void close() {
        running = false;
        connected = false;

        if (in != null) {
            try {
                in.close();
            } catch (IOException e) {
                //swallow
            }
        }

        if (out != null) {
            try {
                out.close();
            } catch (IOException e) {
                //swallow
            }
        }

        if (s != null) {
            try {
                s.close();
            } catch (IOException e) {
                //swallow
            }
        }

        in = null;
        out = null;
        s = null;
        driver.closeConnection(this);
    }

    // @Override
    // public void release() {
    //     driver.releaseConnection(this);
    // }


    // @Override
    // public OpMsg getReplyFor(int msgid, long timeout) throws MorphiumDriverException {
    //     long start = System.currentTimeMillis();
    //     while (!incoming.containsKey(msgid)) {
    //         if (lastRead == 0) {
    //             log.error(this+": never read?");
    //         } else if (System.currentTimeMillis() - LastRead > 1000) {
    //             log.error(this+": No reader thread?");
    //         }
    //         try {
    //             synchronized (incoming) {
    //                 incoming.wait(timeout);
    //             }
    //         } catch (InterruptedException e) {
    //             //throw new RuntimeException("Interrupted", e);
    //             //swallow
    //         }
    //
    //         if (!running) {
    //             return null;
    //         }
    //
    //         if (System.currentTimeMillis() - start > 2 * timeout) {
    //             close();
    //             throw new MorphiumDriverException("server did not answer in time: " + timeout + "ms");
    //         }
    //     }
    //
    //     synchronized (incomingTimes) {
    //         incomingTimes.remove(msgid);
    //     }
    //
    //     if (!incoming.containsKey(msgid)) {
    //         log.warn("Did not get reply within " + timeout + "ms");
    //     }
    //
    //     stats.get(REPLY_PROCESSED).incrementAndGet();
    //     return incoming.remove(msgid);
    // }
    //
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

            stats.get(MSG_SENT).incrementAndGet();

            if (driver.getCompression() != OpCompressed.COMPRESSOR_NOOP) {
                var opc = new OpCompressed();
                opc.setOriginalOpCode(OpMsg.OP_CODE);
                opc.setCompressorId(driver.getCompression());
                q.setFlags(0);
                byte[] data = q.getPayload();
                opc.setCompressedMessage(data);
                opc.setSize(data.length + 4);
                opc.setMessageId(msgId.incrementAndGet());
                opc.setUncompressedSize(data.length);
                // log.info(Utils.getHex(opc.bytes()));
                out.write(opc.bytes());
            } else {
                out.write(q.bytes());
            }

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
        return readNextMessage(driver.getMaxWaitTime());//getReplyFor(q.getMessageId(), driver.getMaxWaitTime());
    }

    @Override
    public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
        OpMsg reply = readNextMessage(driver.getMaxWaitTime());//getReplyFor(id, driver.getMaxWaitTime());

        if (reply == null) {
            return null;
        }

        if (reply.hasCursor()) {
            return getSingleDocAndKillCursor(reply);
        }

        if (reply.getFirstDoc().get("ok").equals(0.0)) {
            throw new MorphiumDriverException((String) reply.getFirstDoc().get("errmsg"));
        }

        return reply.getFirstDoc();
    }

    @Override
    public int sendCommand(MongoCommand cmd) throws MorphiumDriverException {
        OpMsg q = new OpMsg();
        q.setMessageId(msgId.incrementAndGet());
        q.setFirstDoc(Doc.of(cmd.asMap()));
        sendQuery(q);
        return q.getMessageId();
    }

    @Override
    public int getSourcePort() {
        if (s == null) {
            return 0;
        }

        return s.getLocalPort();
    }

    @Override
    public void watch(WatchCommand command) throws MorphiumDriverException {
        int maxWait = command.getMaxTimeMS();

        if (command.getDb() == null) {
            command.setDb("1");    //watch all dbs!
        }

        if (command.getMaxTimeMS() == null || command.getMaxTimeMS() <= 0) {
            maxWait = driver.getReadTimeout();
        }

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
                reply = readNextMessage(maxWait);//getReplyFor(msg.getMessageId(), command.getMaxTimeMS());
            } catch (MorphiumDriverException e) {
                if (e.getMessage().contains("server did not answer in time: ") || e.getMessage().contains("Read timed out")) {
                    log.debug("timeout in watch - restarting");
                    msg.setMessageId(msgId.incrementAndGet());
                    sendQuery(msg);
                    continue;
                }

                throw (e);
            }

            //log.info("got answer for watch!");
            checkForError(reply);

            if (reply == null) {
                log.debug("Got null as reply");
                throw new MorphiumDriverException("Could not watch - reply is null");
            }

            Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");

            if (cursor == null) {
                throw new MorphiumDriverException("Could not watch - cursor is null");
            }

            // log.debug("CursorID:" + cursor.get("id").toString());
            long cursorId = Long.parseLong(cursor.get("id").toString());
            command.setMetaData("cursor", cursorId);
            List<Map<String, Object >> result = (List<Map<String, Object >> ) cursor.get("firstBatch");

            if (result == null) {
                result = (List<Map<String, Object >>) cursor.get("nextBatch");
            }
            if (result != null && !result.isEmpty()) {
                for (Map<String, Object> o : result) {
                    command.getCb().incomingData(o, System.currentTimeMillis() - start);
                    docsProcessed++;
                }
                // } else {
                // log.info("No/empty result");
            }

            if (!command.getCb().isContinued()) {
                String coll = command.getColl();

                if (coll == null) {
                    coll = "1";
                }

                killCursors(command.getDb(), coll, cursorId);
                command.setMetaData("duration", System.currentTimeMillis() - start);
                break;
            }
            if (cursorId != 0) {
                msg = new OpMsg();
                msg.setMessageId(msgId.incrementAndGet());
                String[] ns = cursor.get("ns").toString().split("\\.");
                var db = ns[0];
                String col = cursor.get("ns").toString();

                if (ns[0].length() + 1 < cursor.get("ns").toString().length()) {
                    col = ((String)cursor.get("ns")).substring(ns[0].length() + 1);
                }

                // if (ns.length > 2) {
                //     for (int i = 2; i < ns.length; i++) {
                //         col = col + "." + ns[i];
                //     }
                // }
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
    public List<Map<String, Object >> readAnswerFor(int queryId) throws MorphiumDriverException {
        return readAnswerFor(getAnswerFor(queryId, driver.getDefaultBatchSize()));
        //return readBatches(queryId, driver.getDefaultBatchSize());
    }

    @Override
    public MorphiumCursor getAnswerFor(int queryId, int batchSize) throws MorphiumDriverException {
        OpMsg reply = readNextMessage(driver.getMaxWaitTime());//getReplyFor(queryId, driver.getMaxWaitTime());
        checkForError(reply);

        if (reply == null) {
            return new SingleBatchCursor(List.of());
        }

        if (reply.hasCursor()) {
            return new SingleMongoConnectionCursor(this, batchSize, true, reply).setServer(connectedTo);
        }

        else if (reply.getFirstDoc().containsKey("results")) {
            return new SingleBatchCursor((List<Map<String, Object >>) reply.getFirstDoc().get("results"));
        }

        return new SingleElementCursor(reply.getFirstDoc());
    }

    private void checkForError(OpMsg msg) throws MorphiumDriverException {
        if (msg == null || msg.getFirstDoc() == null) {
            return;
        }

        if (msg.getFirstDoc().containsKey("ok") && !msg.getFirstDoc().get("ok").equals(1.0)) {
            throw new MorphiumDriverException("Error: " + msg.getFirstDoc().get("code") + " - " + msg.getFirstDoc().get("errmsg"));
        }
    }

    @Override
    public List<Map<String, Object >> readAnswerFor(MorphiumCursor crs) throws MorphiumDriverException {
        List<Map<String, Object >> ret = new ArrayList<>();

        while (crs.hasNext()) {
            ret.addAll(crs.getBatch());
            crs.ahead(crs.getBatch().size());
        }

        return ret;
    }

    private Map<String, Object> getSingleDocAndKillCursor(OpMsg msg) throws MorphiumDriverException {
        if (!msg.hasCursor()) {
            return null;
        }

        if (!(msg.getFirstDoc().get("cursor") instanceof Map)) {
            log.error("Cursor has wrong type: {}", msg.getFirstDoc().get("cursor").toString());
            return null;
        }

        Map<String, Object> cursor = (Map<String, Object>) msg.getFirstDoc().get("cursor");
        Map<String, Object> ret = null;

        if (cursor.containsKey("firstBatch")) {
            List<Map<String, Object >> lst = (List<Map<String, Object >> ) cursor.get("firstBatch");

            if (lst != null && !lst.isEmpty()) {
                ret = lst.get(0);
            }
        } else {
            List<Map<String, Object >> lst = (List<Map<String, Object >> ) cursor.get("nextBatch");

            if (lst != null && !lst.isEmpty()) {
                ret = lst.get(0);
            }
        }

        String[] namespace = cursor.get("ns").toString().split("\\.");
        killCursors(namespace[0], namespace[1], (Long) cursor.get("id"));
        return ret;
    }

}
