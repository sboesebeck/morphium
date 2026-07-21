package de.caluga.morphium.driver.wire;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.morphium.driver.commands.KillCursorsCommand;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.commands.auth.SaslAuthCommand;
import de.caluga.morphium.driver.commands.auth.X509AuthCommand;
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
    // Cache the source port so it's available even after socket is closed
    // This is needed for proper cleanup in PooledDriver.releaseConnection()
    private volatile int cachedSourcePort = 0;

    private AtomicInteger msgId = new AtomicInteger(1000);

    // Extra client-side wait on top of a watch getMore's maxTimeMS: the server answers
    // within maxTimeMS, this grace covers network and processing time. Only a truly broken
    // connection exceeds it.
    static final int WATCH_READ_GRACE_MS = 10_000;

    //    private List<OpMsg> replies = Collections.synchronizedList(new ArrayList<>());
    // private Thread readerThread = null;
    // private Map<Integer, OpMsg> incoming = new HashMap<>();
    // private Map<Integer, Long> incomingTimes = new ConcurrentHashMap<>();
    private volatile boolean running = true;

    private Map<MorphiumDriver.DriverStatsKey, AtomicDecimal> stats;
    private String connectedTo;
    private int connectedToPort;
    private volatile boolean connected = false;
    private MorphiumDriver driver;

    private String authDb = null;
    private String user = null;
    private String password = null;

    /**
     * Subject DN extracted from the client certificate after TLS handshake.
     * Populated automatically when {@link DriverBase#isUseSSL()} is {@code true}
     * and the SSLContext contains a client certificate (keystore entry).
     * Used as the {@code user} field in MONGODB-X509 authentication.
     */
    private String x509SubjectDn = null;

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
        // Cache the source port for later use (needed for cleanup even after socket close)
        cachedSourcePort = s.getLocalPort();
        // The handshake against a freshly connected host must answer quickly. Using
        // maxWaitTime here would make connects to half-dead hosts (TCP accepted, mongod
        // frozen) hang for up to a minute - stalling startup and the heartbeat.
        int helloTimeout = drv.getConnectionTimeout() > 0 ? Math.max(drv.getConnectionTimeout(), 2000) : 5000;
        var hello = getHelloResult(true, helloTimeout);
        connectedTo = host;
        connectedToPort = port;
        //log.info("Connected to "+connectedTo+":"+port);
        connected = true;
        return hello;
    }

    /**
     * Extracts the subject DN from the client certificate after the TLS handshake.
     * The subject DN is used as the {@code user} field in MONGODB-X509 authentication.
     * If no client certificate was presented (server-only TLS), {@link #x509SubjectDn}
     * remains {@code null} and the server will derive the identity from the handshake.
     */
    private void extractX509SubjectDn(javax.net.ssl.SSLSocket sslSocket) {
        try {
            java.security.cert.Certificate[] localCerts =
                    sslSocket.getSession().getLocalCertificates();
            if (localCerts != null && localCerts.length > 0
                    && localCerts[0] instanceof java.security.cert.X509Certificate) {
                java.security.cert.X509Certificate cert =
                        (java.security.cert.X509Certificate) localCerts[0];
                // RFC 2253 format: "CN=foo,O=bar,C=DE" – required by MongoDB Atlas
                x509SubjectDn = cert.getSubjectX500Principal()
                        .getName(javax.security.auth.x500.X500Principal.RFC2253);
                log.debug("X.509 client certificate subject DN: {}", x509SubjectDn);
            } else {
                log.debug("No client certificate in TLS session – X.509 auth will rely on server-side DN extraction");
            }
        } catch (Exception e) {
            log.warn("Could not extract X.509 subject DN from TLS session: {}", e.getMessage());
        }
    }

    /**
     * Performs MONGODB-X509 authentication.
     *
     * <p>The client certificate subject DN ({@link #x509SubjectDn}) is passed as
     * the {@code user} field.  If it is {@code null} (no client cert in TLS session),
     * the server derives the identity automatically (MongoDB 3.4+).
     *
     * <p>If an explicit {@link #user} was set via {@link #setCredentials} that value
     * takes precedence, allowing the caller to override the auto-extracted DN.
     */
    private void authenticateX509() throws MorphiumDriverException {
        String subjectDn = (user != null && !user.isBlank()) ? user : x509SubjectDn;

        log.debug("Authenticating with MONGODB-X509, user='{}'",
                subjectDn != null ? subjectDn : "<derived from TLS session>");

        X509AuthCommand cmd = new X509AuthCommand(this);
        cmd.setUser(subjectDn);
        cmd.execute();
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

        // Extract the subject DN from the client certificate (if present).
        // This is needed for MONGODB-X509 authentication – the subject DN is the MongoDB username.
        extractX509SubjectDn(sslSocket);

        log.debug("SSL connection established to {}:{}", host, port);
        return sslSocket;
    }

    public HelloResult getHelloResult(boolean includeClient) throws MorphiumDriverException {
        return getHelloResult(includeClient, getDriver().getMaxWaitTime());
    }

    /**
     * Hello with an explicit reply timeout. Used by the connection handshake and the
     * heartbeat: a hello against a host that accepted the TCP connection but never
     * answers (frozen VM, network partition) must not block for maxWaitTime -
     * otherwise dead-host detection takes minutes.
     */
    public HelloResult getHelloResult(boolean includeClient, int timeoutMs) throws MorphiumDriverException {
        OpMsg result = null;
        long start = System.currentTimeMillis();

        while (null == result) {
            // pass this connection so the client metadata (driver name, appName) can be resolved
            HelloCommand cmd = new HelloCommand(this);

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
            result = sendAndWaitForReply(msg, timeoutMs);

            if (result == null && System.currentTimeMillis() - start > timeoutMs) {
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

        String mechanism = driver.getAuthMechanism();

        if ("MONGODB-X509".equalsIgnoreCase(mechanism)) {
            // X.509 authentication: client certificate was already presented in the TLS
            // handshake. We now send the authenticate command with the subject DN.
            authenticateX509();
        } else if (authDb != null) {
            // Standard SCRAM-SHA-1 / SCRAM-SHA-256 authentication
            SaslAuthCommand auth = new SaslAuthCommand(this);

            auth.setUser(user).setDb(authDb).setPassword(password);

            if (hello.getSaslSupportedMechs() == null || hello.getSaslSupportedMechs().isEmpty()) {
                // Cosmos DB does not return saslSupportedMechs in hello response —
                // default to SCRAM-SHA-256 which is the standard mechanism.
                log.warn("No SASL mechanisms offered by server — defaulting to SCRAM-SHA-256");
                auth.setMechanism("SCRAM-SHA-256");
            } else if (hello.getSaslSupportedMechs().contains("SCRAM-SHA-256")) {
                auth.setMechanism("SCRAM-SHA-256");
            } else {
                auth.setMechanism("SCRAM-SHA-1");
            }

            try {
                auth.execute();
            } catch (Exception e) {
                throw new MorphiumDriverException("Error Authenticating", e);
            }
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
        // Normalize to lowercase for consistent hostname matching in connection pools
        return connectedTo.toLowerCase() + ":" + connectedToPort;
    }

    public String getConnectedToHost() {
        // Normalize to lowercase for consistent hostname matching in connection pools
        return connectedTo.toLowerCase();
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

        // For watch/tailable scenarios, limit consecutive socket timeouts
        // This allows calling code to check isContinued() periodically
        // The timeout parameter is the TOTAL time we wait for a reply. It must not be
        // multiplied by retries: a frozen/partitioned host would otherwise block callers
        // for timeout*100 (with maxWaitTime=60s that is over an hour) and dead-host
        // detection would never kick in.
        long deadline = timeout > 0 ? System.currentTimeMillis() + timeout : Long.MAX_VALUE;

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
                    // the outer OP_COMPRESSED header carries the real responseTo - without it,
                    // reply/request matching would flag every compressed reply as out-of-sync
                    message.setResponseTo(opc.getResponseTo());
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
                // Raw SocketTimeoutException from parseFromStream means 0 bytes were consumed:
                // the stream is still message-aligned, retrying the read is safe.
                if (System.currentTimeMillis() >= deadline) {
                    // No reply within the caller's total timeout. A late reply may still arrive
                    // on this connection - the next user would read a stale answer (watch() reads
                    // without responseTo verification!). Close instead of handing back a landmine.
                    log.debug("socket timeout - deadline reached, closing connection");
                    close();
                    return null;
                }
                log.debug("socket timeout - retrying until deadline");
            } catch (Exception e) {
                // MorphiumDriverNetworkException is always fatal for this connection, even with a
                // SocketTimeoutException cause: parseFromStream wraps a mid-message timeout that
                // way ("stream desynchronized") - retrying the parse would read payload as header.
                if (!(e instanceof MorphiumDriverNetworkException) && e.getCause() instanceof SocketTimeoutException) {
                    if (System.currentTimeMillis() >= deadline) {
                        log.debug("socket timeout - deadline reached, closing connection");
                        close();
                        return null;
                    }
                    log.debug("socket timeout - retrying until deadline");
                    continue;
                } else if (running) {
                    log.warn("Connection error on {} (port {}), closing connection: {}",
                            connectedTo, getSourcePort(), e.getMessage());
                    close();
                    // Preserve MorphiumDriverNetworkException so NetworkCallHelper can retry
                    if (e instanceof MorphiumDriverNetworkException) {
                        throw (MorphiumDriverNetworkException) e;
                    }
                    // Check if the cause is a network exception (e.g. wrapped by RuntimeException)
                    if (e.getCause() instanceof MorphiumDriverNetworkException) {
                        throw (MorphiumDriverNetworkException) e.getCause();
                    }
                    throw new MorphiumDriverNetworkException("Connection error: " + e.getMessage(), e);
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
    public synchronized void sendQuery(OpMsg q) throws MorphiumDriverException {
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
                // per spec the OP_COMPRESSED header carries the requestID of the original
                // message - a fresh id would make the server reply to an id nobody waits for
                opc.setMessageId(q.getMessageId());
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
            close();
            // Preserve MorphiumDriverNetworkException so NetworkCallHelper can retry
            if (e instanceof MorphiumDriverNetworkException) {
                throw (MorphiumDriverNetworkException) e;
            }
            if (e.getCause() instanceof MorphiumDriverNetworkException) {
                throw (MorphiumDriverNetworkException) e.getCause();
            }
            if (e instanceof java.io.IOException) {
                throw new MorphiumDriverNetworkException("Error sending Request: " + e.getMessage(), e);
            }
            throw new MorphiumDriverException("Error sending Request: ", e);
        }
    }

    public synchronized OpMsg sendAndWaitForReply(OpMsg q) throws MorphiumDriverException {
        return sendAndWaitForReply(q, driver.getMaxWaitTime());
    }

    public synchronized OpMsg sendAndWaitForReply(OpMsg q, int timeout) throws MorphiumDriverException {
        sendQuery(q);
        return readReplyFor(q.getMessageId(), timeout);
    }

    /**
     * Reads the next message and verifies it actually answers the given request.
     * If the reply's responseTo does not match, this connection's stream is out of sync
     * (a reply was left unread by an earlier aborted/timed-out request) - every subsequent
     * caller would receive its predecessor's answer (seen in prod as "cursor id not found"
     * on fresh finds, 2026-07-11/12). The connection is poisoned: close it and throw a
     * retriable network exception so callers retry on a fresh connection.
     */
    private OpMsg readReplyFor(int requestId, int timeout) throws MorphiumDriverException {
        OpMsg reply = readNextMessage(timeout);

        if (reply != null && reply.getResponseTo() != requestId) {
            log.error("Connection to {} out of sync: expected reply to request {}, got reply to {} - closing connection",
                connectedTo, requestId, reply.getResponseTo());
            close();
            throw new MorphiumDriverNetworkException("Connection out of sync: expected reply to request "
                + requestId + ", got reply to " + reply.getResponseTo());
        }

        return reply;
    }

    @Override
    public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
        OpMsg reply = readReplyFor(id, driver.getMaxWaitTime());

        if (reply == null) {
            return null;
        }

        if (reply.hasCursor()) {
            return getSingleDocAndKillCursor(reply);
        }

        if (reply.getFirstDoc().get("ok").equals(0.0)) {
            String errmsg = (String) reply.getFirstDoc().get("errmsg");
            Object codeObj = reply.getFirstDoc().get("code");
            int code = codeObj instanceof Number ? ((Number) codeObj).intValue() : -1;
            String formattedMsg = "Error: " + code + " - " + errmsg;

            // Error 251 (NoSuchTransaction): close poisoned connection, throw retriable exception
            if (code == 251) {
                log.warn("Transient transaction error on connection (code 251) — closing connection: {}", errmsg);
                close();
                var ex = new MorphiumDriverNetworkException(formattedMsg);
                ex.setMongoCode(code);
                throw ex;
            }

            // BSON document size limit: 10334 (legacy) or 17280 (newer storage path) — both mean
            // the single document exceeded MongoDB's hard 16 MB BSON limit. Surface as a
            // dedicated subtype so callers can catch it without parsing error strings.
            if (code == 10334 || code == 17280
                    || (errmsg != null && errmsg.contains("BSONObj size"))) {
                var ex = new MorphiumDocumentTooLargeException(formattedMsg);
                ex.setMongoCode(codeObj);
                throw ex;
            }

            var ex = new MorphiumDriverException(formattedMsg);
            ex.setMongoCode(codeObj);
            throw ex;
        }

        return reply.getFirstDoc();
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized int sendCommand(MongoCommand cmd) throws MorphiumDriverException {
        OpMsg q = new OpMsg();
        q.setMessageId(msgId.incrementAndGet());
        q.setFirstDoc(Doc.of(cmd.asMap()));
        sendQuery(q);
        return q.getMessageId();
    }

    @Override
    public int getSourcePort() {
        if (s != null) {
            // Update cache while socket is available
            cachedSourcePort = s.getLocalPort();
        }
        // Return cached value (works even after socket is closed)
        return cachedSourcePort;
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

        // Track the last resume token to use when restarting the watch
        // This prevents duplicate events when the watch restarts due to timeout or cursor exhaustion
        @SuppressWarnings("unchecked")
        final Map<String, Object>[] lastResumeToken = new Map[]{null};

        OpMsg startMsg = new OpMsg();
        int batchSize = command.getBatchSize() == null ? driver.getDefaultBatchSize() : command.getBatchSize();
        startMsg.setMessageId(msgId.incrementAndGet());
        startMsg.setFirstDoc(command.asMap());
        long start = System.currentTimeMillis();
        sendQuery(startMsg);
        OpMsg msg = startMsg;
        command.setMetaData("server", getConnectedTo());
        long docsProcessed = 0;
        boolean registrationCallbackCalled = false;

        long watchIterations = 0;
        while (true) {
            watchIterations++;
            OpMsg reply = null;

            try {
                // The server answers a getMore within maxTimeMS (empty batch on no events).
                // Waiting only maxWait client-side loses the race against network/processing
                // time: the client hits its deadline just before the reply arrives, "restarts"
                // the stream in place and thereby (a) leaks the previous server-side cursor
                // (seen as hundreds of idle $changeStream cursors in prod) and (b) leaves the
                // late reply unread in the stream - desyncing every following read (Error 43
                // on unrelated queries, planner stall 2026-07-11/12). Grant a grace period.
                reply = readNextMessage(maxWait + WATCH_READ_GRACE_MS);
            } catch (MorphiumDriverException e) {
                if (e.getMessage().contains("server did not answer in time: ") || e.getMessage().contains("Read timed out")) {
                    log.debug("WATCH: timeout, resending query");
                    msg.setMessageId(msgId.incrementAndGet());
                    sendQuery(msg);
                    continue;
                }
                throw (e);
            }

            //log.info("got answer for watch!");

            // Handle null reply (no answer within maxTimeMS + grace)
            // Check isContinued() to allow caller to detect staleness and decide to stop
            if (reply == null) {
                log.debug("Got null as reply - checking if should continue");
                if (!command.getCb().isContinued()) {
                    log.debug("Callback indicates stop - exiting watch loop");
                    break;
                }

                // No reply although the server must answer within maxTimeMS: this connection
                // is suspect - a late reply may still be in flight. Restarting the stream on
                // the same connection would desync the reply stream and leak the server-side
                // cursor. Close and let the caller (ChangeStreamMonitor) resume on a fresh
                // connection using its tracked resume token.
                if (lastResumeToken[0] != null) {
                    command.setResumeAfter(lastResumeToken[0]);
                }
                log.warn("watch: no reply within maxTimeMS+{}ms grace on {} - closing connection, caller should resume", WATCH_READ_GRACE_MS, connectedTo);
                close();
                throw new MorphiumDriverNetworkException("watch: no reply within maxTimeMS + grace - connection closed, resume on a fresh connection");
            }

            checkForError(reply);

            @SuppressWarnings("unchecked")
            Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");

            if (cursor == null) {
                throw new MorphiumDriverException("Could not watch - cursor is null");
            }

            // log.debug("CursorID:" + cursor.get("id").toString());
            long cursorId = Long.parseLong(cursor.get("id").toString());
            command.setMetaData("cursor", cursorId);

            // PoppyDB-specific, best-effort: some servers (PoppyDB primaries) piggyback their
            // current change-stream sequence on the initial aggregate response, following the
            // same convention as the "poppyResumeSequence" resumeAfter marker. A real MongoDB
            // server never sends this field, so it is simply absent there. Stash it as generic
            // command metadata (same mechanism as "cursor"/"server" above) so callers with access
            // to this command instance - e.g. a registrationCallback - can read the primary's
            // sequence at the exact moment the watch was established, without widening the
            // registrationCallback's Runnable signature.
            Object primarySequence = reply.getFirstDoc().get("poppyPrimarySequence");
            if (primarySequence != null) {
                command.setMetaData("poppyPrimarySequence", primarySequence);
            }

            // Call registration callback once the watch cursor is established
            // This signals to ChangeStreamMonitor that the watch is ready to receive events
            if (!registrationCallbackCalled && command.getRegistrationCallback() != null) {
                registrationCallbackCalled = true;
                try {
                    command.getRegistrationCallback().run();
                } catch (Exception e) {
                    log.warn("Registration callback failed: {}", e.getMessage());
                }
            }

            @SuppressWarnings("unchecked")
            List<Map<String, Object >> result = (List<Map<String, Object >> ) cursor.get("firstBatch");

            if (result == null) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object >> nextBatchResult = (List<Map<String, Object >>) cursor.get("nextBatch");
                result = nextBatchResult;
            }
            // Track whether we should exit after processing events
            boolean shouldExit = false;
            if (result != null && !result.isEmpty()) {
                log.info("WATCH: received batch of {} events for coll={} (iter={})", result.size(), command.getColl(), watchIterations);
                for (Map<String, Object> o : result) {
                    // Capture resume token from each event (_id is the resume token in change streams)
                    @SuppressWarnings("unchecked")
                    Map<String, Object> eventResumeToken = (Map<String, Object>) o.get("_id");
                    if (eventResumeToken != null) {
                        lastResumeToken[0] = eventResumeToken;
                    }
                    long cbStart = System.currentTimeMillis();
                    command.getCb().incomingData(o, System.currentTimeMillis() - start);
                    long cbDur = System.currentTimeMillis() - cbStart;
                    if (cbDur > 100) {
                        log.warn("WATCH: callback took {}ms for coll={}", cbDur, command.getColl());
                    }
                    docsProcessed++;
                    // Check isContinued() after EACH event, matching InMemoryDriver behavior
                    // This ensures we stop immediately when callback returns false
                    if (!command.getCb().isContinued()) {
                        log.info("WATCH: isContinued returned false, will exit - coll={}", command.getColl());
                        shouldExit = true;
                        break;
                    }
                }
            }

            if (shouldExit || !command.getCb().isContinued()) {
                log.debug("WATCH: exiting loop - shouldExit={}, isContinued={}", shouldExit, command.getCb().isContinued());
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
            } else {
                log.debug("WATCH: cursor exhausted, restarting");
                // Use resume token if available to prevent duplicate events
                if (lastResumeToken[0] != null) {
                    command.setResumeAfter(lastResumeToken[0]);
                    startMsg.setFirstDoc(command.asMap());
                    log.debug("Resuming from token after cursor exhausted");
                }
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
        OpMsg reply = readReplyFor(queryId, driver.getMaxWaitTime());
        checkForError(reply);

        if (reply == null) {
            return new SingleBatchCursor(List.of());
        }

        if (reply.hasCursor()) {
            return new SingleMongoConnectionCursor(this, batchSize, true, reply).setServer(connectedTo);
        }

        else if (reply.getFirstDoc().containsKey("results")) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> results = (List<Map<String, Object>>) reply.getFirstDoc().get("results");
            return new SingleBatchCursor(results);
        }

        return new SingleElementCursor(reply.getFirstDoc());
    }

    private void checkForError(OpMsg msg) throws MorphiumDriverException {
        if (msg == null || msg.getFirstDoc() == null) {
            return;
        }

        if (msg.getFirstDoc().containsKey("ok") && !msg.getFirstDoc().get("ok").equals(1.0)) {
            Object codeObj = msg.getFirstDoc().get("code");
            int code = codeObj instanceof Number ? ((Number) codeObj).intValue() : -1;
            String errmsg = "Error: " + codeObj + " - " + msg.getFirstDoc().get("errmsg");

            // Error 251 (NoSuchTransaction): the server-side session on this connection
            // has an aborted transaction (e.g. from a previous write conflict or timeout).
            // Close the TCP connection to force server-side session cleanup, and throw a
            // retriable network exception so callers can retry on a fresh connection.
            if (code == 251) {
                log.warn("Transient transaction error on connection (code 251) — closing connection: {}", errmsg);
                close();
                var ex = new MorphiumDriverNetworkException(errmsg);
                ex.setMongoCode(code);
                throw ex;
            }

            var ex = new MorphiumDriverException(errmsg);
            ex.setMongoCode(codeObj);
            throw ex;
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

        @SuppressWarnings("unchecked")
        Map<String, Object> cursor = (Map<String, Object>) msg.getFirstDoc().get("cursor");
        Map<String, Object> ret = null;

        if (cursor.containsKey("firstBatch")) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object >> lst = (List<Map<String, Object >> ) cursor.get("firstBatch");

            if (lst != null && !lst.isEmpty()) {
                ret = lst.get(0);
            }
        } else {
            @SuppressWarnings("unchecked")
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
