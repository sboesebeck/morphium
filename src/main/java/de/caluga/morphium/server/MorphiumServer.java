package de.caluga.morphium.server;

import static java.lang.Thread.sleep;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.Utils;
import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.bson.MongoTimestamp;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.HelloResult;
import de.caluga.morphium.driver.wire.SingleMongoConnectDriver;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.OpQuery;
import de.caluga.morphium.driver.wireprotocol.OpReply;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

public class MorphiumServer {

    private static Logger log = LoggerFactory.getLogger(MorphiumServer.class);
    private InMemoryDriver drv;
    private int port;
    private String host;
    private AtomicInteger msgId = new AtomicInteger(1000);
    private AtomicInteger cursorId = new AtomicInteger(1000);

    private ThreadPoolExecutor executor;
    private boolean running = true;
    private ServerSocket serverSocket;
    private static int compressorId = OpCompressed.COMPRESSOR_NOOP;
    private String rsName = "";
    private String hostSeed = "";
    private List<String> hosts = new ArrayList<>();
    private Map<String, Integer> hostPriorities = new java.util.concurrent.ConcurrentHashMap<>();
    private boolean primary = true;
    private String primaryHost;
    private int priority = 0;
    private final List<ChangeStreamMonitor> replicationMonitors = new ArrayList<>();
    private volatile boolean replicationStarted = false;

    public MorphiumServer(int port, String host, int maxThreads, int minThreads) {
        this.drv = new InMemoryDriver();
        this.port = port;
        this.host = host;
        drv.connect();
        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        executor.setMaximumPoolSize(maxThreads);
        executor.setCorePoolSize(minThreads);
    }

    public MorphiumServer() {
        this(17017, "localhost", 100, 10);
    }

    public static void main(String[] args) throws Exception {
        int idx = 0;
        log.info("Starting up server... parsing commandline params");
        String host = "localhost";
        int port = 17017;
        int maxThreads = 1000;
        int minThreads = 10;
        String rsNameArg = "";
        String hostSeedArg = "";
        List<String> hostsArg = new ArrayList<>();
        Map<String, Integer> hostPrioritiesArg = new java.util.concurrent.ConcurrentHashMap<>();

        while (idx < args.length) {
            switch (args[idx]) {
                case "-p":
                case "--port":
                    port = Integer.parseInt(args[idx + 1]);
                    idx += 2;
                    break;

                case "-mt":
                case "--maxThreads":
                    maxThreads = Integer.parseInt(args[idx + 1]);
                    idx += 2;
                    break;

                case "-mint":
                case "--minThreads":
                    minThreads = Integer.parseInt(args[idx + 1]);
                    idx += 2;
                    break;

                case "-h":
                case "--host":
                    host = args[idx + 1];
                    idx += 2;
                    break;

                case "-rs":
                case "--replicaset":
                    rsNameArg = args[idx + 1];
                    hostSeedArg = args[idx + 2];
                    idx += 3;
                    hostsArg = new ArrayList<>();
                    hostPrioritiesArg = new java.util.concurrent.ConcurrentHashMap<>();
                    int basePrio = 1000;

                    for (int i = 0; i < hostSeedArg.split(",").length; i++) {
                        var s = hostSeedArg.split(",")[i].trim();
                        int rsport = 27017;
                        String hst = s;
                        int prio = basePrio - i;

                        if (s.contains("|")) {
                            var parts = s.split("\\|");
                            hst = parts[0];

                            try {
                                prio = Integer.parseInt(parts[1]);
                            } catch (NumberFormatException e) {
                                log.warn("Invalid priority {}, using default {}", parts[1], prio);
                            }
                        }

                        if (hst.contains(":")) {
                            rsport = Integer.parseInt(hst.split(":")[1]);
                            hst = hst.split(":")[0];
                        }

                        String entry = hst + ":" + rsport;
                        hostsArg.add(entry);
                        hostPrioritiesArg.put(entry, prio);
                    }

                    break;

                case "-c":
                case "--compressor":
                    if (args[idx + 1].equals("snappy")) {
                        compressorId = OpCompressed.COMPRESSOR_SNAPPY;
                    } else if (args[idx + 1].equals("zstd")) {
                        compressorId = OpCompressed.COMPRESSOR_ZSTD;
                    } else if (args[idx + 1].equals("none")) {
                        compressorId = OpCompressed.COMPRESSOR_NOOP;
                    } else if (args[idx + 1].equals("zlib")) {
                        compressorId = OpCompressed.COMPRESSOR_ZLIB;
                    } else {
                        log.error("Unknown parameter for compressor {}", args[idx + 1]);
                        System.exit(1);
                    }

                    idx += 2;
                    break;

                default:
                    log.error("unknown parameter " + args[idx]);
                    System.exit(1);
            }
        }

        log.info("Starting server...");
        var srv = new MorphiumServer(port, host, maxThreads, minThreads);
        srv.configureReplicaSet(rsNameArg, hostsArg, hostPrioritiesArg);
        srv.start();

        if (!rsNameArg.isEmpty()) {
            log.info("Building replicaset with seed {}", hostSeedArg);
            srv.startReplicaReplication();
        }

        while (srv.running) {
            log.info("Alive and kickin'");
            sleep(10000);
        }
    }

    private InMemoryDriver getDriver() {
        return drv;
    }

    private HelloResult getHelloResult() {
        HelloResult res = new HelloResult();
        res.setHelloOk(true);
        res.setLocalTime(new Date());
        res.setOk(1.0);

        if (hosts == null || hosts.isEmpty()) {
            res.setHosts(Arrays.asList(host + ":" + port));
        } else {
            res.setHosts(hosts);
        }

        res.setConnectionId(1);
        res.setMaxWireVersion(17);
        res.setMinWireVersion(13);
        res.setMaxMessageSizeBytes(100000);
        res.setMaxBsonObjectSize(10000);
        res.setWritablePrimary(primary);
        res.setSecondary(!primary);
        res.setSetName(rsName);
        res.setPrimary(primary ? host + ":" + port : primaryHost);
        res.setMe(host + ":" + port);
        res.setLogicalSessionTimeoutMinutes(30);
        // res.setMsg("ok");
        res.setMsg("MorphiumServer V0.1ALPHA");
        return res;
    }

    public int getConnectionCount() {
        return executor.getActiveCount();
    }

    public void start() throws IOException, InterruptedException {
        log.info("Opening port " + port);
        serverSocket = new ServerSocket(port);
        drv.setHostSeed(host + ":" + port);
        drv.setReplicaSet(rsName != null && !rsName.isEmpty());
        drv.setReplicaSetName(rsName == null ? "" : rsName);
        executor.prestartAllCoreThreads();
        log.info("Port opened, waiting for incoming connections");
        new Thread(()-> {
            while (running) {
                Socket s = null;

                try {
                    s = serverSocket.accept();
                } catch (IOException e) {
                    if (e.getMessage().contains("Socket closed")) {
                        log.info("Server socket closed");
                        break;
                    }

                    log.error("Serversocket error", e);
                    terminate();
                    break;
                }

                log.info("Incoming connection: " + executor.getPoolSize());
                Socket finalS = s;
                //new Thread(() -> incoming(finalS)).start();
                executor.execute(() -> incoming(finalS));
            }

        }).start();

        if (!primary && rsName != null && !rsName.isEmpty()) {
            startReplicaReplication();
        }
    }

    public void incoming(Socket s) {
        log.info("handling incoming connection...{}", executor.getPoolSize());

        try {
            s.setSoTimeout(0);
            s.setTcpNoDelay(true);
            s.setKeepAlive(true);
            var in = s.getInputStream();
            var out = s.getOutputStream();
            int id = 0;
            //            OpMsg r = new OpMsg();
            //            r.setFlags(2);
            //            r.setMessageId(msgId.incrementAndGet());
            //            r.setResponseTo(id);
            var answer = getHelloResult().toMsg();

            //            r.setFirstDoc(answer);
            //            log.info("flush...");
            //            out.write(r.bytes());
            //            out.flush();
            //            log.info("Sent hello result");
            while (s.isConnected()) {
                log.debug("Thread {} waiting for incoming message", Thread.currentThread().getId());
                var msg = WireProtocolMessage.parseFromStream(in);
                log.debug("---> Thread {} got message", Thread.currentThread().getId());

                //probably closed
                if (msg == null) {
                    log.info("Null");
                    break;
                }

                // log.debug("got incoming msg: " + msg.getClass().getSimpleName());
                Map<String, Object> doc = null;

                if (msg instanceof OpQuery) {
                    var q = (OpQuery) msg;
                    id = q.getMessageId();
                    doc = q.getDoc();

                    if (doc.containsKey("ismaster") || doc.containsKey("isMaster")) {
                        // ismaster via OpQuery (legacy)
                        log.debug("OpQuery->isMaster");
                        var r = new OpReply();
                        r.setFlags(2);
                        r.setMessageId(msgId.incrementAndGet());
                        r.setResponseTo(id);
                        r.setNumReturned(1);
                        var res = getHelloResult();
                        log.debug("Sending isMaster response via OpReply: {}", res.toMsg());
                        r.setDocuments(Arrays.asList(res.toMsg()));

                        if (compressorId != OpCompressed.COMPRESSOR_NOOP) {
                            OpCompressed cmp = new OpCompressed();
                            cmp.setMessageId(r.getMessageId());
                            cmp.setResponseTo(id);
                            cmp.setOriginalOpCode(r.getOpCode());
                            cmp.setCompressorId(compressorId);
                            byte[] originalPayload = r.getPayload();
                            cmp.setUncompressedSize(originalPayload.length);
                            cmp.setCompressedMessage(originalPayload);
                            log.debug("Sending compressed OpReply: {} bytes (uncompressed: {} bytes)", cmp.bytes().length, originalPayload.length);
                            out.write(cmp.bytes());
                        } else {
                            log.debug("Sending OpReply: {} bytes", r.bytes().length);
                            out.write(r.bytes());
                        }

                        out.flush();
                        log.debug("Sent isMaster result via OpQuery");
                        continue;
                    }

                    var r = new OpReply();
                    Doc d = Doc.of("$err", "OP_QUERY is no longer supported. The client driver may require an upgrade.", "code", 5739101, "ok", 0.0);
                    r.setFlags(2);
                    r.setMessageId(msgId.incrementAndGet());
                    r.setResponseTo(id);
                    r.setNumReturned(1);
                    r.setDocuments(Arrays.asList(d));
                    out.write(r.bytes());
                    out.flush();
                    log.debug("Sent out error because OPQuery not allowed anymore!");
                    log.debug(Utils.toJsonString(doc));
                    // Thread.sleep(1000);
                    continue;
                } else if (msg instanceof OpMsg) {
                    var m = (OpMsg) msg;
                    doc = ((OpMsg) msg).getFirstDoc();
                    log.debug("Message flags: " + m.getFlags());
                    id = m.getMessageId();
                }

                log.debug("Incoming " + Utils.toJsonString(doc));
                String cmd = doc.keySet().stream().findFirst().get();
                log.debug("Handling command " + cmd);
                OpMsg reply = new OpMsg();
                reply.setResponseTo(msg.getMessageId());
                reply.setMessageId(msgId.incrementAndGet());

                switch (cmd) {
                    case "getCmdLineOpts":
                        answer = Doc.of("argv", List.of(), "parsed", Map.of());
                        break;

                    case "buildInfo":
                        answer = Doc.of("version", "5.0.0-ALPHA", "buildEnvironment", Doc.of("distarch", "java", "targetarch", "java"));
                        answer.put("ok", 1.0);
                        reply.setFirstDoc(answer);
                        break;

                    case "ismaster":
                    case "isMaster":
                    case "hello":
                        log.debug("OpMsg->hello/ismaster");
                        answer = getHelloResult().toMsg();
                        log.debug("Hello response: {}", answer);
                        break;

                    case "getFreeMonitoringStatus":
                        answer = Doc.of("state", "disabled", "message", "", "url", "", "userReminder", "", "ok", 1.0);
                        break;

                    case "ping":
                        answer = Doc.of("ok", 1.0);
                        break;

                    case "endSessions":
                        // Session management - just acknowledge since we don't track sessions
                        answer = Doc.of("ok", 1.0);
                        break;

                    case "startSession":
                        // Session management - return a dummy session
                        answer = Doc.of("id", Map.of("id", java.util.UUID.randomUUID()), "timeoutMinutes", 30, "ok", 1.0);
                        break;

                    case "refreshSessions":
                        // Session refresh - just acknowledge
                        answer = Doc.of("ok", 1.0);
                        break;

                    case "abortTransaction":
                    case "commitTransaction":
                        // Transaction commands - just acknowledge since we don't support transactions
                        answer = Doc.of("ok", 1.0);
                        break;

                    case "replSetStepDown":
                        stepDown();
                        answer = Doc.of("ok", 1.0, "primary", primaryHost);
                        break;

                    case "getLog":
                        if (doc.get(cmd).equals("startupWarnings")) {
                            answer = Doc.of("totalLinesWritten", 0, "log", List.of(), "ok", 1.0);
                            break;
                        } else {
                            log.warn("Unknown log " + doc.get(cmd));
                            answer = Doc.of("ok", 0, "errmsg", "unknown logr");
                        }

                        break;

                    case "getParameter":
                        if (Integer.valueOf(1).equals(doc.get("featureCompatibilityVersion"))) {
                            answer = Doc.of("version", "5.0", "ok", 1.0);
                        } else {
                            answer = Doc.of("ok", 0, "errmsg", "no such parameter");
                        }

                        break;

                    default:
                        try {
                            if (!primary && isWriteCommand(cmd)) {
                                answer = Doc.of("ok", 0.0, "errmsg", "not primary", "code", 10107, "codeName", "NotWritablePrimary");
                                break;
                            }

                            AtomicInteger msgid = new AtomicInteger(0);

                            if (doc.containsKey("pipeline") && ((Map)((List)doc.get("pipeline")).get(0)).containsKey("$changeStream")) {
                                WatchCommand wcmd = new WatchCommand(drv).fromMap(doc);
                                final int myCursorId = cursorId.incrementAndGet();
                                wcmd.setCb(new DriverTailableIterationCallback() {
                                    private boolean first = true;
                                    private String batch = "firstBatch";
                                    @Override
                                    public void incomingData(Map<String, Object> data, long dur) {
                                        try {
                                            // log.info("Incoming data...");
                                            var crs =  Doc.of(batch, List.of(data), "ns", wcmd.getDb() + "." + wcmd.getColl(), "id", myCursorId);
                                            var answer = Doc.of("ok", 1.0);

                                            // log.info("Data: {}", data);

                                            if (crs != null) answer.put("cursor", crs);

                                            answer.put("$clusterTime", Doc.of("clusterTime", new MongoTimestamp(System.currentTimeMillis())));
                                            answer.put("operationTime", new MongoTimestamp(System.currentTimeMillis()));
                                            reply.setFirstDoc(answer);

                                            if (first) {
                                                first = false;
                                                batch = "nextBatch";
                                            }

                                            if (compressorId != OpCompressed.COMPRESSOR_NOOP) {
                                                OpCompressed cmp = new OpCompressed();
                                                cmp.setMessageId(reply.getMessageId());
                                                cmp.setResponseTo(reply.getResponseTo());
                                                cmp.setCompressedMessage(reply.bytes());
                                                out.write(cmp.bytes());
                                            } else {
                                                out.write(reply.bytes());
                                            }

                                            out.flush();
                                        } catch (Exception e) {
                                            log.error("Errror during watch", e);
                                        }
                                    }

                                    @Override
                                    public boolean isContinued() {
                                        return true;
                                    }
                                });

                                int mid = drv.runCommand(wcmd);
                                msgid.set(mid);
                            } else {
                                msgid.set(drv.runCommand(new GenericCommand(drv).fromMap(doc)));
                            }

                            var crs = drv.readSingleAnswer(msgid.get());
                            answer = Doc.of("ok", 1.0);

                            if (crs != null) answer.putAll(crs);
                        } catch (Exception e) {
                            answer = Doc.of("ok", 0, "errmsg", "no such command: '{}" + cmd + "'");
                            log.error("No such command {}", cmd, e);
                            // log.warn("errror running command " + cmd, e);
                        }

                        break;
                }

                answer.put("$clusterTime", Doc.of("clusterTime", new MongoTimestamp(System.currentTimeMillis())));
                answer.put("operationTime", new MongoTimestamp(System.currentTimeMillis()));
                reply.setFirstDoc(answer);
                log.debug("Final response being sent: {}", answer);

                if (compressorId != OpCompressed.COMPRESSOR_NOOP) {
                    OpCompressed cmsg = new OpCompressed();
                    cmsg.setMessageId(reply.getMessageId());
                    cmsg.setResponseTo(reply.getResponseTo());
                    cmsg.setCompressorId(compressorId);
                    cmsg.setOriginalOpCode(reply.getOpCode());
                    byte[] originalPayload = reply.getPayload();
                    cmsg.setUncompressedSize(originalPayload.length);
                    cmsg.setCompressedMessage(originalPayload);
                    var b = cmsg.bytes();
                    log.debug("Server sending {} bytes (compressed), uncompressed: {} bytes, responseTo: {}", b.length, originalPayload.length, cmsg.getResponseTo());
                    out.write(b);
                } else {
                    var b = reply.bytes();
                    log.debug("Server sending {} bytes, responseTo: {}", b.length, reply.getResponseTo());
                    out.write(b);
                }

                out.flush();
                log.debug("Sent answer for cmd: {}", cmd);
            }

            s.close();
            in.close();
            out.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
            // e.printStackTrace();
            // System.exit(0);
        }

        log.info("Thread finished!");
        s = null;
    }

    public void terminate() {
        running = false;

        if (serverSocket != null) {
            try {
                serverSocket.close();
                serverSocket = null;
            } catch (IOException e) {
                //swallow
            }
        }

        executor.shutdownNow();
        executor = null;
        replicationMonitors.forEach(ChangeStreamMonitor::terminate);
    }

    public void stepDown() {
        if (!primary) {
            return;
        }

        log.info("Stepping down {}", host + ":" + port);
        primary = false;
        hostPriorities.put(host + ":" + port, Integer.MIN_VALUE);
        primaryHost = findPrimaryHost(host + ":" + port);
        replicationStarted = false;
        startReplicaReplication();
    }

    private String findPrimaryHost(String exclude) {
        String best = null;
        int bestPrio = Integer.MIN_VALUE;

        for (var e : hostPriorities.entrySet()) {
            if (exclude != null && exclude.equals(e.getKey())) {
                continue;
            }

            if (e.getValue() != null && e.getValue() > bestPrio) {
                bestPrio = e.getValue();
                best = e.getKey();
            }
        }

        if (best == null && hosts != null && !hosts.isEmpty()) {
            best = hosts.get(0);
        }

        return best == null ? host + ":" + port : best;
    }

    public boolean isPrimary() {
        return primary;
    }

    public String getPrimaryHost() {
        return primaryHost;
    }

    public void configureReplicaSet(String name, List<String> hostList, Map<String, Integer> priorities) {
        rsName = name == null ? "" : name;
        hosts = hostList == null ? new ArrayList<>() : hostList;

        if (priorities != null) {
            hostPriorities.clear();
            hostPriorities.putAll(priorities);
        }

        primaryHost = findPrimaryHost(null);
        priority = hostPriorities.getOrDefault(host + ":" + port, 0);
        primary = rsName.isEmpty() || (host + ":" + port).equals(primaryHost);
        drv.setReplicaSet(!rsName.isEmpty());
        drv.setReplicaSetName(rsName);
        drv.setHostSeed(hosts);
    }

    private boolean isWriteCommand(String cmd) {
        return List.of("insert", "update", "delete", "findandmodify", "findAndModify", "createIndexes", "create")
                   .contains(cmd);
    }

    private void startReplicaReplication() {
        if (replicationStarted || primary || hosts == null || hosts.isEmpty()) {
            return;
        }

        replicationStarted = true;
        new Thread(() -> {
            while (running) {
                try {
                    String target = primaryHost != null ? primaryHost : hosts.get(0);
                    log.info("Starting replication from {}", target);
                    MorphiumConfig cfg = new MorphiumConfig("admin", 10, 10000, 1000);
                    cfg.setDriverName(SingleMongoConnectDriver.driverName);
                    cfg.setHostSeed(target);
            Morphium morphium = new Morphium(cfg);

            for (String db : morphium.listDatabases()) {
                MorphiumConfig cfg2 = new MorphiumConfig(db, 10, 10000, 1000);
                cfg2.setDriverName(SingleMongoConnectDriver.driverName);
                        cfg2.setHostSeed(target);
                        Morphium morphium2 = new Morphium(cfg2);
                        ChangeStreamMonitor mtr = new ChangeStreamMonitor(morphium2, null, true);
                        mtr.addListener((evt)-> {
                            try {
                                if (evt.getOperationType().equals("insert")) {
                                    drv.insert(db, evt.getCollectionName(), List.of(evt.getFullDocument()), null);
                                } else if (evt.getOperationType().equals("delete")) {
                                    drv.delete(db, evt.getCollectionName(), Map.of("_id", evt.getDocumentKey()), null, false, null, null);
                                } else if (evt.getOperationType().equals("update")) {
                                    Map<String, Object> updated = evt.getUpdatedFields();
                                    drv.update(db, evt.getCollectionName(), Map.of("_id", evt.getDocumentKey()), null, Doc.of("$set", updated), false, false, null, null);
                                }
                            } catch (MorphiumDriverException e) {
                                log.error("Exception", e);
                            }

                            return true;
                        });
                        mtr.start();
                        replicationMonitors.add(mtr);
                    }

                    morphium.close();
                    break;
                } catch (Exception e) {
                    log.error("Could not setup replication, retrying", e);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }).start();
    }
}
