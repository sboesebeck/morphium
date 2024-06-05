package de.caluga.morphium.server;

import static java.lang.Thread.sleep;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.bson.MongoTimestamp;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.HelloResult;
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

    public MorphiumServer(int port, String host, int maxThreads, int minThreads) {
        this.drv = new InMemoryDriver();
        this.port = port;
        this.host = host;
        drv.connect();
        // BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>() {
        //     @SuppressWarnings("CommentedOutCode")
        //     @Override
        //     public boolean offer(Runnable e) {
        //         /*
        //          * Offer it to the queue if there is 0 items already queued, else
        //          * return false so the TPE will add another thread. If we return false
        //          * and max threads have been reached then the RejectedExecutionHandler
        //          * will be called which will do the put into the queue.
        //          */
        //         int poolSize = executor.getPoolSize();
        //         int maximumPoolSize = executor.getMaximumPoolSize();
        //
        //         if (poolSize >= maximumPoolSize || poolSize > executor.getActiveCount()) {
        //             return super.offer(e);
        //         } else {
        //             return false;
        //         }
        //     }
        // };
        // BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        // executor = new ThreadPoolExecutor(
        //     minThreads,
        //     maxThreads,
        //     10000,
        //     TimeUnit.MILLISECONDS,
        //     new LinkedBlockingQueue<>());
        // executor.setRejectedExecutionHandler((r, executor) -> {
        //     try {
        //         /*
        //          * This does the actual put into the queue. Once the max threads
        //          * have been reached, the tasks will then queue up.
        //          */
        //         executor.getQueue().put(r);
        //     } catch (InterruptedException e) {
        //         Thread.currentThread().interrupt();
        //     }
        // });
        // // // noinspection unused,unused
        // executor.setThreadFactory(new ThreadFactory() {
        //     private final AtomicInteger num = new AtomicInteger(1);
        //     @Override
        //     public Thread newThread(Runnable r) {
        //         Thread ret = new Thread(r, "server_" + num);
        //         num.set(num.get() + 1);
        //         ret.setDaemon(true);
        //         return ret;
        //     }
        // });
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

                default:
                    log.error("unknown parameter " + args[idx]);
                    System.exit(1);
            }
        }

        log.info("Starting server...");
        var srv = new MorphiumServer(port, host, maxThreads, minThreads);
        srv.start();

        while (srv.running) {
            log.info("Alive and kickin'");
            sleep(10000);
        }
    }

    private HelloResult getHelloResult() {
        HelloResult res = new HelloResult();
        res.setHelloOk(true);
        res.setLocalTime(new Date());
        res.setOk(1.0);
        res.setHosts(Arrays.asList(host + ":" + port));
        res.setConnectionId(1);
        res.setMaxWireVersion(17);
        res.setMinWireVersion(13);
        res.setMaxMessageSizeBytes(100000);
        res.setMaxBsonObjectSize(10000);
        res.setWritablePrimary(true);
        res.setMe(host + ":" + port);
        // res.setMsg("ok");
        res.setMsg("MorphiumServer V0.1");
        return res;
    }

    public void start() throws IOException, InterruptedException {
        log.info("Opening port " + port);
        serverSocket = new ServerSocket(port);
        drv.setHostSeed(host + ":" + port);
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
                // log.info("Thread {} waiting for incoming message", Thread.currentThread().getId());
                var msg = WireProtocolMessage.parseFromStream(in);
                // log.info("---> Thread {} got message", Thread.currentThread().getId());

                //probably closed
                if (msg == null) break;

                // log.info("got incoming msg: " + msg.getClass().getSimpleName());
                Map<String, Object> doc = null;

                if (msg instanceof OpQuery) {
                    var q = (OpQuery) msg;
                    id = q.getMessageId();
                    doc = q.getDoc();

                    if (doc.containsKey("ismaster") || doc.containsKey("isMaster")) {
                        // ismaster
                        // log.info("OpMsg->isMaster");
                        var r = new OpReply();
                        r.setFlags(2);
                        r.setMessageId(msgId.incrementAndGet());
                        r.setResponseTo(id);
                        r.setNumReturned(1);
                        var res = getHelloResult();
                        OpMsg reply = new OpMsg();
                        reply.setFirstDoc(res.toMsg());
                        //
                        // reply.setMessageId(msgId.incrementAndGet());
                        // reply.setResponseTo(id);
                        // out.write(reply.bytes());
                        r.setDocuments(Arrays.asList(res.toMsg()));
                        out.write(r.bytes());
                        out.flush();
                        // log.info("Sent hello result");
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
                    log.info("Sent out error because OPQuery not allowed anymore!");
                    log.info(Utils.toJsonString(doc));
                    // Thread.sleep(1000);
                    continue;
                } else if (msg instanceof OpMsg) {
                    var m = (OpMsg) msg;
                    doc = ((OpMsg) msg).getFirstDoc();
                    // log.info("Message flags: " + m.getFlags());
                    id = m.getMessageId();
                }

                // log.info("Incoming " + Utils.toJsonString(doc));
                String cmd = doc.keySet().stream().findFirst().get();
                // log.info("Handling command " + cmd);
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
                        answer = getHelloResult().toMsg();
                        reply.setFirstDoc(answer);
                        break;

                    case "getFreeMonitoringStatus":
                        answer = Doc.of("state", "disabled", "message", "", "url", "", "userReminder", "");
                        break;

                    case "ping":
                        answer = Doc.of();
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

                                            out.write(reply.bytes());
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
                            answer = Doc.of("ok", 0, "errmsg", "no such command: '" + cmd + "'");
                            log.warn("errror running command " + cmd, e);
                        }

                        break;
                }

                answer.put("$clusterTime", Doc.of("clusterTime", new MongoTimestamp(System.currentTimeMillis())));
                answer.put("operationTime", new MongoTimestamp(System.currentTimeMillis()));
                reply.setFirstDoc(answer);
                out.write(reply.bytes());
                out.flush();
                // log.info("Sent answer!");
            }

            s.close();
            in.close();
            out.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
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
    }
}
