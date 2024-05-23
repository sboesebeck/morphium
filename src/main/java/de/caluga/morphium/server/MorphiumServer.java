package de.caluga.morphium.server;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.bson.MongoTimestamp;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.HelloResult;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.OpQuery;
import de.caluga.morphium.driver.wireprotocol.OpReply;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MorphiumServer {

    private static Logger log = LoggerFactory.getLogger(MorphiumServer.class);
    private InMemoryDriver drv;
    private int port;
    private String host;
    private AtomicInteger msgId = new AtomicInteger(1000);

    private ThreadPoolExecutor executor;

    public MorphiumServer(int port, String host, int maxThreads, int minThreads) {
        this.drv = new InMemoryDriver();
        this.port = port;
        this.host = host;
        drv.connect();
        executor =
            new ThreadPoolExecutor(
            minThreads,
            maxThreads,
            10000,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(maxThreads));
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
        res.setMsg("ok");
        return res;
    }

    public void start() throws IOException, InterruptedException {
        log.info("Opening port " + port);
        ServerSocket ssoc = new ServerSocket(port);
        drv.setHostSeed(host + ":" + port);
        executor.prestartAllCoreThreads();
        log.info("Port opened, waiting for incoming connections");

        while (true) {
            var s = ssoc.accept();
            log.info("Incoming connection");
            executor.execute(() -> incoming(s));
        }
    }

    public void incoming(Socket s) {
        try {
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
            while (true) {
                var msg = WireProtocolMessage.parseFromStream(in);
                if (msg==null) continue;
                log.info("got incoming msg: " + msg.getClass().getSimpleName());
                Map<String, Object> doc = null;

//                if (msg instanceof OpQuery) {
//                    var q = (OpQuery) msg;
//                    id = q.getMessageId();
//                    doc = q.getDoc();
//
//                    if (doc.containsKey("ismaster") || doc.containsKey("isMaster")) {
//                        // ismaster
//                        log.info("OpMsg->isMaster");
//                         r = new OpReply();
//                        r.setFlags(2);
//                        r.setMessageId(msgId.incrementAndGet());
//                        r.setResponseTo(id);
//                        r.setNumReturned(1);
//                         res = getHelloResult();
//                        //                                OpMsg reply=new OpMsg();
//                        //                                reply.setFirstDoc(res.toMsg());
//                        //
//                        // reply.setMessageId(msgId.incrementAndGet());
//                        //                                reply.setResponseTo(id);
//                        //                                out.write(reply.bytes());
//                        r.setDocuments(Arrays.asList(res.toMsg()));
//                        out.write(r.bytes());
//                        out.flush();
//                        log.info("Sent hello result");
//                        continue;
//                    }

//                     r = new OpReply();
//                    Doc d = Doc.of("$err", "OP_QUERY is no longer supported. The client driver may require an upgrade.", "code", 5739101, "ok", 0.0);
//                    r.setFlags(2);
//                    r.setMessageId(msgId.incrementAndGet());
//                    r.setResponseTo(id);
//                    r.setNumReturned(1);
//                    r.setDocuments(Arrays.asList(d));
//                    out.write(r.bytes());
//                    out.flush();
//                    log.info("Sent out error because OPQuery not allowed anymore!");
//                    log.info(Utils.toJsonString(doc));
                    // Thread.sleep(1000);
//                    continue;
//                } else if (msg instanceof OpMsg) {
                    var m = (OpMsg) msg;
                    doc = ((OpMsg) msg).getFirstDoc();
                    log.info("Message flags: " + m.getFlags());
                    id = m.getMessageId();
//                }

                log.info("Incoming " + Utils.toJsonString(doc));
                String cmd = doc.keySet().stream().findFirst().get();
                log.info("Handling command " + cmd);
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
                            int msgid = drv.runCommand(new GenericCommand(drv).fromMap(doc));
                            var crs = drv.readSingleAnswer(msgid);
                            answer = Doc.of("ok", 1.0);
                            answer.putAll(crs);
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
                log.info("Sent answer!");
            }

            //            log.info("Thread finished!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
