package de.caluga.morphium.server;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.wire.HelloResult;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.OpQuery;
import de.caluga.morphium.driver.wireprotocol.OpReply;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MorphiumServer {

    Logger log = LoggerFactory.getLogger(MorphiumServer.class);

    public void start(int port) throws IOException, InterruptedException {
        log.info("Opening port " + port);
        ServerSocket ssoc = new ServerSocket(port);
        while (true) {
            var s = ssoc.accept();
            log.info("Incoming connection");
            var in = s.getInputStream();
            var out = s.getOutputStream();
            AtomicInteger msgId = new AtomicInteger(1000);
            new Thread(() -> {
                try {
                    Thread.sleep(1000);
                    MAIN:
                    while (true) {
                        while (in.available() == 0) {
                            if (!s.isConnected() || s.isClosed()) {
                                break MAIN;
                            }
                            Thread.yield();
                        }
                        var msg = WireProtocolMessage.parseFromStream(in);
                        log.info("got incoming msg: " + msg.getClass().getSimpleName());
                        Map<String, Object> doc = null;
                        int id = 0;
                        if (msg instanceof OpQuery) {
                            var q = (OpQuery) msg;
                            id = q.getMessageId();
                            doc = q.getDoc();
                            OpReply r = new OpReply();
                            Doc d = Doc.of("$err", "OP_QUERY is no longer supported. The client driver may require an upgrade.", "code", 5739101, "ok", 0.0);
                            r.setFlags(2);
                            r.setMessageId(msgId.incrementAndGet());
                            r.setResponseTo(id);
                            r.setNumReturned(1);
                            r.setDocuments(Arrays.asList(d));
                            out.write(r.bytes());
                            out.flush();
                            log.info("Sent out error because OPQuery not allowed anymore!");
                            Thread.sleep(1000);
                            continue;
                        } else if (msg instanceof OpMsg) {
                            var m = (OpMsg) msg;
                            doc = ((OpMsg) msg).getFirstDoc();
                            id = m.getMessageId();
                        }
                        log.info("Incoming: " + Utils.toJsonString(doc));
                        HelloResult res = new HelloResult();
                        res.setWritablePrimary(true);
//                res.setArbiterOnly(false);
                        res.setConnectionId(1);
                        res.setOk(1.0);
//                res.setSecondary(false);
//                res.setSetName(null);
//                res.setHidden(false);
                        res.setMaxWireVersion(17);
                        res.setMinWireVersion(0);
//                res.setMe("localhost:17017");
                        res.setHelloOk(true);
                        res.setMaxBsonObjectSize(16777216);
                        res.setMaxMessageSizeBytes(480000000);
                        res.setLocalTime(new Date());
//                res.setSetVersion(1);
                        res.setReadOnly(false);

                        Map<String, Object> firstDoc = res.toMsg();
                        firstDoc.put("logicalSessionTimeoutMinutes", 30);
//                firstDoc.put("topologyVersion", Doc.of("processId",new MorphiumId(),"counter",0));

                        OpReply reply = new OpReply();
                        reply.setDocuments(Arrays.asList(firstDoc));
                        reply.setNumReturned(1);
                        reply.setMessageId(msgId.incrementAndGet());
                        reply.setResponseTo(id);
                        ;
//                            OpMsg outMsg = new OpMsg().setFirstDoc(firstDoc);
//                            log.info("Sending out reply for " + id + ": " + Utils.toJsonString(outMsg.getFirstDoc()));
//                            outMsg.setMessageId(msgId.incrementAndGet());
//                            outMsg.setResponseTo(id);
//                            outMsg.setFlags(OpMsg.EXHAUST_ALLOWED);
//
//                            out.write(outMsg.bytes());
                        out.write(reply.bytes());
                        out.flush();
                        Thread.sleep(100);
                        if (in.available() != 0) {
                            msg = OpMsg.parseFromStream(in);
                        } else {
                            log.info("Nothing incoming");
                        }
                        Thread.sleep(15000);

                    }
                    log.info("Thread finished!");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }).start();

        }
    }
}
