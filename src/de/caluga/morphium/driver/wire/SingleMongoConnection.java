package de.caluga.morphium.driver.wire;

import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.HelloCommand;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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


    @Override
    public HelloResult connect(String host, int port) throws MorphiumDriverException {
        try {
            log.info("Connecting to " + host + ":" + port);
            s = new Socket(host, port);
            out = s.getOutputStream();
            in = s.getInputStream();
        } catch (IOException e) {
            throw new MorphiumDriverException("Connection failed", e);
        }
        HelloCommand cmd = new HelloCommand(null);
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(cmd.asMap());
        var result = sendAndWaitForReply(msg);
        Map<String, Object> firstDoc = result.getFirstDoc();
        var hello = HelloResult.fromMsg(firstDoc);
        connectedTo = host;
        connectedToPort = port;

        startReaderThread();
        connected = true;
        return hello;
    }

    @Override
    public String getConnectedTo() {
        return connectedTo;
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
        return incoming.remove(msgid);
    }


    @Override
    public void sendQuery(OpMsg q) throws MorphiumDriverException {

    }

    @Override
    public OpMsg sendAndWaitForReply(OpMsg q) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
        return null;
    }

    @Override
    public List<Map<String, Object>> readAnswerFor(int queryId) throws MorphiumDriverException {
        return null;
    }

    @Override
    public MorphiumCursor getAnswerFor(int queryId) throws MorphiumDriverException {
        return null;
    }

    @Override
    public List<Map<String, Object>> readAnswerFor(MorphiumCursor crs) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> getSingleDocAndKillCursor(OpMsg msg) throws MorphiumDriverException {
        return null;
    }

    @Override
    public List<Map<String, Object>> readBatches(int waitingfor, int batchSize) throws MorphiumDriverException {
        return null;
    }
}
