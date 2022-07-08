package de.caluga.morphium.driver.wire;

import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wireprotocol.OpMsg;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface MongoConnection {
    HelloResult connect(String host, int port) throws IOException, MorphiumDriverException;

    void disconnect();

    boolean isConnected();

    String getConnectedTo();

    //Command handling
    boolean replyAvailableFor(int msgId);

    OpMsg getReplyFor(int msgid, long timeout) throws MorphiumDriverException;

    void sendQuery(OpMsg q) throws MorphiumDriverException;

    OpMsg sendAndWaitForReply(OpMsg q) throws MorphiumDriverException;

    Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException;

    List<Map<String, Object>> readAnswerFor(int queryId) throws MorphiumDriverException;

    MorphiumCursor getAnswerFor(int queryId) throws MorphiumDriverException;

    List<Map<String, Object>> readAnswerFor(MorphiumCursor crs) throws MorphiumDriverException;

    Map<String, Object> getSingleDocAndKillCursor(OpMsg msg) throws MorphiumDriverException;

    List<Map<String, Object>> readBatches(int waitingfor, int batchSize) throws MorphiumDriverException;


}
