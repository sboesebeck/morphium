package de.caluga.morphium.driver.wire;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.MongoCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.wireprotocol.OpMsg;

public interface MongoConnection extends Closeable {
    void setCredentials(String authDb, String userName, String password);

    HelloResult connect(MorphiumDriver drv, String host, int port) throws IOException, MorphiumDriverException;

    MorphiumDriver getDriver();

    int getSourcePort();

    void close();

    // void release();

    boolean isConnected();

    /**
     * True while a sent request's reply has not been read from this connection yet. Such
     * a connection must not be reused/pooled - the next user would read its predecessor's
     * answer (wire desync, "out of sync: expected reply to X, got Y"). Default false for
     * implementations without request/reply bookkeeping (e.g. the in-memory driver, which
     * answers synchronously).
     */
    default boolean hasPendingReplies() {
        return false;
    }

    String getConnectedTo();

    String getConnectedToHost();

    int getConnectedToPort();

    void closeIteration(MorphiumCursor crs) throws MorphiumDriverException;

    Map<String, Object> killCursors(String db, String coll, long... ids) throws MorphiumDriverException;

    //Command handling
    // boolean replyAvailableFor(int msgId);

    // OpMsg getReplyFor(int msgid, long timeout) throws MorphiumDriverException;
    OpMsg readNextMessage(int timeout) throws MorphiumDriverException;

    //    void sendQuery(OpMsg q) throws MorphiumDriverException;

    //    OpMsg sendAndWaitForReply(OpMsg q) throws MorphiumDriverException;

    Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException;

    void watch(WatchCommand settings) throws MorphiumDriverException;

    List<Map<String, Object>> readAnswerFor(int queryId) throws MorphiumDriverException;

    MorphiumCursor getAnswerFor(int queryId, int batchsize) throws MorphiumDriverException;

    List<Map<String, Object>> readAnswerFor(MorphiumCursor crs) throws MorphiumDriverException;

    //    Map<String, Object> getSingleDocAndKillCursor(OpMsg msg) throws MorphiumDriverException;

    //    List<Map<String, Object>> readBatches(int waitingfor, int batchSize) throws MorphiumDriverException;

    //    int sendCommand(Map<String, Object> cmd) throws MorphiumDriverException;
    int sendCommand(MongoCommand cmd) throws MorphiumDriverException;
}
