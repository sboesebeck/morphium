package de.caluga.morphium.driver;/**
 * Created by stephan on 15.10.15.
 */

import de.caluga.morphium.Morphium;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;

import java.io.Closeable;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;

/**
 * Morphium driver interface
 * <p>
 * All drivers need to implement this interface. you can add your own drivers to morphium. These are actually not
 * limited to be mongodb drivers. There is also an InMemory implementation.
 **/
@SuppressWarnings({"BooleanMethodIsAlwaysInverted", "RedundantThrows", "UnusedReturnValue"})
public interface MorphiumDriver extends Closeable {

    //     _______. _______ .___________.___________. __  .__   __.   _______      _______.
    //    /       ||   ____||           |           ||  | |  \ |  |  /  _____|    /       |
    //   |   (----`|  |__   `---|  |----`---|  |----`|  | |   \|  | |  |  __     |   (----`
    //    \   \    |   __|      |  |        |  |     |  | |  . `  | |  | |_ |     \   \
    //.----)   |   |  |____     |  |        |  |     |  | |  |\   | |  |__| | .----)   |
    //|_______/    |_______|    |__|        |__|     |__| |__| \__|  \______| |_______/
    String getName();
    int getCompression();
    MorphiumDriver setCompression(int type);
    int getIdleSleepTime();
    void setIdleSleepTime(int sl);
    @SuppressWarnings("unused")
    int getMaxBsonObjectSize();

    void setMaxBsonObjectSize(int maxBsonObjectSize);

    int getMaxMessageSize();

    void setMaxMessageSize(int maxMessageSize);

    int getMaxWriteBatchSize();

    void setMaxWriteBatchSize(int maxWriteBatchSize);

    @SuppressWarnings("unused")
    boolean isReplicaSet();

    void setReplicaSet(boolean replicaSet);

    /**
     * Returns true if the backend is an InMemory database (either InMemoryDriver directly
     * or connecting to a MorphiumServer with InMemory backend).
     * This is useful for tests that need to skip features not supported by the InMemory backend.
     */
    boolean isInMemoryBackend();

    boolean getDefaultJ();

    int getDefaultWriteTimeout();

    void setDefaultWriteTimeout(int wt);

    int getMaxWaitTime();
    int getServerSelectionTimeout();
    void setServerSelectionTimeout(int timeoutInMS);

    void setMaxWaitTime(int maxWaitTime);

    String[] getCredentials(String db);

    List<String> getHostSeed();

    void setHostSeed(List<String> hosts);

    void setHostSeed(String... host);

    void setConnectionUrl(String connectionUrl) throws MalformedURLException;

    void connect() throws MorphiumDriverException;

    void connect(String replicaSetName) throws MorphiumDriverException;

    boolean isConnected();

    boolean isReplicaset();

    /**
     * Check if connected to a MorphiumServer (in-memory MongoDB implementation).
     * This allows clients to use MorphiumServer-specific optimizations.
     */
    boolean isMorphiumServer();

    public <T, R> Aggregator<T, R> createAggregator(Morphium morphium, Class<? extends T> type, Class<? extends R> resultType);

    List<String> listDatabases() throws MorphiumDriverException;

    List<String> listCollections(String db, String pattern) throws MorphiumDriverException;

    String getReplicaSetName();

    void setReplicaSetName(String replicaSetName);

    @SuppressWarnings("unused")
    //    Map<String, String[]> getCredentials();

    //    void setCredentials(Map<String, String[]> credentials);

    //    void setCredentialsFor(String db, String user, String password);

    int getRetriesOnNetworkError();

    MorphiumDriver setRetriesOnNetworkError(int r);

    int getSleepBetweenErrorRetries();

    MorphiumDriver setSleepBetweenErrorRetries(int s);

    int getMaxConnections();

    MorphiumDriver setMaxConnections(int maxConnections);

    int getMinConnections();

    MorphiumDriver setMinConnections(int minConnections);

    boolean isRetryReads();

    MorphiumDriver setRetryReads(boolean retryReads);

    boolean isRetryWrites();

    MorphiumDriver setRetryWrites(boolean retryWrites);

    int getReadTimeout();

    void setReadTimeout(int readTimeout);

    int getMinConnectionsPerHost();

    void setMinConnectionsPerHost(int minConnectionsPerHost);

    int getMaxConnectionsPerHost();

    void setMaxConnectionsPerHost(int maxConnectionsPerHost);

    void setCredentials(String db, String login, String pwd);

    boolean isCapped(String db, String coll) throws MorphiumDriverException;


    Map<String, Integer> getNumConnectionsByHost();

    ////////////////////////////////////////////////////
    // .___________..______          ___      .__   __.      _______.     ___       ______ .___________. __    ______   .__   __.      _______.
    //|           ||   _  \        /   \     |  \ |  |     /       |    /   \     /      ||           ||  |  /  __  \  |  \ |  |     /       |
    //`---|  |----`|  |_)  |      /  ^  \    |   \|  |    |   (----`   /  ^  \   |  ,----'`---|  |----`|  | |  |  |  | |   \|  |    |   (----`
    //    |  |     |      /      /  /_\  \   |  . `  |     \   \      /  /_\  \  |  |         |  |     |  | |  |  |  | |  . `  |     \   \
    //    |  |     |  |\  \----./  _____  \  |  |\   | .----)   |    /  _____  \ |  `----.    |  |     |  | |  `--'  | |  |\   | .----)   |
    //    |__|     | _| `._____/__/     \__\ |__| \__| |_______/    /__/     \__\ \______|    |__|     |__|  \______/  |__| \__| |_______/


    MorphiumTransactionContext startTransaction(boolean autoCommit);

    boolean isTransactionInProgress();

    void commitTransaction() throws MorphiumDriverException;

    MorphiumTransactionContext getTransactionContext();

    void setTransactionContext(MorphiumTransactionContext ctx);

    void abortTransaction() throws MorphiumDriverException;


    //.______       __    __  .__   __.      ______   ______   .___  ___. .___  ___.      ___      .__   __.  _______
    //|   _  \     |  |  |  | |  \ |  |     /      | /  __  \  |   \/   | |   \/   |     /   \     |  \ |  | |       \
    //|  |_)  |    |  |  |  | |   \|  |    |  ,----'|  |  |  | |  \  /  | |  \  /  |    /  ^  \    |   \|  | |  .--.  |
    //|      /     |  |  |  | |  . `  |    |  |     |  |  |  | |  |\/|  | |  |\/|  |   /  /_\  \   |  . `  | |  |  |  |
    //|  |\  \----.|  `--'  | |  |\   |    |  `----.|  `--'  | |  |  |  | |  |  |  |  /  _____  \  |  |\   | |  '--'  |
    //| _| `._____| \______/  |__| \__|     \______| \______/  |__|  |__| |__|  |__| /__/     \__\ |__| \__| |_______/

    //    SingleElementResult runCommandSingleResult(SingleResultCommand cmd) throws MorphiumDriverException;
    //
    //    CursorResult runCommand(MultiResultCommand cmd) throws MorphiumDriverException;
    //
    //    ListResult runCommandList(MultiResultCommand cmd) throws MorphiumDriverException;

    //Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException;


    Map<String, Object> getReplsetStatus() throws MorphiumDriverException;

    Map<String, Object> getDBStats(String db) throws MorphiumDriverException;

    Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException;


    //CursorResult runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException;


    //RunCommandResult sendCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException;

    int getMaxConnectionLifetime();

    void setMaxConnectionLifetime(int timeout);

    int getMaxConnectionIdleTime();

    void setMaxConnectionIdleTime(int time);

    int getConnectionTimeout();

    void setConnectionTimeout(int timeout);

    int getDefaultW();

    void setDefaultW(int w);

    int getHeartbeatFrequency();

    void setHeartbeatFrequency(int heartbeatFrequency);

    ReadPreference getDefaultReadPreference();

    void setDefaultReadPreference(ReadPreference rp);

    int getDefaultBatchSize();

    void setDefaultBatchSize(int defaultBatchSize);

    boolean isUseSSL();
    void setUseSSL(boolean useSSL);

    javax.net.ssl.SSLContext getSslContext();
    void setSslContext(javax.net.ssl.SSLContext sslContext);

    boolean isSslInvalidHostNameAllowed();
    void setSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed);

    boolean isDefaultJ();

    void setDefaultJ(boolean j);


    //____    __    ____  ___   .___________.  ______  __    __
    //\   \  /  \  /   / /   \  |           | /      ||  |  |  |
    // \   \/    \/   / /  ^  \ `---|  |----`|  ,----'|  |__|  |
    //  \            / /  /_\  \    |  |     |  |     |   __   |
    //   \    /\    / /  _____  \   |  |     |  `----.|  |  |  |
    //    \__/  \__/ /__/     \__\  |__|      \______||__|  |__|
    void watch(WatchCommand settings) throws MorphiumDriverException;

    //  ______   ______   .__   __. .__   __.  _______   ______ .___________. __    ______   .__   __.      _______.
    // /      | /  __  \  |  \ |  | |  \ |  | |   ____| /      ||           ||  |  /  __  \  |  \ |  |     /       |
    //|  ,----'|  |  |  | |   \|  | |   \|  | |  |__   |  ,----'`---|  |----`|  | |  |  |  | |   \|  |    |   (----`
    //|  |     |  |  |  | |  . `  | |  . `  | |   __|  |  |         |  |     |  | |  |  |  | |  . `  |     \   \
    //|  `----.|  `--'  | |  |\   | |  |\   | |  |____ |  `----.    |  |     |  | |  `--'  | |  |\   | .----)   |
    // \______| \______/  |__| \__| |__| \__| |_______| \______|    |__|     |__|  \______/  |__| \__| |_______/
    //MongoConnection getConnection(ConnectionType type);

    MongoConnection getReadConnection(ReadPreference rp);

    MongoConnection getPrimaryConnection(WriteConcern wc) throws MorphiumDriverException;

    void releaseConnection(MongoConnection con);
    void closeConnection(MongoConnection con);

    boolean exists(String db, String coll) throws MorphiumDriverException;


    boolean exists(String db) throws MorphiumDriverException;


    BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc);

    Map<DriverStatsKey, Double> getDriverStats();
    void setLocalThreshold(int threshold);
    int getLocalThreshold();


    enum DriverStatsKey {
        CONNECTIONS_OPENED, CONNECTIONS_CLOSED, CONNECTIONS_BORROWED, CONNECTIONS_RELEASED, CONNECTIONS_IN_POOL,
        ERRORS, FAILOVERS,
        MSG_SENT, REPLY_PROCESSED, REPLY_IN_MEM, REPLY_RECEIVED,
        THREADS_CREATED, CONNECTIONS_IN_USE,
        THREADS_WAITING_FOR_CONNECTION,
    }
    enum CompressionType {
        NONE, ZLIB, SNAPPY,
    }
}
