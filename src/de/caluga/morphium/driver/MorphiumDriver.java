package de.caluga.morphium.driver;/**
 * Created by stephan on 15.10.15.
 */

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.mongodb.Maximums;

import java.util.List;
import java.util.Map;

/**
 * Morphium driver interface
 * <p>
 * All drivers need to implement this interface. you can add your own drivers to morphium. These are actually not
 * limited to be mongodb drivers. There is also an InMemory implementation.
 **/
@SuppressWarnings("BooleanMethodIsAlwaysInverted")
public interface MorphiumDriver {

    void setCredentials(String db, String login, char[] pwd);

    @SuppressWarnings("unused")
    boolean isReplicaset();

    @SuppressWarnings("unused")
    String[] getCredentials(String db);

    @SuppressWarnings("unused")
    boolean isDefaultFsync();

    @SuppressWarnings("unused")
    void setDefaultFsync(boolean j);

    String[] getHostSeed();

    void setHostSeed(String... host);

    @SuppressWarnings("unused")
    int getMaxConnectionsPerHost();

    void setMaxConnectionsPerHost(int mx);

    @SuppressWarnings("unused")
    int getMinConnectionsPerHost();

    void setMinConnectionsPerHost(int mx);

    @SuppressWarnings("unused")
    int getMaxConnectionLifetime();

    void setMaxConnectionLifetime(int timeout);

    @SuppressWarnings("unused")
    int getMaxConnectionIdleTime();

    void setMaxConnectionIdleTime(int time);

    @SuppressWarnings("unused")
    int getSocketTimeout();

    void setSocketTimeout(int timeout);

    @SuppressWarnings("unused")
    int getConnectionTimeout();

    void setConnectionTimeout(int timeout);

    @SuppressWarnings("unused")
    int getDefaultW();

    @SuppressWarnings("unused")
    void setDefaultW(int w);

    @SuppressWarnings("unused")
    int getMaxBlockintThreadMultiplier();

    @SuppressWarnings("unused")
    int getHeartbeatFrequency();

    void setHeartbeatFrequency(int heartbeatFrequency);

    @SuppressWarnings("unused")
    void setDefaultBatchSize(int defaultBatchSize);

    @SuppressWarnings("unused")
    void setCredentials(Map<String, String[]> credentials);

    @SuppressWarnings("unused")
    int getHeartbeatSocketTimeout();

    void setHeartbeatSocketTimeout(int heartbeatSocketTimeout);

    @SuppressWarnings("unused")
    boolean isUseSSL();

    @SuppressWarnings("unused")
    void setUseSSL(boolean useSSL);

    @SuppressWarnings("unused")
    boolean isDefaultJ();

    @SuppressWarnings("unused")
    void setDefaultJ(boolean j);

    @SuppressWarnings("unused")
    int getWriteTimeout();

    @SuppressWarnings("unused")
    void setWriteTimeout(int writeTimeout);

    @SuppressWarnings("unused")
    int getLocalThreshold();

    void setLocalThreshold(int thr);

    void setMaxBlockingThreadMultiplier(int m);

    @SuppressWarnings("unused")
    void heartBeatFrequency(int t);

    @SuppressWarnings("unused")
    void heartBeatSocketTimeout(int t);

    @SuppressWarnings("unused")
    void useSsl(boolean ssl);

    void connect() throws MorphiumDriverException;

    void setDefaultReadPreference(ReadPreference rp);

    void connect(String replicasetName) throws MorphiumDriverException;

    @SuppressWarnings("unused")
    Maximums getMaximums();

    boolean isConnected();

    @SuppressWarnings("unused")
    int getDefaultWriteTimeout();

    @SuppressWarnings("unused")
    void setDefaultWriteTimeout(int wt);

    @SuppressWarnings("unused")
    int getRetriesOnNetworkError();

    @SuppressWarnings("unused")
    void setRetriesOnNetworkError(int r);

    @SuppressWarnings("unused")
    int getSleepBetweenErrorRetries();

    @SuppressWarnings("unused")
    void setSleepBetweenErrorRetries(int s);

    void close() throws MorphiumDriverException;

    Map<String, Object> getReplsetStatus() throws MorphiumDriverException;

    @SuppressWarnings("unused")
    Map<String, Object> getDBStats(String db) throws MorphiumDriverException;

    @SuppressWarnings("unused")
    Map<String, Object> getOps(long threshold) throws MorphiumDriverException;

    Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException;

    MorphiumCursor initIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, Map<String, Object> findMetaData) throws MorphiumDriverException;

    MorphiumCursor nextIteration(MorphiumCursor crs) throws MorphiumDriverException;

    void closeIteration(MorphiumCursor crs) throws MorphiumDriverException;

    List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference rp, Map<String, Object> findMetaData) throws MorphiumDriverException;

    long count(String db, String collection, Map<String, Object> query, ReadPreference rp) throws MorphiumDriverException;

    /**
     * just insert - no special handling
     *
     * @param db
     * @param collection
     * @param objs
     * @param wc
     * @throws MorphiumDriverException
     */
    void insert(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException;

    /**
     * store - if id == null, create it...
     *
     * @param db
     * @param collection
     * @param objs
     * @param wc
     * @throws MorphiumDriverException
     */
    void store(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException;


    Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> op, boolean multiple, boolean upsert, WriteConcern wc) throws MorphiumDriverException;

    Map<String, Object> delete(String db, String collection, Map<String, Object> query, boolean multiple, WriteConcern wc) throws MorphiumDriverException;

    void drop(String db, String collection, WriteConcern wc) throws MorphiumDriverException;

    @SuppressWarnings("unused")
    void drop(String db, WriteConcern wc) throws MorphiumDriverException;

    @SuppressWarnings("unused")
    boolean exists(String db) throws MorphiumDriverException;

    List<Object> distinct(String db, String collection, String field, Map<String, Object> filter, ReadPreference rp) throws MorphiumDriverException;

    boolean exists(String db, String collection) throws MorphiumDriverException;

    List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException;

    @SuppressWarnings("unused")
    List<String> getCollectionNames(String db) throws MorphiumDriverException;

    Map<String, Object> group(String db, String coll, Map<String, Object> query, Map<String, Object> initial, String jsReduce, String jsFinalize, ReadPreference rp, String... keys) throws MorphiumDriverException;

    List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, ReadPreference readPreference) throws MorphiumDriverException;

    @SuppressWarnings("unused")
    boolean isSocketKeepAlive();

    void setSocketKeepAlive(boolean socketKeepAlive);

    @SuppressWarnings("unused")
    int getHeartbeatConnectTimeout();

    void setHeartbeatConnectTimeout(int heartbeatConnectTimeout);

    @SuppressWarnings("unused")
    int getMaxWaitTime();

    void setMaxWaitTime(int maxWaitTime);

    boolean isCapped(String db, String coll) throws MorphiumDriverException;

    BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc);

    void createIndex(String db, String collection, Map<String, Object> index, Map<String, Object> options) throws MorphiumDriverException;
}
