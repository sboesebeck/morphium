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
 *
 * All drivers need to implement this interface. you can add your own drivers to morphium. These are actually not
 * limited to be mongodb drivers. There is also an InMemory implementation.
 **/
@SuppressWarnings("BooleanMethodIsAlwaysInverted")
public interface MorphiumDriver {

    void setCredentials(String db, String login, char[] pwd);

    boolean isReplicaset();

    String[] getCredentials(String db);

    boolean isDefaultFsync();

    void setDefaultFsync(boolean j);

    String[] getHostSeed();

    void setHostSeed(String... host);

    int getMaxConnectionsPerHost();

    void setMaxConnectionsPerHost(int mx);

    int getMinConnectionsPerHost();

    void setMinConnectionsPerHost(int mx);

    int getMaxConnectionLifetime();

    void setMaxConnectionLifetime(int timeout);

    int getMaxConnectionIdleTime();

    void setMaxConnectionIdleTime(int time);

    int getSocketTimeout();

    void setSocketTimeout(int timeout);

    int getConnectionTimeout();

    void setConnectionTimeout(int timeout);

    int getDefaultW();

    void setDefaultW(int w);

    int getMaxBlockintThreadMultiplier();

    int getHeartbeatFrequency();

    void setHeartbeatFrequency(int heartbeatFrequency);

    void setDefaultBatchSize(int defaultBatchSize);

    void setCredentials(Map<String, String[]> credentials);

    int getHeartbeatSocketTimeout();

    void setHeartbeatSocketTimeout(int heartbeatSocketTimeout);

    boolean isUseSSL();

    void setUseSSL(boolean useSSL);

    boolean isDefaultJ();

    void setDefaultJ(boolean j);

    int getWriteTimeout();

    void setWriteTimeout(int writeTimeout);

    int getLocalThreshold();

    void setLocalThreshold(int thr);

    void setMaxBlockingThreadMultiplier(int m);

    void heartBeatFrequency(int t);

    void heartBeatSocketTimeout(int t);

    void useSsl(boolean ssl);

    void connect() throws MorphiumDriverException;

    void setDefaultReadPreference(ReadPreference rp);

    void connect(String replicasetName) throws MorphiumDriverException;

    Maximums getMaximums();

    boolean isConnected();

    int getDefaultWriteTimeout();

    void setDefaultWriteTimeout(int wt);

    int getRetriesOnNetworkError();

    void setRetriesOnNetworkError(int r);

    int getSleepBetweenErrorRetries();

    void setSleepBetweenErrorRetries(int s);

    void close() throws MorphiumDriverException;

    Map<String, Object> getReplsetStatus() throws MorphiumDriverException;

    Map<String, Object> getDBStats(String db) throws MorphiumDriverException;

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

    void drop(String db, WriteConcern wc) throws MorphiumDriverException;

    boolean exists(String db) throws MorphiumDriverException;

    List<Object> distinct(String db, String collection, String field, Map<String, Object> filter, ReadPreference rp) throws MorphiumDriverException;

    boolean exists(String db, String collection) throws MorphiumDriverException;

    List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException;

    List<String> getCollectionNames(String db) throws MorphiumDriverException;

    Map<String, Object> group(String db, String coll, Map<String, Object> query, Map<String, Object> initial, String jsReduce, String jsFinalize, ReadPreference rp, String... keys) throws MorphiumDriverException;

    List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, ReadPreference readPreference) throws MorphiumDriverException;

    boolean isSocketKeepAlive();

    void setSocketKeepAlive(boolean socketKeepAlive);

    int getHeartbeatConnectTimeout();

    void setHeartbeatConnectTimeout(int heartbeatConnectTimeout);

    int getMaxWaitTime();

    void setMaxWaitTime(int maxWaitTime);

    boolean isCapped(String db, String coll) throws MorphiumDriverException;

    BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc);

    void createIndex(String db, String collection, Map<String, Object> index, Map<String, Object> options) throws MorphiumDriverException;
}
