package de.caluga.morphium.driver;/**
 * Created by stephan on 15.10.15.
 */

import java.util.List;
import java.util.Map;

/**
 * TODO: Add Documentation here
 **/
public interface MorphiumMongoDriver {

    void setCredentials(String db, String login, char[] pwd);

    String[] getCredentials(String db);

    boolean isDefaultFsync();

    String[] getHostSeed();

    int getMaxConnectionsPerHost();

    int getMinConnectionsPerHost();

    int getMaxConnectionLifetime();

    int getMaxConnectionIdleTime();

    int getSocketTimeout();

    int getConnectionTimeout();

    int getDefaultW();

    int getMaxBlockintThreadMultiplier();

    int getHeartbeatFrequency();

    int getHeartbeatSocketTimeout();

    boolean isUseSSL();

    boolean isDefaultJ();

    int getWriteTimeout();

    int getLocalThreshold();

    void setHostSeed(String... host);

    void setMaxConnectionsPerHost(int mx);

    void setMinConnectionsPerHost(int mx);

    void setMaxConnectionLifetime(int timeout);

    void setMaxConnectionIdleTime(int time);

    void setSocketTimeout(int timeout);

    void setConnectionTimeout(int timeout);

    void setDefaultW(int w);

    void setMaxBlockingThreadMultiplier(int m);

    void heartBeatFrequency(int t);

    void heartBeatSocketTimeout(int t);

    void useSsl(boolean ssl);

    void connect();

    void connect(String replicasetName);

    boolean isConnected();

    void setDefaultJ(boolean j);

    void setDefaultWriteTimeout(int wt);

    void setLocalThreshold(int thr);

    void setDefaultFsync(boolean j);

    void close();

    Map<String, Object> getStats();

    Map<String, Object> getOps(long threshold);

    Map<String, Object> runCommand(String db, Map<String, Object> cmd);

    Map<String, Object> runCommand(String db, String collection, Map<String, Object> cmd);

    List<Map<String, Object>> find(String db, String collection, Map<String, Object> query);

    Map<String, Object> insert(String db, String collection, List<Map<String, Object>> objs);

    Map<String, Object> udate(String db, String collection, Map<String, Object> query, Map<String, Object> op);

    Map<String, Object> delete(String db, String collection, Map<String, Object> query);

    Map<String, Object> drop(String db, String collection);

    Map<String, Object> drop(String db);

    boolean exists(String db);

    boolean exists(String db, String collection);

    Map<String, Object> getIndexes(String db, String collection);

    List<String> getCollectionNames(String db);

    Map<String, Object> killCursors(String db, String collection, List<Long> cursorIds);

    Map<String, Object> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse);

    boolean isSocketKeepAlive();

    void setSocketKeepAlive(boolean socketKeepAlive);

    int getHeartbeatConnectTimeout();

    void setHeartbeatConnectTimeout(int heartbeatConnectTimeout);

    int getMaxWaitTime();

    void setMaxWaitTime(int maxWaitTime);
}
