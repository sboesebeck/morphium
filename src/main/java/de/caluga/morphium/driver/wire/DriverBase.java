package de.caluga.morphium.driver.wire;

import de.caluga.morphium.MorphiumConfig.CompressionType;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bson.BsonEncoder;
import de.caluga.morphium.driver.bson.UUIDRepresentation;
import de.caluga.morphium.driver.commands.ListCollectionsCommand;
import de.caluga.morphium.driver.commands.ListDatabasesCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.mongodb.Maximums;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * User: Stephan Bösebeck
 * Date: 03.12.15
 * Time: 22:36
 * <p>
 * <p>
 * Base for custom drivers
 */
@SuppressWarnings("WeakerAccess")
public abstract class DriverBase implements MorphiumDriver {
    private final Logger log = LoggerFactory.getLogger(DriverBase.class);
    private volatile AtomicInteger rqid = new AtomicInteger(Math.abs((int)(System.currentTimeMillis() % 100000)));
    private int maxWait = 1000;
    private int maxBsonObjectSize = 12 * 1025 * 1024;
    private int maxMessageSize = 16 * 1024 * 1024;
    private int maxWriteBatchSize = 1000;
    private ReadPreference defaultRP = ReadPreference.primary();
    private boolean replicaSet = false;
    private String replicaSetName = null;
    private int retriesOnNetworkError = 5;
    private int sleepBetweenRetries = 100;
    private boolean defaultJ = false;
    private Set<String> hostSeed;
    private int heartbeatFrequency = 2000;
    private boolean useSSL = false;
    private int defaultW = 1;
    private int connectionTimeout = 1000;
    private int maxConnectionIdleTime = 100000;
    private int maxConnectionLifetime = 600000;
    private int minConnectionsPerHost = 1;
    private int maxConnectionsPerHost = 100;
    private int defaultWriteTimeout = 10000;
    private int batchSize = 100;
    private boolean retryReads = false;
    private boolean retryWrites = true;
    private int readTimeout = 30000;
    private int compressionType = 0;
    private int localThreshold = -1;

    private ThreadLocal<MorphiumTransactionContext> transactionContext = new ThreadLocal<>();

    private String authDb = null;
    private String user;
    private String password;

    public DriverBase() {
        //startHousekeeping();
    }

    @Override
    public void setConnectionUrl(String connectionUrl) throws MalformedURLException {
        URL u = new URL(connectionUrl);

        if (!u.getProtocol().equals("mongodb")) {
            throw new MalformedURLException("unsupported protocol: " + u.getProtocol());
        }
    }



    @Override
    public int getCompression() {
        return this.compressionType;
    }

    @Override
    public MorphiumDriver setCompression(int type) {
        log.debug("Setting compression to {}", type);
        this.compressionType = type;
        return this;
    }

    @Override
    public List<String> listCollections(String db, String regex) throws MorphiumDriverException {
        MongoConnection primaryConnection = getPrimaryConnection(null);
        ListCollectionsCommand cmd = null;

        try {
            cmd = new ListCollectionsCommand(primaryConnection);
            cmd.setDb(db).setNameOnly(true);

            if (regex != null) {
                cmd.setFilter(Doc.of("name", Pattern.compile(regex)));
            }

            var lst = cmd.execute();

            if (cmd.getConnection() != null) {
                cmd.releaseConnection();
                log.warn("connection not released!?!?");
            }

            List<String> colNames = new ArrayList<>();

            for (Map<String, Object> doc : lst) {
                if (doc.containsKey("name")) {
                    String name = doc.get("name").toString();
                    colNames.add(name);
                }
            }

            return colNames;
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }
    }

    public int getNumHostsInSeed() {
        return hostSeed.size();
    }

    public void removeFromHostSeed(String host) {
        hostSeed.remove(host);
    }

    public void addToHostSeed(String host) {
        hostSeed.add(host);
    }

    @Override
    public void setHostSeed(String... hosts) {
        hostSeed = Collections.synchronizedSet(new LinkedHashSet<>());

        for (String h : hosts) {
            hostSeed.add(h);
        }
    }

    @Override
    public int getRetriesOnNetworkError() {
        return retriesOnNetworkError;
    }

    @Override
    public MorphiumDriver setRetriesOnNetworkError(int r) {
        retriesOnNetworkError = r;
        return this;
    }

    @Override
    public int getSleepBetweenErrorRetries() {
        return sleepBetweenRetries;
    }

    @Override
    public MorphiumDriver setSleepBetweenErrorRetries(int s) {
        sleepBetweenRetries = s;
        return this;
    }

    @Override
    public void setCredentials(String db, String login, String pwd) {
        authDb = db;
        user = login;
        password = pwd;
    }

    @Override
    public int getMaxConnections() {
        return maxConnectionsPerHost;
    }

    public String getAuthDb() {
        return authDb;
    }

    public DriverBase setAuthDb(String authDb) {
        this.authDb = authDb;
        return this;
    }

    public String getUser() {
        return user;
    }

    public DriverBase setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public DriverBase setPassword(String password) {
        this.password = password;
        return this;
    }

    @Override
    public MorphiumDriver setMaxConnections(int maxConnections) {
        maxConnectionsPerHost = maxConnections;
        return this;
    }

    @Override
    public int getMinConnections() {
        return getMinConnectionsPerHost();
    }

    @Override
    public MorphiumDriver setMinConnections(int minConnections) {
        minConnectionsPerHost = minConnections;
        return this;
    }

    @Override
    public boolean isRetryReads() {
        return retryReads;
    }

    @Override
    public MorphiumDriver setRetryReads(boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    @Override
    public boolean isRetryWrites() {
        return retryWrites;
    }

    @Override
    public MorphiumDriver setRetryWrites(boolean retryWrites) {
        this.retryWrites = retryWrites;
        return this;
    }

    public String getUuidRepresentation() {
        return UUIDRepresentation.STANDARD.name();
    }

    public void setUuidRepresentation(String uuidRepresentation) {
    }

    @Override
    public int getReadTimeout() {
        return readTimeout;
    }

    @Override
    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    @Override
    public int getMinConnectionsPerHost() {
        return minConnectionsPerHost;
    }

    @Override
    public void setMinConnectionsPerHost(int minConnectionsPerHost) {
        this.minConnectionsPerHost = minConnectionsPerHost;
    }

    @Override
    public int getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    @Override
    public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
        this.maxConnectionsPerHost = maxConnectionsPerHost;
    }

    @Override
    public boolean isReplicaset() {
        return replicaSet;
    }

    @Override
    public List<String> listDatabases() throws MorphiumDriverException {
        if (!isConnected()) {
            return null;
        }

        MongoConnection primaryConnection = getPrimaryConnection(null);
        ListDatabasesCommand cmd = null;

        try {
            cmd = new ListDatabasesCommand(primaryConnection);
            var msg = primaryConnection.sendCommand(cmd);
            Map<String, Object> res = primaryConnection.readSingleAnswer(msg);
            List<String> ret = new ArrayList<>();

            if (res.get("databases") != null) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object >> lst = (List<Map<String, Object >> ) res.get("databases");

                for (Map<String, Object> db : lst) {
                    if (db.get("name") != null) {
                        ret.add(db.get("name").toString());
                    } else {
                        log.error("No DB Name for this entry...");
                    }
                }
            }

            return ret;
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }
    }

    public boolean exists(String db, String collection) throws MorphiumDriverException {
        var ret = listCollections(db, null);

        for (var c : ret) {
            if (c.equals(collection)) {
                return true;
            }
        }

        return false;
    }


    @Override
    /**
     * @return unmodifiable copy
     */
    public List<String> getHostSeed() {
        if (hostSeed == null) {
            return null;
        }

        return Collections.unmodifiableList(new ArrayList<>(hostSeed));
    }

    @Override
    public String getReplicaSetName() {
        return replicaSetName;
    }

    @Override
    public void setReplicaSetName(String replicaSetName) {
        this.replicaSetName = replicaSetName;
    }

    @Override
    @SuppressWarnings("unused")
    public int getMaxBsonObjectSize() {
        return maxBsonObjectSize;
    }

    @Override
    public void setMaxBsonObjectSize(int maxBsonObjectSize) {
        this.maxBsonObjectSize = maxBsonObjectSize;
    }

    @Override
    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    @Override
    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    public int getMaxWriteBatchSize() {
        return maxWriteBatchSize;
    }

    @Override
    public void setMaxWriteBatchSize(int maxWriteBatchSize) {
        this.maxWriteBatchSize = maxWriteBatchSize;
    }

    @Override
    @SuppressWarnings("unused")
    public boolean isReplicaSet() {
        return replicaSet;
    }

    @Override
    public void setReplicaSet(boolean replicaSet) {
        this.replicaSet = replicaSet;
    }

    public int getNextId() {
        synchronized (DriverBase.class) {
            return rqid.incrementAndGet();
        }
    }

    @Override
    public boolean getDefaultJ() {
        return defaultJ;
    }

    @Override
    public int getDefaultWriteTimeout() {
        return defaultWriteTimeout;
    }

    @Override
    public void setDefaultWriteTimeout(int wt) {
        defaultWriteTimeout = wt;
    }

    @Override
    public int getMaxWaitTime() {
        return this.maxWait;
    }

    @Override
    public void setMaxWaitTime(int maxWaitTime) {
        this.maxWait = maxWaitTime;
    }

    @Override
    public String[] getCredentials(String db) {
        return new String[0];
    }

    @Override
    public void setHostSeed(List<String> hosts) {
        hostSeed = Collections.synchronizedSet(new LinkedHashSet<>());
        hostSeed.addAll(hosts);
    }

    @Override
    public int getMaxConnectionLifetime() {
        return maxConnectionLifetime;
    }

    @Override
    public void setMaxConnectionLifetime(int timeout) {
        maxConnectionLifetime = timeout;
    }

    @Override
    public int getMaxConnectionIdleTime() {
        return maxConnectionIdleTime;
    }

    @Override
    public void setMaxConnectionIdleTime(int time) {
        maxConnectionIdleTime = time;
    }

    @Override
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    @Override
    public void setConnectionTimeout(int timeout) {
        connectionTimeout = timeout;
    }

    @Override
    public int getDefaultW() {
        return defaultW;
    }

    @Override
    public void setDefaultW(int w) {
        defaultW = w;
    }

    @Override
    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    @Override
    public void setHeartbeatFrequency(int heartbeatFrequency) {
        this.heartbeatFrequency = heartbeatFrequency;
    }

    public Maximums getMaximums() {
        Maximums max = new Maximums();
        max.setMaxBsonSize(maxBsonObjectSize);
        max.setMaxMessageSize(maxMessageSize);
        max.setMaxWriteBatchSize(maxWriteBatchSize);
        return max;
    }

    @Override
    public ReadPreference getDefaultReadPreference() {
        return defaultRP;
    }

    @Override
    public void setDefaultReadPreference(ReadPreference rp) {
        defaultRP = rp;
    }

    @Override
    public int getDefaultBatchSize() {
        return batchSize;
    }

    @Override
    public void setDefaultBatchSize(int defaultBatchSize) {
        this.batchSize = defaultBatchSize;
    }

    @Override
    public boolean isUseSSL() {
        return useSSL;
    }

    @Override
    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    @Override
    public boolean isDefaultJ() {
        return defaultJ;
    }

    @Override
    public void setDefaultJ(boolean j) {
        defaultJ = j;
    }


    public void heartBeatFrequency(int t) {
        heartbeatFrequency = t;
    }

    public void useSsl(boolean ssl) {
        useSSL = ssl;
    }


    @Override
    public MorphiumTransactionContext startTransaction(boolean autoCommit) {
        if (this.transactionContext.get() != null) {
            throw new IllegalArgumentException("Transaction in progress");
        }

        MorphiumTransactionContextImpl ctx = new MorphiumTransactionContextImpl();
        ctx.setLsid(UUID.randomUUID());
        ctx.setTxnNumber((long) getNextId());
        this.transactionContext.set(ctx);
        return ctx;
    }

    @Override
    public MorphiumTransactionContext getTransactionContext() {
        return transactionContext.get();
    }

    @Override
    public void setTransactionContext(MorphiumTransactionContext ctx) {
        if (transactionContext.get() != null) {
            throw new IllegalArgumentException("Transaction already in progress!");
        }

        if (ctx instanceof MorphiumTransactionContextImpl) {
            transactionContext.set((MorphiumTransactionContextImpl) ctx);
        } else {
            throw new IllegalArgumentException("Transaction context of wrong type!");
        }
    }

    @Override
    public boolean isTransactionInProgress() {
        return transactionContext.get() != null;
    }

    protected void clearTransactionContext() {
        transactionContext.remove();
    }

    public abstract void watch(WatchCommand settings) throws MorphiumDriverException;


    public abstract boolean isCapped(String db, String coll) throws MorphiumDriverException;

    public Logger getLog() {
        return log;
    }

    public AtomicInteger getRqid() {
        return rqid;
    }

    public void setRqid(AtomicInteger rqid) {
        this.rqid = rqid;
    }

    public int getMaxWait() {
        return maxWait;
    }

    public void setMaxWait(int maxWait) {
        this.maxWait = maxWait;
    }

    public ReadPreference getDefaultRP() {
        return defaultRP;
    }

    public void setDefaultRP(ReadPreference defaultRP) {
        this.defaultRP = defaultRP;
    }

    public int getSleepBetweenRetries() {
        return sleepBetweenRetries;
    }

    public void setSleepBetweenRetries(int sleepBetweenRetries) {
        this.sleepBetweenRetries = sleepBetweenRetries;
    }

    public void setHostSeed(Set<String> hostSeed) {
        this.hostSeed = hostSeed instanceof SequencedSet ?
                        (SequencedSet<String>) hostSeed :
                        Collections.synchronizedSet(new LinkedHashSet<>(hostSeed));
    }

    // SequencedSet helper methods for better host management
    public String getFirstHost() {
        return hostSeed != null && !hostSeed.isEmpty() ? hostSeed.iterator().next() : null;
    }

    public String getLastHost() {
        if (hostSeed != null && !hostSeed.isEmpty()) {
            // Get last element without removing it
            synchronized (hostSeed) {
                return hostSeed.stream().reduce((first, second) -> second).orElse(null);
            }
        }
        return null;
    }

    public void addHostFirst(String host) {
        if (hostSeed != null) {
            // For thread-safe SequencedSet operations, we need to create a new set
            Set<String> newHostSeed = Collections.synchronizedSet(new LinkedHashSet<>());
            newHostSeed.add(host);
            newHostSeed.addAll(hostSeed);
            this.hostSeed = newHostSeed;
        }
    }

    public void addHostLast(String host) {
        if (hostSeed != null) {
            hostSeed.add(host); // LinkedHashSet maintains insertion order
        }
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(int compressionType) {
        this.compressionType = compressionType;
    }

    public int getLocalThreshold() {
        return localThreshold;
    }

    public void setLocalThreshold(int localThreshold) {
        this.localThreshold = localThreshold;
    }

    public void setTransactionContext(ThreadLocal<MorphiumTransactionContext> transactionContext) {
        this.transactionContext = transactionContext;
    }
}
