package de.caluga.morphium.driver.mongodb;/**
 * Created by stephan on 05.11.15.
 */

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.ClientSessionOptions;
import com.mongodb.CursorType;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.event.ConnectionAddedEvent;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionCheckOutStartedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolCreatedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionPoolOpenedEvent;
import com.mongodb.event.ConnectionReadyEvent;
import com.mongodb.event.ConnectionRemovedEvent;

import de.caluga.morphium.Collation;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.driver.DriverTailableIterationCallback;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriver;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumDriverOperation;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.MorphiumTransactionContext;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.bulk.BulkRequestContext;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.PatternCodec;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static de.caluga.morphium.aggregation.Aggregator.GeoNearFields.query;

@SuppressWarnings({"WeakerAccess", "deprecation", "MagicConstant"})
public class MongoDriver implements MorphiumDriver {
    private final Logger log = LoggerFactory.getLogger(MongoDriver.class);
    private String[] hostSeed;

    private int maxConnections = 100;
    private int minConnections = 10;

    private int maxConnectionLifetime = 60000;
    private int maxConnectionIdleTime = 20000;
    private int connectionTimeout = 1000;
    private int defaultW = 1;
    private int heartbeatFrequency = 1000;
    private boolean defaultJ = false;
    private int writeTimeout = 1000;
    private int localThreshold = 15;
    private boolean defaultFsync;
    private int maxWaitTime;
    private int serverSelectionTimeout;
    private int readTimeout = 1000;
    private boolean retryReads = false;
    private boolean retryWrites = false;
    private String uuidRepresentation;

    //SSL-Settings
    private boolean useSSL = false;
    private SSLContext sslContext = null;
    private boolean sslInvalidHostNameAllowed = false;


    private int defaultBatchSize = 100;
    private int retriesOnNetworkError = 2;
    private int sleepBetweenErrorRetries = 500;

    private ReadPreference defaultReadPreference;

    private Map<String, String[]> credentials = new HashMap<>();
    private MongoClient mongo;
    private Maximums maximums;

    private final List<CommandListener> commandListeners = new Vector<>();
    private final List<ClusterListener> clusterListeners = new Vector<>();
    private final List<ConnectionPoolListener> connectionPoolListeners = new Vector<>();

    private boolean replicaset;


    private final ThreadLocal<MongoTransactionContext> currentTransaction = new ThreadLocal<>();

    @Override
    public boolean isReplicaset() {
        return replicaset;
    }

    @Override
    public void setCredentials(String db, String login, char[] pwd) {
        String[] cred = new String[2];
        cred[0] = login;
        cred[1] = new String(pwd);
        credentials.put(db, cred);
    }

    @Override
    public List<String> listDatabases() throws MorphiumDriverException {
        if (!isConnected()) {
            return null;
        }
        Map<String, Object> command = new HashMap<>();
        command.put("listDatabases", 1);
        Map<String, Object> res = runCommand("admin", command);
        List<String> ret = new ArrayList<>();
        if (res.get("databases") != null) {
            //noinspection unchecked
            List<Map<String, Object>> lst = (List<Map<String, Object>>) res.get("databases");
            for (Map<String, Object> db : lst) {
                if (db.get("name") != null) {
                    ret.add(db.get("name").toString());
                } else {
                    log.error("No DB Name for this entry...");
                }
            }
        }
        return ret;
    }

    @Override
    public void addCommandListener(CommandListener cmd) {
        commandListeners.add(cmd);
    }

    @Override
    public void removeCommandListener(CommandListener cmd) {
        commandListeners.remove(cmd);
    }

    @Override
    public void addClusterListener(ClusterListener cl) {
        clusterListeners.add(cl);
    }

    @Override
    public void removeClusterListener(ClusterListener cl) {
        clusterListeners.remove(cl);
    }

    @Override
    public void addConnectionPoolListener(ConnectionPoolListener cpl) {
        connectionPoolListeners.add(cpl);
    }

    @Override
    public void removeConnectionPoolListener(ConnectionPoolListener cpl) {
        connectionPoolListeners.remove(cpl);
    }


    @Override
    public List<String> listCollections(String db, String pattern) throws MorphiumDriverException {
        if (!isConnected()) {
            return null;
        }
        Map<String, Object> command = new LinkedHashMap<>();
        command.put("listCollections", 1);
        if (pattern != null) {
            Map<String, Object> query = new HashMap<>();
            query.put("name", Pattern.compile(pattern));
            command.put("filter", query);
        }
        Map<String, Object> res = runCommand(db, command);
        List<Map<String, Object>> colList = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        addToListFromCursor(db, colList, res);

        for (Map<String, Object> col : colList) {
            colNames.add(col.get("name").toString());
        }
        return colNames;
    }

    @SuppressWarnings("unchecked")
    private void addToListFromCursor(String db, List<Map<String, Object>> data, Map<String, Object> res) throws MorphiumDriverException {
        boolean valid;
        //noinspection unchecked
        Map<String, Object> crs = (Map<String, Object>) res.get("cursor");
        do {
            if (crs.get("firstBatch") != null) {
                //noinspection unchecked
                data.addAll((List<Map<String, Object>>) crs.get("firstBatch"));
            } else if (crs.get("nextBatch") != null) {
                data.addAll((List<Map<String, Object>>) crs.get("firstBatch"));
            }
            //next iteration.
            Map<String, Object> doc = new LinkedHashMap<>();
            if (crs.get("id") != null && !crs.get("id").toString().equals("0")) {
                valid = true;
                doc.put("getMore", crs.get("id"));
                crs = runCommand(db, doc);
            } else {
                valid = false;
            }

        } while (valid);
    }

    public ReadPreference getDefaultReadPreference() {
        return defaultReadPreference;
    }

    @Override
    public void setDefaultReadPreference(ReadPreference defaultReadPreference) {
        this.defaultReadPreference = defaultReadPreference;
    }

    @Override
    public String[] getCredentials(String db) {
        return credentials.get(db);
    }

    @Override
    public boolean isDefaultFsync() {
        return defaultFsync;
    }

    @Override
    public void setDefaultFsync(boolean j) {
        defaultFsync = j;
    }

    @Override
    public String[] getHostSeed() {
        return hostSeed;
    }

    @Override
    public void setHostSeed(String... host) {
        hostSeed = host;
    }

    @Override
    public int getMaxConnections() {
        return maxConnections;
    }

    @Override
    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    @Override
    public int getMinConnections() {
        return minConnections;
    }

    @Override
    public void setMinConnections(int minConnections) {
        this.minConnections = minConnections;
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

    @Override
    public void setDefaultBatchSize(int defaultBatchSize) {
        this.defaultBatchSize = defaultBatchSize;
    }

    @Override
    public void setCredentials(Map<String, String[]> credentials) {
        this.credentials = credentials;
    }

    @SuppressWarnings("unused")
    public void setMongo(MongoClient mongo) {
        this.mongo = mongo;
    }


    @Override
    public boolean isRetryReads() {
        return retryReads;
    }

    @Override
    public void setRetryReads(boolean retryReads) {
        this.retryReads = retryReads;
    }

    @Override
    public boolean isRetryWrites() {
        return retryWrites;
    }

    @Override
    public void setRetryWrites(boolean retryWrites) {
        this.retryWrites = retryWrites;
    }

    @Override
    public String getUuidRepresentation() {
        return uuidRepresentation;
    }

    @Override
    public void setUuidRepresentation(String uuidRepresentation) {
        this.uuidRepresentation = uuidRepresentation;
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

    @Override
    public int getLocalThreshold() {
        return localThreshold;
    }

    @Override
    public void setLocalThreshold(int thr) {
        localThreshold = thr;
    }

    @Override
    public void heartBeatFrequency(int t) {
        heartbeatFrequency = t;
    }


    @Override
    public void useSsl(boolean ssl) {
        useSSL = ssl;
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
    public void connect() throws MorphiumDriverException {
        connect(null);
    }

    @Override
    public void connect(String replicasetName) throws MorphiumDriverException {
        try {
            if (maxConnections == 0) {
                maxConnections = 1;
            }
            if (minConnections == 0) {
                minConnections = 1;
            }
            MongoClientSettings.Builder o = MongoClientSettings.builder();
            o.writeConcern(de.caluga.morphium.driver.WriteConcern.getWc(getDefaultW(), isDefaultFsync(), isDefaultJ(), getDefaultWriteTimeout()).toMongoWriteConcern());
            //read preference check
            o.retryReads(retryReads);
            o.retryWrites(retryWrites);
            o.addCommandListener(new CommandListener() {
                @Override
                public void commandStarted(CommandStartedEvent event) {
                    for (CommandListener cl : commandListeners) {
                        cl.commandStarted(event);
                    }
                }

                @Override
                public void commandSucceeded(CommandSucceededEvent event) {
                    for (CommandListener cl : commandListeners) {
                        cl.commandSucceeded(event);
                    }
                }

                @Override
                public void commandFailed(CommandFailedEvent event) {
                    //log.error("Command failed: " + event.getCommandName(), event.getThrowable());
                    for (CommandListener cl : commandListeners) {
                        cl.commandFailed(event);
                    }
                }
            });
            o.applyToSocketSettings(socketSettings -> {
                socketSettings.connectTimeout(getConnectionTimeout(), TimeUnit.MILLISECONDS);
                socketSettings.readTimeout(getReadTimeout(), TimeUnit.MILLISECONDS);
            });

            o.applyToConnectionPoolSettings(connectionPoolSettings -> {
                connectionPoolSettings.maxConnectionIdleTime(maxConnectionIdleTime, TimeUnit.MILLISECONDS);
                connectionPoolSettings.maxConnectionLifeTime(maxConnectionLifetime, TimeUnit.MILLISECONDS);
                connectionPoolSettings.maintenanceFrequency(getHeartbeatFrequency(), TimeUnit.MILLISECONDS);
                connectionPoolSettings.maxSize(maxConnections);
                connectionPoolSettings.minSize(minConnections);
                connectionPoolSettings.maxWaitTime(maxWaitTime, TimeUnit.MILLISECONDS);
                connectionPoolSettings.addConnectionPoolListener(new ConnectionPoolListener() {
                    @Override
                    public void connectionPoolOpened(ConnectionPoolOpenedEvent event) {
                        for (ConnectionPoolListener cpl : connectionPoolListeners) {
                            cpl.connectionPoolOpened(event);
                        }
                    }

                    @Override
                    public void connectionPoolCreated(ConnectionPoolCreatedEvent event) {
                        for (ConnectionPoolListener cpl : connectionPoolListeners) {
                            cpl.connectionPoolCreated(event);
                        }
                    }

                    @Override
                    public void connectionPoolCleared(ConnectionPoolClearedEvent event) {
                        for (ConnectionPoolListener cpl : connectionPoolListeners) {
                            cpl.connectionPoolCleared(event);
                        }
                    }

                    @Override
                    public void connectionPoolClosed(ConnectionPoolClosedEvent event) {
                        for (ConnectionPoolListener cpl : connectionPoolListeners) {
                            cpl.connectionPoolClosed(event);
                        }
                    }

                    @Override
                    public void connectionCheckOutStarted(ConnectionCheckOutStartedEvent event) {
                        for (ConnectionPoolListener cpl : connectionPoolListeners) {
                            cpl.connectionCheckOutStarted(event);
                        }
                    }

                    @Override
                    public void connectionCheckedOut(ConnectionCheckedOutEvent event) {
                        for (ConnectionPoolListener cpl : connectionPoolListeners) {
                            cpl.connectionCheckedOut(event);
                        }
                    }

                    @Override
                    public void connectionCheckOutFailed(ConnectionCheckOutFailedEvent event) {
                        for (ConnectionPoolListener cpl : connectionPoolListeners) {
                            cpl.connectionCheckOutFailed(event);
                        }
                    }

                    @Override
                    public void connectionCheckedIn(ConnectionCheckedInEvent event) {
                        for (ConnectionPoolListener cpl : connectionPoolListeners) {
                            cpl.connectionCheckedIn(event);
                        }
                    }

                    @Override
                    public void connectionAdded(ConnectionAddedEvent event) {
                        for (ConnectionPoolListener cpl : connectionPoolListeners) {
                            cpl.connectionAdded(event);
                        }
                    }

                    @Override
                    public void connectionCreated(ConnectionCreatedEvent event) {
                        for (ConnectionPoolListener cpl : connectionPoolListeners) {
                            cpl.connectionCreated(event);
                        }
                    }

                    @Override
                    public void connectionReady(ConnectionReadyEvent event) {
                        for (ConnectionPoolListener cpl : connectionPoolListeners) {
                            cpl.connectionReady(event);
                        }
                    }

                    @Override
                    public void connectionRemoved(ConnectionRemovedEvent event) {
                        for (ConnectionPoolListener cpl : connectionPoolListeners) {
                            cpl.connectionRemoved(event);
                        }
                    }

                    @Override
                    public void connectionClosed(ConnectionClosedEvent event) {
                        for (ConnectionPoolListener cpl : connectionPoolListeners) {
                            cpl.connectionClosed(event);
                        }
                    }
                });
            });

            o.applyToClusterSettings(clusterSettings -> {
                clusterSettings.serverSelectionTimeout(getConnectionTimeout(), TimeUnit.MILLISECONDS);
                if (hostSeed.length > 1) {
                    clusterSettings.mode(ClusterConnectionMode.MULTIPLE);
                } else {
                    clusterSettings.mode(ClusterConnectionMode.SINGLE);
                }
                if (replicasetName != null) {
                    clusterSettings.requiredReplicaSetName(replicasetName);
                }

                List<ServerAddress> hosts = new ArrayList<>();
                for (String host : hostSeed) {
                    hosts.add(new ServerAddress(host));
                }
                clusterSettings.hosts(hosts);
                clusterSettings.serverSelectionTimeout(getServerSelectionTimeout(), TimeUnit.MILLISECONDS);
                clusterSettings.localThreshold(getLocalThreshold(), TimeUnit.MILLISECONDS);
                clusterSettings.addClusterListener(new ClusterListener() {
                    @Override
                    public void clusterOpening(ClusterOpeningEvent event) {
                        for (ClusterListener cl : clusterListeners) {
                            cl.clusterOpening(event);
                        }
                    }

                    @Override
                    public void clusterClosed(ClusterClosedEvent event) {
                        for (ClusterListener cl : clusterListeners) {
                            cl.clusterClosed(event);
                        }
                    }

                    @Override
                    public void clusterDescriptionChanged(ClusterDescriptionChangedEvent event) {
                        //log.info("Cluster description changed: " + event.getNewDescription().toString());
                        for (ClusterListener cl : clusterListeners) {
                            cl.clusterDescriptionChanged(event);
                        }
                    }
                });
                //clusterSettings.serverSelector(new ReadPreferenceServerSelector(defaultReadPreference));

            });

            if (isUseSSL()) {
                o.applyToSslSettings(sslSettings -> {
                    sslSettings.enabled(true);
                    sslSettings.invalidHostNameAllowed(isSslInvalidHostNameAllowed());
                    sslSettings.context(getSslContext());

                });
            }

            if (this.uuidRepresentation != null && !this.uuidRepresentation.isEmpty()) {
                o.uuidRepresentation(UuidRepresentation.valueOf(this.uuidRepresentation));
            }

//            o.connectTimeout(getConnectionTimeout());
//            o.connectionsPerHost(getMaxConnectionsPerHost());
//            o.threadsAllowedToBlockForConnectionMultiplier(getMaxBlockintThreadMultiplier());
            //        o.cursorFinalizerEnabled(isCursorFinalizerEnabled()); //Deprecated?
            //        o.alwaysUseMBeans(isAlwaysUseMBeans());
//            o.heartbeatConnectTimeout(getHeartbeatConnectTimeout());
//            o.heartbeatFrequency(getHeartbeatFrequency());
//            o.heartbeatSocketTimeout(getHeartbeatSocketTimeout());
//            o.minConnectionsPerHost(getMinConnectionsPerHost());
//            o.minHeartbeatFrequency(getHeartbeatFrequency());
//            o.localThreshold(getLocalThreshold());
//            o.maxConnectionIdleTime(getMaxConnectionIdleTime());
//            o.maxConnectionLifeTime(getMaxConnectionLifetime());
//            if (replicasetName != null) {
//                o.requiredReplicaSetName(replicasetName);
//            }
//            o.maxWaitTime(getMaxWaitTime());
//            o.serverSelectionTimeout(getServerSelectionTimeout());

            for (Map.Entry<String, String[]> e : credentials.entrySet()) {
                MongoCredential cred = MongoCredential.createCredential(e.getValue()[0], e.getKey(), e.getValue()[1].toCharArray());
                o.credential(cred);
            }
            mongo = MongoClients.create(o.build());

            try {
                Document res = mongo.getDatabase("local").runCommand(new BasicDBObject("isMaster", true));
                if (res.get("setName") != null) {
                    replicaset = true;
                    if (hostSeed.length == 1) {
                        log.warn("have to reconnect to cluster... only one host specified, but its a replicaset");
                        o.applyToClusterSettings(builder -> builder.mode(ClusterConnectionMode.MULTIPLE));
                        mongo.close();
                        mongo = MongoClients.create(o.build());
                    }

                }
            } catch (MongoCommandException mce) {
                if (mce.getCode() == 20) {
                    //most likely a connection to a mongos,
                    //swallow error!
                    replicaset = false;
                } else {
                    throw new MorphiumDriverException("Error getting replicaset status", mce);
                }
            }
        } catch (Exception e) {
            throw new MorphiumDriverException("Error creating connection to mongo", e);
        }
    }

    @Override
    public Maximums getMaximums() {
        if (maximums == null) {
            maximums = new Maximums();
            try {
                Map<String, Object> cmd = new HashMap<>();
                cmd.put("isMaster", 1);
                Map<String, Object> res = runCommand("admin", cmd);
                maximums.setMaxBsonSize((Integer) res.get("maxBsonObjectSize"));
                maximums.setMaxMessageSize((Integer) res.get("maxMessageSizeBytes"));
                maximums.setMaxWriteBatchSize((Integer) res.get("maxWriteBatchSize"));
            } catch (Exception e) {
                log.error("Error reading max avalues from DB", e);
                //            maxBsonSize = 0;
                //            maxMessageSize = 0;
                //            maxWriteBatchSize = 0;
            }
        }
        return maximums;
    }

    @Override
    public boolean isConnected() {
        return mongo != null;
    }

    @Override
    public int getDefaultWriteTimeout() {
        return writeTimeout;
    }

    @Override
    public void setDefaultWriteTimeout(int wt) {
        writeTimeout = wt;
    }

    @Override
    public void close() throws MorphiumDriverException {
        try {
            if (currentTransaction.get() != null) {
                log.warn("Closing while transaction in progress - aborting!");
                abortTransaction();
            }
            mongo.close();
            mongo = null;
        } catch (Exception e) {
            //throw new MorphiumDriverException("error closing", e);
        }
    }

    @Override
    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        return DriverHelper.doCall(() -> {
            Document ret = mongo.getDatabase("admin").runCommand(new BasicDBObject("replSetGetStatus", 1));
            @SuppressWarnings("unchecked") List<Document> mem = (List) ret.get("members");
            if (mem == null) {
                return null;
            }
            //noinspection unchecked
            mem.stream().filter(d -> d.get("optime") instanceof Map).forEach(d -> d.put("optime", ((Map<String, Document>) d.get("optime")).get("ts")));
            return convertBSON(ret);
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
        //        return null;
    }

    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        return DriverHelper.doCall(() -> {
            Document ret = mongo.getDatabase(db).runCommand(new BasicDBObject("dbstats", 1));
            return convertBSON(ret);
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
        //        return null;
    }

    @Override
    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        return DriverHelper.doCall(() -> {
            Document ret = mongo.getDatabase(db).runCommand(new Document("collStats", coll));
            return convertBSON(ret);
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
        //        return null;
    }

    @Override
    public Map<String, Object> getOps(long threshold) {
        throw new RuntimeException("Not implemented yet, sorry...");
        //        return null;
    }

    @Override
    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(cmd);
        return DriverHelper.doCall(() -> {
            Document ret;
            if (currentTransaction.get() != null) {
                ret = mongo.getDatabase(db).runCommand(currentTransaction.get().getSession(), new BasicDBObject(cmd));
            } else {
                ret = mongo.getDatabase(db).runCommand(new BasicDBObject(cmd));

            }
            return convertBSON(ret);
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    public MorphiumCursor initAggregationIteration(String db, String collection, List<Map<String, Object>> aggregationPipeline, ReadPreference readPreference, Collation collation, int batchSize, final Map<String, Object> findMetaData) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(aggregationPipeline);
        //noinspection ConstantConditions
        return DriverHelper.doCall(() -> {

            MongoDatabase database = mongo.getDatabase(db);
            //                MongoDatabase database = mongo.getDatabase(db);
//            DBCollection coll = getColl(database, collection, readPreference, null);
            MongoCollection<Document> c = getCollection(database, collection, readPreference, null);
            List<BasicDBObject> pipe = new ArrayList<>();
            aggregationPipeline.stream().forEach((x) -> pipe.add(new BasicDBObject(x)));
            AggregateIterable<Document> it = currentTransaction.get() == null ? c.aggregate(pipe) : c.aggregate(currentTransaction.get().getSession(), pipe);

            if (batchSize != 0) {
                it.batchSize(batchSize);
            } else {
                it.batchSize(defaultBatchSize);
            }
            if (collation != null) {
                com.mongodb.client.model.Collation col = getCollation(collation);
                it.collation(col);
            }
            MongoCursor<Document> ret = it.iterator();
//            DBCursor ret = coll.find(new BasicDBObject(query), projection != null ? new BasicDBObject(projection) : null);
            handleMetaData(findMetaData, ret);

            List<Map<String, Object>> values = new ArrayList<>();

            while (ret.hasNext()) {
                Document d = ret.next();
                Map<String, Object> obj = convertBSON(d);
                values.add(obj);
                int cnt = values.size();
                if ((cnt >= batchSize && batchSize != 0) || (cnt >= 1000 && batchSize == 0)) {
                    break;
                }
            }
            MorphiumCursor crs = new MorphiumCursor();
            crs.setBatchSize(batchSize);

            if (values.size() < batchSize || values.size() < 1000 && batchSize == 0) {
                ret.close();
            } else {
                crs.setInternalCursorObject(ret);
            }
//            if (ret.hasNext() && ret.getServerCursor() != null) {
////                crs.setCursorId(ret.getServerCursor().getId());
////            }
            crs.setBatch(values);
            return crs;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    public MorphiumCursor initIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, Collation collation, final Map<String, Object> findMetaData) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        //noinspection ConstantConditions
        return DriverHelper.doCall(() -> {

            MongoDatabase database = mongo.getDatabase(db);
            //                MongoDatabase database = mongo.getDatabase(db);
//            DBCollection coll = getColl(database, collection, readPreference, null);
            MongoCollection<Document> c = getCollection(database, collection, readPreference, null);
            FindIterable<Document> it = currentTransaction.get() == null ? c.find(new BasicDBObject(query)) : c.find(currentTransaction.get().getSession(), new BasicDBObject(query));
            if (projection != null && !projection.isEmpty()) {
                it.projection(new BasicDBObject(projection));
            }
            if (sort != null && !sort.isEmpty()) {
                it.sort(new BasicDBObject(sort));
            }
            if (skip != 0) {
                it.skip(skip);
            }
            if (limit != 0) {
                it.limit(limit);
            }
            if (batchSize != 0) {
                it.batchSize(batchSize);
            } else {
                it.batchSize(defaultBatchSize);
            }
            if (collation != null) {
                com.mongodb.client.model.Collation col = getCollation(collation);
                it.collation(col);
            }
            MongoCursor<Document> ret = it.iterator();
//            DBCursor ret = coll.find(new BasicDBObject(query), projection != null ? new BasicDBObject(projection) : null);
            handleMetaData(findMetaData, ret);

            List<Map<String, Object>> values = new ArrayList<>();

            while (ret.hasNext()) {
                Document d = ret.next();
                Map<String, Object> obj = convertBSON(d);
                values.add(obj);
                int cnt = values.size();
                if ((cnt >= batchSize && batchSize != 0) || (cnt >= 1000 && batchSize == 0)) {
                    break;
                }
            }

            MorphiumCursor crs = new MorphiumCursor();
            crs.setBatchSize(batchSize);

            if (values.size() < batchSize || values.size() < 1000 && batchSize == 0) {
                ret.close();
            } else {
                crs.setInternalCursorObject(ret);
            }
//            if (ret.hasNext() && ret.getServerCursor() != null) {
////                crs.setCursorId(ret.getServerCursor().getId());
////            }
            crs.setBatch(values);
            return crs;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }


    @Override
    public void watch(String db, int maxWaitTime, boolean fullDocumentOnUpdate, List<Map<String, Object>> pipeline, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        watch(db, null, maxWaitTime, fullDocumentOnUpdate, pipeline, cb);
    }

    private void processChangeStreamEvent(DriverTailableIterationCallback cb, ChangeStreamDocument<Document> doc, long start) {
        try {
            Map<String, Object> obj = new HashMap<>();
            obj.put("clusterTime", Objects.requireNonNull(doc.getClusterTime()).getValue());
            if (doc.getDocumentKey().get("_id") instanceof BsonNull) {
                return;
            }

            if (doc.getDocumentKey() != null && doc.getDocumentKey().get("_id") instanceof BsonObjectId) {
                obj.put("documentKey", new MorphiumId(((BsonObjectId) doc.getDocumentKey().get("_id")).getValue().toByteArray()));
            }
            obj.put("operationType", doc.getOperationType().getValue());
            if (doc.getFullDocument() != null) {
                obj.put("fullDocument", new LinkedHashMap<>(doc.getFullDocument()));
            }
            if (doc.getResumeToken() != null) {
                obj.put("resumeToken", new LinkedHashMap<String, Object>(doc.getResumeToken()));
            }
            if (doc.getNamespace() != null) {
                obj.put("collectionName", doc.getNamespace().getCollectionName());
                obj.put("dbName", doc.getNamespace().getDatabaseName());
            }
            if (doc.getUpdateDescription() != null) {
                obj.put("removedFields", doc.getUpdateDescription().getRemovedFields());
                obj.put("updatedFields", new LinkedHashMap<String, Object>(doc.getUpdateDescription().getUpdatedFields()));
            }

            DriverHelper.replaceBsonValues(obj);
            cb.incomingData(obj, System.currentTimeMillis() - start);
        } catch (IllegalArgumentException e) {
            //"Drop is not a valid OperationType" -> Bug in Driver
        }
    }

    @Override
    public void watch(String db, String collection, int maxWaitTime, boolean fullDocumentOnUpdate, List<Map<String, Object>> pipeline, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        DriverHelper.doCall(() -> {
            List<Bson> p;
            if (pipeline == null) {
                p = Collections.emptyList();
            } else {
                p = new ArrayList<>();
                for (Map<String, Object> o : pipeline) {
                    p.add(new BasicDBObject(o));
                }
            }

            while (cb.isContinued()) {
                if (mongo == null) break;

                ChangeStreamIterable<Document> it;
                if (collection != null) {
                    it = mongo.getDatabase(db).getCollection(collection).watch(p);
                } else {
                    it = mongo.getDatabase(db).watch(p);
                }
                it.maxAwaitTime(maxWaitTime, TimeUnit.MILLISECONDS);
                it.batchSize(defaultBatchSize);
                it.fullDocument(fullDocumentOnUpdate ? FullDocument.UPDATE_LOOKUP : FullDocument.DEFAULT);
//                it.startAtOperationTime(new BsonTimestamp(System.currentTimeMillis()-250));
                MongoCursor<ChangeStreamDocument<Document>> iterator = it.iterator();
                long start = System.currentTimeMillis();
                while (cb.isContinued()) {
                    if (mongo == null) {
                        break; //connection closed!
                    }
                    ChangeStreamDocument<Document> doc = iterator.tryNext();
                    if (doc == null) {
                        try {
                            Thread.sleep(250);
                        } catch (InterruptedException e) {
                            //swallow
                        }
                        continue;
                    }
                    if (cb.isContinued()) {
                        processChangeStreamEvent(cb, doc, start);
                    }
                }
                iterator.close();

            }

            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }


    @Override
    public void tailableIteration(String db, String collection, final Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, int timeout, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        //noinspection ConstantConditions
        DriverHelper.doCall(() -> {
            MongoDatabase database = mongo.getDatabase(db);
            //                MongoDatabase database = mongo.getDatabase(db);
            MongoCollection<Document> coll = getCollection(database, collection, readPreference, null);
            FindIterable<Document> ret = currentTransaction.get() == null ? coll.find(new BasicDBObject(query)) : coll.find(currentTransaction.get().getSession(), new BasicDBObject(query));
            if (projection != null) {
                ret.projection(new BasicDBObject(projection));
            }

            if (sort != null) {
                ret = ret.sort(new BasicDBObject(sort));
            }
            if (skip != 0) {
                ret = ret.skip(skip);
            }
            if (limit != 0) {
                ret = ret.limit(limit);
            }
            if (batchSize != 0) {
                ret.batchSize(batchSize);
            } else {
                ret.batchSize(defaultBatchSize);
            }

            ret.cursorType(CursorType.TailableAwait);
            if (timeout == 0) {
                ret.noCursorTimeout(true);
            } else {
                ret.maxAwaitTime(timeout, TimeUnit.MILLISECONDS);
                ret.maxTime(timeout, TimeUnit.MILLISECONDS);
            }
            long start = System.currentTimeMillis();
            for (Document d : ret) {
                if (mongo == null || d == null) break; //connection closed!
                Map<String, Object> obj = convertBSON(d);
                cb.incomingData(obj, System.currentTimeMillis() - start);
                if (!cb.isContinued()) {
                    break;
                }
            }
            //no close possible
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    private void handleMetaData(Map<String, Object> findMetaData, MongoCursor<Document> ret) {
        if (findMetaData != null) {
            if (ret.getServerAddress() != null) {
                findMetaData.put("server", ret.getServerAddress().getHost() + ":" + ret.getServerAddress().getPort());
            }
            if (ret.getServerCursor() != null) {
                findMetaData.put("cursorId", ret.getServerCursor().getId());
            }
        }
    }

    @Override
    public MorphiumCursor nextIteration(final MorphiumCursor crs) throws MorphiumDriverException {
        //noinspection ConstantConditions
        return DriverHelper.doCall(() -> {
            List<Map<String, Object>> values = new ArrayList<>();
            int batchSize = crs.getBatchSize();
            @SuppressWarnings("unchecked") MongoCursor<Document> ret = (MongoCursor<Document>) crs.getInternalCursorObject();
            if (ret == null) {
                return null; //finished
            }
            while (ret.hasNext()) {
                Document d = ret.next();
                Map<String, Object> obj = convertBSON(d);
                values.add(obj);
                int cnt = values.size();
                if (cnt >= batchSize && batchSize != 0 || cnt >= 1000 && batchSize == 0) {
                    break;
                }
                if (mongo == null) {
                    return null; //connection closed!
                }
            }
            MorphiumCursor crs1 = new MorphiumCursor();
            crs1.setBatchSize(batchSize);
//            crs1.setCursorId(ret.getCursorId());
            if ((batchSize != 0 && values.size() < batchSize) || (batchSize == 0 && values.size() < 1000)) {
                ret.close();
            } else {
                crs1.setInternalCursorObject(ret);
            }
            crs1.setBatch(values);
            return crs1;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        DriverHelper.doCall(() -> {
            log.debug("Closing iterator / cursor");
            if (crs != null) {
                @SuppressWarnings("unchecked") MongoCursor<Document> ret = (MongoCursor<Document>) crs.getInternalCursorObject();
                ret.close();
            }
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, Collation collation, final Map<String, Object> findMetaData) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        //noinspection unused
        return DriverHelper.doCall(() -> {
            MongoDatabase database = mongo.getDatabase(db);
            MongoCollection<Document> coll = getCollection(database, collection, currentTransaction.get() == null ? readPreference : ReadPreference.primary(), null);

            FindIterable<Document> it = currentTransaction.get() == null ? coll.find(new BasicDBObject(query)) : coll.find(currentTransaction.get().session, new BasicDBObject(query));

            if (projection != null) {
                it.projection(new BasicDBObject(projection));
            }
            if (sort != null) {
                it.sort(new BasicDBObject(sort));
            }
            if (skip != 0) {
                it.skip(skip);
            }
            if (limit != 0) {
                it.limit(limit);
            }
            if (batchSize != 0) {
                it.batchSize(batchSize);
            } else {
                it.batchSize(defaultBatchSize);
            }
            it.maxAwaitTime(getMaxWaitTime(), TimeUnit.MILLISECONDS);
            it.maxTime(getMaxWaitTime(), TimeUnit.MILLISECONDS);
            if (collation != null) {
                com.mongodb.client.model.Collation col = getCollation(collation);
                it.collation(col);
            }
            MongoCursor<Document> ret = it.iterator();
            handleMetaData(findMetaData, ret);

            List<Map<String, Object>> values = new ArrayList<>();
            while (ret.hasNext()) {
                Document d = ret.next();
                Map<String, Object> obj = convertBSON(d);
                values.add(obj);
            }
            ret.close();
            return values;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    private com.mongodb.client.model.Collation getCollation(Collation collation) {
        if (collation == null) return null;
        com.mongodb.client.model.Collation.Builder bld = com.mongodb.client.model.Collation.builder();
        if (collation.getLocale() != null) {
            bld.locale(collation.getLocale());
        } else {
            bld.locale("simple");
        }
        if (collation.getBackwards() != null)
            bld.backwards(collation.getBackwards());
        if (collation.getCaseLevel() != null)
            bld.caseLevel(collation.getCaseLevel());
        if (collation.getAlternate() != null)
            bld.collationAlternate(collation.getAlternate().equals(Collation.Alternate.NON_IGNORABLE) ? CollationAlternate.NON_IGNORABLE : CollationAlternate.SHIFTED);
        if (collation.getCaseFirst() != null)
            bld.collationCaseFirst(CollationCaseFirst.fromString(collation.getCaseFirst().getMongoText()));
        if (collation.getMaxVariable() != null)
            bld.collationMaxVariable(CollationMaxVariable.fromString(collation.getMaxVariable().getMongoText()));
        if (collation.getStrength() != null)
            bld.collationStrength(CollationStrength.fromInt(collation.getStrength().getMongoValue()));

        return bld.build();
    }

    private <E extends Object> Map<String, Object> convertBSON(Map<String, E> d) {
        Map<String, Object> obj = new HashMap<>();

        for (Entry<String, E> entry: d.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof BsonTimestamp) {
                value = (((BsonTimestamp) value).getTime() * 1000);
            } else if (value instanceof BsonDocument) {
                value = convertBSON((BsonDocument) value);
            } else if (value instanceof BsonBoolean) {
                value = ((BsonBoolean) value).getValue();
            } else if (value instanceof BsonDateTime) {
                value = ((BsonDateTime) value).getValue();
            } else if (value instanceof BsonInt32) {
                value = ((BsonInt32) value).getValue();
            } else if (value instanceof BsonInt64) {
                value = ((BsonInt64) value).getValue();
            } else if (value instanceof BsonDouble) {
                value = ((BsonDouble) value).getValue();
            } else if (value instanceof BsonUndefined) {
                value = null;
            } else if (value instanceof BsonRegularExpression) {
                BsonRegularExpression bsonRegularExpression = (BsonRegularExpression)value;
                try {
                    Method getOptionsAsIntMethod = PatternCodec.class.getDeclaredMethod("getOptionsAsInt", BsonRegularExpression.class);
                    getOptionsAsIntMethod.setAccessible(true);
                    value = Pattern.compile(bsonRegularExpression.getPattern(), (int) getOptionsAsIntMethod.invoke(null, bsonRegularExpression));
                } catch (Exception e) {
                    log.debug(e.toString(), e);
                }
            } else if (value instanceof ObjectId) {
                value = new MorphiumId(((ObjectId) value).toByteArray());
            } else if (value instanceof BasicDBList) {
                value = convertBSON(Collections.singletonMap("list", new ArrayList(((BasicDBList) value)))).get("list");
            } else //noinspection ConditionCoveredByFurtherCondition,DuplicateCondition,DuplicateCondition
                if (value instanceof BasicBSONObject
                        || value instanceof Document
                        || value instanceof BSONObject) {
                    value = convertBSON((Map<String, Object>) value);
                } else if (value instanceof Binary) {
                    Binary b = (Binary) value;
                    value = b.getData();
                } else if (value instanceof BsonString) {
                    value = value.toString();
                } else if (value instanceof List) {
                    List<Object> v = new ArrayList<>();

                    for (Object o : (List) value) {
                        if (o instanceof BSONObject || o instanceof BsonValue || o instanceof Map)
                        //noinspection unchecked
                        {
                            //noinspection unchecked
                            v.add(convertBSON((Map<String, Object>) o));
                        } else if (o instanceof ObjectId) {
                            //noinspection unchecked
                            v.add(new MorphiumId(((ObjectId) o).toString()));
                        } else
                        //noinspection unchecked
                        {
                            //noinspection unchecked
                            v.add(o);
                        }
                    }
                    value = v;
                } else //noinspection ConstantConditions
                    if (value instanceof BsonArray) {
                        value = convertBSON(Collections.singletonMap("list", new ArrayList(((BsonArray) value).getValues()))).get("list");
                    } else //noinspection ConstantConditions,DuplicateCondition
                        if (value instanceof Document) {
                            value = convertBSON((Document) value);
                        } else //noinspection ConstantConditions,DuplicateCondition
                            if (value instanceof BSONObject) {
                                value = convertBSON((Map<String, Object>) value);
                            }
            obj.put(entry.getKey(), value);
        }
        return obj;
    }

    public MongoCollection<Document> getCollection(MongoDatabase database, String collection, ReadPreference readPreference, de.caluga.morphium.driver.WriteConcern wc) {
        MongoCollection<Document> coll = database.getCollection(collection);
        com.mongodb.ReadPreference prf;

        if (readPreference == null) {
            readPreference = defaultReadPreference;
        }
        if (readPreference != null) {
            TagSet tags = null;
            if (readPreference.getTagSet() != null) {
                List<Tag> tagList = readPreference.getTagSet().entrySet().stream().map(e -> new Tag(e.getKey(), e.getValue())).collect(Collectors.toList());
                tags = new TagSet(tagList);

            }
            switch (readPreference.getType()) {
                case NEAREST:
                    if (tags != null) {
                        prf = com.mongodb.ReadPreference.nearest(tags);
                    } else {
                        prf = com.mongodb.ReadPreference.nearest();
                    }
                    break;
                case PRIMARY:
                    prf = com.mongodb.ReadPreference.primary();
                    if (tags != null) {
                        log.warn("Cannot use tags with primary only read preference!");
                    }
                    break;
                case PRIMARY_PREFERRED:
                    if (tags != null) {
                        prf = com.mongodb.ReadPreference.primaryPreferred(tags);
                    } else {
                        prf = com.mongodb.ReadPreference.primaryPreferred();
                    }
                    break;
                case SECONDARY:
                    if (tags != null) {
                        prf = com.mongodb.ReadPreference.secondary(tags);
                    } else {
                        prf = com.mongodb.ReadPreference.secondary();
                    }
                    break;
                case SECONDARY_PREFERRED:
                    if (tags != null) {
                        prf = com.mongodb.ReadPreference.secondaryPreferred(tags);
                    } else {
                        prf = com.mongodb.ReadPreference.secondaryPreferred();
                    }
                    break;
                default:
                    log.error("Unhandeled read preference: " + readPreference);
                    prf = null;

            }
            if (prf != null) {
                coll = coll.withReadPreference(prf);
            }
        }

        if (wc != null) {
            com.mongodb.WriteConcern writeConcern;
            if (wc.getW() < 0) {
                //majority
                writeConcern = com.mongodb.WriteConcern.MAJORITY;
            } else {
                writeConcern = wc.toMongoWriteConcern();
            }
            coll = coll.withWriteConcern(writeConcern);
        }
        return coll;
    }

    @Override
    public long count(String db, String collection, Map<String, Object> query, Collation collation, ReadPreference rp) {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        MongoDatabase database = mongo.getDatabase(db);
        MongoCollection<Document> coll = getCollection(database, collection, rp, null);
        CountOptions co = new CountOptions();
        if (collation != null) {
            com.mongodb.client.model.Collation col = getCollation(collation);
            co.collation(col);
        }
        if (currentTransaction.get() != null) {
            if (collation != null) {
                return coll.countDocuments(currentTransaction.get().getSession(), new BasicDBObject(query), co);
            }
            return coll.countDocuments(currentTransaction.get().getSession(), new BasicDBObject(query));
        } else {
            if (collation != null) {
                return coll.countDocuments(new BasicDBObject(query), co);
            }
            return coll.countDocuments(new BasicDBObject(query));
        }
    }

    @Override
    public long estimatedDocumentCount(String db, String collection, ReadPreference rp) {
        MongoDatabase database = mongo.getDatabase(db);
        MongoCollection<Document> coll = getCollection(database, collection, rp, null);
        return coll.estimatedDocumentCount();
     }

    @Override
    public Map<String, Integer> store(String db, String collection, List<Map<String, Object>> objs, de.caluga.morphium.driver.WriteConcern wc) throws MorphiumDriverException {

        return DriverHelper.doCall(() -> {
            DriverHelper.replaceMorphiumIdByObjectId(objs);
            MongoCollection<Document> c = mongo.getDatabase(db).getCollection(collection);
            Map<String, Integer> ret = new HashMap<>();
            //                mongo.getDB(db).getCollection(collection).save()
            int total = objs.size();
            int updated = 0;
            for (Map<String, Object> toUpdate : objs) {

                Document filter = new Document();
                Object id = toUpdate.get("_id");
//                o.upsert(id == null);
                if (id instanceof MorphiumId) {
                    id = new ObjectId(id.toString());
                }
                filter.put("_id", id);
                //Hack to detect versioning
                if (toUpdate.get(MorphiumDriver.VERSION_NAME) != null) {
                    filter.put(MorphiumDriver.VERSION_NAME, toUpdate.get(MorphiumDriver.VERSION_NAME));
                    toUpdate.put(MorphiumDriver.VERSION_NAME, (Long) toUpdate.get(MorphiumDriver.VERSION_NAME) + 1l);
                }

                //                    toUpdate.remove("_id");
                //                    Document update = new Document("$set", toUpdate);
                Document tDocument = new Document(toUpdate);

                for (String k : tDocument.keySet()) {
                    if (tDocument.get(k) instanceof byte[]) {
                        BsonBinary b = new BsonBinary((byte[]) tDocument.get(k));
                        tDocument.put(k, b);
                    }
                }
                ReplaceOptions r = new ReplaceOptions();
                if (tDocument.get("_id") instanceof String) { //should only be necessary when dealing with strings
                    r.collation(com.mongodb.client.model.Collation.builder().locale("simple").build());
                }
                r.upsert(true);

                tDocument.remove("_id"); //not needed
                //noinspection unchecked
                try {

                    UpdateResult res;
                    if (currentTransaction.get() == null) {
                        //noinspection unchecked
                        res = c.replaceOne(filter, tDocument, r);
                    } else {
                        //noinspection unchecked
                        res = c.replaceOne(currentTransaction.get().getSession(), filter, tDocument, r);
                    }
                    updated += res.getModifiedCount();
                    id = toUpdate.get("_id");
                    if (id instanceof ObjectId) {
                        toUpdate.put("_id", new MorphiumId(((ObjectId) id).toHexString()));
                    }
                    if (toUpdate.get(MorphiumDriver.VERSION_NAME) != null && res.getModifiedCount() == 0) {
                        throw new MorphiumDriverException("Version mismatch!");
                    }
                } catch (MongoWriteException e) {
                    //log.error("",e);
                    if (e.getMessage().contains("E11000 duplicate key error")) {
                        throw new ConcurrentModificationException("Version mismach - write failed", e);
                    } else {
                        throw e;
                    }
                }

            }
            ret.put("total", total);
            ret.put("modified", updated);
            return ret;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    public void insert(String db, String collection, List<Map<String, Object>> objs, de.caluga.morphium.driver.WriteConcern wc) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(objs);
        if (objs == null || objs.isEmpty()) {
            return;
        }
        final List<Document> lst = objs.stream().map(Document::new).collect(Collectors.toList());

        for (Document d : lst) {
            for (String k : d.keySet()) {
                if (d.get(k) instanceof byte[]) {
                    BsonBinary b = new BsonBinary((byte[]) d.get(k));
                    d.put(k, b);
                }
            }
        }
        DriverHelper.doCall(() -> {
            MongoCollection<Document> c = mongo.getDatabase(db).getCollection(collection);
            if (lst.size() == 1) {
                //noinspection unchecked
                InsertOneOptions op = new InsertOneOptions().bypassDocumentValidation(true);
                if (currentTransaction.get() == null) {
                    //noinspection unchecked
                    c.insertOne(lst.get(0), op);
                } else {
                    //noinspection unchecked
                    c.insertOne(currentTransaction.get().getSession(), lst.get(0), op);
                }
            } else {
                InsertManyOptions imo = new InsertManyOptions();
                imo.ordered(false);
                imo.bypassDocumentValidation(true);
                //noinspection unchecked
                if (currentTransaction.get() == null) {
                    //noinspection unchecked
                    c.insertMany(lst, imo);
                } else {
                    //noinspection unchecked
                    c.insertMany(currentTransaction.get().getSession(), lst, imo);
                }
            }

//            for (int i = 0; i < lst.size(); i++) {
//                Object id = lst.get(i).get("_id");
//                if (id instanceof ObjectId) {
//                    id = new MorphiumId(((ObjectId) id).toHexString());
//                }
//                objs.get(i).put("_id", id);
//            }
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);

    }

    @Override
    public Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> op, boolean multiple, boolean upsert, Collation collation, de.caluga.morphium.driver.WriteConcern wc) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        DriverHelper.replaceMorphiumIdByObjectId(op);
        return DriverHelper.doCall(() -> {
            UpdateOptions opts = new UpdateOptions();
            WriteConcern w = null;
            if (wc == null)
                w = de.caluga.morphium.driver.WriteConcern.getWc(getDefaultW(), isDefaultFsync(), isDefaultJ(), getDefaultWriteTimeout()).toMongoWriteConcern();
            else w = wc.toMongoWriteConcern();
            if (collation != null) {
                com.mongodb.client.model.Collation col = getCollation(collation);
                opts.collation(col);
            }
            opts.upsert(upsert);

            UpdateResult res;
            if (multiple) {
                if (currentTransaction.get() == null) {

                    res = mongo.getDatabase(db).getCollection(collection).withWriteConcern(w).updateMany(new BasicDBObject(query), new BasicDBObject(op), opts);
                } else {
                    res = mongo.getDatabase(db).getCollection(collection).withWriteConcern(w).updateMany(currentTransaction.get().getSession(), new BasicDBObject(query), new BasicDBObject(op), opts);
                }
            } else {
                if (currentTransaction.get() == null) {
                    res = mongo.getDatabase(db).getCollection(collection).withWriteConcern(w).updateOne(new BasicDBObject(query), new BasicDBObject(op), opts);
                } else {
                    res = mongo.getDatabase(db).getCollection(collection).withWriteConcern(w).updateOne(currentTransaction.get().getSession(), new BasicDBObject(query), new BasicDBObject(op), opts);

                }
            }
            Map<String, Object> ret = new HashMap<>();
            if (w.isAcknowledged()) {
                ret.put("matched", res.getMatchedCount());
                ret.put("modified", res.getModifiedCount());
                ret.put("acc", res.wasAcknowledged());
            }
            return ret;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);

    }

    @Override
    public Map<String, Object> delete(String db, String collection, Map<String, Object> query, boolean multiple, Collation collation, de.caluga.morphium.driver.WriteConcern wc) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        return DriverHelper.doCall(() -> {
            MongoDatabase database = mongo.getDatabase(db);
            MongoCollection<Document> coll = database.getCollection(collection);
            DeleteResult res;
            DeleteOptions opts = new DeleteOptions();
            if (collation != null) {
                com.mongodb.client.model.Collation col = getCollation(collation);
                opts.collation(col);
            }

            if (multiple) {
                if (currentTransaction.get() == null) {
                    res = coll.deleteMany(new BasicDBObject(query), opts);
                } else {
                    res = coll.deleteMany(currentTransaction.get().getSession(), new BasicDBObject(query), opts);

                }
            } else {
                if (currentTransaction.get() == null) {
                    res = coll.deleteOne(new BasicDBObject(query), opts);
                } else {
                    res = coll.deleteOne(currentTransaction.get().getSession(), new BasicDBObject(query), opts);
                }
            }
            Map<String, Object> r = new HashMap<>();
            r.put("deleted", res.getDeletedCount());
            r.put("acc", res.wasAcknowledged());

            return r;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    public void drop(String db, String collection, de.caluga.morphium.driver.WriteConcern wc) throws MorphiumDriverException {
        DriverHelper.doCall(() -> {
            MongoDatabase database = mongo.getDatabase(db);
            MongoCollection<Document> coll = database.getCollection(collection);
            if (currentTransaction.get() != null) {
                coll.drop(currentTransaction.get().getSession());
            } else {
                coll.drop();
            }
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    public void drop(String db, de.caluga.morphium.driver.WriteConcern wc) throws
            MorphiumDriverException {
        DriverHelper.doCall(() -> {
            MongoDatabase database = mongo.getDatabase(db);
            if (wc != null) {
                com.mongodb.WriteConcern writeConcern = wc.toMongoWriteConcern();
                database = database.withWriteConcern(writeConcern);
            }
            if (currentTransaction.get() != null) {
                database.drop(currentTransaction.get().getSession());
            } else {
                database.drop();
            }
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    public boolean exists(String db) {
        for (String dbName : mongo.listDatabaseNames()) {
            if (dbName.equals(db)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<Object> distinct(String db, String collection, String field, final Map<String, Object> filter, Collation collation, ReadPreference rp) throws MorphiumDriverException {
        DriverHelper.replaceMorphiumIdByObjectId(filter);
        final List<Object> ret = new ArrayList<>();
        DriverHelper.doCall(() -> {
            DistinctIterable<?> it;
            if (currentTransaction.get() == null) {
                //need to find return-type for distinct otherwise the damn driver will throw exeptions!
                List<Map<String, Object>> r = find(db, collection, filter, null, new BasicDBObject(field, 1), 0, 1, 1, defaultReadPreference, collation, null);
                if (r == null || r.size() == 0) return null;
                DistinctIterable<? extends Object> lst = getCollection(mongo.getDatabase(db), collection, getDefaultReadPreference(), null).distinct(field, new BasicDBObject(filter), r.get(0).get(field).getClass());
                for (Object o : lst) {
                    ret.add(o);
                }
            } else {

                List<Map<String, Object>> r = find(db, collection, filter, null, new BasicDBObject(field, 1), 0, 1, 1, defaultReadPreference, collation, null);
                if (r == null || r.size() == 0) return null;

                it = getCollection(mongo.getDatabase(db), collection, getDefaultReadPreference(), null).distinct(currentTransaction.get().getSession(), field, new BasicDBObject(filter), r.get(0).get(field).getClass());
                if (collation != null) {
                    com.mongodb.client.model.Collation col = getCollation(collation);
                    it.collation(col);
                }
                for (Object d : it) {
                    ret.add(d);
                }
            }
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);

        return ret;
    }

    @Override
    public boolean exists(String db, String collection) throws MorphiumDriverException {
        Map<String, Object> found = DriverHelper.doCall(() -> {


//            if (currentTransaction.get() != null) {
//                result = mongo.getDatabase(db).runCommand(new Document("listCollections", 1));
//                log.warn("Check for databases in multi-document transaction is not possible!");
//
//            } else {
//            }
            for (Document d : mongo.getDatabase(db).listCollections()) {
                if (d.get("name").equals(collection)) {
                    return d;
                }
            }
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);


        return found != null && !found.isEmpty();
    }

    @Override
    public List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException {
        // noinspection unchecked,ConstantConditions
        return DriverHelper.doCall(() -> {
            List<Map<String, Object>> values = new ArrayList<>();

            ListIndexesIterable<Document> indexes;
            if (currentTransaction.get() != null) {
                indexes = mongo.getDatabase(db).getCollection(collection).listIndexes(currentTransaction.get().getSession());
            } else {
                indexes = mongo.getDatabase(db).getCollection(collection).listIndexes();
            }
            for (Document d : indexes) {
                values.add(new HashMap<>(d));
            }
            return values;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);

    }

    @Override
    public List<String> getCollectionNames(String db) throws MorphiumDriverException {
        return DriverHelper.doCall(() -> {
            final List<String> ret = new ArrayList<>();
            if (currentTransaction.get() == null) {
                for (String c : mongo.getDatabase(db).listCollectionNames()) {
                    ret.add(c);
                }
            } else {
                for (String c : mongo.getDatabase(db).listCollectionNames(currentTransaction.get().getSession())) {
                    ret.add(c);
                }
            }
            return ret;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);
    }

    @Override
    public Map<String, Object> findAndOneAndDelete(String db, String col, Map<String, Object> query, Map<String, Integer> sort, Collation collation) {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        FindOneAndDeleteOptions opts = new FindOneAndDeleteOptions();
        if (collation != null) {
            com.mongodb.client.model.Collation c = getCollation(collation);
            opts.collation(c);
        }
        if (sort != null) {
            opts.sort(new BasicDBObject(sort));
        }

        if (currentTransaction.get() != null) {
            Document ret = mongo.getDatabase(db).getCollection(col).findOneAndDelete(currentTransaction.get().getSession(), new BasicDBObject(query));
            return convertBSON(ret);
        }
        Document ret = mongo.getDatabase(db).getCollection(col).findOneAndDelete(new BasicDBObject(query));
        return convertBSON(ret);
    }


    @Override
    public Map<String, Object> findAndOneAndUpdate(String db, String col, Map<String, Object> query, Map<String, Object> update, Map<String, Integer> sort, Collation collation) {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
        if (collation != null) {
            com.mongodb.client.model.Collation c = getCollation(collation);
            opts.collation(c);
        }
        if (sort != null) {
            opts.sort(new BasicDBObject(sort));
        }
        if (currentTransaction.get() != null) {
            Document ret = mongo.getDatabase(db).getCollection(col).findOneAndUpdate(currentTransaction.get().getSession(), new BasicDBObject(query), new BasicDBObject(update), opts);
            return convertBSON(ret);
        }
        Document ret = mongo.getDatabase(db).getCollection(col).findOneAndUpdate(new BasicDBObject(query), new BasicDBObject(update), opts);
        return convertBSON(ret);
    }


    @Override
    public Map<String, Object> findAndOneAndReplace(String db, String col, Map<String, Object> query, Map<String, Object> replacement, Map<String, Integer> sort, Collation collation) {
        DriverHelper.replaceMorphiumIdByObjectId(query);
        FindOneAndReplaceOptions opts = new FindOneAndReplaceOptions();
        if (collation != null) {
            com.mongodb.client.model.Collation c = getCollation(collation);
            opts.collation(c);
        }
        if (sort != null) {
            opts.sort(new BasicDBObject(sort));
        }
        if (currentTransaction.get() != null) {
            Document ret = mongo.getDatabase(db).getCollection(col).findOneAndReplace(currentTransaction.get().getSession(), new BasicDBObject(query), new Document(replacement), opts);
            return convertBSON(ret);
        }
        Document ret = mongo.getDatabase(db).getCollection(col).findOneAndReplace(new BasicDBObject(query), new Document(replacement), opts);
        return convertBSON(ret);
    }


    @Override
    public List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline,
                                               boolean explain, boolean allowDiskUse, Collation collation, ReadPreference readPreference) {
        DriverHelper.replaceMorphiumIdByObjectId(pipeline);
        //noinspection unchecked
        List<BasicDBObject> list = pipeline.stream().map(BasicDBObject::new).collect(Collectors.toList());


        if (explain) {
//            @SuppressWarnings({"unchecked", "ConstantConditions"}) CommandResult ret = getColl(mongo.getDatabase(db), collection, getDefaultReadPreference(), null).explainAggregate(list, null);
//            List<Map<String, Object>> o = new ArrayList<>();
//            o.add(new HashMap<>(ret));
//            return o;
            throw new IllegalArgumentException("Not implemented yet!");
        } else {
            MongoCollection<Document> c = getCollection(mongo.getDatabase(db), collection, getDefaultReadPreference(), null);
            AggregateIterable<Document> it;
            if (currentTransaction.get() == null) {
                //noinspection unchecked,unchecked
                it = c.aggregate(list, Document.class);

            } else {
                //noinspection unchecked,unchecked
                it = c.aggregate(currentTransaction.get().getSession(), list, Document.class);
            }
            it.allowDiskUse(allowDiskUse);
            if (collation != null) {
                com.mongodb.client.model.Collation col = getCollation(collation);
                it.collation(col);
            }
//            @SuppressWarnings("unchecked") Cursor ret = getColl(mongo.getDB(db), collection, getDefaultReadPreference(), null).aggregate(list, opts);
            List<Map<String, Object>> result = new ArrayList<>();

            for (Document doc : it) {
                //noinspection unchecked
                result.add(convertBSON(doc));
            }
            return result;

        }

    }

//
//    @Override
//    public int getHeartbeatConnectTimeout() {
//        return heartbeatConnectTimeout;
//    }
//
//    @Override
//    public void setHeartbeatConnectTimeout(int heartbeatConnectTimeout) {
//        this.heartbeatConnectTimeout = heartbeatConnectTimeout;
//    }

    @Override
    public int getMaxWaitTime() {
        return maxWaitTime;
    }

    @Override
    public void setMaxWaitTime(int maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
    }

    @Override
    public int getServerSelectionTimeout() {
        return serverSelectionTimeout;
    }

    @Override
    public void setServerSelectionTimeout(int serverSelectionTimeout) {
        this.serverSelectionTimeout = serverSelectionTimeout;
    }

    @Override
    public int getRetriesOnNetworkError() {
        return retriesOnNetworkError;
    }

    @Override
    public void setRetriesOnNetworkError(int retriesOnNetworkError) {
        this.retriesOnNetworkError = retriesOnNetworkError;
    }

    public int getSleepBetweenErrorRetries() {
        return sleepBetweenErrorRetries;
    }

    public void setSleepBetweenErrorRetries(int sleepBetweenErrorRetries) {
        this.sleepBetweenErrorRetries = sleepBetweenErrorRetries;
    }

    public Map<String, Object> getCollectionStats(String db, String coll, int scale, boolean verbose) throws MorphiumDriverException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("collStats", coll);
        cmd.put("scale", scale);
        cmd.put("verbose", verbose);
        cmd = runCommand(db, cmd);
        return cmd;
    }

    @Override
    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        Object capped = getCollectionStats(db, coll, 1024, false).get("capped");
        if (capped == null) return false;
        if (capped instanceof String) {
            return capped.equals("true");
        }
        return capped.equals(Boolean.TRUE) || capped.equals(1) || capped.equals(true);
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, de.caluga.morphium.driver.WriteConcern wc) {
        return new MongodbBulkContext(m, db, collection, this, ordered, wc);
    }


    public MongoDatabase getDb(String db) {
        return mongo.getDatabase(db);
    }

    @SuppressWarnings("unused")
    public MongoCollection<Document> getCollection(String db, String coll) {

        return mongo.getDatabase(db).getCollection(coll);
    }


    @Override
    public void createIndex(String db, String collection, Map<String, Object> index, Map<String, Object> options) throws MorphiumDriverException {
        DriverHelper.doCall(() -> {
            // BasicDBObject options1 = options == null ? new BasicDBObject() : new BasicDBObject(options);
            IndexOptions options1 = new IndexOptions();
            Method[] methods = IndexOptions.class.getMethods();
            if (options != null) {
                for (Map.Entry<String, Object> opt : options.entrySet()) {
                    if (opt.getKey().equals("")) continue;
                    try {
                        String name = opt.getKey();
                        if (name.equals("expireAfterSeconds")) {
                            //all is different!!!!
                            options1.expireAfter(((Integer) opt.getValue()).longValue(), TimeUnit.SECONDS);
                        } else {
                            Method method = null;
                            for (Method m : methods) {
                                if (m.getName().equals(opt.getKey())) {
                                    method = m;
                                    break;
                                }
                            }

                            method.setAccessible(true);
                            Object val = opt.getValue();
                            if (!method.getParameterTypes()[0].equals(opt.getValue().getClass())) {
                                if (method.getParameterTypes()[0].equals(boolean.class) || method.getParameterTypes()[0].equals(Boolean.class)) {
                                    val = val.equals("true") || val.equals(1);
                                }
                            }
                            method.invoke(options1, val);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.info("Could not find setting: " + opt.getKey());
                    }
                }
            }
            mongo.getDatabase(db).getCollection(collection).createIndex(new BasicDBObject(index), options1);
            return null;
        }, retriesOnNetworkError, sleepBetweenErrorRetries);

    }


    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing) {
        return mapReduce(db, collection, mapping, reducing, null, null, null);
    }

    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query) {
        return mapReduce(db, collection, mapping, reducing, query, null, null);
    }

    @Override
    public List<Map<String, Object>> mapReduce(String db, String collection, String mapping, String reducing, Map<String, Object> query, Map<String, Object> sorting, Collation collation) {
        MapReduceIterable<Document> res;
        if (currentTransaction.get() == null) {
            res = mongo.getDatabase(db).getCollection(collection).mapReduce(mapping, reducing);
        } else {
            res = mongo.getDatabase(db).getCollection(collection).mapReduce(currentTransaction.get().getSession(), mapping, reducing);
        }
        if (collation != null) {
            com.mongodb.client.model.Collation col = getCollation(collation);
            res.collation(col);
        }
        if (query != null) {
            BasicDBObject v = new BasicDBObject(query);
            res.filter(v);
        }
        if (sorting != null) {
            res.sort(new BasicDBObject(sorting));
        }
        ArrayList<Map<String, Object>> ret = new ArrayList<>();
        for (Document d : res) {
            //noinspection unchecked
            Map<String, Object> value = (Map) d.get("value");
            for (Map.Entry<String, Object> s : value.entrySet()) {
                if (s.getValue() instanceof ObjectId) {
                    value.put(s.getKey(), new MorphiumId(((ObjectId) s.getValue()).toHexString()));
                }
            }
            ret.add(value);
        }

        return ret;
    }


    @Override
    public void startTransaction() {
        if (currentTransaction.get() != null) {
            throw new IllegalArgumentException("Transaction in progress");
        }

        ClientSessionOptions.Builder b = ClientSessionOptions.builder();
        b.causallyConsistent(false);
        b.defaultTransactionOptions(TransactionOptions.builder().readConcern(ReadConcern.DEFAULT).readPreference(com.mongodb.ReadPreference.primary()).build());


        ClientSession ses = mongo.startSession(b.build());
        ses.startTransaction();

        MongoTransactionContext ctx = new MongoTransactionContext();
        ctx.setSession(ses);
        currentTransaction.set(ctx);
    }

    @Override
    public void commitTransaction() {
        if (currentTransaction.get() == null) {
            throw new IllegalArgumentException("No transaction in progress");
        }
        currentTransaction.get().getSession().commitTransaction();
        currentTransaction.set(null);
    }

    @Override
    public MorphiumTransactionContext getTransactionContext() {
        return currentTransaction.get();
    }

    @Override
    public void abortTransaction() {
        if (currentTransaction.get() == null) {
            throw new IllegalArgumentException("No transaction in progress");
        }
        currentTransaction.get().getSession().abortTransaction();
        currentTransaction.set(null);
    }

    @Override
    public void setTransactionContext(MorphiumTransactionContext ctx) {
        if (currentTransaction.get() != null) {
            throw new IllegalArgumentException("Transaction in progress!");
        }
        currentTransaction.set((MongoTransactionContext) ctx);
    }

    @Override
    public SSLContext getSslContext() {
        return this.sslContext;
    }

    @Override
    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public boolean isSslInvalidHostNameAllowed() {
        return sslInvalidHostNameAllowed;
    }

    public void setSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {
        this.sslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
    }
}
