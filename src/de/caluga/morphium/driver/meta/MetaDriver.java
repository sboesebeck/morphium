package de.caluga.morphium.driver.meta;

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.singleconnect.DriverBase;
import de.caluga.morphium.driver.singleconnect.SingleConnectThreaddedDriver;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Stephan Bösebeck
 * Date: 02.12.15
 * Time: 23:56
 * <p>
 * TODO: Add documentation here
 */
public class MetaDriver extends DriverBase {
    private volatile Logger log = new Logger(MetaDriver.class);
    private volatile static long seq;
    private volatile Map<String, List<Connection>> connectionPool = new ConcurrentHashMap<>();
    private volatile Map<String, List<Connection>> connectionsInUse = new ConcurrentHashMap<>();
    private volatile String currentMaster;
    private volatile Vector<String> secondaries = new Vector<>();
    private volatile long fastestAnswer = 10000000;
    private volatile String fastestHost = null;

    private static ReadPreference primary = ReadPreference.primary();
    private static ReadPreference secondary = ReadPreference.secondary();
    private static ReadPreference secondaryPrferred = ReadPreference.secondaryPreferred();
    private static ReadPreference primaryPreferred = ReadPreference.primaryPreferred();
    private static ReadPreference nearest = ReadPreference.primary();

    @Override
    public void connect() throws MorphiumDriverException {
        connect(null);
    }

    @Override
    public void connect(String replicasetName) throws MorphiumDriverException {
        for (String h : getHostSeed()) {
            for (int i = 0; i < getMinConnectionsPerHost(); i++) {
                try {
                    DriverBase d = createDriver(h);
                    d.connect();
                    getConnections(h).add(new Connection(d));
                } catch (MorphiumDriverException e) {
                    log.error("Could not connect to host " + h, e);
                }
            }
        }
        while (currentMaster == null) {
            log.info("Waiting for master...");
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (getHostSeed().length < secondaries.size()) {
            log.info("There are more nodes in replicaset than defined in seed...");
        } else if (getHostSeed().length > secondaries.size()) {
            log.info("some seed hosts were not reachable!");
        }

        if (connectionPool.size() == 0) throw new MorphiumDriverException("Could not connect");
        if (getTotalConnectionCount() == 0) {
            throw new MorphiumDriverException("Connection failed!");
        }

    }

    private int getTotalConnectionCount() {
        int c = 0;
        for (String k : connectionPool.keySet()) {
            c += connectionPool.get(k).size();
        }
        return c;
    }

    private List<Connection> getConnectionsInUse(String h) {
        if (connectionsInUse.get(h) == null) {
            connectionsInUse.put(h, new Vector<>());
        }
        return connectionsInUse.get(h);
    }

    private List<Connection> getConnections(String h) {
        if (connectionPool.get(h) == null) {
            connectionPool.put(h, new Vector<>());
        }
        return connectionPool.get(h);
    }

    @Override
    public boolean isConnected() {
        return getTotalConnectionCount() != 0;
    }

    @Override
    public void close() throws MorphiumDriverException {
        for (String h : connectionPool.keySet()) {
            for (int i = 0; i < connectionPool.get(h).size(); i++) {
                connectionPool.get(h).get(i).getD().close();
            }
        }
        for (String h : connectionsInUse.keySet()) {
            for (int i = 0; i < connectionsInUse.get(h).size(); i++) {
                connectionsInUse.get(h).get(i).getD().close();
            }
        }

    }

    @Override
    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            Map<String, Object> ret = c.getD().getReplsetStatus();
            return ret;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            Map<String, Object> ret = c.getD().getDBStats(db);
            return ret;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public Map<String, Object> getOps(long threshold) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            Map<String, Object> ret = c.getD().getOps(threshold);
            return ret;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            Map<String, Object> ret = c.getD().runCommand(db, cmd);
            return ret;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference rp, Map<String, Object> findMetaData) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(rp);
            List<Map<String, Object>> ret = c.getD().find(db, collection, query, sort, projection, skip, limit, batchSize, rp, findMetaData);
            return ret;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public long count(String db, String collection, Map<String, Object> query, ReadPreference rp) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(rp);
            long ret = c.getD().count(db, collection, query, rp);
            return ret;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public void insert(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primary);
            c.getD().insert(db, collection, objs, wc);
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public void store(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primary);
            c.getD().store(db, collection, objs, wc);
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> op, boolean multiple, boolean upsert, WriteConcern wc) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primary);
            Map<String, Object> ret = c.getD().update(db, collection, query, op, multiple, upsert, wc);
            return ret;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public Map<String, Object> delete(String db, String collection, Map<String, Object> query, boolean multiple, WriteConcern wc) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primary);
            Map<String, Object> ret = c.getD().delete(db, collection, query, multiple, wc);
            return ret;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public void drop(String db, String collection, WriteConcern wc) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primary);
            c.getD().drop(db, collection, wc);

        } finally {
            freeConnection(c);
        }
    }

    @Override
    public void drop(String db, WriteConcern wc) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primary);
            c.getD().drop(db, wc);

        } finally {
            freeConnection(c);
        }
    }

    @Override
    public boolean exists(String db) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            boolean ret = c.getD().exists(db);
            return ret;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public List<Object> distinct(String db, String collection, String field, Map<String, Object> filter, ReadPreference rp) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            List<Object> ret = c.getD().distinct(db, collection, field, filter, rp);
            return ret;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public boolean exists(String db, String collection) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            boolean ret = c.getD().exists(db, collection);
            return ret;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            List<Map<String, Object>> ret = c.getD().getIndexes(db, collection);
            return ret;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public List<String> getCollectionNames(String db) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            List<String> ret = c.getD().getCollectionNames(db);
            return ret;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public Map<String, Object> group(String db, String coll, Map<String, Object> query, Map<String, Object> initial, String jsReduce, String jsFinalize, ReadPreference rp, String... keys) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(rp);
            Map<String, Object> ret = c.getD().group(db, coll, query, initial, jsReduce, jsFinalize, rp, keys);
            return ret;
        } finally {
            freeConnection(c);
        }

    }

    @Override
    public List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, ReadPreference readPreference) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(readPreference);
            List<Map<String, Object>> ret = c.getD().aggregate(db, collection, pipeline, explain, allowDiskUse, readPreference);
            return ret;
        } finally {
            freeConnection(c);
        }

    }

    @Override
    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            return c.getD().isCapped(db, coll);
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {

        return null;
    }

    @Override
    public void createIndex(String db, String collection, Map<String, Object> index, Map<String, Object> options) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            c.getD().createIndex(db, collection, index, options);
        } finally {
            freeConnection(c);
        }
    }

    private DriverBase createDriver(String host) throws MorphiumDriverException {
        DriverBase d = new SingleConnectThreaddedDriver();
        d.setHostSeed(host); //only connecting to one host
        d.setSocketKeepAlive(isSocketKeepAlive());
        d.setLocalThreshold(getLocalThreshold());
        d.setMaxWaitTime(getMaxWaitTime());
        d.setReplicaSetName(getReplicaSetName());
        d.setDefaultW(getDefaultW());
        d.setDefaultReadPreference(getDefaultReadPreference());
        d.setRetriesOnNetworkError(getRetriesOnNetworkError());
        d.setSleepBetweenErrorRetries(getSleepBetweenErrorRetries());
        d.connect(getReplicaSetName());
        return d;
    }

    private Connection getConnection(String host) throws MorphiumDriverException {
        long start = System.currentTimeMillis();
        while (currentMaster == null) {
            if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                throw new MorphiumDriverNetworkException("could not get Master!");
            }
            Thread.yield();

        }
        List<Connection> masterConnections = getConnections(host);
        Connection c = null;
        if (connectionPool.size() == 0) {
            //No connection available - create one
            while (connectionPool.size() == 0) {
                if (connectionsInUse.get(host).size() < getMaxConnectionsPerHost()) {
                    c = new Connection(createDriver(host));
                    break;
                }
                if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                    throw new MorphiumDriverNetworkException("could not get Master!");
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }
        } else {
            c = masterConnections.remove(0); //get first available connection;
        }

        c.setInUse(true);
        c.touch();
        getConnectionsInUse(host).add(c);
        return c;
    }

    private Connection getSecondaryConnection() throws MorphiumDriverException {
        int idx = (int) (Math.random() * secondaries.size());
        return getConnection(secondaries.get(idx));
    }

    private Connection getConnection(ReadPreference rp) throws MorphiumDriverException {

        switch (rp.getType()) {
            case NEAREST:
                if (fastestHost != null)
                    return getConnection(fastestHost);
            case PRIMARY:
                return getMasterConnection();
            case PRIMARY_PREFERRED:
                try {
                    return getMasterConnection();
                } catch (Exception e) {
                    log.warn("could not get master connection...");
                }
            case SECONDARY:
                return getSecondaryConnection();
            case SECONDARY_PREFERRED:
                try {
                    return getSecondaryConnection();
                } catch (Exception e) {
                    return getMasterConnection();
                }
            default:
                log.fatal("Unknown read preference type! returning master!");
                return getMasterConnection();
        }

    }

    private Connection getMasterConnection() throws MorphiumDriverException {
        return getConnection(currentMaster);
    }

    private void freeConnection(Connection c) {
        if (c == null) return;
        getConnectionsInUse(c.getHost()).remove(c);
        getConnections(c.getHost()).add(c);
    }


    private class Connection {
        private DriverBase d;
        private long created;
        private long lru;

        private long id;
        private long optime;

        private boolean inUse = false;
        private boolean master = false;
        private boolean ok = true;
        private long answerTime;

        public Connection(DriverBase dr) throws MorphiumDriverException {
            this.d = dr;
            lru = System.currentTimeMillis();
            created = lru;
            synchronized (Connection.class) {
                id = ++seq;
            }


            //Housekeeping Thread
            new Thread() {
                public void run() {
                    while (d.isConnected()) {
                        try {
                            Map<String, Object> reply = null;
                            try {
                                long start = System.currentTimeMillis();
                                reply = d.runCommand("local", Utils.getMap("isMaster", true));
                                answerTime = System.currentTimeMillis() - start;
                                if (fastestAnswer > answerTime || d.getHostSeed()[0].equals(fastestHost)) {
                                    fastestAnswer = answerTime;
                                    fastestHost = d.getHostSeed()[0];
                                }

                            } catch (MorphiumDriverException e) {
                                log.error("Error with connection - exiting", e);
                                ok = false;
                                try {
                                    d.close();
                                } catch (MorphiumDriverException e1) {
                                }
                                return;
                            }
                            if (reply.get("ismaster").equals(true)) {
                                //got master connection...
                                master = true;
                                currentMaster = getHostSeed()[0];
                            } else {
                                master = false;
                                if (currentMaster == null) {
                                    currentMaster = (String) reply.get("primary");
                                }
                            }

                            if (reply.get("hosts") != null) {
                                secondaries = new Vector<>((List<String>) reply.get("hosts"));
                            }

                            try {
                                sleep(getHeartbeatFrequency());
                            } catch (InterruptedException e) {
                            }
                        } catch (Exception e) {
                            log.error("Connection broken!" + d.getHostSeed()[0]);
                            try {
                                d.close();
                            } catch (MorphiumDriverException e1) {
                            }
                            getConnections(d.getHostSeed()[0]).remove(this);
                            connectionsInUse.get(d.getHostSeed()[0]).remove(this);
                            if (d.getHostSeed()[0].equals(fastestHost)) {
                                fastestHost = null;
                                fastestAnswer = 1000000;
                            }
                            return;
                        }
                    }
                }
            }.start();

        }

        public String getHost() {
            return d.getHostSeed()[0];
        }

        public void close() throws MorphiumDriverException {
            d.close();
        }

        public boolean isOk() {
            return ok;
        }

        public boolean isMaster() {
            return master;
        }

        public boolean isInUse() {
            return inUse;
        }

        public void setInUse(boolean inUse) {
            if (inUse && this.inUse) throw new ConcurrentModificationException("Already in use!");
            this.inUse = inUse;
        }

        public MorphiumDriver getD() {
            return d;
        }

        public void setD(DriverBase d) {
            this.d = d;
        }

        public long getCreated() {
            return created;
        }

        public void setCreated(long created) {
            this.created = created;
        }

        public long getLru() {
            return lru;
        }

        public void setLru(long lru) {
            this.lru = lru;
        }

        public long getOptime() {
            return optime;
        }

        public void setOptime(long optime) {
            this.optime = optime;
        }

        public long getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Connection)) return false;

            Connection that = (Connection) o;

            return id == that.id;

        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }

        public void touch() {
            lru = System.currentTimeMillis();
        }
    }
}
