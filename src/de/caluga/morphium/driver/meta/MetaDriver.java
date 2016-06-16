package de.caluga.morphium.driver.meta;

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.constants.RunCommand;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.singleconnect.BulkContext;
import de.caluga.morphium.driver.singleconnect.DriverBase;
import de.caluga.morphium.driver.singleconnect.SingleConnectCursor;
import de.caluga.morphium.driver.singleconnect.SingleConnectThreaddedDriver;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Stephan Bösebeck
 * Date: 02.12.15
 * Time: 23:56
 * <p>
 * Meta Driver. Uses SingleConnectThreaddedDriver to connect to mongodb replicaset. Not production ready yet, but good
 * for testing.
 */
public class MetaDriver extends DriverBase {
    private Logger log = new Logger(MetaDriver.class);
    private static volatile long seq;
    private Map<String, List<Connection>> connectionPool = new ConcurrentHashMap<>();
    private Map<String, List<Connection>> connectionsInUse = new ConcurrentHashMap<>();
    private String currentMaster;
    private List<String> secondaries = Collections.synchronizedList(new ArrayList<>());
    private List<String> arbiters = Collections.synchronizedList(new ArrayList<>());
    private List<String> tempBlockedHosts = Collections.synchronizedList(new ArrayList<>());
    private long fastestAnswer = 10000000;
    private String fastestHost = null;

    private Map<String, Integer> errorCountByHost = new ConcurrentHashMap<>();

    private static ReadPreference primary = ReadPreference.primary();
    private static ReadPreference secondary = ReadPreference.secondary();
    private static ReadPreference secondaryPreferred = ReadPreference.secondaryPreferred();
    private static ReadPreference primaryPreferred = ReadPreference.primaryPreferred();
    private static ReadPreference nearest = ReadPreference.primary();

    private static volatile int roundrobin = 0;
    private boolean connected = false;
    private long fastestHostTimestamp = System.currentTimeMillis();

    @Override
    public void connect() throws MorphiumDriverException {
        connect(null);
    }

    @Override
    public void connect(String replicasetName) throws MorphiumDriverException {
//        setMaxConnectionsPerHost(1);
        for (String h : getHostSeed()) {
            createConnectionsForPool(h);
        }
        connected = true;
        //some Housekeeping
        new Thread() {

            @Override
            public void run() {
                while (isConnected()) {
                    try {
                        Thread.sleep(2500);
                    } catch (InterruptedException e) {
                    }

                    for (int i = secondaries.size() - 1; i >= 0; i--) {
                        errorCountByHost.putIfAbsent(secondaries.get(i), 0);
                        if (errorCountByHost.get(secondaries.get(i)) > 10) {
                            //temporary disabling host
                            String sec = secondaries.remove(i);
                            tempBlockedHosts.add(sec);
                        } else {
                            decErrorCount(secondaries.get(i));
                        }

                    }
                    for (int i = 0; i < tempBlockedHosts.size(); i++) {
                        if (errorCountByHost.get(tempBlockedHosts.get(i)) == 0) {
                            String sec = tempBlockedHosts.remove(i);
                            secondaries.add(sec);
                        } else {
                            decErrorCount(tempBlockedHosts.get(i));
                        }
                    }


                    for (String h : getHostSeed()) {
                        if (arbiters.contains(h)) continue;

                        for (int i = getTotalConnectionsForHost(h); i < getMinConnectionsPerHost(); i++) {
                            log.debug("Underrun - need to add connections...");
                            try {
                                DriverBase d = createDriver(h);
                                d.setSlaveOk(true);
                                d.connect();
                                getConnections(h).add(new Connection(d));
                            } catch (MorphiumDriverException e) {
                                log.error("Could not connect to host " + h, e);
                            }
                        }
                    }


                    log.debug("total connections: " + getTotalConnectionCount() + " / " + (getMaxConnectionsPerHost() * connectionPool.size()));
                    for (String s : connectionPool.keySet()) {
                        int inUse = 0;
                        if (connectionsInUse.get(s) != null) inUse = connectionsInUse.get(s).size();

                        log.debug("  Host: " + s + "   " + getTotalConnectionsForHost(s) + " / " + getMaxConnectionsPerHost() + "   in Use: " + inUse);
                    }
                    log.debug("Fastest host: " + fastestHost + " with " + fastestAnswer + "ms");
                    log.debug("current master: " + currentMaster);

                }
                log.debug("Metadriver killed - terminating housekeeping thread");
            }
        }.start();
        while (currentMaster == null) {
            log.debug("Waiting for master...");
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        while (connectionPool.get(currentMaster) == null || getTotalConnectionsForHost(currentMaster) < getMinConnectionsPerHost()) {
            log.debug("no connection to current master yet! Retrying...");
            try {
                DriverBase d = createDriver(currentMaster);
                d.connect();
                getConnections(currentMaster).add(new Connection(d));
            } catch (MorphiumDriverException e) {
                log.error("Could not connect to master " + currentMaster, e);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }
        }
        if (getHostSeed().length < secondaries.size()) {
            log.debug("There are more nodes in replicaset than defined in seed...");
            for (int i = 0; i < secondaries.size(); i++) {
                String h = secondaries.get(i);
                if (getConnections(h).size() == 0) {
                    try {
                        createConnectionsForPool(h);
                    } catch (Exception e) {
                        log.info("Exception during creation of connection for pool", e);
                    }
                }
            }
        } else if (getHostSeed().length > secondaries.size()) {
            log.info("some seed hosts were not reachable!");
        }

        if (connectionPool.isEmpty()) throw new MorphiumDriverException("Could not connect");
        if (getTotalConnectionCount() == 0) {
            throw new MorphiumDriverException("Connection failed!");
        }

        connected = true;
    }

    private int getTotalConnectionsForHost(String h) {
        int inUse = getConnectionsInUse(h) == null ? 0 : getConnectionsInUse(h).size();
        int avail = getConnections(h) == null ? 0 : getConnections(h).size();
        return inUse + avail;
    }

    private void createConnectionsForPool(String h) {
        for (int i = getTotalConnectionsForHost(h); i < getMinConnectionsPerHost(); i++) {
            try {
                log.info("Initial connect to host " + h);
                DriverBase d = createDriver(h);
                d.connect();
                getConnections(h).add(new Connection(d));
            } catch (MorphiumDriverException e) {
                log.error("Could not connect to host " + h, e);
            }
        }
    }

    private int getTotalConnectionCount() {
        int c = 0;
        for (Map.Entry<String, List<Connection>> stringListEntry : connectionPool.entrySet()) {
            c += stringListEntry.getValue().size();
        }
        for (Map.Entry<String, List<Connection>> stringListEntry : connectionsInUse.entrySet()) {
            c += stringListEntry.getValue().size();
        }
        return c;
    }

    private List<Connection> getConnectionsInUse(String h) {
        connectionsInUse.putIfAbsent(h, Collections.synchronizedList(new ArrayList<>()));
        return connectionsInUse.get(h);
    }

    private List<Connection> getConnections(String h) {
        connectionPool.putIfAbsent(h, Collections.synchronizedList(new ArrayList<>()));
        return connectionPool.get(h);
    }

    @Override
    public boolean isConnected() {
        return connected && getTotalConnectionCount() != 0;
    }

    @Override
    public void close() throws MorphiumDriverException {
        connected = false;
        for (Map.Entry<String, List<Connection>> stringListEntry : connectionPool.entrySet()) {
            while (!stringListEntry.getValue().isEmpty()) {
                try {
                    Connection c = stringListEntry.getValue().remove(0);
                    c.close();
                } catch (Exception e) {
                }
            }
        }
        for (Map.Entry<String, List<Connection>> stringListEntry : connectionsInUse.entrySet()) {
            while (!stringListEntry.getValue().isEmpty()) {
                try {
                    Connection c = stringListEntry.getValue().remove(0);
                    c.close();
                } catch (Exception e) {
                }
            }
        }
        while (getTotalConnectionCount() > 0) {
            log.error("Still connected?!?!? " + getTotalConnectionCount());
            close();
        }
    }

    @Override
    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            return c.getD().getReplsetStatus();
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            return c.getD().getDBStats(db);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public Map<String, Object> getOps(long threshold) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            return c.getD().getOps(threshold);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            return c.getD().runCommand(db, cmd);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public MorphiumCursor initIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, Map<String, Object> findMetaData) throws MorphiumDriverException {
        Connection c = getConnection(readPreference);
        return c.getD().initIteration(db, collection, query, sort, projection, skip, limit, batchSize, readPreference, findMetaData);

    }

    @Override
    public MorphiumCursor nextIteration(MorphiumCursor crs) throws MorphiumDriverException {
        //Stay at the same connection
        SingleConnectCursor c = (SingleConnectCursor) crs.getInternalCursorObject();
        return c.getDriver().nextIteration(crs);
    }

    @Override
    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        if (crs == null) return; //already closed
        SingleConnectCursor internalCursor = (SingleConnectCursor) crs.getInternalCursorObject();
        internalCursor.getDriver().closeIteration(crs);
        for (Map.Entry<String, List<Connection>> stringListEntry : connectionsInUse.entrySet()) {
            for (Connection c : stringListEntry.getValue()) {
                if (c.getD().equals(internalCursor.getDriver())) {
                    freeConnection(c);
                    return;
                }
            }
        }
        throw new MorphiumDriverException("Could not free connection - not in use or closed");
    }

    @Override
    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference rp, Map<String, Object> findMetaData) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(rp);
            return c.getD().find(db, collection, query, sort, projection, skip, limit, batchSize, rp, findMetaData);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public long count(String db, String collection, Map<String, Object> query, ReadPreference rp) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(rp);
            return c.getD().count(db, collection, query, rp);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
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
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
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
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public Map<String, Object> update(String db, String collection, List<Map<String, Object>> updateCommand, boolean ordered, WriteConcern wc) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primary);
            return ((DriverBase) c.getD()).update(db, collection, updateCommand, ordered, wc);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> op, boolean multiple, boolean upsert, WriteConcern wc) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primary);
            return c.getD().update(db, collection, query, op, multiple, upsert, wc);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public Map<String, Object> delete(String db, String collection, Map<String, Object> query, boolean multiple, WriteConcern wc) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primary);
            return c.getD().delete(db, collection, query, multiple, wc);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
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

        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
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

        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public boolean exists(String db) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            return c.getD().exists(db);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public List<Object> distinct(String db, String collection, String field, Map<String, Object> filter, ReadPreference rp) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            return c.getD().distinct(db, collection, field, filter, rp);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public boolean exists(String db, String collection) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            return c.getD().exists(db, collection);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            return c.getD().getIndexes(db, collection);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public List<String> getCollectionNames(String db) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            return c.getD().getCollectionNames(db);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public Map<String, Object> group(String db, String coll, Map<String, Object> query, Map<String, Object> initial, String jsReduce, String jsFinalize, ReadPreference rp, String... keys) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(rp);
            return c.getD().group(db, coll, query, initial, jsReduce, jsFinalize, rp, keys);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }

    }

    @Override
    public List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, ReadPreference readPreference) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(readPreference);
            return c.getD().aggregate(db, collection, pipeline, explain, allowDiskUse, readPreference);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
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
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return new BulkContext(m, db, collection, this, ordered, getMaxWriteBatchSize(), wc);
    }

    @Override
    public void createIndex(String db, String collection, Map<String, Object> index, Map<String, Object> options) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primary);
            c.getD().createIndex(db, collection, index, options);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) incErrorCount(c.getHost());
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    private DriverBase createDriver(String host) throws MorphiumDriverException {
        DriverBase d = new SingleConnectThreaddedDriver();
        d.setHostSeed(host); //only connecting to one host
        d.setSocketKeepAlive(isSocketKeepAlive());
        d.setSocketTimeout(getSocketTimeout());
        d.setConnectionTimeout(getConnectionTimeout());
        d.setDefaultWriteTimeout(getDefaultWriteTimeout());
        d.setSleepBetweenErrorRetries(getSleepBetweenErrorRetries());
        d.setRetriesOnNetworkError(getRetriesOnNetworkError());
        d.setLocalThreshold(getLocalThreshold());
        d.setMaxWaitTime(getMaxWaitTime());
        d.setReplicaSetName(getReplicaSetName());
        d.setDefaultW(getDefaultW());
        d.setDefaultReadPreference(getDefaultReadPreference());
        d.connect(getReplicaSetName());
        d.setSlaveOk(true);
        return d;
    }

    private Connection getConnection(String host) throws MorphiumDriverException {
        long start = System.currentTimeMillis();
//        log.info(Thread.currentThread().getId()+": connections for "+host+": "+getTotalConnectionsForHost(host));
        Connection c = null;
        while (c == null) {
            try {
                if (getConnections(host).size() != 0) {
                    c = getConnections(host).remove(0); //get first available connection;
                    if (c == null) {
                        log.fatal("Hä? could not get connection from pool");
                    } else {
                        break;
                    }
                }
            } catch (Exception e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                }
                //somebody was faster - retry
                //TODO: Remove recursion!
                return getConnection(host);
            }

            if (getConnections(host).isEmpty() && getTotalConnectionsForHost(host) < getMaxConnectionsPerHost()) {
                c = new Connection(createDriver(host));
                break;
            }

            while (getConnections(host).isEmpty() && getTotalConnectionsForHost(host) >= getMaxConnectionsPerHost()) {
                if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                    throw new MorphiumDriverNetworkException("could not get Connection! Waited >" + getMaxWaitTime() + "ms");
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }


        }

        c.setInUse(true);
        c.touch();
        getConnectionsInUse(host).add(c);
//        if (getTotalConnectionsForHost(host)>getMaxConnectionsPerHost()){
//            log.error("Connections limit exceeded!!!!");
//        }
//        log.info(Thread.currentThread().getId()+": connections for "+host+": "+getTotalConnectionsForHost(host)+"    ---- end");

        return c;
    }

    private Connection getSecondaryConnection() throws MorphiumDriverException {
//        int idx = (int) (System.currentTimeMillis() % secondaries.size());
        //balancing

        String least = null;
        int min = 9999;
        for (String h : secondaries) {
            if (connectionsInUse.get(h) == null) {
                min = 0;
                least = h;
            } else if (connectionsInUse.get(h).size() < min) {
                least = h;
                min = connectionPool.get(h).size();
            }
        }
        return getConnection(least);
    }

    private Connection getConnection(ReadPreference rp) throws MorphiumDriverException {
        if (rp == null) rp = secondaryPreferred;
        switch (rp.getType()) {
            case PRIMARY:
                return getMasterConnection();
            case PRIMARY_PREFERRED:
                try {
                    return getMasterConnection();
                } catch (Exception e) {
                    log.warn("could not get master connection...", e);
                }
            case NEAREST:
                if (fastestHost != null)
                    return getConnection(fastestHost);
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
        long start = System.currentTimeMillis();
        while (currentMaster == null) {
            if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                throw new MorphiumDriverNetworkException("could not get Master!");
            }
            Thread.yield();

        }
        return getConnection(currentMaster);
    }

    private void freeConnection(Connection c) {
        if (c == null) return;
        getConnectionsInUse(c.getHost()).remove(c);
        c.setInUse(false);

        getConnections(c.getHost()).add(c);
    }

    private void incErrorCount(String host) {
        if (errorCountByHost.get(host) == null) {
            errorCountByHost.put(host, 1);
        } else {
            errorCountByHost.put(host, errorCountByHost.get(host) + 1);
        }

    }

    private void decErrorCount(String host) {
        if (errorCountByHost.get(host) == null) {
            errorCountByHost.put(host, 0);
        } else {
            errorCountByHost.put(host, errorCountByHost.get(host) - 1);
        }

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
        private boolean arbiter = false;

        public Connection(DriverBase dr) throws MorphiumDriverException {
            this.d = dr;
            lru = System.currentTimeMillis();
            created = lru;
            synchronized (Connection.class) {
                id = ++seq;
            }


            //Housekeeping Thread
            new Thread() {
                @Override
                public void run() {
                    if (!d.isConnected()) {
                        log.error("Not connected!!!!");
                        return;
                    }
                    while (d.isConnected()) {
                        try {
                            if (!inUse && System.currentTimeMillis() - created > getMaxConnectionLifetime()) {
                                connectionPool.get(getHost()).remove(Connection.this);
                                if (inUse) {
                                    //some other thread was faster
                                    continue;
                                }
                                log.debug("Maximum life time reached, killing myself");
                                try {
                                    d.close();
                                } catch (MorphiumDriverException e) {
                                }

//                                while (getTotalConnectionsForHost(getHost()) < getMinConnectionsPerHost()) {
//                                    DriverBase b = createDriver(getHost());
//                                    connectionPool.get(getHost()).add(new Connection(b));
//                                }
                                break;
                            }
                            if (!inUse && System.currentTimeMillis() - lru > getMaxConnectionIdleTime()) {
                                if (connectionPool.get(getHost()).size() > getMinConnectionsPerHost()) {
                                    connectionPool.get(getHost()).remove(Connection.this);
                                    if (inUse) {
                                        //some other thread won
                                        continue;
                                    }
                                    try {
                                        d.close();
                                    } catch (MorphiumDriverException e) {
                                    }
                                    break;
                                }

                            }
                            Map<String, Object> reply = null;
                            try {
                                answerTime = 99999;
                                long start = System.currentTimeMillis();
                                reply = d.runCommand(RunCommand.Command.local.name(), Utils.getMap(RunCommand.Response.ismaster.name(), true));
                                answerTime = System.currentTimeMillis() - start;
                                setReplicaSet(getFromReply(reply, RunCommand.Response.setName) != null && getFromReply(reply, RunCommand.Response.primary) != null);

                            } catch (MorphiumDriverException e) {
                                if (e.getMongoCode() != null && e.getMongoCode().toString().equals(RunCommand.ErrorCode.UNABLE_TO_CONNECT.getCode())) {
                                    ok = true;
                                    return;
                                }
                                log.error("Error with connection - exiting", e);
                                ok = false;
                                try {
                                    d.close();
                                } catch (MorphiumDriverException e1) {
                                }
                                return;
                            }

                            if (!arbiter && getFromReply(reply, RunCommand.Response.arbiterOnly) != null && getFromReply(reply, RunCommand.Response.arbiterOnly).equals(true)) {
//                                log.info("I'm an arbiter! Staying alive anyway!");
                                if (!arbiters.contains(getHost())) arbiters.add(getHost());
                                if (secondaries.contains(getHost())) secondaries.remove(getHost());
//                                connectionPool.get(getHost()).remove(Connection.this);
                                arbiter = true;
                            }

                            if (!arbiter && getFromReply(reply, RunCommand.Response.ismaster).equals(true)) {
                                //got master connection...
                                master = true;
                                currentMaster = getHost();
                            } else if (!arbiter && getFromReply(reply, RunCommand.Response.secondary).equals(true)) {
                                master = false;
                                if (currentMaster == null) {
                                    currentMaster = (String) getFromReply(reply, RunCommand.Response.primary);
                                }
                                if (currentMaster == null) {
                                    log.error("No master in replicaset!");
                                }
                                if (!secondaries.contains(getHost()) && !tempBlockedHosts.contains(getHost()))
                                    secondaries.add(getHost());
                            } else {
                                master = false;
                                if (currentMaster == null) {
                                    currentMaster = (String) getFromReply(reply, RunCommand.Response.primary);
                                }
                            }

                            if (isReplicaset()) {
                                if (getFromReply(reply, RunCommand.Response.secondary).equals(false) && getFromReply(reply, RunCommand.Response.ismaster).equals(false)) {
                                    //recovering?!?!?
                                    secondaries.remove(getHost());
                                } else {
                                    if (!arbiter && (fastestAnswer > answerTime || d.getHostSeed()[0].equals(fastestHost))) {
                                        long tm = System.currentTimeMillis() - fastestHostTimestamp;
                                        long timeout = (long) ((2000 * Math.random()) + 100);
                                        if (tm < timeout) {
                                            fastestAnswer = answerTime;
                                            fastestHost = d.getHostSeed()[0];
                                            fastestHostTimestamp = System.currentTimeMillis();
                                        } else if (tm > timeout && fastestHost != null && fastestHost.equals(getHost())) {
                                            fastestHost = null;
                                            fastestAnswer = 99999999;
                                        }
                                    }
                                    if (getFromReply(reply, RunCommand.Response.hosts) != null && secondaries.isEmpty()) {
                                        Vector<String> s = new Vector<>((List<String>) getFromReply(reply, RunCommand.Response.hosts));
                                        for (int i = 0; i < s.size(); i++) {
                                            String hn = s.get(i);
                                            String adr = getHostAdress(hn);
                                            secondaries.add(adr);
                                        }
                                    }
                                }

                            }


                                //todo: check for missing secondaries, like some that were added during runtime
                                try {
                                    sleep(getHeartbeatFrequency());
                                } catch (InterruptedException e) {
                                }


                        } catch (Exception e) {
                            log.error("Connection broken!" + d.getHostSeed()[0], e);
                            try {
                                d.close();
                            } catch (MorphiumDriverException e1) {
                            }
                            try {
                                getConnections(d.getHostSeed()[0]).remove(Connection.this);
                                connectionsInUse.get(d.getHostSeed()[0]).remove(Connection.this);
                            } catch (Exception ex) {
                            }
                            if (d.getHostSeed()[0].equals(fastestHost)) {
                                fastestHost = null;
                                fastestAnswer = 1000000;
                            }
                            return;
                        }
                    }
                    if (connectionsInUse.get(getHost()) != null)
                        connectionsInUse.get(getHost()).remove(Connection.this);
                    if (connectionPool.get(getHost()) != null)
                        connectionPool.get(getHost()).remove(Connection.this);
                    if (isMaster() && getHost().equals(currentMaster)) {
                        currentMaster = null;
                    }
                }
            }.start();

        }


        private Object getFromReply(Map<String, Object> reply, RunCommand.Response response) {
            return reply.get(response.name());
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
            if (o.getClass() != this.getClass()) return false;

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
