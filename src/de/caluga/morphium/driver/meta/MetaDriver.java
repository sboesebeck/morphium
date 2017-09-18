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
import de.caluga.morphium.driver.wireprotocol.OpQuery;
import de.caluga.morphium.driver.wireprotocol.OpReply;

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
    private static final ReadPreference primary = ReadPreference.primary();
    private static final ReadPreference secondaryPreferred = ReadPreference.secondaryPreferred();
    private static final ReadPreference primaryPreferred = ReadPreference.primaryPreferred();
    private static volatile long seq;
    private final Logger log = new Logger(MetaDriver.class);
    private final Map<String, List<Connection>> connectionPool = new ConcurrentHashMap<>();
    private final Map<String, List<Connection>> connectionsInUse = new ConcurrentHashMap<>();
    private final List<String> secondaries = Collections.synchronizedList(new ArrayList<>());
    private final List<String> arbiters = Collections.synchronizedList(new ArrayList<>());
    private final List<String> tempBlockedHosts = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, Integer> errorCountByHost = new ConcurrentHashMap<>();
    private String currentMaster;
    private long fastestAnswer = 10000000;
    private String fastestHost = null;
    private boolean connected = false;
    private long fastestHostTimestamp = System.currentTimeMillis();

    @Override
    public void connect() throws MorphiumDriverException {
        connect(null);
    }

    @Override
    public void connect(String replicasetName) throws MorphiumDriverException {
        //        setMaxConnectionsPerHost(1);
        connected = true;

        for (String h : getHostSeed()) {
            createConnectionsForPool(h);
        }
        //some Housekeeping
        new Thread() {

            @Override
            public void run() {
                setName("MetaDriver-housekeeping");
                while (isConnected()) {
                    //noinspection EmptyCatchBlock
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
                        if (arbiters.contains(h)) {
                            continue;
                        }
                        for (int i = getTotalConnectionsForHost(h); i < getMinConnectionsPerHost(); i++) {
                            if (!connected) {
                                break;
                            }
                            //                            log.debug("Underrun - need to add connections...");
                            try {
                                DriverBase d = createAndConnectDriver(h);
                                getConnections(h).add(new Connection(d));
                            } catch (MorphiumDriverException e) {
                                log.error("Could not connect to host " + h, e);
                            }
                        }
                    }


                    log.debug("total connections: " + getTotalConnectionCount() + " / " + (getMaxConnectionsPerHost() * connectionPool.size()));
                    for (String s : connectionPool.keySet()) {
                        int inUse = 0;
                        if (connectionsInUse.get(s) != null) {
                            inUse = connectionsInUse.get(s).size();
                        }

                        log.debug("  Host: " + s + "   " + getTotalConnectionsForHost(s) + " / " + getMaxConnectionsPerHost() + "   in Use: " + inUse);
                    }
                    log.debug("Fastest host: " + fastestHost + " with " + fastestAnswer + "ms");
                    log.debug("current master: " + currentMaster);

                }
                log.debug("Metadriver killed - terminating housekeeping thread");
            }
        }.start();

        connected = true;
        //Housekeeping Thread
        Thread t = new Thread() {
            @Override
            public void run() {

                while (connected) {
                    try {
                        for (String host : connectionPool.keySet()) {
                            for (int i = 0; i < connectionPool.get(host).size(); i++) {
                                //                        for (Connection c : connectionPool.get(host)) {
                                if (i > connectionPool.get(host).size()) {
                                    break;
                                }
                                Connection c = connectionPool.get(host).get(i);
                                housekeep(c);
                                if (connectionsInUse.get(c.getHost()) != null) {
                                    connectionsInUse.get(c.getHost()).remove(c);
                                }
                                if (connectionPool.get(c.getHost()) != null) {
                                    connectionPool.get(c.getHost()).remove(c);
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("Exception during houskeeping", e);
                    }
                    //noinspection EmptyCatchBlock
                    try {
                        sleep(getHeartbeatFrequency());
                    } catch (InterruptedException e) {
                    }
                }
            }

            public void housekeep(Connection c) {
                if (c.getD() == null) {
                    return;
                }
                if (!c.getD().isConnected()) {
                    log.error("Not connected!!!!");
                    return;
                }

                try {
                    if (!c.inUse && System.currentTimeMillis() - c.created > getMaxConnectionLifetime()) {
                        connectionPool.get(c.getHost()).remove(c);
                        if (c.inUse) {
                            //some other thread was faster
                            return;
                        }
                        log.debug("Maximum life time reached, killing myself");
                        //noinspection EmptyCatchBlock
                        try {
                            c.close();
                        } catch (MorphiumDriverException e) {
                        }

                        //                                while (getTotalConnectionsForHost(getHost()) < getMinConnectionsPerHost()) {
                        //                                    DriverBase b = createDriver(getHost());
                        //                                    connectionPool.get(getHost()).add(new Connection(b));
                        //                                }
                        return;
                    }
                    if (!c.inUse && System.currentTimeMillis() - c.lru > getMaxConnectionIdleTime()) {
                        if (connectionPool.get(c.getHost()).size() > getMinConnectionsPerHost()) {
                            connectionPool.get(c.getHost()).remove(c);
                            if (c.inUse) {
                                //some other thread won
                                return;
                            }
                            //noinspection EmptyCatchBlock
                            try {
                                c.close();
                            } catch (MorphiumDriverException e) {
                            }
                            return;
                        }

                    }
                    Map<String, Object> reply;
                    try {
                        c.answerTime = 99999;
                        long start = System.currentTimeMillis();
                        reply = c.getD().runCommand(RunCommand.Command.local.name(), Utils.getMap(RunCommand.Response.ismaster.name(), true));
                        c.answerTime = System.currentTimeMillis() - start;
                        setReplicaSet(c.getFromReply(reply, RunCommand.Response.setName) != null && c.getFromReply(reply, RunCommand.Response.primary) != null);

                    } catch (MorphiumDriverException e) {
                        if (e.getMongoCode() != null && e.getMongoCode().toString().equals(RunCommand.ErrorCode.UNABLE_TO_CONNECT.getCode())) {
                            c.ok = true;
                            return;
                        }
                        log.error("Error with connection - exiting", e);
                        c.ok = false;
                        //noinspection EmptyCatchBlock
                        try {
                            c.close();
                        } catch (MorphiumDriverException e1) {
                        }
                        return;
                    }

                    if (!c.arbiter && c.getFromReply(reply, RunCommand.Response.arbiterOnly) != null && c.getFromReply(reply, RunCommand.Response.arbiterOnly).equals(true)) {
                        //                                log.info("I'm an arbiter! Staying alive anyway!");
                        if (!arbiters.contains(c.getHost())) {
                            arbiters.add(c.getHost());
                        }
                        if (secondaries.contains(c.getHost())) {
                            secondaries.remove(c.getHost());
                        }
                        //                                connectionPool.get(getHost()).remove(Connection.this);
                        c.arbiter = true;
                    }

                    if (!c.arbiter && c.getFromReply(reply, RunCommand.Response.ismaster).equals(true)) {
                        //got master connection...
                        c.master = true;
                        currentMaster = c.getHost();
                    } else if (!c.arbiter && c.getFromReply(reply, RunCommand.Response.secondary).equals(true)) {
                        c.master = false;
                        if (currentMaster == null) {
                            currentMaster = (String) c.getFromReply(reply, RunCommand.Response.primary);
                        }
                        if (currentMaster == null) {
                            log.error("No master in replicaset!");
                        }
                        if (!secondaries.contains(c.getHost()) && !tempBlockedHosts.contains(c.getHost())) {
                            secondaries.add(c.getHost());
                        }
                    } else {
                        c.master = false;
                        if (currentMaster == null) {
                            currentMaster = (String) c.getFromReply(reply, RunCommand.Response.primary);
                        }
                    }

                    if (isReplicaset()) {
                        if (c.getFromReply(reply, RunCommand.Response.secondary).equals(false) && c.getFromReply(reply, RunCommand.Response.ismaster).equals(false)) {
                            //recovering?!?!?
                            secondaries.remove(c.getHost());
                        } else {
                            if (!c.arbiter && (fastestAnswer > c.answerTime || c.getD().getHostSeed()[0].equals(fastestHost))) {
                                long tm = System.currentTimeMillis() - fastestHostTimestamp;
                                long timeout = (long) ((2000 * Math.random()) + 100);
                                if (tm < timeout) {
                                    fastestAnswer = c.answerTime;
                                    fastestHost = c.getD().getHostSeed()[0];
                                    fastestHostTimestamp = System.currentTimeMillis();
                                } else if (tm > timeout && fastestHost != null && fastestHost.equals(c.getHost())) {
                                    fastestHost = null;
                                    fastestAnswer = 99999999;
                                }
                            }
                            if (c.getFromReply(reply, RunCommand.Response.hosts) != null && secondaries.isEmpty()) {
                                @SuppressWarnings("unchecked") Vector<String> s = new Vector<>((List<String>) c.getFromReply(reply, RunCommand.Response.hosts));
                                //noinspection ForLoopReplaceableByForEach
                                for (int i = 0; i < s.size(); i++) {
                                    String hn = s.get(i);
                                    String adr = getHostAdress(hn);
                                    secondaries.add(adr);
                                }
                            }
                        }

                    }


                } catch (Exception e) {
                    log.error("Connection broken!" + c.getD().getHostSeed()[0], e);
                    //noinspection EmptyCatchBlock
                    try {
                        c.getD().close();
                    } catch (MorphiumDriverException e1) {
                    }
                    //noinspection EmptyCatchBlock
                    try {
                        getConnections(c.getD().getHostSeed()[0]).remove(c);
                        connectionsInUse.get(c.getD().getHostSeed()[0]).remove(c);
                    } catch (Exception ex) {
                    }
                    if (c.getD().getHostSeed()[0].equals(fastestHost)) {
                        fastestHost = null;
                        fastestAnswer = 1000000;
                    }
                }
            }
        };
        t.setDaemon(true);
        t.start();
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
                DriverBase d = createAndConnectDriver(currentMaster);
                getConnections(currentMaster).add(new Connection(d));
            } catch (MorphiumDriverException e) {
                log.error("Could not connect to master " + currentMaster, e);
            }
            //noinspection EmptyCatchBlock
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }
        }
        if (getHostSeed().length < secondaries.size()) {
            log.debug("There are more nodes in replicaset than defined in seed...");
            //noinspection ForLoopReplaceableByForEach
            for (int i = 0; i < secondaries.size(); i++) {
                String h = secondaries.get(i);
                if (getConnections(h).isEmpty()) {
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

        if (connectionPool.isEmpty()) {
            throw new MorphiumDriverException("Could not connect");
        }
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
                //                log.info("Initial connect to host " + h);
                DriverBase d = createAndConnectDriver(h);
                getConnections(h).add(new Connection(d));
            } catch (MorphiumDriverException e) {
                log.error("Could not connect to host " + h, e);
            }
        }
    }

    private int getTotalConnectionCount() {
        int c = 0;
        for (String k : connectionPool.keySet()) {
            c += connectionPool.get(k).size();
        }
        for (String k : connectionsInUse.keySet()) {
            c += connectionsInUse.get(k).size();
        }
        return c;
    }

    private List<Connection> getConnectionsInUse(String h) {
        connectionsInUse.putIfAbsent(h, Collections.synchronizedList(new ArrayList<>()));
        return connectionsInUse.get(h);
    }

    private List<Connection> getConnections(String h) {
        if (h == null) {
            log.error("Host is null - cannot get pool!");
            return null;
        }
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
        closeConnections(connectionPool);
        closeConnections(connectionsInUse);
        while (getTotalConnectionCount() > 0) {
            log.error("Still connected?!?!? " + getTotalConnectionCount());
            close();
        }
    }

    private void closeConnections(Map<String, List<Connection>> pool) {
        for (String h : pool.keySet()) {
            while (!pool.get(h).isEmpty()) {
                //noinspection EmptyCatchBlock
                try {
                    Connection c = pool.get(h).remove(0);
                    c.close();
                } catch (Exception e) {
                }
            }
        }
    }

    @Override
    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(primaryPreferred);
            return c.getD().getReplsetStatus();
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
        if (crs == null) {
            return; //already closed
        }
        SingleConnectCursor internalCursor = (SingleConnectCursor) crs.getInternalCursorObject();
        internalCursor.getDriver().closeIteration(crs);
        for (String srv : connectionsInUse.keySet()) {
            for (Connection c : connectionsInUse.get(srv)) {
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
            throw ex;
        } finally {
            freeConnection(c);
        }

    }

    @Override
    public void tailableIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, int timeout, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        Connection c = null;
        try {
            c = getConnection(readPreference);
            c.getD().tailableIteration(db, collection, query, sort, projection, skip, limit, batchSize, readPreference, timeout, cb);
        } catch (MorphiumDriverNetworkException ex) {
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
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
            if (c != null) {
                incErrorCount(c.getHost());
            }
            throw ex;
        } finally {
            freeConnection(c);
        }
    }

    private DriverBase createAndConnectDriver(String host) throws MorphiumDriverException {
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
        if (!connected) {
            return null; //bail out before creating a thread in vain
        }
        d.connect(getReplicaSetName());
        d.setSlaveOk(true);
        return d;
    }

    private Connection getConnection(String host) throws MorphiumDriverException {
        long start = System.currentTimeMillis();
        //        log.info(Thread.currentThread().getId()+": connections for "+host+": "+getTotalConnectionsForHost(host));
        Connection c;
        while (true) {
            try {
                if (!getConnections(host).isEmpty()) {
                    c = getConnections(host).remove(0); //get first available connection;
                    if (c == null) {
                        log.fatal("Hä? could not get connection from pool");
                    } else {
                        break;
                    }
                }
            } catch (Exception e) {
                //noinspection EmptyCatchBlock
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                }
                //somebody was faster - retry
            }

            if (getConnections(host).isEmpty() && getTotalConnectionsForHost(host) < getMaxConnectionsPerHost()) {
                c = new Connection(createAndConnectDriver(host));
                break;
            }

            while (getConnections(host).isEmpty() && getTotalConnectionsForHost(host) >= getMaxConnectionsPerHost()) {
                if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                    throw new MorphiumDriverNetworkException("could not get Connection! Waited >" + getMaxWaitTime() + "ms");
                }
                //noinspection EmptyCatchBlock
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

        String least = currentMaster;
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
        if (rp == null) {
            rp = secondaryPreferred;
        }
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
                if (fastestHost != null) {
                    return getConnection(fastestHost);
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
        if (c == null) {
            return;
        }
        getConnectionsInUse(c.getHost()).remove(c);
        c.setInUse(false);

        getConnections(c.getHost()).add(c);
    }

    private void incErrorCount(String host) {
        errorCountByHost.merge(host, 1, (a, b) -> a + b);

    }

    private void decErrorCount(String host) {
        errorCountByHost.merge(host, 0, (a, b) -> a - b);
    }

    @Override
    protected void sendQuery(OpQuery q) throws MorphiumDriverException {

    }

    @Override
    protected OpReply getReply(long waitingFor, int timeout) throws MorphiumDriverException {
        return null;
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

        @SuppressWarnings("RedundantThrows")
        public Connection(DriverBase dr) throws MorphiumDriverException {
            if (dr == null) {
                throw new IllegalArgumentException("Cannot create connection to null");
            }
            this.d = dr;
            lru = System.currentTimeMillis();
            created = lru;
            synchronized (Connection.class) {
                id = ++seq;
            }

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

        @SuppressWarnings("unused")
        public boolean isOk() {
            return ok;
        }

        @SuppressWarnings("unused")
        public boolean isMaster() {
            return master;
        }

        @SuppressWarnings("unused")
        public boolean isInUse() {
            return inUse;
        }

        public void setInUse(boolean inUse) {
            if (inUse && this.inUse) {
                throw new ConcurrentModificationException("Already in use!");
            }
            this.inUse = inUse;
        }

        public MorphiumDriver getD() {
            return d;
        }

        @SuppressWarnings("unused")
        public void setD(DriverBase d) {
            this.d = d;
        }

        @SuppressWarnings("unused")
        public long getCreated() {
            return created;
        }

        @SuppressWarnings("unused")
        public void setCreated(long created) {
            this.created = created;
        }

        @SuppressWarnings("unused")
        public long getLru() {
            return lru;
        }

        @SuppressWarnings("unused")
        public void setLru(long lru) {
            this.lru = lru;
        }

        @SuppressWarnings("unused")
        public long getOptime() {
            return optime;
        }

        @SuppressWarnings("unused")
        public void setOptime(long optime) {
            this.optime = optime;
        }

        @SuppressWarnings("unused")
        public long getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null) {
                return false;
            }
            if (o.getClass() != this.getClass()) {
                return false;
            }

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
