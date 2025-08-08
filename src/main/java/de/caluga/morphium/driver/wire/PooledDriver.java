package de.caluga.morphium.driver.wire;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.AggregatorImpl;
import de.caluga.morphium.annotations.Driver;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.MorphiumTransactionContext;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.ReadPreferenceType;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.bulk.BulkRequest;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.bulk.DeleteBulkRequest;
import de.caluga.morphium.driver.bulk.InsertBulkRequest;
import de.caluga.morphium.driver.bulk.UpdateBulkRequest;
import de.caluga.morphium.driver.commands.AbortTransactionCommand;
import de.caluga.morphium.driver.commands.CollStatsCommand;
import de.caluga.morphium.driver.commands.CommitTransactionCommand;
import de.caluga.morphium.driver.commands.CurrentOpCommand;
import de.caluga.morphium.driver.commands.DbStatsCommand;
import de.caluga.morphium.driver.commands.DeleteMongoCommand;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.driver.commands.KillCursorsCommand;
import de.caluga.morphium.driver.commands.ListCollectionsCommand;
import de.caluga.morphium.driver.commands.ReplicastStatusCommand;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;
import de.caluga.morphium.driver.commands.WatchCommand;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
@Driver(name = "PooledDriver", description = "Driver with connection pool")
public class PooledDriver extends DriverBase {
    public static final String driverName = "PooledDriver";
    private final Map<String, BlockingQueue<ConnectionContainer >> connectionPool;
    private final Map<Integer, ConnectionContainer> borrowedConnections;
    private final Map<DriverStatsKey, AtomicDecimal> stats;
    private long fastestTime = 10000;
    private int idleSleepTime = 5;
    private String fastestHost = null;
    private Map<String, Long> pingTimesPerHost = new ConcurrentHashMap<>();
    private final Logger log = LoggerFactory.getLogger(PooledDriver.class);
    private String primaryNode;
    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5, Thread.ofVirtual().name("MCon-", 0).factory());

    private final AtomicInteger lastSecondaryNode = new AtomicInteger(0);
    private final Map<String, Thread> hostThreads = new ConcurrentHashMap<>();
    private int serverSelectionTimeout = 2000;

    public PooledDriver() {
        connectionPool = new HashMap<>();
        borrowedConnections = Collections.synchronizedMap(new HashMap<>());
        stats = new ConcurrentHashMap<>();

        for (var e : DriverStatsKey.values()) {
            stats.put(e, new AtomicDecimal(0));
        }
    }


    @Override
    public int getServerSelectionTimeout() {
        return serverSelectionTimeout;
    }


    @Override
    public void setServerSelectionTimeout(int timeoutInMS) {
        this.serverSelectionTimeout = timeoutInMS;
    }


    @Override
    public void connect(String replSet) throws MorphiumDriverException {
        //creating min connections for each host
        for (String host : getHostSeed()) {
            connectionPool.put(host, new LinkedBlockingQueue<>());
        }

        setReplicaSet(getHostSeed().size() > 1);
        startHeartbeat();
    }

    @Override
    public ReadPreference getDefaultReadPreference() {
        return ReadPreference.nearest();
    }

    @Override
    public synchronized void removeFromHostSeed(String host) {
        super.removeFromHostSeed(host);
        pingTimesPerHost.remove(host);

        if (getNumHostsInSeed() == 0) {
            if (lastHostsFromHello == null) {
                log.warn("Wanted to remove last host in hostseed, but last hosts is null");
                addToHostSeed(host);
            } else {
                setHostSeed(lastHostsFromHello);
            }
        }
    }


    private ScheduledFuture<?> heartbeat;
    private final Object waitCounterSignal = new Object();
    private final Map<String, AtomicInteger> waitCounter = new ConcurrentHashMap<>();
    private List<String> lastHostsFromHello = null;

    private String getHost(String hostPort) {
        if (hostPort == null) {
            return "";
        }

        String[] h = hostPort.split(":");
        return h[0];
    }

    private int getPortFromHost(String host) {
        String[] h = host.split(":");

        if (h.length == 1) {
            return 27017;
        }

        return Integer.parseInt(h[1]);
    }

    @Override
    public void connect() throws MorphiumDriverException {
        connect(null);
    }

    private void handleHelloResult(HelloResult hello, String hostConnected) {
        if (hello == null) return;

        if (hello.getWritablePrimary() != null && hello.getWritablePrimary() && hello.getMe() == null) {
            if (hello.getWritablePrimary() && primaryNode == null) {
                primaryNode = hostConnected;
            } else if (!hostConnected.equals(primaryNode)) {
                log.warn("Primary failover? {} -> {}", primaryNode, hello.getMe());
                stats.get(DriverStatsKey.FAILOVERS).incrementAndGet();
                primaryNode = hostConnected;
            } else if (!hello.getWritablePrimary() && hostConnected.equals(primaryNode)) {
                log.error("Primary node is not me {}", hello.getMe());
                primaryNode = null;
            }
        } else if (hello.getWritablePrimary() != null && hello.getMe() != null) {
            if (primaryNode == null && hello.getPrimary() != null) {
                primaryNode = hello.getPrimary();
            } else if (hello.getWritablePrimary() && !hello.getMe().equals(primaryNode)) {
                log.warn("Primary failover? {} -> {}", primaryNode, hello.getMe());
                stats.get(DriverStatsKey.FAILOVERS).incrementAndGet();
                primaryNode = hello.getMe();
            } else if (!hello.getWritablePrimary() && hello.getMe().equals(primaryNode)) {
                log.error("Primary node is not me {}", hello.getMe());
                primaryNode = null;
            }
        }

        if (hello.getHosts() != null && !hello.getHosts().isEmpty()) {
            lastHostsFromHello = hello.getHosts();

            for (String hst : hello.getHosts()) {
                synchronized (connectionPool) {
                    if (!connectionPool.containsKey(hst)) {
                        // log.debug("new host needs to be added: " + hst);
                        connectionPool.put(hst, new LinkedBlockingQueue<>());
                    }
                }

                addToHostSeed(hst);
            }

            for (String hst : getHostSeed()) {
                if (!hello.getHosts().contains(hst)) {
                    removeFromHostSeed(hst);

                    synchronized (waitCounter) {
                        waitCounter.remove(hst);
                    }
                }
            }

            //only closing connections when info comes from primary
            List<ConnectionContainer> toClose = new ArrayList<>();

            synchronized (connectionPool) {
                for (String host : new ArrayList<>(connectionPool.keySet())) {
                    if (!hello.getHosts().contains(host)) {
                        log.warn("Host {} is not part of the replicaset anymore!", host);
                        removeFromHostSeed(host);

                        synchronized (waitCounter) {
                            waitCounter.remove(host);
                        }

                        BlockingQueue<ConnectionContainer> lst = connectionPool.remove(host);
                        ArrayList<Integer> toDelete = new ArrayList<>();

                        for (var e : new ArrayList<>(borrowedConnections.entrySet())) {
                            if (e.getValue().getCon().getConnectedToHost().equals(host)) {
                                toDelete.add(e.getKey());
                            }
                        }

                        for (Integer i : toDelete) {
                            borrowedConnections.remove(i);
                        }

                        if (fastestHost != null && fastestHost.equals(host)) {
                            fastestHost = null;
                            fastestTime = 10000;
                        }

                        toClose.addAll(lst);
                    }
                }

                for (ConnectionContainer con : toClose) {
                    try {
                        con.getCon().close();

                    } catch (Exception ex) {
                    }
                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                }
            }
        }
    }

    protected synchronized void startHeartbeat() {
        if (heartbeat == null) {
            //thread to create new connections instantly if a thread is waiting
            //this thread pauses until waitCounter.notifyAll() is called
            new Thread() {
                public void run() {
                    while (heartbeat != null) {
                        try {
                            synchronized (waitCounterSignal) {
                                waitCounterSignal.wait();
                            }

                            for (String hst : getHostSeed()) {
                                try {
                                    BlockingQueue<ConnectionContainer> queue = null;

                                    synchronized (connectionPool) {
                                        queue = connectionPool.get(hst);
                                    }

                                    int loopCounter = 0;

                                    while (getHostSeed().contains(hst) && queue != null && loopCounter < getMaxConnectionsPerHost() &&
                                            (queue.size() < getWaitCounterForHost(hst) && getTotalConnectionsToHost(hst) < getMaxConnectionsPerHost())) {
                                        loopCounter++;
                                        log.debug("Creating connection to {} - WaitCounter is {}", hst, getWaitCounterForHost(hst));
                                        // System.out.println("Creating new connection to " + hst + " WaitCounter is: " + waitCounter.get(hst).get());
                                        createNewConnection(hst);
                                    }
                                } catch (Exception e) {
                                    log.error("Could not create connection to {}", hst, e);
                                    //removing connections, probably all broken now
                                    onConnectionError(hst);
                                }
                            }
                        } catch (Throwable e) {
                            log.error("error", e);
                            stats.get(DriverStatsKey.ERRORS).incrementAndGet();
                        }
                    }
                }
            } .start();
            heartbeat = executor.scheduleWithFixedDelay(() -> {
                //check every host in pool if available
                // create NEW Connection to host -> if error, remove host from connectionPool
                //        send HelloCommand to host
                //        process helloCommand (primary etc)

                for (var hst : getHostSeed()) {
                    BlockingQueue<ConnectionContainer> connectionPoolForHost = null;

                    synchronized (connectionPool) {
                        connectionPoolForHost = connectionPool.get(hst);
                    }

                    if (connectionPoolForHost != null) {
                        try {
                            //checking for lifetime of connections
                            var len = connectionPoolForHost.size();

                            for (int i = 0; i < len; i++) {
                                var connection = connectionPoolForHost.poll(1, TimeUnit.MILLISECONDS);

                                if (connection == null) break;

                                long now = System.currentTimeMillis();

                                if ((connection.getLastUsed() < now - getMaxConnectionIdleTime()) || connection.getCreated() < now - getMaxConnectionLifetime()) {
                                    log.debug("connection to host:{} too long idle {}ms or just too old {}ms -> remove", connection.getCon().getConnectedToHost(), getMaxConnectionIdleTime(), getMaxConnectionLifetime());

                                    try {
                                        connection.getCon().close();
                                    } catch (Exception e) {
                                        //swallow
                                    }
                                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                } else {
                                    connectionPoolForHost.add(connection);
                                }
                            }
                        } catch (Throwable e) {
                        }
                    }

                    if (hostThreads.containsKey(hst)) continue;

                    Thread t = new Thread(() -> {

                        try {
                            waitCounter.putIfAbsent(hst, new AtomicInteger());
                            ConnectionContainer container = null;

                            synchronized (connectionPool) {
                                if (connectionPool.get(hst) == null) {
                                    log.warn("No connectionPool for host {} creating new ConnectionContainer", hst);
                                    container = new ConnectionContainer(new SingleMongoConnection());
                                } else {
                                    container = connectionPool.get(hst).poll(1, TimeUnit.MILLISECONDS);
                                }
                            }

                            if (container != null) {
                                long start = System.currentTimeMillis();
                                HelloResult result;

                                if (container.getCon().isConnected()) {
                                    result = container.getCon().getHelloResult(false);
                                } else {
                                    result = container.getCon().connect(this, getHost(hst), getPortFromHost(hst));
                                }

                                long dur = System.currentTimeMillis() - start;

                                if (dur < fastestTime) {
                                    fastestTime = dur;
                                    fastestHost = hst;
                                }

                                pingTimesPerHost.put(hst, dur);
                                // container.touch();
                                handleHelloResult(result, getHost(hst) + ":" + getPortFromHost(hst));

                                synchronized (connectionPool) {
                                    if (connectionPool.containsKey(hst) && getTotalConnectionsToHost(hst) < getMaxConnectionsPerHost()) {
                                        connectionPool.get(hst).add(container);
                                    } else {
                                        container.getCon().close();
                                        stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                    }
                                }
                            }

                            BlockingQueue<ConnectionContainer> queue = null;

                            synchronized (connectionPool) {
                                queue = connectionPool.get(hst);
                            }

                            int wait = getWaitCounterForHost(hst);
                            int loopCounter = 0;

                            while (getHostSeed().contains(hst) && queue != null && loopCounter < getMaxConnectionsPerHost() &&
                                    ((queue.size() < wait && getTotalConnectionsToHost(hst) < getMaxConnectionsPerHost()) || getTotalConnectionsToHost(hst) < getMinConnectionsPerHost())) {
                                // log.info("Creating new connection to {}", hst);
                                // System.out.println("Creating new connection to " + hst);
                                loopCounter++;
                                // log.debug("Creating connection to {} - totalConnections to host is {}", hst, getTotalConnectionsToHost(hst));
                                createNewConnection(hst);
                            }

                            // log.info("Finished connection creation");
                        } catch (Throwable e) {
                            log.error("Could not create connection to host " + hst, e);
                            onConnectionError(hst);
                        } finally {
                            hostThreads.remove(hst);
                        }
                    });
                    hostThreads.put(hst, t);
                    t.start();
                }
            }, 0, getHeartbeatFrequency(), TimeUnit.MILLISECONDS);
        } else {
            // log.debug("Heartbeat already scheduled...");
        }
    }

    private int getWaitCounterForHost(String hst) {
        synchronized (waitCounter) {
            waitCounter.putIfAbsent(hst, new AtomicInteger());
            return waitCounter.get(hst).get();
        }
    }

    private void onConnectionError(String host) {
        //empty pool for host, as connection to it failed
        stats.get(DriverStatsKey.ERRORS).incrementAndGet();
        BlockingQueue<ConnectionContainer> connectionsList = null;

        if (getHostSeed() != null && !getHostSeed().isEmpty() && !getHostSeed().contains(host)) {
            synchronized (connectionPool) {
                //Do not remove ConnectionPool for host, if it is still in host-seed!
                connectionsList = connectionPool.remove(host);
            }
        }

        if (host.equals(primaryNode)) {
            primaryNode = null;
        }

        if (host.equals(fastestHost)) {
            fastestHost = null;
            fastestTime = 10000;
        }

        if (connectionsList != null) {
            for (var c : connectionsList) {
                try {
                    c.getCon().close();
                } catch (Exception ex) {
                    //swallow
                }
                stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
            }
        }
    }

    private void createNewConnection(String hst) throws Exception {
        // log.info("Heartbeat: WaitCounter for host {} is {}, TotalCon {} ", hst, waitCounter.get(hst).get(), getTotalConnectionsToHost(hst));
        //        log.debug("Creating connection to {}", hst);
        synchronized (connectionPool) {
            if (!connectionPool.containsKey(hst)) {
                return;
            }
        }

        var con = new SingleMongoConnection();

        if (getAuthDb() != null) {
            con.setCredentials(getAuthDb(), getUser(), getPassword());
        }

        long start = System.currentTimeMillis();
        HelloResult result = con.connect(this, getHost(hst), getPortFromHost(hst));
        stats.get(DriverStatsKey.CONNECTIONS_OPENED).incrementAndGet();

        synchronized (connectionPool) {
            if (connectionPool.containsKey(hst) && (connectionPool.get(hst).size() < getWaitCounterForHost(hst) && getTotalConnectionsToHost(hst) < getMaxConnectionsPerHost() ||
                                                    getTotalConnectionsToHost(hst) < getMinConnectionsPerHost())) {
                var cont = new ConnectionContainer(con);
                connectionPool.get(hst).add(cont);
            } else {

                stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                con.close();
            }
        }

        long dur = System.currentTimeMillis() - start;

        if (dur < fastestTime) {
            fastestTime = dur;
            fastestHost = hst;
        }

        handleHelloResult(result, getHost(hst) + ":" + getPortFromHost(hst));
    }

    @Override
    public void watch(WatchCommand settings) throws MorphiumDriverException {
        MongoConnection con = null;

        try {
            con = getPrimaryConnection(null);
            con.watch(settings);
        } finally {
            if (con != null) {
                releaseConnection(con);
            }
        }
    }

    private int getTotalConnectionsToHost(String h) {
        int borrowed = 0;
        Collection<ConnectionContainer> values = new ArrayList<>(borrowedConnections.values());

        for (var c : values) {
            if (c.getCon().getConnectedTo().equals(h)) {
                borrowed++;
            }
        }

        synchronized (connectionPool) {
            if (connectionPool.get(h) == null) {
                return borrowed;
            }

            return borrowed + connectionPool.get(h).size();
        }
    }

    private MongoConnection borrowConnection(String host) throws MorphiumDriverException {
        // log.debug("borrowConnection {}", host);
        if (host == null) throw new MorphiumDriverException("Cannot connect to host null!");

        // if pool is empty  -> wait increaseWaitCounter
        //
        // if connection available in pool -> put in borrowedConnections -> return That
        boolean needToDecrement = false;

        try {
            ConnectionContainer bc = null;
            BlockingQueue<ConnectionContainer> queue = null;

            synchronized (connectionPool) {
                //connectionPool.putIfAbsent(host, new LinkedBlockingQueue<>());
                if (!connectionPool.containsKey(host)) {
                    log.error("No connectionpool for host {}", host);
                    throw new MorphiumDriverException("No connectionpool for " + host + " available");
                }

                queue = connectionPool.get(host);

                if (queue.isEmpty()) {
                    synchronized (waitCounter) {
                        waitCounter.putIfAbsent(host, new AtomicInteger());

                        if (getWaitCounterForHost(host) < getMaxConnectionsPerHost()) {
                            waitCounter.get(host).incrementAndGet();
                            needToDecrement = true;
                        }

                        //although we probably won't get a connection, notify anyways
                        //
                        synchronized (waitCounterSignal) {
                            waitCounterSignal.notifyAll();
                        }
                    }
                }
            }

            do {
                if (getServerSelectionTimeout() <= 0) {
                    bc = queue.poll(Integer.MAX_VALUE, TimeUnit.SECONDS);
                } else {
                    bc = queue.poll(getServerSelectionTimeout(), TimeUnit.MILLISECONDS);
                }

                if (bc == null) {
                    log.error("Connection timeout");
                    log.error("Connections to {}: {}", host, getTotalConnectionsToHost(host));
                    log.error("WaitingThreads for {}: {}", host, getWaitCounterForHost(host));
                    throw new MorphiumDriverException("Could not get connection to " + host + " in time " + getMaxWaitTime() + "ms");
                }

                if (bc.getCon().getSourcePort() == 0) {
                    //broken
                    stats.get(DriverStatsKey.ERRORS).incrementAndGet();
                }
            } while (bc.getCon().getSourcePort() == 0);

            bc.touch();
            borrowedConnections.put(bc.getCon().getSourcePort(), bc);
            stats.get(DriverStatsKey.CONNECTIONS_BORROWED).incrementAndGet();
            return bc.getCon();
        } catch (InterruptedException iex) {
            //swallow - might happen when closing
            //throw new MorphiumDriverException("Waiting for connection was aborted");
            return new SingleMongoConnection();
        } finally {
            if (needToDecrement && getWaitCounterForHost(host) > 0) {
                AtomicInteger atomicInteger = waitCounter.get(host);

                if (atomicInteger != null) {
                    atomicInteger.decrementAndGet();
                }
            }
        }
    }

    @Override
    public MongoConnection getReadConnection(ReadPreference rp) {
        try {
            if (getHostSeed().size() == 1 || !isReplicaSet()) {
                // no replicaset
                if (primaryNode == null) {
                    return borrowConnection(getHostSeed().get(0));
                }

                return borrowConnection(primaryNode);
            }

            if (rp == null) {
                rp = getDefaultReadPreference();
            }

            var type = rp.getType();

            if (isTransactionInProgress()) {
                type = ReadPreferenceType.PRIMARY;
            }

            switch (type) {
                case PRIMARY:
                    long start = System.currentTimeMillis();
                    long timeout = getServerSelectionTimeout();

                    if (timeout <= 0) {
                        timeout = 1000;
                    }

                    while (primaryNode == null) {
                        if (System.currentTimeMillis() - start > timeout) {
                            throw new MorphiumDriverException("No primary node defined - not connected yet?");
                        }

                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            //swallow
                        }
                    }

                    return borrowConnection(primaryNode);

                case NEAREST:

                    // check fastest answer time
                    if (fastestHost != null) {
                        try {
                            return borrowConnection(fastestHost);
                        } catch (MorphiumDriverException e) {
                            stats.get(DriverStatsKey.ERRORS).incrementAndGet();
                            log.warn("Could not get connection to fastest host, trying primary", e);
                        }
                    }

                case PRIMARY_PREFERRED:
                    synchronized (connectionPool) {
                        if (null != connectionPool.get(primaryNode) && !connectionPool.get(primaryNode).isEmpty()) {
                            try {
                                return borrowConnection(primaryNode);
                            } catch (MorphiumDriverException e) {
                                stats.get(DriverStatsKey.ERRORS).incrementAndGet();
                                log.warn("Could not get connection to {} trying secondary", primaryNode);
                            }
                        }
                    }

                case SECONDARY_PREFERRED:
                case SECONDARY:
                    int retry = 0;

                    while (true) {
                        // round-robin
                        String host = null;

                        synchronized (lastSecondaryNode) {
                            lastSecondaryNode.incrementAndGet();
                            List<String> hostSeed = getHostSeed();

                            if (lastSecondaryNode.get() >= hostSeed.size()) {
                                lastSecondaryNode.set(0);
                                retry++;
                            }

                            if (hostSeed.get(lastSecondaryNode.get()).equals(primaryNode)) {
                                lastSecondaryNode.incrementAndGet();

                                if (lastSecondaryNode.get() >= hostSeed.size()) {
                                    lastSecondaryNode.set(0);
                                    retry++;
                                }
                            }

                            if (getLocalThreshold() > 0) {
                                if (!pingTimesPerHost.containsValue(host) || pingTimesPerHost.containsKey(host) && pingTimesPerHost.get(host) <= fastestTime + getLocalThreshold()) {
                                    host = hostSeed.get(lastSecondaryNode.get());
                                } else {
                                    continue;
                                }
                            } else {
                                host = hostSeed.get(lastSecondaryNode.get());
                            }
                        }

                        try {
                            return borrowConnection(host);
                        } catch (MorphiumDriverException e) {
                            if (retry > getRetriesOnNetworkError()) {
                                log.error("Could not get Connection - abort");
                                stats.get(DriverStatsKey.ERRORS).incrementAndGet();
                                throw (e);
                            }

                            log.warn("could not get connection to secondary node '{}'- trying other replicaset node", host, e);
                            onConnectionError(host);

                            try {
                                Thread.sleep(getSleepBetweenErrorRetries());
                            } catch (InterruptedException e1) {
                                // Swallow
                            }
                        }
                    }

                default:
                    throw new IllegalArgumentException("Unhandeled Readpreferencetype " + rp.getType());
            }
        } catch (MorphiumDriverException e) {
            log.error("Error getting connection", e);
            stats.get(DriverStatsKey.ERRORS).incrementAndGet();
            throw new RuntimeException(e);
        }
    }

    @Override
    public MongoConnection getPrimaryConnection(WriteConcern wc) throws MorphiumDriverException {
        if (primaryNode == null) {
            throw new MorphiumDriverException("No primary node found - connection not established yet?");
        }

        return borrowConnection(primaryNode);
    }
    @Override
    public void closeConnection(MongoConnection con) {
        releaseConnection(con);

        synchronized (connectionPool) {
            for (String k : connectionPool.keySet()) {
                for (ConnectionContainer c : new ArrayList<>(connectionPool.get(k))) { // avoid concurrendModification
                    if (c.getCon() == con) {
                        connectionPool.get(k).remove(c);
                        return;
                    }
                }
            }
        }
    }
    public Map<Integer, ConnectionContainer> getBorrowedConnections() {
        synchronized (connectionPool) {
            return new HashMap<>(borrowedConnections);
        }
    }
    @Override
    public void releaseConnection(MongoConnection con) {
        stats.get(DriverStatsKey.CONNECTIONS_RELEASED).incrementAndGet();

        if (con == null) {
            return;
        }

        if (heartbeat == null) {
            return; //shutting down
        }

        if (!(con instanceof SingleMongoConnection)) {
            throw new IllegalArgumentException("Got connection of wrong type back!");
        }

        if (con.getSourcePort() != 0) { //sourceport== 0 probably closed or broken
            var c = borrowedConnections.remove(con.getSourcePort());

            if (c == null) {
                //log.debug("Returning not borrowed connection!?!?");
                if (con.isConnected()) {
                    // c = new Connection((SingleMongoConnection) con);

                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                    con.close();
                }

                return;
            }

            if (con.getConnectedTo() != null) {
                synchronized (connectionPool) {
                    //connectionPool.putIfAbsent(con.getConnectedTo(), new LinkedBlockingQueue<>());
                    var connectionContainer = connectionPool.get(con.getConnectedTo());

                    if (null != connectionContainer) {
                        connectionContainer.add(c);
                    }
                }
            }
        } else {
            List<Integer> sourcePortsToDelete = new ArrayList<>();

            for (int port : new ArrayList<Integer>(borrowedConnections.keySet())) {
                ConnectionContainer connectionContainer = borrowedConnections.get(port);

                if (connectionContainer == null || connectionContainer.getCon() == null || connectionContainer.getCon().getSourcePort() == 0) {
                    sourcePortsToDelete.add(port);
                }
            }

            for (int port : sourcePortsToDelete) {
                borrowedConnections.remove(port);
            }

            stats.get(DriverStatsKey.CONNECTIONS_RELEASED).incrementAndGet();
        }
    }
    public boolean isConnected() {
        synchronized (connectionPool) {
            for (var c : connectionPool.keySet()) {
                if (getTotalConnectionsToHost(c) != 0) {
                    return true;
                }
            }
        }

        return false;
    }
    @Override
    public int getIdleSleepTime() {
        return idleSleepTime;
    }
    @Override
    public void setIdleSleepTime(int sl) {
        idleSleepTime = sl;
    }
    @Override
    public <T, R> Aggregator<T, R> createAggregator(Morphium morphium, Class<? extends T > type, Class <? extends R > resultType) {
        return new AggregatorImpl<>(morphium, type, resultType);
    }
    @Override
    public String getName() {
        return driverName;
    }
    @Override
    public void setConnectionUrl(String connectionUrl) {
    }
    @Override
    public void close() {
        if (heartbeat != null) {
            heartbeat.cancel(true);
        }

        heartbeat = null;

        if (executor != null) {
            executor.shutdownNow();
        }

        synchronized (connectionPool) {
            for (var e : new ArrayList<>(connectionPool.entrySet())) {
                for (var c : new ArrayList<>(e.getValue())) {
                    try {
                        c.getCon().close();
                    } catch (Exception ex) {
                    }
                }

                connectionPool.get(e.getKey()).clear();
                stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
            }
        }
    }
    protected void killCursors(String db, String coll, long... ids) throws MorphiumDriverException {
        List<Long> cursorIds = new ArrayList<>();

        for (long l : ids) {
            if (l != 0) {
                cursorIds.add(l);
            }
        }

        if (cursorIds.isEmpty()) {
            return;
        }

        KillCursorsCommand k = new KillCursorsCommand(null).setCursors(cursorIds).setDb(db).setColl(coll);
        var ret = k.execute();
        // log.debug("killed cursor");
    }
    @Override
    public void commitTransaction() throws MorphiumDriverException {
        if (getTransactionContext() == null) {
            throw new IllegalArgumentException("No transaction in progress, cannot commit");
        }

        MorphiumTransactionContext ctx = getTransactionContext();
        MongoConnection con = getPrimaryConnection(null);
        var cmd = new CommitTransactionCommand(con).setTxnNumber(ctx.getTxnNumber()).setAutocommit(false).setLsid(ctx.getLsid());
        cmd.execute();
        // getPrimaryConnection(null).sendCommand(Doc.of("commitTransaction", 1,
        // "txnNumber", ctx.getTxnNumber(), "autocommit", false, "lsid", Doc.of("id",
        // ctx.getLsid()), "$db", "admin"));
        clearTransactionContext();
        releaseConnection(con);
    }
    @Override
    public void abortTransaction() throws MorphiumDriverException {
        if (getTransactionContext() == null) {
            throw new IllegalArgumentException("No transaction in progress, cannot abort");
        }

        MongoConnection con = getPrimaryConnection(null);

        try {
            MorphiumTransactionContext ctx = getTransactionContext();
            var cmd = new AbortTransactionCommand(con).setTxnNumber(ctx.getTxnNumber()).setAutocommit(false).setLsid(ctx.getLsid());
            cmd.execute();
        } finally {
            releaseConnection(con);
            clearTransactionContext();
        }
    }
    @Override
    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        MongoConnection con = null;

        try {
            con = getPrimaryConnection(null);
            ReplicastStatusCommand cmd = new ReplicastStatusCommand(con);
            var result = cmd.execute();
            @SuppressWarnings("unchecked")
            List<Doc> mem = (List) result.get("members");

            if (mem == null) {
                return null;
            }

            // noinspection unchecked
            mem.stream().filter(d -> d.get("optime") instanceof Map).forEach(d -> d.put("optime", ((Map<String, Doc>) d.get("optime")).get("ts")));
            return result;
        } finally {
            releaseConnection(con);
        }
    }
    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        MongoConnection con = null;

        try {
            con = getPrimaryConnection(null);
            return new DbStatsCommand(con).setDb(db).execute();
        } finally {
            releaseConnection(con);
        }
    }
    @Override
    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        CollStatsCommand cmd = new CollStatsCommand(getPrimaryConnection(null)).setColl(coll).setDb(db);
        return cmd.execute();
    }
    public List<Map<String, Object >> currentOp(int threshold) throws MorphiumDriverException {
        CurrentOpCommand cmd = null;

        try {
            cmd = new CurrentOpCommand(getPrimaryConnection(null)).setColl("admin").setSecsRunning(threshold);
            return cmd.execute();
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }
    }
    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        if (crs == null) {
            return;
        }

        killCursors(crs.getDb(), crs.getCollection(), crs.getCursorId());
    }
    public Map<String, Object> getDbStats(String db, boolean withStorage) throws MorphiumDriverException {
        return new NetworkCallHelper<Map<String, Object >> ().doCall(() -> {
            OpMsg msg = new OpMsg();
            msg.setMessageId(getNextId());
            Map<String, Object> v = Doc.of("dbStats", 1, "scale", 1024);
            v.put("$db", db);

            if (withStorage) {
                v.put("freeStorage", 1);
            }
            msg.setFirstDoc(v);
            // connection.sendQuery(msg);
            OpMsg reply = null; // connection.getReplyFor(msg.getMessageId(), getMaxWaitTime());
            return reply.getFirstDoc();
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }
    public boolean exists(String db) throws MorphiumDriverException {
        // noinspection EmptyCatchBlock
        try {
            getDBStats(db);
            return true;
        } catch (MorphiumDriverException e) {
        }

        return false;
    }
    public Map<String, Object> getDbStats(String db) throws MorphiumDriverException {
        return getDbStats(db, false);
    }
    private List<Map<String, Object >> getCollectionInfo(String db, String collection) throws MorphiumDriverException {
        // noinspection unchecked
        return new NetworkCallHelper<List<Map<String, Object >>> ().doCall(() -> {
            var con = getReadConnection(ReadPreference.primary());
            ListCollectionsCommand cmd = new ListCollectionsCommand(con);
            cmd.setDb(db);
            cmd.setFilter(Doc.of("name", collection));
            return cmd.execute();
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }
    @Override
    public Map<String, Integer> getNumConnectionsByHost() {
        Map<String, Integer> ret = new HashMap<>();

        synchronized (connectionPool) {
            for (var e : connectionPool.entrySet()) {
                ret.put(e.getKey(), e.getValue().size());
            }

            for (var e : borrowedConnections.values()) {
                ret.put(e.getCon().getConnectedTo(), ret.get(e.getCon().getConnectedTo()).intValue() + 1);
            }
        }

        return ret;
    }
    @Override
    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        List<Map<String, Object >> lst = getCollectionInfo(db, coll);
        try {
            if (!lst.isEmpty() && lst.get(0).get("name") != null && lst.get(0).get("name").equals(coll)) {
                Object capped = ((Map) lst.get(0).get("options")).get("capped");
                return capped != null && capped.equals(true);
            }
        } catch (Exception e) {
            log.error("Error", e);
            stats.get(DriverStatsKey.ERRORS).incrementAndGet();
        }
        return false;
    }
    private class ConnectionContainer {
        private SingleMongoConnection con;
        private long created;
        private long lastUsed;

        public ConnectionContainer(SingleMongoConnection con) {
            this.con = con;
            created = System.currentTimeMillis();
            lastUsed = System.currentTimeMillis();
        }

        public void touch() {
            lastUsed = System.currentTimeMillis();
        }

        public SingleMongoConnection getCon() {
            return con;
        }

        public ConnectionContainer setCon(SingleMongoConnection con) {
            this.con = con;
            return this;
        }

        public long getCreated() {
            return created;
        }

        public ConnectionContainer setCreated(long created) {
            this.created = created;
            return this;
        }

        public long getLastUsed() {
            return lastUsed;
        }

        public ConnectionContainer setLastUsed(long lastUsed) {
            this.lastUsed = lastUsed;
            return this;
        }
    }
    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return new BulkRequestContext(m) {
            private final List<BulkRequest> requests = new ArrayList<>();
            public Doc execute() {
                try {
                    for (BulkRequest r : requests) {
                        if (r instanceof InsertBulkRequest) {
                            InsertMongoCommand settings = new InsertMongoCommand(getPrimaryConnection(null));
                            settings.setDb(db).setColl(collection).setComment("Bulk insert").setDocuments(((InsertBulkRequest) r).getToInsert());
                            settings.execute();
                            settings.releaseConnection();
                        } else if (r instanceof UpdateBulkRequest) {
                            UpdateBulkRequest up = (UpdateBulkRequest) r;
                            UpdateMongoCommand upCmd = new UpdateMongoCommand(getPrimaryConnection(null));
                            upCmd.setColl(collection).setDb(db).setUpdates(Arrays.asList(Doc.of("q", up.getQuery(), "u", up.getCmd(), "upsert", up.isUpsert(), "multi", up.isMultiple())));
                            upCmd.execute();
                            upCmd.releaseConnection();
                        } else if (r instanceof DeleteBulkRequest) {
                            DeleteBulkRequest dbr = ((DeleteBulkRequest) r);
                            DeleteMongoCommand del = new DeleteMongoCommand(getPrimaryConnection(null));
                            del.setColl(collection).setDb(db).setDeletes(Arrays.asList(Doc.of("q", dbr.getQuery(), "limit", dbr.isMultiple() ? 0 : 1)));
                            del.execute();
                            del.releaseConnection();
                        } else {
                            throw new RuntimeException("Unknown operation " + r.getClass().getName());
                        }
                    }
                } catch (MorphiumDriverException e) {
                    log.error("Got exception: ", e);
                    stats.get(DriverStatsKey.ERRORS).incrementAndGet();
                }

                return new Doc();
            }
            public UpdateBulkRequest addUpdateBulkRequest() {
                UpdateBulkRequest up = new UpdateBulkRequest();
                requests.add(up);
                return up;
            }
            public InsertBulkRequest addInsertBulkRequest(List<Map<String, Object >> toInsert) {
                InsertBulkRequest in = new InsertBulkRequest(toInsert);
                requests.add(in);
                return in;
            }
            public DeleteBulkRequest addDeleteBulkRequest() {
                DeleteBulkRequest del = new DeleteBulkRequest();
                requests.add(del);
                return del;
            }
        };
    }
    @Override
    public Map<DriverStatsKey, Double> getDriverStats() {
        Map<DriverStatsKey, Double> m = new HashMap<>();

        for (var e : stats.entrySet()) {
            m.put(e.getKey(), e.getValue().get());
        }

        synchronized (connectionPool) {
            for (var l : connectionPool.values()) {
                m.put(DriverStatsKey.CONNECTIONS_IN_POOL, m.get(DriverStatsKey.CONNECTIONS_IN_POOL) + l.size());

                for (var con : l) {
                    for (var entry : con.getCon().getStats().entrySet()) {
                        m.put(entry.getKey(), m.get(entry.getKey()).doubleValue() + entry.getValue());
                    }
                }
            }
        }

        m.put(DriverStatsKey.CONNECTIONS_IN_USE, Double.valueOf(borrowedConnections.size()));
        int waiting = 0;

        for (String hst : new ArrayList<String>(waitCounter.keySet())) {
            waiting += getWaitCounterForHost(hst);
        }

        m.put(DriverStatsKey.THREADS_WAITING_FOR_CONNECTION, Double.valueOf(waiting));
        return m;
    }
}
