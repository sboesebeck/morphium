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

    public record PingStats(
                    long lastPing,
                    long averagePing,
                    long minPing,
                    long maxPing,
                    int sampleCount,
                    long lastUpdated
    ) {
        public PingStats updateWith(long newPing) {
            if (sampleCount == 0) {
                return new PingStats(newPing, newPing, newPing, newPing, 1, System.currentTimeMillis());
            }

            long newAvg = (averagePing * sampleCount + newPing) / (sampleCount + 1);
            return new PingStats(
                                   newPing,
                                   newAvg,
                                   Math.min(minPing, newPing),
                                   Math.max(maxPing, newPing),
                                   Math.min(sampleCount + 1, 100), // Cap at 100 samples
                                   System.currentTimeMillis()
                   );
        }
    }

    private record StatsSnapshot(
                    Map<DriverStatsKey, Double> driverStats,
                    int totalPooledConnections,
                    int totalBorrowedConnections,
                    int totalWaitingThreads,
                    Map<DriverStatsKey, Double> aggregatedConnectionStats
    ) {
        Map<DriverStatsKey, Double> toMap() {
            Map<DriverStatsKey, Double> result = new HashMap<>(driverStats);
            result.put(DriverStatsKey.CONNECTIONS_IN_POOL, (double) totalPooledConnections);
            result.put(DriverStatsKey.CONNECTIONS_IN_USE, (double) totalBorrowedConnections);
            result.put(DriverStatsKey.THREADS_WAITING_FOR_CONNECTION, (double) totalWaitingThreads);
            result.putAll(aggregatedConnectionStats);
            return result;
        }
    }

    public static final String driverName = "PooledDriver";
    private final Map<String, Host> hosts = new ConcurrentHashMap<>();
    private volatile boolean running;
    private final Map<Integer, ConnectionContainer> borrowedConnections;
    private final Map<DriverStatsKey, AtomicDecimal> stats;
    private volatile long fastestTime = 10000;
    private int idleSleepTime = 5;
    private volatile String fastestHost = null;
    private final Logger log = LoggerFactory.getLogger(PooledDriver.class);
    private volatile String primaryNode;
    private final Object primaryNodeLock = new Object();  // Lock for primaryNode updates only
    private volatile boolean inMemoryBackend = false;
    private volatile boolean morphiumServer = false;
    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5,
        Thread.ofVirtual().name("MCon-", 0).factory());

    private final AtomicInteger lastSecondaryNode = new AtomicInteger(0);
    private final Map<String, Thread> hostThreads = new ConcurrentHashMap<>();
    private int serverSelectionTimeout = 2000;

    // Stats caching
    private volatile StatsSnapshot cachedStats = null;
    private volatile long lastStatsUpdate = 0;
    private static final long STATS_CACHE_TTL = 1000; // 1 second
    private final AtomicLong statsDirtyCounter = new AtomicLong(0);
    private volatile long cachedStatsDirtyCounter = -1;

    public PooledDriver() {
        running = true;
        borrowedConnections = new ConcurrentHashMap<>();
        stats = new ConcurrentHashMap<>();

        for (var e : DriverStatsKey.values()) {
            stats.put(e, new AtomicDecimal(0));
        }
    }

    private void markStatsDirty() {
        statsDirtyCounter.incrementAndGet();
        cachedStats = null;
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
        // creating min connections for each host
        for (String host : getHostSeed()) {
            hosts.put(host, new Host(getHost(host), getPortFromHost(host)));
        }

        setReplicaSet(getHostSeed().size() > 1);

        // Proactively establish at least one connection per seed to discover primary immediately.
        // Relying solely on the async heartbeat can lead to races where early operations (e.g. exists/listCollections
        // during Morphium startup) run before primary discovery, causing intermittent "No primary node found".
        for (String host : new ArrayList<>(getHostSeed())) {
            try {
                createNewConnection(host);
            } catch (Exception e) {
                // swallow: unreachable seed(s) are handled by the heartbeat/error logic
                if (log.isDebugEnabled()) {
                    log.debug("Initial connect to seed {} failed", host, e);
                }
            }
        }

        startHeartbeat();

        // Wait (briefly) for primary discovery on replica sets.
        if (isReplicaSet() && getHostSeed().size() > 1) {
            long start = System.currentTimeMillis();
            long timeout = getServerSelectionTimeout();
            if (timeout <= 0) timeout = 1000;

            while (primaryNode == null) {
                if (System.currentTimeMillis() - start > timeout) {
                    throw new MorphiumDriverException("No primary node found - not connected yet?");
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new MorphiumDriverException("Interrupted while waiting for primary discovery", ie);
                }
            }
        } else if (primaryNode == null && !getHostSeed().isEmpty()) {
            // Non-replicaset: treat first seed as primary.
            primaryNode = getHostSeed().get(0);
        }
    }

    @Override
    public ReadPreference getDefaultReadPreference() {
        // Defaulting to PRIMARY keeps behavior consistent with MongoDB drivers and avoids
        // surprising "read-your-writes" issues on replica sets when no explicit RP is set.
        return ReadPreference.primary();
    }

    @Override
    public synchronized void removeFromHostSeed(String host) {
        super.removeFromHostSeed(host);
        Host removed = hosts.remove(host);

        if (removed != null) {
            // Close pooled connections for the removed host to avoid untracked open sockets and drifting stats.
            for (var c : new ArrayList<>(removed.getConnectionPool())) {
                try {
                    c.getCon().close();
                } catch (Exception ignored) {
                }
                stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                markStatsDirty();
            }
            removed.getConnectionPool().clear();
        }

        if (getNumHostsInSeed() == 0) {
            if (lastHostsFromHello == null) {
                log.warn("Wanted to remove last host in hostseed, but last hosts is null");
                addToHostSeed(host);
            } else {
                setHostSeed(lastHostsFromHello);
            }
        }
    }

    private volatile ScheduledFuture<?> heartbeat;
    private final Object waitCounterSignal = new Object();
    private List<String> lastHostsFromHello = null;
    /**
     * Some replica sets advertise members using hostnames that differ from the seed list
     * (e.g. short hostnames vs FQDNs). Keep a best-effort alias map from server-reported
     * host:port to the actually reachable host:port we connected to, so primary selection
     * and pool lookups keep working.
     */
    private final ConcurrentHashMap<String, String> hostAliases = new ConcurrentHashMap<>();

    private String resolveAlias(String hostPort) {
        if (hostPort == null) return null;
        return hostAliases.getOrDefault(hostPort, hostPort);
    }

    private void registerAlias(String serverReported, String connectedAs) {
        if (serverReported == null || connectedAs == null) return;
        hostAliases.putIfAbsent(serverReported, connectedAs);
        hostAliases.putIfAbsent(connectedAs, connectedAs);
    }

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
        if (!running) return;
        if (hello == null)
            return;

        // Check if connected to MorphiumServer (InMemory backend)
        if (Boolean.TRUE.equals(hello.getMorphiumServer())) {
            morphiumServer = true;
        }
        if (Boolean.TRUE.equals(hello.getInMemoryBackend())) {
            inMemoryBackend = true;
        }

        // Keep track of server-advertised vs reachable names.
        registerAlias(hello.getMe(), hostConnected);

        // IMPORTANT: Add hosts from hello BEFORE checking for primary node.
        // Otherwise, when connecting to a secondary first, the advertised primary
        // won't be in the hosts map and we can't set primaryNode from it.
        if (hello.getHosts() != null && !hello.getHosts().isEmpty()) {
            lastHostsFromHello = hello.getHosts();

            for (String hst : hello.getHosts()) {
                String resolved = resolveAlias(hst);
                if (!hosts.containsKey(resolved)) {
                    hosts.put(resolved, new Host(getHost(resolved), getPortFromHost(resolved)));
                }

                addToHostSeed(resolved);
            }
        }

        // Synchronize only the primaryNode update logic to prevent race conditions
        // during concurrent heartbeat processing from multiple hosts
        synchronized (primaryNodeLock) {
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
                if (hello.getWritablePrimary() && primaryNode == null) {
                    // Prefer the actually reachable address we used for this connection.
                    primaryNode = hostConnected;
                } else if (hello.getWritablePrimary() && !hostConnected.equals(primaryNode)) {
                    log.warn("Primary failover? {} -> {}", primaryNode, hostConnected);
                    stats.get(DriverStatsKey.FAILOVERS).incrementAndGet();
                    primaryNode = hostConnected;
                } else if (!hello.getWritablePrimary() && hostConnected.equals(primaryNode)) {
                    log.error("Primary node is not me {}", hello.getMe());
                    primaryNode = null;
                } else if (primaryNode == null && hello.getPrimary() != null) {
                    // Only use the advertised primary if it maps to a known/reachable host key.
                    String advertised = resolveAlias(hello.getPrimary());
                    if (hosts.containsKey(advertised)) {
                        primaryNode = advertised;
                    }
                }
            }
        }

        if (hello.getHosts() != null && !hello.getHosts().isEmpty()) {

            // Do NOT remove existing host seed entries here.
            // Users might provide a seed list using different (but resolvable) hostnames than those
            // advertised by the replica set configuration. Removing seeds can make the driver lose
            // the only reachable addresses and lead to "No primary node found".

            // Build a set of resolved hostnames from hello.getHosts() for comparison
            // This handles the case where the server reports names like "macbook:27017" but
            // we connected as "localhost:27017" - both should be considered the same host
            java.util.Set<String> resolvedHelloHosts = new java.util.HashSet<>();
            for (String h : hello.getHosts()) {
                resolvedHelloHosts.add(resolveAlias(h));
            }

            // only closing connections when info comes from primary
            List<ConnectionContainer> toClose = new ArrayList<>();
            for (var it = hosts.entrySet().iterator(); it.hasNext();) {
                var entry = it.next();
                var host = entry.getKey();
                if (!resolvedHelloHosts.contains(host)) {
                    log.warn("Host {} is not part of the replicaset anymore!", host);
                    it.remove();
                    Host h = entry.getValue();
                    removeFromHostSeed(host);

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

                    toClose.addAll(h.getConnectionPool());
                }
            }

            for (ConnectionContainer con : toClose) {
                try {
                    con.getCon().close();
                } catch (Exception ex) {
                }
                stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                markStatsDirty();
            }
        }
    }

        protected synchronized void startHeartbeat() {
            if (heartbeat == null) {
                heartbeat = executor.scheduleWithFixedDelay(() -> {
                    // check every host in pool if available
                    // create NEW Connection to host -> if error, remove host from connectionPool
                    // send HelloCommand to host
                    // process helloCommand (primary etc)
    
                    for (var entry : hosts.entrySet()) {
                        var hst = entry.getKey();
                        var host = entry.getValue();
                        BlockingQueue<ConnectionContainer> connectionPoolForHost = host.getConnectionPool();
    
                        if (connectionPoolForHost != null) {
                            try {
                                // checking for lifetime of connections
                                var len = connectionPoolForHost.size();
    
                                for (int i = 0; i < len; i++) {
                                    var connection = connectionPoolForHost.poll(1, TimeUnit.MILLISECONDS);
    
                                    if (connection == null)
                                        break;
                                    host.incrementInternalInUseConnections();
    
                                    try {
                                        long now = System.currentTimeMillis();

                                        if ((connection.getLastUsed() < now - getMaxConnectionIdleTime())
                                                || connection.getCreated() < now - getMaxConnectionLifetime()) {
                                            log.debug("connection to host:{} too long idle {}ms or just too old {}ms -> remove",
                                                    connection.getCon().getConnectedToHost(), getMaxConnectionIdleTime(),
                                                    getMaxConnectionLifetime());

                                            try {
                                                connection.getCon().close();
                                            } catch (Exception e) {
                                                // swallow
                                            }
                                            stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                            markStatsDirty();
                                        } else {
                                            connectionPoolForHost.add(connection);
                                        }
                                    } finally {
                                        host.decrementInternalInUseConnections();
                                    }
                                }
                            } catch (Throwable e) {
                            }
                        }
    
                        if (hostThreads.containsKey(hst))
                            continue;
    
                        Thread t = Thread.ofVirtual().name("HeartbeatCheck-" + hst).start(() -> {
    
                            try {
                                ConnectionContainer container = null;
    
                                if (host.getConnectionPool() == null) {
                                    log.warn("No connectionPool for host {} creating new ConnectionContainer", hst);
                                    container = new ConnectionContainer(new SingleMongoConnection());
                                } else {
                                    container = host.getConnectionPool().poll(1, TimeUnit.MILLISECONDS);
                                }
    
                                if (container != null) {
                                    host.incrementInternalInUseConnections();
                                    try {
                                        long start = System.currentTimeMillis();
                                        HelloResult result;

                                        if (container.getCon().isConnected()) {
                                            result = container.getCon().getHelloResult(false);
                                        } else {
                                            result = container.getCon().connect(this, getHost(hst), getPortFromHost(hst));
                                        }

                                        long dur = System.currentTimeMillis() - start;

                                        PingStats newStats = host.getPingStats().updateWith(dur);
                                        host.setPingStats(newStats);
                                        host.resetFailures();

                                        // Use record patterns to update fastest host
                                        updateFastestHost(hst, newStats);
                                        // container.touch();
                                        handleHelloResult(result, String.format("%s:%d", getHost(hst), getPortFromHost(hst)));

                                        if (hosts.containsKey(hst)
                                                && getTotalConnectionsToHost(hst) < getMaxConnectionsPerHost()) {
                                            host.getConnectionPool().add(container);
                                        } else {
                                            container.getCon().close();
                                            stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                            markStatsDirty();
                                        }
                                    } finally {
                                        host.decrementInternalInUseConnections();
                                    }
                                }
    
                                BlockingQueue<ConnectionContainer> queue = host.getConnectionPool();
    
                                int wait = host.getWaitCounter();
                                int loopCounter = 0;
    
                                while (getHostSeed().contains(hst) && queue != null
                                        && loopCounter < getMaxConnectionsPerHost() &&
                                        ((queue.size() < wait
                                          && getTotalConnectionsToHost(hst) < getMaxConnectionsPerHost())
                                         || getTotalConnectionsToHost(hst) < getMinConnectionsPerHost())) {
                                    // log.info("Creating new connection to {}", hst);
                                    // System.out.println("Creating new connection to " + hst);
                                    loopCounter++;
                                    // log.debug("Creating connection to {} - totalConnections to host is {}", hst,
                                    // getTotalConnectionsToHost(hst));
                                    createNewConnection(hst);
                                }
    
                                // log.info("Finished connection creation");
                            } catch (Throwable e) {
                                log.error("Could not create connection to host {}", hst, e);
                                onConnectionError(hst);
                            } finally {
                                hostThreads.remove(hst);
                            }
                        });
                        hostThreads.put(hst, t);
                    }
                }, 0, getHeartbeatFrequency(), TimeUnit.MILLISECONDS);
                // thread to create new connections instantly if a thread is waiting
                // this thread pauses until waitCounter.notifyAll() is called
                            Thread.ofVirtual().name("ConnectionWaiter").start(() -> {
                                while (running) {
                                    try {
                                        synchronized (waitCounterSignal) {
                                            waitCounterSignal.wait();
                                        }

                                        for (String hst : getHostSeed()) {
                                            try {
                                                if (hosts.get(hst) == null) continue;

                                                // Calculate how many new connections we need
                                                int waitCount = getWaitCounterForHost(hst);
                                                int poolSize = hosts.get(hst).getConnectionPool().size();
                                                int totalConnections = getTotalConnectionsToHost(hst);
                                                int maxConnections = getMaxConnectionsPerHost();

                                                // Number of connections to create (limited by max and available capacity)
                                                int needed = Math.min(waitCount - poolSize, maxConnections - totalConnections);

                                                if (needed > 0 && getHostSeed().contains(hst)) {
                                                    // Create connections in parallel for burst scenarios
                                                    int parallelCreators = Math.min(needed, 10); // Cap at 10 parallel creators
                                                    final String host = hst;

                                                    for (int i = 0; i < parallelCreators; i++) {
                                                        Thread.ofVirtual().name("ConnectionCreator-" + i).start(() -> {
                                                            try {
                                                                // Each creator can create multiple connections
                                                                while (running && getHostSeed().contains(host)
                                                                        && hosts.get(host).getConnectionPool().size() < getWaitCounterForHost(host)
                                                                        && getTotalConnectionsToHost(host) < getMaxConnectionsPerHost()) {
                                                                    createNewConnection(host);
                                                                }
                                                            } catch (Exception e) {
                                                                log.debug("Connection creator finished: {}", e.getMessage());
                                                            }
                                                        });
                                                    }
                                                }
                                            } catch (Exception e) {
                                                log.error("Could not create connection to {}", hst, e);
                                                // removing connections, probably all broken now
                                                onConnectionError(hst);
                                            }
                                        }
                                    } catch (Throwable e) {
                                        log.error("error", e);
                                        stats.get(DriverStatsKey.ERRORS).incrementAndGet();
                                    }
                                }
                            });            } else {
                // log.debug("Heartbeat already scheduled...");
            }
        }
    private int getWaitCounterForHost(String hst) {
        Host host = hosts.get(hst);
        if (host == null) {
            return 0;
        }
        return host.getWaitCounter();
    }
    
    // Helper method using record patterns for ping stats
    private void updateFastestHost(String host, PingStats stats) {
        switch (stats) {
            case PingStats(var last, var avg, var min, var max, var count, var updated) 
                when avg < fastestTime -> {
                    fastestTime = avg;
                    fastestHost = host;
                }
            default -> { /* no update needed */ }
        }
    }

    private void onConnectionError(String host) {
        if (!running) return;
        // empty pool for host, as connection to it failed
        stats.get(DriverStatsKey.ERRORS).incrementAndGet();
        Host h = hosts.get(host);
        if (h == null) {
            return;
        }
        h.incrementFailures();
        if (h.getFailures() > Host.MAX_FAILURES) {
            hosts.remove(host);
            BlockingQueue<ConnectionContainer> connectionsList = h.getConnectionPool();


            // Do not remove seed hosts based on replica-set member strings:
            // the replica set may advertise members under different hostnames (e.g. short names)
            // than the client's resolvable seed list (e.g. FQDNs). Removing here can break primary discovery.

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
                        // swallow
                    }
                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                    markStatsDirty();
                }
            }
        }
    }

    private void createNewConnection(String hst) throws Exception {
        if (!running) return;
        Host host = hosts.get(hst);
        if (host == null) {
            return;
        }

        // Reserve a slot by incrementing pending counter BEFORE creating connection
        // This prevents race condition where multiple threads all pass the limit check
        // and then all create connections exceeding the limit
        // Note: getTotalConnectionsToHost now includes pendingConnectionCreations
        synchronized (host) {
            int total = getTotalConnectionsToHost(hst);
            if (total >= getMaxConnectionsPerHost()) {
                return; // Already at or above max connections (including pending creations)
            }
            // Reserve our slot - this will be seen by other threads via getTotalConnectionsToHost
            host.incrementPendingConnectionCreations();
        }

        SingleMongoConnection con = null;
        try {
            con = new SingleMongoConnection();

            if (getAuthDb() != null) {
                con.setCredentials(getAuthDb(), getUser(), getPassword());
            }

            long start = System.currentTimeMillis();
            HelloResult result = con.connect(this, getHost(hst), getPortFromHost(hst));
            stats.get(DriverStatsKey.CONNECTIONS_OPENED).incrementAndGet();
            markStatsDirty();

            long dur = System.currentTimeMillis() - start;

            // Add to pool if still needed
            synchronized (host) {
                // Add to pool if:
                // 1. There are waiters and we're under max, OR
                // 2. Pool size is below minimum (use pool.size(), not total, to avoid off-by-one with pending slot)
                if ((host.getConnectionPool().size() < host.getWaitCounter()
                                                    && getTotalConnectionsToHost(hst) < getMaxConnectionsPerHost())
                                                    || host.getConnectionPool().size() < getMinConnectionsPerHost()) {
                    var cont = new ConnectionContainer(con);
                    host.getConnectionPool().add(cont);
                    markStatsDirty();
                    con = null; // Don't close, it's now in the pool
                } else {
                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                    markStatsDirty();
                }
            }

            PingStats newStats = host.getPingStats().updateWith(dur);
            host.setPingStats(newStats);
            host.resetFailures();
            updateFastestHost(hst, newStats);
            handleHelloResult(result, String.format("%s:%d", getHost(hst), getPortFromHost(hst)));
        } finally {
            // Release our reserved slot
            host.decrementPendingConnectionCreations();
            // Close connection if it wasn't added to pool
            if (con != null) {
                try {
                    con.close();
                } catch (Exception ignored) {
                }
            }
        }
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
        Host host = hosts.get(h);
        if (host == null) {
            return 0;
        }
        // Include pendingConnectionCreations which tracks connections currently being created
        // This prevents race conditions where multiple threads think they're under the limit
        return host.getBorrowedConnections() + host.getConnectionPool().size() + host.getPendingConnectionCreations();
    }

    private MongoConnection borrowConnection(String host) throws MorphiumDriverException {
        if (!running) throw new MorphiumDriverException("Driver is shutting down");
        // log.debug("borrowConnection {}", host);
        if (host == null)
            throw new MorphiumDriverException("Cannot connect to host null!");

        // if pool is empty -> wait increaseWaitCounter
        //
        // if connection available in pool -> put in borrowedConnections -> return That
        boolean needToDecrement = false;
        Host h = hosts.get(host);
        if (h == null) {
            throw new MorphiumDriverException("No such host: " + host);
        }
        try {
            ConnectionContainer bc = null;
            BlockingQueue<ConnectionContainer> queue = h.getConnectionPool();

            if (queue.isEmpty()) {
                if (h.getWaitCounter() < getMaxConnectionsPerHost()) {
                    h.incrementWaitCounter();
                    needToDecrement = true;
                }

                // although we probably won't get a connection, notify anyways
                //
                synchronized (waitCounterSignal) {
                    waitCounterSignal.notifyAll();
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
                    throw new MorphiumDriverException(
                                    String.format("Could not get connection to %s in time %dms", host, getServerSelectionTimeout()));
                }

                if (bc.getCon() == null || bc.getCon().getSourcePort() == 0 || !bc.getCon().isConnected()) {
                    stats.get(DriverStatsKey.ERRORS).incrementAndGet();
                    try {
                        if (bc.getCon() != null) {
                            bc.getCon().close();
                        }
                    } catch (Exception ignored) {
                    }
                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                    markStatsDirty();
                    bc = null;
                }
            } while (bc == null);

            bc.touch();
            borrowedConnections.put(bc.getCon().getSourcePort(), bc);
            h.incrementBorrowedConnections();
            stats.get(DriverStatsKey.CONNECTIONS_BORROWED).incrementAndGet();
            markStatsDirty();
            return bc.getCon();
        } catch (InterruptedException iex) {
            // swallow - might happen when closing
            Thread.currentThread().interrupt();
            throw new MorphiumDriverException("Waiting for connection was aborted");
            // return new SingleMongoConnection();
        } finally {
            if (needToDecrement && h.getWaitCounter() > 0) {
                h.decrementWaitCounter();
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

            // Force PRIMARY reads for InMemory backend (MorphiumServer) to ensure read-your-writes consistency
            // InMemory backend replication is eventually consistent, so NEAREST/SECONDARY reads may return stale data
            if (inMemoryBackend && type != ReadPreferenceType.PRIMARY) {
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
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Interrupted while waiting for primary connection", e);
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
                    if (primaryNode != null && hosts.get(primaryNode) != null && !hosts.get(primaryNode).getConnectionPool().isEmpty()) {
                        try {
                            return borrowConnection(primaryNode);
                        } catch (MorphiumDriverException e) {
                            stats.get(DriverStatsKey.ERRORS).incrementAndGet();
                            log.warn("Could not get connection to {} trying secondary", primaryNode);
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
                                Host h = hosts.get(hostSeed.get(lastSecondaryNode.get()));
                                if (h == null) {
                                    continue;
                                }
                                PingStats stats = h.getPingStats();
                                // Using pattern matching for records
                                switch (stats) {
                                    case null -> host = hostSeed.get(lastSecondaryNode.get());
                                    case PingStats(var lastPing, var avgPing, var minPing, var maxPing, var count, var updated)
                                        when avgPing <= fastestTime + getLocalThreshold() ->
                                            host = hostSeed.get(lastSecondaryNode.get());
                                    default -> {
                                        continue; // Skip hosts that don't meet threshold
                                    }
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

                            log.warn("could not get connection to secondary node '{}'- trying other replicaset node",
                                     host, e);
                            onConnectionError(host);

                            try {
                                Thread.sleep(getSleepBetweenErrorRetries());
                            } catch (InterruptedException e1) {
                                Thread.currentThread().interrupt(); // Properly handle interruption
                                throw new MorphiumDriverException("Interrupted while getting read connection");
                            }
                        }
                    }
                    // Note: while(true) loop always returns or throws, so this case never falls through

                default:
                    throw new IllegalArgumentException("Unhandled ReadPreferenceType " + rp.getType());
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
            // Race-free: connect() returns after scheduling heartbeat, but primary discovery happens async.
            // Also handles short windows during primary failover where primaryNode is cleared temporarily.
            long start = System.currentTimeMillis();
            long timeout = Math.max(getServerSelectionTimeout(), (long) getHeartbeatFrequency() * 2L + 500L);

            if (timeout <= 0) {
                timeout = 1000;
            }

            // If not a replicaset, there is no "primary" concept - use seed host.
            if (!isReplicaSet() || getHostSeed().size() <= 1) {
                if (getHostSeed().isEmpty()) {
                    throw new MorphiumDriverException("No host seed configured");
                }
                primaryNode = getHostSeed().get(0);
            } else {
                while (primaryNode == null) {
                    if (System.currentTimeMillis() - start > timeout) {
                        throw new MorphiumDriverException("No primary node found - not connected yet?");
                    }

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new MorphiumDriverException("Interrupted while waiting for primary connection", e);
                    }
                }
            }
        }

        return borrowConnection(primaryNode);
    }

    @Override
    public void closeConnection(MongoConnection con) {
        releaseConnection(con);

        for (Host h : hosts.values()) {
            for (ConnectionContainer c : new ArrayList<>(h.getConnectionPool())) { // avoid concurrendModification
                if (c.getCon() == con) {
                    h.getConnectionPool().remove(c);
                    return;
                }
            }
        }
    }

    public Map<Integer, ConnectionContainer> getBorrowedConnections() {
        return new HashMap<>(borrowedConnections);
    }

    @Override
    public void releaseConnection(MongoConnection con) {
        if (con == null) {
            return;
        }

        if (!running) {
            // Shutting down - just remove from borrowed connections map.
            // Don't call con.close() here because it would call back to closeConnection()
            // and cause infinite recursion. The PooledDriver.close() method will close
            // all borrowed connections anyway.
            borrowedConnections.remove(con.getSourcePort());
            return;
        }
        stats.get(DriverStatsKey.CONNECTIONS_RELEASED).incrementAndGet();
        markStatsDirty();

        if (!(con instanceof SingleMongoConnection)) {
            throw new IllegalArgumentException("Got connection of wrong type back!");
        }

        if (con.getSourcePort() != 0) { // sourceport== 0 probably closed or broken
            var c = borrowedConnections.remove(con.getSourcePort());
            markStatsDirty();

            if (c == null) {
                // log.debug("Returning not borrowed connection!?!?");
                if (con.isConnected()) {
                    // c = new Connection((SingleMongoConnection) con);

                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                    markStatsDirty();
                    con.close();
                }

                return;
            }

            if (con.getConnectedTo() != null) {
                Host h = hosts.get(con.getConnectedTo());
                if (h != null) {
                    h.getConnectionPool().add(c);
                    h.decrementBorrowedConnections();
                    markStatsDirty();
                } else {
                    // Host was removed from pool - try to find by alias or just log
                    // The borrowed counter was on the original Host object which is now gone
                    // so we just close the connection - the counter is gone with the Host
                    log.debug("Host {} no longer available, closing connection", con.getConnectedTo());
                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                    markStatsDirty();
                    try {
                        con.close();
                    } catch (Exception ignored) {
                    }
                }
            } else {
                // Connection has no target host - close it and try to decrement any matching host
                log.debug("Connection has no target host, closing");
                stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                markStatsDirty();
                try {
                    con.close();
                } catch (Exception ignored) {
                }
            }
        } else {
            // Connection is broken (sourcePort == 0) - clean up stale entries and decrement borrowed counters
            List<Integer> sourcePortsToDelete = new ArrayList<>();
            Map<String, Integer> hostsToDecrement = new HashMap<>();

            for (int port : new ArrayList<Integer>(borrowedConnections.keySet())) {
                ConnectionContainer connectionContainer = borrowedConnections.get(port);

                if (connectionContainer == null || connectionContainer.getCon() == null
                        || connectionContainer.getCon().getSourcePort() == 0) {
                    sourcePortsToDelete.add(port);
                    // Track which host this connection belonged to so we can decrement its counter
                    if (connectionContainer != null && connectionContainer.getCon() != null) {
                        String hostKey = connectionContainer.getCon().getConnectedTo();
                        if (hostKey != null) {
                            hostsToDecrement.merge(hostKey, 1, Integer::sum);
                        }
                    }
                }
            }

            for (int port : sourcePortsToDelete) {
                borrowedConnections.remove(port);
            }

            // Decrement borrowed counters for affected hosts
            for (Map.Entry<String, Integer> entry : hostsToDecrement.entrySet()) {
                Host h = hosts.get(entry.getKey());
                if (h != null) {
                    for (int i = 0; i < entry.getValue(); i++) {
                        h.decrementBorrowedConnections();
                    }
                }
            }

            if (!sourcePortsToDelete.isEmpty()) {
                markStatsDirty();
            }
        }
    }

    public boolean isConnected() {
        for (var c : hosts.keySet()) {
            if (getTotalConnectionsToHost(c) != 0) {
                return true;
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
    public <T, R> Aggregator<T, R> createAggregator(Morphium morphium, Class <? extends T > type,
            Class <? extends R > resultType) {
        return new AggregatorImpl<>(morphium, type, resultType);
    }

    @Override
    public String getName() {
        return driverName;
    }

    @Override
    public boolean isInMemoryBackend() {
        return inMemoryBackend;
    }

    @Override
    public boolean isMorphiumServer() {
        return morphiumServer;
    }

    @Override
    public void setConnectionUrl(String connectionUrl) {
    }

    @Override
    public void close() {
        if (!running) {
            return;
        }
        running = false;
        if (heartbeat != null) {
            heartbeat.cancel(true);
        }

        heartbeat = null;

        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
        }

        // Close all borrowed connections first - these are in active use
        // Important: close them properly instead of just clearing the map,
        // otherwise they become orphaned/leaked
        for (var entry : new ArrayList<>(borrowedConnections.values())) {
            try {
                if (entry.getCon() != null) {
                    entry.getCon().close();
                }
            } catch (Exception ex) {
                // ignore errors during close
            }
        }
        borrowedConnections.clear();

        // Now close pooled (idle) connections
        for (Host h : hosts.values()) {
            for (var c : new ArrayList<>(h.getConnectionPool())) {
                try {
                    c.getCon().close();
                } catch (Exception ex) {
                }
            }

            h.getConnectionPool().clear();
        }
        hosts.clear();
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

        try {
            var cmd = new CommitTransactionCommand(con).setTxnNumber(ctx.getTxnNumber()).setAutocommit(false)
            .setLsid(ctx.getLsid());
            cmd.execute();
        } finally {
            clearTransactionContext();
            releaseConnection(con);
        }
    }

    @Override
    public void abortTransaction() throws MorphiumDriverException {
        if (getTransactionContext() == null) {
            throw new IllegalArgumentException("No transaction in progress, cannot abort");
        }

        MongoConnection con = getPrimaryConnection(null);

        try {
            MorphiumTransactionContext ctx = getTransactionContext();
            var cmd = new AbortTransactionCommand(con).setTxnNumber(ctx.getTxnNumber()).setAutocommit(false)
            .setLsid(ctx.getLsid());
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
            mem.stream().filter(d -> d.get("optime") instanceof Map)
               .forEach(d -> d.put("optime", ((Map<String, Doc>) d.get("optime")).get("ts")));
            return result;
        } finally {
            releaseConnection(con);
        }
    }

    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        return getDBStats(db, false);
    }

    public Map<String, Object> getDBStats(String db, boolean withStorage) throws MorphiumDriverException {
        MongoConnection con = null;

        try {
            con = getPrimaryConnection(null);
            return new DbStatsCommand(con).setDb(db).setWithStorage(withStorage).execute();
        } finally {
            releaseConnection(con);
        }
    }

    @Override
    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        MongoConnection con = null;
        CollStatsCommand cmd = null;
        try {
            con = getPrimaryConnection(null);
            cmd = new CollStatsCommand(con).setColl(coll).setDb(db);
            var result = cmd.execute();
            cmd.releaseConnection();
            cmd = null;
            con = null;
            return result;
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            } else if (con != null) {
                releaseConnection(con);
            }
        }
    }

    public List<Map<String, Object>> currentOp(int threshold) throws MorphiumDriverException {
        MongoConnection con = null;
        CurrentOpCommand cmd = null;

        try {
            con = getPrimaryConnection(null);
            cmd = new CurrentOpCommand(con).setColl("admin").setSecsRunning(threshold);
            var result = cmd.execute();
            cmd.releaseConnection();
            cmd = null;
            con = null;
            return result;
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            } else if (con != null) {
                releaseConnection(con);
            }
        }
    }

    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        if (crs == null) {
            return;
        }

        killCursors(crs.getDb(), crs.getCollection(), crs.getCursorId());
    }

    public boolean exists(String db) throws MorphiumDriverException {
        List<String> databases = listDatabases();
        return databases != null && databases.contains(db);
    }

    private List<Map<String, Object>> getCollectionInfo(String db, String collection) throws MorphiumDriverException {
        // noinspection unchecked
        return new NetworkCallHelper<List<Map<String, Object>>>().doCall(() -> {
            var con = getReadConnection(ReadPreference.primary());
            try {
                ListCollectionsCommand cmd = new ListCollectionsCommand(con);
                cmd.setDb(db);
                cmd.setFilter(Doc.of("name", collection));
                return cmd.execute();
            } finally {
                releaseConnection(con);
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public Map<String, Integer> getNumConnectionsByHost() {
        Map<String, Integer> ret = new HashMap<>();

        for (var e : hosts.entrySet()) {
            ret.put(e.getKey(), e.getValue().getConnectionPool().size());
        }

        for (var e : borrowedConnections.values()) {
            ret.put(e.getCon().getConnectedTo(), ret.get(e.getCon().getConnectedTo()).intValue() + 1);
        }

        return ret;
    }

    @Override
    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        if (!running) return false;
        List<Map<String, Object>> lst = getCollectionInfo(db, coll);
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

    public static class ConnectionContainer {
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
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered,
            WriteConcern wc) {
        return new BulkRequestContext(m) {
            private final List<BulkRequest> requests = new ArrayList<>();

            public Doc execute() {
                int delCount = 0;
                int matchedCount = 0;
                int insertCount = 0;
                int modifiedCount = 0;
                List<Object> upsertedIds = new ArrayList<>();

                try {
                    if (requests.isEmpty()) {
                        return Doc.of(
                            "num_deleted", 0,
                            "num_matched", 0,
                            "num_inserted", 0,
                            "num_modified", 0,
                            "num_upserts", 0
                        );
                    }

                    for (BulkRequest r : requests) {
                        switch (r) {
                            case InsertBulkRequest insert -> {
                                if (insert.getToInsert() == null || insert.getToInsert().isEmpty()) {
                                    break;
                                }

                                MongoConnection con = null;
                                InsertMongoCommand settings = null;
                                try {
                                    con = getPrimaryConnection(wc);
                                    settings = new InsertMongoCommand(con);
                                    settings.setDb(db).setColl(collection).setComment("Bulk insert")
                                            .setDocuments(insert.getToInsert())
                                            .setWriteConcern(wc != null ? wc.asMap() : null);
                                    Map<String, Object> result = settings.execute();
                                    settings.releaseConnection();
                                    settings = null;
                                    con = null;
                                    insertCount += insert.getToInsert().size();
                                } finally {
                                    if (settings != null) {
                                        settings.releaseConnection();
                                    } else if (con != null) {
                                        releaseConnection(con);
                                    }
                                }
                            }
                            case UpdateBulkRequest update -> {
                                MongoConnection con = null;
                                UpdateMongoCommand upCmd = null;
                                try {
                                    con = getPrimaryConnection(wc);
                                    upCmd = new UpdateMongoCommand(con);
                                    upCmd.setColl(collection).setDb(db).setUpdates(Arrays.asList(Doc.of("q", update.getQuery(), "u",
                                            update.getCmd(), "upsert", update.isUpsert(), "multi", update.isMultiple())))
                                        .setWriteConcern(wc != null ? wc.asMap() : null);
                                    Map<String, Object> result = upCmd.execute();
                                    upCmd.releaseConnection();
                                    upCmd = null;
                                    con = null;
                                    if (result.containsKey("n")) {
                                        matchedCount += ((Number) result.get("n")).intValue();
                                    }
                                    if (result.containsKey("nModified")) {
                                        modifiedCount += ((Number) result.get("nModified")).intValue();
                                    }
                                    if (result.containsKey("upserted")) {
                                        @SuppressWarnings("unchecked")
                                        List<Map<String, Object>> upserted = (List<Map<String, Object>>) result.get("upserted");
                                        for (Map<String, Object> u : upserted) {
                                            upsertedIds.add(u.get("_id"));
                                        }
                                    }
                                } finally {
                                    if (upCmd != null) {
                                        upCmd.releaseConnection();
                                    } else if (con != null) {
                                        releaseConnection(con);
                                    }
                                }
                            }
                            case DeleteBulkRequest delete -> {
                                MongoConnection con = null;
                                DeleteMongoCommand del = null;
                                try {
                                    con = getPrimaryConnection(wc);
                                    del = new DeleteMongoCommand(con);
                                    del.setColl(collection).setDb(db).setDeletes(
                                                       Arrays.asList(Doc.of("q", delete.getQuery(), "limit", delete.isMultiple() ? 0 : 1)))
                                        .setWriteConcern(wc != null ? wc.asMap() : null);
                                    Map<String, Object> result = del.execute();
                                    del.releaseConnection();
                                    del = null;
                                    con = null;
                                    if (result.containsKey("n")) {
                                        delCount += ((Number) result.get("n")).intValue();
                                    }
                                } finally {
                                    if (del != null) {
                                        del.releaseConnection();
                                    } else if (con != null) {
                                        releaseConnection(con);
                                    }
                                }
                            }
                            default -> throw new RuntimeException("Unknown operation " + r.getClass().getName());
                        }
                    }
                } catch (MorphiumDriverException e) {
                    log.error("Got exception: ", e);
                    stats.get(DriverStatsKey.ERRORS).incrementAndGet();
                }

                // Build result document
                Doc res = Doc.of(
                    "num_deleted", delCount,
                    "num_matched", matchedCount,
                    "num_inserted", insertCount,
                    "num_modified", modifiedCount,
                    "num_upserts", upsertedIds.size()
                );

                if (!upsertedIds.isEmpty()) {
                    res.put("upsertedIds", upsertedIds);
                }

                return res;
            }

            public UpdateBulkRequest addUpdateBulkRequest() {
                UpdateBulkRequest up = new UpdateBulkRequest();
                requests.add(up);
                return up;
            }

            public InsertBulkRequest addInsertBulkRequest(List<Map<String, Object>> toInsert) {
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
        if (!running) return new HashMap<>();
        long now = System.currentTimeMillis();

        // Return cached stats if recent enough
        if (cachedStats != null && now - lastStatsUpdate < STATS_CACHE_TTL
                && cachedStatsDirtyCounter == statsDirtyCounter.get()) {
            return cachedStats.toMap();
        }

        // Batch collect all stats in one operation
        StatsSnapshot snapshot = collectStatsSnapshot();
        cachedStats = snapshot;
        cachedStatsDirtyCounter = statsDirtyCounter.get();
        lastStatsUpdate = now;

        return snapshot.toMap();
    }

    private StatsSnapshot collectStatsSnapshot() {
        // Collect driver stats
        Map<DriverStatsKey, Double> driverStats = new HashMap<>();
        for (var e : stats.entrySet()) {
            driverStats.put(e.getKey(), e.getValue().get());
        }

        // Batch collect connection stats in single synchronized block
        int totalPooled = 0;
        Map<DriverStatsKey, Double> connStats = new HashMap<>();

        for (Host h : hosts.values()) {
            totalPooled += h.getConnectionPool().size() + h.getInternalInUseConnections();

            for (var con : h.getConnectionPool()) {
                for (var entry : con.getCon().getStats().entrySet()) {
                    connStats.merge(entry.getKey(), entry.getValue(), Double::sum);
                }
            }
        }

        int totalBorrowed = borrowedConnections.size();

        // Calculate waiting threads efficiently
        int waiting = 0;
        for (Host h : hosts.values()) {
            waiting += h.getWaitCounter();
        }

        return new StatsSnapshot(driverStats, totalPooled, totalBorrowed, waiting, connStats);
    }
}
