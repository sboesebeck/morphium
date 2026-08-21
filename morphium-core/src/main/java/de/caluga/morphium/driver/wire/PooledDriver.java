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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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
    // package-private (not private) so tests in this package can register a Host directly
    // without a real connect() - same rationale as handleHelloResult's visibility.
    final Map<String, Host> hosts = new ConcurrentHashMap<>();
    private volatile boolean running;
    private final Map<Integer, ConnectionContainer> borrowedConnections;
    private final Map<DriverStatsKey, AtomicDecimal> stats;
    private volatile long fastestTime = 10000;
    private int idleSleepTime = 5;
    private volatile String fastestHost = null;
    private final Logger log = LoggerFactory.getLogger(PooledDriver.class);
    private volatile String primaryNode;
    private final Object primaryNodeLock = new Object();  // Lock for primaryNode updates only
    // Last error seen while trying to establish a connection to any seed host. Surfaced in the
    // "No primary node found" timeout exception so callers see the actual cause (e.g. a TLS
    // handshake failure) instead of a bare timeout message. Also exposed via
    // getLastConnectFailure() for the non-replicaset path, where connect() itself never throws
    // (it tolerates the failed seed and falls back to "treat first seed as primary") - a caller
    // polling isConnected() has no other way to learn why it's still false.
    private volatile Throwable lastConnectFailure;
    private volatile boolean inMemoryBackend = false;
    private volatile boolean poppyDB = false;
    private volatile boolean cosmosDB = false;
    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5,
        Thread.ofPlatform().name("MCon-", 0).factory());

    private final AtomicInteger lastSecondaryNode = new AtomicInteger(0);
    // Package-private: the heartbeat's per-host bookkeeping is asserted on directly by
    // PooledDriverRediscoveryTest - a stale entry here silently disables discovery for that host.
    final Map<String, Thread> hostThreads = new ConcurrentHashMap<>();

    // #330: watchdog for silent heartbeat cycles (zero successful hellos)
    private volatile int consecutiveCyclesWithoutHello = 0;
    private static final int SILENT_CYCLE_WARN_THRESHOLD = 10; // log warning every N silent cycles
    private static final int SILENT_CYCLE_RESEED_THRESHOLD = 30; // force full reseed after N silent cycles
    // Reset at start of each heartbeatCycle, incremented by handleHelloResult on valid hello
    private final AtomicInteger successfulHellosThisCycle = new AtomicInteger(0);

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
            String normalizedHost = normalizeHostKey(host);
            hosts.put(normalizedHost, new Host(getHost(host), getPortFromHost(host)));
        }

        setReplicaSet(getHostSeed().size() > 1 || (replSet != null && !replSet.isEmpty()));

        if (replSet != null && !replSet.isEmpty()) {
            setReplicaSetName(replSet);
        }

        // Proactively establish at least one connection per seed to discover primary immediately.
        // Relying solely on the async heartbeat can lead to races where early operations (e.g. exists/listCollections
        // during Morphium startup) run before primary discovery, causing intermittent "No primary node found".
        for (String host : new ArrayList<>(getHostSeed())) {
            try {
                createNewConnection(host);
            } catch (Exception e) {
                // swallow: unreachable seed(s) are handled by the heartbeat/error logic, but remember
                // the failure so a subsequent "No primary node found" timeout can report the real cause.
                lastConnectFailure = e;
                if (log.isDebugEnabled()) {
                    log.debug("Initial connect to seed {} failed", host, e);
                }
            }
        }

        startHeartbeat();

        // Wait (briefly) for primary discovery on replica sets.
        if (isReplicaSet()) {
            long start = System.currentTimeMillis();
            long timeout = getServerSelectionTimeout();
            if (timeout <= 0) timeout = 1000;

            while (primaryNode == null) {
                if (System.currentTimeMillis() - start > timeout) {
                    Throwable cause = lastConnectFailure;
                    String detail = cause == null ? "" : " - last connection error: " + cause;
                    throw new MorphiumDriverException("No primary node found - not connected yet?" + detail, cause);
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new MorphiumDriverException("Interrupted while waiting for primary discovery", ie);
                }
            }

            // Ensure at least one pooled connection to the discovered primary exists.
            // When connecting to a fresh replica set (e.g. PoppyDB RS), the initial
            // createNewConnection() calls happen before election completes — all nodes report
            // as secondary, so the primary's pool may be empty or the connection was created
            // when the node was still secondary. Without this, borrowConnection(primaryNode)
            // would block indefinitely waiting for a connection that never arrives.
            String normalizedPrimary = normalizeHostKey(primaryNode);
            Host primaryHost = hosts.get(normalizedPrimary);
            if (primaryHost != null && primaryHost.getConnectionPool().isEmpty()) {
                try {
                    createNewConnection(normalizedPrimary);
                } catch (Exception e) {
                    log.warn("Could not create initial connection to discovered primary {}", normalizedPrimary, e);
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
        super.removeFromHostSeed(normalizeHostKey(host));
        String normalized = normalizeHostKey(host);
        Host removed = hosts.remove(normalized);

        // #330: clean up orphaned hostThreads entry for removed host
        hostThreads.remove(normalized);

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

    // Package-private: PooledDriverHeartbeatResilienceTest asserts that a dead heartbeat is
    // revived rather than left dead (a dead heartbeat means no topology discovery at all).
    volatile ScheduledFuture<?> heartbeat;
    private volatile Thread connectionWaiter;
    // Use ReentrantLock + Condition instead of synchronized + wait/notify
    // to avoid pinning carrier threads when using virtual threads
    private final ReentrantLock waitCounterLock = new ReentrantLock();
    private final Condition waitCounterCondition = waitCounterLock.newCondition();
    private List<String> lastHostsFromHello = null;
    /**
     * Some replica sets advertise members using hostnames that differ from the seed list
     * (e.g. short hostnames vs FQDNs). Keep a best-effort alias map from server-reported
     * host:port to the actually reachable host:port we connected to, so primary selection
     * and pool lookups keep working.
     */
    private final ConcurrentHashMap<String, String> hostAliases = new ConcurrentHashMap<>();

    /** currently known primary (normalized host:port) - null while failover is in progress */
    public String getPrimaryNode() {
        return primaryNode;
    }

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

    /**
     * Normalize host string to always include port.
     * This ensures consistent keys in the hosts map, matching what
     * SingleMongoConnection.getConnectedTo() returns.
     * 
     * Examples:
     * - "server.example.com" -> "server.example.com:27017"
     * - "server.example.com:27017" -> "server.example.com:27017"
     * - "server.example.com:27018" -> "server.example.com:27018"
     * - "SERVER.example.com:27017" -> "server.example.com:27017" (case-insensitive)
     */
    String normalizeHostKey(String hostPort) {
        if (hostPort == null) return null;
        // Normalize to lowercase for case-insensitive hostname matching
        // This prevents pool exhaustion when MongoDB reports hostnames with different
        // casing than what was used in the seed list (e.g., SERV-MSG1 vs serv-msg1)
        String normalized = hostPort.toLowerCase();
        if (normalized.contains(":")) {
            return normalized;  // Already has port
        }
        return normalized + ":27017";  // Add default port
    }

    /**
     * Override to ensure all host seeds are normalized (lowercase + port).
     */
    @Override
    public void addToHostSeed(String host) {
        super.addToHostSeed(normalizeHostKey(host));
    }

    @Override
    public void setHostSeed(String... hosts) {
        String[] normalized = new String[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            normalized[i] = normalizeHostKey(hosts[i]);
        }
        super.setHostSeed(normalized);
    }

    @Override
    public void setHostSeed(java.util.List<String> hosts) {
        super.setHostSeed(hosts.stream().map(this::normalizeHostKey).toList());
    }

    @Override
    public void setHostSeed(java.util.Set<String> hostSeed) {
        super.setHostSeed(hostSeed.stream().map(this::normalizeHostKey).collect(java.util.stream.Collectors.toSet()));
    }

    @Override
    public void connect() throws MorphiumDriverException {
        connect(null);
    }

    /** package-private for testing */
    void handleHelloResult(HelloResult hello, String hostConnected) {
        if (!running) return;
        if (hello == null)
            return;

        // #330: count successful hello for silent-cycle watchdog
        successfulHellosThisCycle.incrementAndGet();

        // Adopt the wire limits the server advertises - they exist precisely so clients
        // bound what they send (message splitting, batch sizing, document size checks)
        if (hello.getMaxMessageSizeBytes() != null) {
            setMaxMessageSize(hello.getMaxMessageSizeBytes());
        }

        if (hello.getMaxWriteBatchSize() != null) {
            setMaxWriteBatchSize(hello.getMaxWriteBatchSize());
        }

        if (hello.getMaxBsonObjectSize() != null) {
            setMaxBsonObjectSize(hello.getMaxBsonObjectSize());
        }

        // Detect backend type from hello handshake
        if (!poppyDB && Boolean.TRUE.equals(hello.getPoppyDB())) {
            poppyDB = true;
            log.info("Detected PoppyDB backend (host: {})", hostConnected);
        }
        if (!inMemoryBackend && Boolean.TRUE.equals(hello.getInMemoryBackend())) {
            inMemoryBackend = true;
            log.info("Detected InMemory backend (host: {})", hostConnected);
        }
        if (!cosmosDB) {
            cosmosDB = detectCosmosDB(hello, hostConnected);
            if (cosmosDB) {
                log.info("Detected Azure CosmosDB backend (host: {}, setName: {})",
                         hostConnected, hello.getSetName());
            }
        }

        // Auto-detect replica set from hello response.
        // Single-node replica sets (common in dev/test with Docker) have only one seed host
        // and no configured RS name, so connect() sets replicaSet=false. The hello response
        // from the server contains the actual setName — use it to upgrade to RS mode.
        // Double-checked locking: handleHelloResult is called concurrently by heartbeat threads.
        if (!isReplicaSet() && hello.getSetName() != null && !hello.getSetName().isEmpty()) {
            synchronized (primaryNodeLock) {
                if (!isReplicaSet()) {
                    log.info("Auto-detected replica set '{}' from hello response (host: {})",
                             hello.getSetName(), hostConnected);
                    setReplicaSet(true);
                    setReplicaSetName(hello.getSetName());
                }
            }
        }

        // Keep track of server-advertised vs reachable names.
        registerAlias(hello.getMe(), hostConnected);

        // IMPORTANT: Add hosts from hello BEFORE checking for primary node.
        // Otherwise, when connecting to a secondary first, the advertised primary
        // won't be in the hosts map and we can't set primaryNode from it.
        if (hello.getHosts() != null && !hello.getHosts().isEmpty()) {
            lastHostsFromHello = hello.getHosts();

            for (String hst : hello.getHosts()) {
                String resolved = normalizeHostKey(resolveAlias(hst));
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
                    // Recover immediately if THIS SAME reply already names the real new primary,
                    // rather than nulling out and passively waiting for some future, unrelated
                    // hello to arrive from that host - which can take arbitrarily long if its own
                    // heartbeat cycle hasn't come around yet. Seen on a real rapid double
                    // failover (ex-primary steps down, a node briefly wins, a much
                    // higher-priority node immediately takes over via priority takeover): the
                    // driver got stuck retrying the FIRST ex-primary for 20+ seconds even though
                    // the second node's own "I'm not primary" reply already named the real
                    // winner.
                    String advertised = resolveAdvertisedPrimary(hello);
                    if (advertised != null) {
                        log.warn("Primary failover? {} -> {} (re-resolved from {}'s own hello)",
                                primaryNode, advertised, hostConnected);
                        stats.get(DriverStatsKey.FAILOVERS).incrementAndGet();
                        primaryNode = advertised;
                    } else {
                        primaryNode = null;
                    }
                } else if (primaryNode == null && hello.getPrimary() != null) {
                    String advertised = resolveAdvertisedPrimary(hello);
                    if (advertised != null) {
                        primaryNode = advertised;
                    }
                }
            }
        }

        // #330: membership REMOVAL is only authoritative coming from the PRIMARY. During a
        // rolling restart, secondaries and in-election nodes answer hellos too - acting on
        // their (possibly partial or transitional) host lists is how one unlucky client eroded
        // its entire topology and went silent. Additions above stay accepted from every hello;
        // removals require the primary's word.
        if (Boolean.TRUE.equals(hello.getWritablePrimary())
                && hello.getHosts() != null && !hello.getHosts().isEmpty()) {

            // Do NOT remove existing host seed entries here.
            // Users might provide a seed list using different (but resolvable) hostnames than those
            // advertised by the replica set configuration. Removing seeds can make the driver lose
            // the only reachable addresses and lead to "No primary node found".

            // Build a set of resolved hostnames from hello.getHosts() for comparison
            // This handles the case where the server reports names like "macbook:27017" but
            // we connected as "localhost:27017" - both should be considered the same host.
            // #330: MUST use the exact same normalization as the add path above
            // (normalizeHostKey on top of resolveAlias) - comparing normalized map keys
            // against un-normalized names made a hello that advertises a member in a
            // different case (SERV-MSG1 vs serv-msg1) remove the very host it had just
            // added, eroding hosts map AND seed to empty within a few hellos.
            java.util.Set<String> resolvedHelloHosts = new java.util.HashSet<>();
            for (String h : hello.getHosts()) {
                resolvedHelloHosts.add(normalizeHostKey(resolveAlias(h)));
            }

            // only closing connections when info comes from primary
            List<ConnectionContainer> toClose = new ArrayList<>();
            for (var it = hosts.entrySet().iterator(); it.hasNext();) {
                var entry = it.next();
                var host = entry.getKey();
                if (!resolvedHelloHosts.contains(host)) {
                    log.warn("Host {} is not part of the replicaset anymore!", host);
                    it.remove();
                    // #330: clean up orphaned hostThreads entry for removed host
                    hostThreads.remove(host);
                    Host h = entry.getValue();
                    removeFromHostSeed(host);

                    ArrayList<Integer> toDelete = new ArrayList<>();

                    for (var e : new ArrayList<>(borrowedConnections.entrySet())) {
                        // Use borrowedFromHost to find connections that were borrowed from this host
                        String borrowedFrom = e.getValue().getBorrowedFromHost();
                        if (host.equals(borrowedFrom)) {
                            toDelete.add(e.getKey());
                        }
                    }

                    for (Integer i : toDelete) {
                        if (borrowedConnections.remove(i) != null) {
                            h.decrementBorrowedConnections();
                        }
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

    /**
     * Resolves hello.getPrimary() to a known/reachable host key, or null if it doesn't map to
     * one. Must be normalized like the hosts-map keys (lowercase + default port): replica set
     * configs may advertise members with different casing than the client seed, or without a
     * port.
     */
    private String resolveAdvertisedPrimary(HelloResult hello) {
        if (hello.getPrimary() == null) {
            return null;
        }
        String advertised = normalizeHostKey(resolveAlias(hello.getPrimary()));
        return hosts.containsKey(advertised) ? advertised : null;
    }

    protected synchronized void startHeartbeat() {
        if (heartbeat == null) {
            // #330: remember the configured seed before any runtime path can erode it - the
            // last-line-of-defense source for reseedIfAllHostsEvicted.
            captureInitialHostSeed();
            // #330: use self-rescheduling instead of scheduleWithFixedDelay so the heartbeat
            // can revive itself even if externally cancelled or if the executor drops it.
            // First cycle runs immediately (delay=0), subsequent cycles use the configured frequency.
            scheduleHeartbeatCycle(0);
            startConnectionWaiter();
        }
    }

    /**
     * Schedules a single heartbeat cycle. The cycle will reschedule itself in a finally block,
     * ensuring the heartbeat continues even after exceptions or external cancellation attempts.
     * This replaces scheduleWithFixedDelay which permanently cancels the task on any throw.
     *
     * @param initialDelay initial delay in milliseconds (0 for first run, then heartbeatFrequency)
     */
    private void scheduleHeartbeatCycle(long initialDelay) {
        if (!running) return;
        heartbeat = executor.schedule(() -> {
            try {
                heartbeatCycle();
            } catch (Throwable e) {
                long now = System.currentTimeMillis();
                if (now - lastHeartbeatFailureLogMs > 60_000) {
                    lastHeartbeatFailureLogMs = now;
                    log.error("Heartbeat cycle failed - retrying on the next cycle", e);
                }
            } finally {
                if (running) {
                    scheduleHeartbeatCycle(getHeartbeatFrequency());
                }
            }
        }, initialDelay, TimeUnit.MILLISECONDS);
    }

    /** Throttles the heartbeat-failure log so a persistent failure cannot flood it every cycle. */
    private volatile long lastHeartbeatFailureLogMs = 0;

    private void heartbeatCycle() {
        // check every host in pool if available
        // create NEW Connection to host -> if error, remove host from connectionPool
        // send HelloCommand to host
        // process helloCommand (primary etc)

        // Cleanup dead borrowed connections - connections that are no longer connected
        // but still tracked as borrowed (can happen on network errors, timeouts, etc.)
        try {
            List<Integer> deadBorrowedPorts = new ArrayList<>();
            Map<String, Integer> hostsToDecrement = new HashMap<>();

            for (var bcEntry : new ArrayList<>(borrowedConnections.entrySet())) {
                ConnectionContainer cc = bcEntry.getValue();
                if (cc == null || cc.getCon() == null || !cc.getCon().isConnected()) {
                    deadBorrowedPorts.add(bcEntry.getKey());
                    // Use borrowedFromHost for correct counter decrement (not getConnectedTo which may have changed)
                    if (cc != null && cc.getBorrowedFromHost() != null) {
                        hostsToDecrement.merge(cc.getBorrowedFromHost(), 1, Integer::sum);
                    }
                }
            }

            // Remove dead entries and decrement counters atomically per-entry.
            // Only decrement when remove() actually returned the entry — another thread
            // (releaseConnection) may have already handled it.
            for (int port : deadBorrowedPorts) {
                ConnectionContainer removed = borrowedConnections.remove(port);
                if (removed != null) {
                    String hostKey = removed.getBorrowedFromHost();
                    if (hostKey != null) {
                        Host h = hosts.get(hostKey);
                        if (h == null) {
                            String hostOnly = hostKey.split(":")[0];
                            h = hosts.get(hostOnly);
                        }
                        if (h != null) {
                            h.decrementBorrowedConnections();
                        }
                    }
                    if (removed.getCon() != null) {
                        try { removed.getCon().close(); } catch (Exception ignore) {}
                    }
                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                }
            }

            if (!deadBorrowedPorts.isEmpty()) {
                log.warn("Cleaned up {} dead borrowed connections", deadBorrowedPorts.size());
                markStatsDirty();
            }
        } catch (Exception e) {
            log.debug("Error during borrowed connection cleanup", e);
        }

        reseedIfAllHostsEvicted();

        // #330: track successful hellos this cycle for the silent-cycle watchdog
        successfulHellosThisCycle.set(0);

        for (var entry : hosts.entrySet()) {
            // Cooperative shutdown: close() flips running and then waits for the executor -
            // a cycle that keeps spawning host checks past that point re-populates the
            // bookkeeping close() is about to clear.
            if (!running) {
                return;
            }

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
                                // Use offer() with timeout to prevent lock convoy
                                try {
                                    if (!connectionPoolForHost.offer(connection, 100, TimeUnit.MILLISECONDS)) {
                                        log.warn("Could not return connection to pool within timeout - closing");
                                        try { connection.getCon().close(); } catch (Exception ignored) {}
                                        stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                        markStatsDirty();
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    try { connection.getCon().close(); } catch (Exception ignored) {}
                                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                    markStatsDirty();
                                }
                            }
                        } finally {
                            host.decrementInternalInUseConnections();
                        }
                    }
                } catch (Throwable e) {
                }
            }

            // Self-healing claim check: an entry whose thread is no longer alive is
            // leftover bookkeeping, not a check in flight. Skipping on it forever is how
            // a host silently dropped out of discovery for good (#304).
            Thread runningCheck = hostThreads.get(hst);

            if (runningCheck != null) {
                if (runningCheck.isAlive()) {
                    continue;
                }

                hostThreads.remove(hst, runningCheck);
            }

            Thread t = Thread.ofPlatform().name("HeartbeatCheck-" + hst).unstarted(() -> {

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
                        boolean containerDisposed = false;
                        try {
                            long start = System.currentTimeMillis();
                            HelloResult result;

                            if (!container.getCon().isConnected()) {
                                // Connection was closed — discard it and let createNewConnection
                                // (below) create a fresh one. Trying to reconnect a closed
                                // SingleMongoConnection causes "Socket is closed" errors because
                                // the old socket state leaks into the new connect() attempt.
                                try { container.getCon().close(); } catch (Exception ignored) {}
                                stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                markStatsDirty();
                                containerDisposed = true;
                            } else {
                                // Bounded hello: a frozen/partitioned host must fail the
                                // heartbeat check within ~a heartbeat, not after maxWaitTime.
                                // Otherwise eviction (MAX_FAILURES) takes minutes and all
                                // in-flight operations stay stuck on dead connections.
                                result = container.getCon().getHelloResult(false,
                                        Math.max(2000, getHeartbeatFrequency()));

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
                                    // Use offer() with timeout to prevent lock convoy
                                    try {
                                        if (!host.getConnectionPool().offer(container, 100, TimeUnit.MILLISECONDS)) {
                                            log.warn("Could not return connection to pool within timeout - closing");
                                            try { container.getCon().close(); } catch (Exception ignored) {}
                                            stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                            markStatsDirty();
                                        }
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        try { container.getCon().close(); } catch (Exception ignored) {}
                                        stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                        markStatsDirty();
                                    }
                                } else {
                                    container.getCon().close();
                                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                    markStatsDirty();
                                }
                                containerDisposed = true;
                            }
                        } finally {
                            host.decrementInternalInUseConnections();
                            // If getHelloResult()/connect() threw an exception, the container
                            // was polled from the pool but never returned or closed — close it
                            // to prevent connection leak (it's not in borrowedConnections either).
                            if (!containerDisposed && container.getCon() != null) {
                                try { container.getCon().close(); } catch (Exception ignored) {}
                                stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                markStatsDirty();
                            }
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
                        loopCounter++;
                        // log.debug("Creating connection to {} - totalConnections to host is {}", hst,
                        // getTotalConnectionsToHost(hst));
                        createNewConnection(hst);
                    }

                    // log.info("Finished connection creation");
                } catch (Throwable e) {
                    // full stacktrace only on the first failure - a host that is down
                    // for a while would otherwise flood the log every heartbeat
                    lastConnectFailure = e;
                    Host failedHost = hosts.get(normalizeHostKey(hst));
                    if (failedHost == null || failedHost.getFailures() == 0) {
                        log.error("Could not create connection to host {}", hst, e);
                    } else {
                        log.warn("Still cannot connect to host {} ({} consecutive failures): {}",
                                 hst, failedHost.getFailures(), e.getMessage());
                    }
                    onConnectionError(hst);
                } finally {
                    // Two-arg remove: never drop another cycle's claim, only our own.
                    hostThreads.remove(hst, Thread.currentThread());
                }
            });
            // Claim BEFORE starting: a check against a host that refuses connections
            // finishes in microseconds, and with the claim written afterwards its own
            // remove could run first - leaving an entry nobody ever clears again (#304).
            hostThreads.put(hst, t);

            try {
                t.start();
            } catch (Throwable e) {
                hostThreads.remove(hst, t);
                throw e;
            }
        }

        // #330: silent-cycle watchdog - if zero successful hellos this cycle, increment counter
        // and log warning / force reseed at thresholds
        int successful = successfulHellosThisCycle.get();
        if (successful == 0) {
            consecutiveCyclesWithoutHello++;
            if (consecutiveCyclesWithoutHello % SILENT_CYCLE_WARN_THRESHOLD == 0) {
                log.warn("Heartbeat: {} consecutive cycles with zero successful hellos - topology discovery may be stalled",
                        consecutiveCyclesWithoutHello);
            }
            if (consecutiveCyclesWithoutHello >= SILENT_CYCLE_RESEED_THRESHOLD) {
                log.error("Heartbeat: {} consecutive cycles with zero successful hellos - forcing full topology reseed from host seed",
                        consecutiveCyclesWithoutHello);

                // Close the stale pools before dropping the Host objects - after this many
                // silent cycles the connections are dead weight, but dropping them unclosed
                // would leak sockets and drift the stats.
                for (Host stale : hosts.values()) {
                    BlockingQueue<ConnectionContainer> pool = stale.getConnectionPool();
                    ConnectionContainer c;

                    while (pool != null && (c = pool.poll()) != null) {
                        try { c.getCon().close(); } catch (Exception ignored) {}
                        stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                    }
                }

                markStatsDirty();
                hosts.clear();
                reseedIfAllHostsEvicted();
                consecutiveCyclesWithoutHello = 0;
            }
        } else {
            consecutiveCyclesWithoutHello = 0;
        }

        // #330: clean up orphaned hostThreads entries for hosts no longer in hosts map
        // (happens when onConnectionError/handleHelloResult removes a host but the bookkeeping remains)
        hostThreads.entrySet().removeIf(entry -> !hosts.containsKey(entry.getKey()));
    }

    private void startConnectionWaiter() {
        // A revive re-enters startHeartbeat(), so this can run more than once per driver - and
        // every extra waiter spawns its own connection-creator threads on signalAll. The
        // reference has to be STORED, not just checked: without the assignment below the field
        // stayed null and this guard never fired.
        if (connectionWaiter != null && connectionWaiter.isAlive()) {
            return;
        }

        // thread to create new connections instantly if a thread is waiting
        // this thread pauses until waitCounterCondition.signalAll() is called
        connectionWaiter = Thread.ofPlatform().name("ConnectionWaiter").start(() -> {
            long lastHeartbeatHealthCheck = 0;
            while (running) {
                try {
                    // #330: heartbeat health watchdog - ensure heartbeat is scheduled
                    // Check every 5 seconds (connection waiter wakes up on signal or timeout)
                    long now = System.currentTimeMillis();
                    if (now - lastHeartbeatHealthCheck > 5000) {
                        lastHeartbeatHealthCheck = now;
                        ScheduledFuture<?> current = heartbeat;
                        if (current != null && current.isDone()) {
                            log.warn("Heartbeat watchdog detected dead heartbeat (done={}) - restarting topology discovery", current.isDone());
                            heartbeat = null;
                            startHeartbeat();
                        }
                    }

                    waitCounterLock.lock();
                    try {
                        waitCounterCondition.await(1, TimeUnit.SECONDS); // timeout to allow periodic health check
                    } finally {
                        waitCounterLock.unlock();
                    }

                    for (String hst : getHostSeed()) {
                        String normalizedHst = normalizeHostKey(hst);
                        try {
                            if (hosts.get(normalizedHst) == null) continue;

                            // Calculate how many new connections we need
                            int waitCount = getWaitCounterForHost(normalizedHst);
                            int poolSize = hosts.get(normalizedHst).getConnectionPool().size();
                            int totalConnections = getTotalConnectionsToHost(normalizedHst);
                            int maxConnections = getMaxConnectionsPerHost();

                            // Number of connections to create (limited by max and available capacity)
                            int needed = Math.min(waitCount - poolSize, maxConnections - totalConnections);

                            if (needed > 0 && hosts.containsKey(normalizedHst)) {
                                // Create connections in parallel for burst scenarios
                                int parallelCreators = Math.min(needed, 10); // Cap at 10 parallel creators
                                final String host = normalizedHst;

                                for (int i = 0; i < parallelCreators; i++) {
                                    Thread.ofPlatform().name("ConnectionCreator-" + i).start(() -> {
                                        try {
                                            // Each creator can create multiple connections
                                            while (running && hosts.containsKey(host)
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
                            log.error("Could not create connection to {}", normalizedHst, e);
                            // removing connections, probably all broken now
                            onConnectionError(normalizedHst);
                        }
                    }
                } catch (Throwable e) {
                    log.error("error", e);
                    stats.get(DriverStatsKey.ERRORS).incrementAndGet();
                }
            }
        });
    }

    private int getWaitCounterForHost(String hst) {
        Host host = hosts.get(normalizeHostKey(hst));
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

    /**
     * After a full cluster outage every host may have been evicted by
     * onConnectionError() (MAX_FAILURES exceeded on all of them). The heartbeat
     * only iterates the hosts map, and handleHelloResult() — the only place that
     * (re-)adds hosts — only runs from heartbeat threads. With an empty map the
     * driver could therefore never recover, even after the cluster returned (#233).
     * Re-seeding from the configured host seed restarts the normal discovery
     * cycle (hello → handleHelloResult → primary election).
     */
    // #330: the seed the driver was ORIGINALLY configured with, captured at connect() and
    // never touched by any runtime path. The running host seed can be eroded by membership
    // updates during pathological failover windows - and an empty running seed used to make
    // reseedIfAllHostsEvicted a silent no-op: the heartbeat kept cycling over an empty hosts
    // map, spawning nothing, logging nothing, forever. This copy is the last line of defense.
    private volatile List<String> initialHostSeed;

    void captureInitialHostSeed() {
        if (initialHostSeed == null && getHostSeed() != null && !getHostSeed().isEmpty()) {
            initialHostSeed = List.copyOf(getHostSeed());
        }
    }

    void reseedIfAllHostsEvicted() {
        if (!hosts.isEmpty() || getHostSeed() == null) {
            return;
        }

        // #330: a fully eroded running seed can never recover on its own - restore the
        // originally configured seed first, loudly. Without this, every later reseed is a
        // silent no-op and the client is bus-dead while looking perfectly healthy.
        List<String> original = initialHostSeed;

        if (getHostSeed().isEmpty() && original != null && !original.isEmpty()) {
            log.error("Host seed is EMPTY (eroded by membership updates) - restoring the {} "
                    + "originally configured seed host(s) for re-discovery", original.size());

            for (String seedHost : original) {
                addToHostSeed(seedHost);
            }
        }

        for (String seedHost : getHostSeed()) {
            String normalizedHost = normalizeHostKey(seedHost);
            hosts.putIfAbsent(normalizedHost, new Host(getHost(normalizedHost), getPortFromHost(normalizedHost)));
        }

        if (!hosts.isEmpty()) {
            log.warn("All hosts had been evicted - re-seeded {} host(s) from the host seed for re-discovery", hosts.size());
        }
    }

    // Package-private for testing (PooledDriverHeartbeatResilienceTest)
    void onConnectionError(String host) {
        if (!running) return;
        // empty pool for host, as connection to it failed
        stats.get(DriverStatsKey.ERRORS).incrementAndGet();
        String normalizedHost = normalizeHostKey(host);
        Host h = hosts.get(normalizedHost);
        if (h == null) {
            return;
        }
        h.incrementFailures();
        if (h.getFailures() > Host.MAX_FAILURES) {
            hosts.remove(normalizedHost);
            // #330: clean up orphaned hostThreads entry for removed host
            hostThreads.remove(normalizedHost);
            BlockingQueue<ConnectionContainer> connectionsList = h.getConnectionPool();


            // Do not remove seed hosts based on replica-set member strings:
            // the replica set may advertise members under different hostnames (e.g. short names)
            // than the client's resolvable seed list (e.g. FQDNs). Removing here can break primary discovery.

            if (normalizedHost.equals(primaryNode)) {
                primaryNode = null;
            }

            if (normalizedHost.equals(fastestHost)) {
                fastestHost = null;
                fastestTime = 10000;
            }

            // Clean up borrowed connections for the removed host (same as handleHelloResult).
            // Without this, borrowed entries linger and their counters are never decremented,
            // causing permanent drift if the host is later re-added with a fresh Host object.
            ArrayList<Integer> borrowedToDelete = new ArrayList<>();
            for (var e : new ArrayList<>(borrowedConnections.entrySet())) {
                String borrowedFrom = e.getValue().getBorrowedFromHost();
                if (normalizedHost.equals(borrowedFrom)) {
                    borrowedToDelete.add(e.getKey());
                }
            }
            for (Integer port : borrowedToDelete) {
                ConnectionContainer removed = borrowedConnections.remove(port);
                if (removed != null) {
                    h.decrementBorrowedConnections();
                    // Close the connection: threads blocked in a read on this dead host
                    // (e.g. frozen VM, network partition) are woken up immediately with a
                    // network error and can retry on the new primary. Without this they
                    // hang until the socket read timeout (maxWaitTime) expires.
                    try {
                        removed.getCon().close();
                    } catch (Exception ignored) {
                    }
                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                }
            }
            if (!borrowedToDelete.isEmpty()) {
                log.warn("Cleaned up {} borrowed connections for removed host {}", borrowedToDelete.size(), normalizedHost);
                markStatsDirty();
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
        // Normalize host key to always include port
        hst = normalizeHostKey(hst);
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
            // A connect just succeeded - a caller polling isConnected()/getLastConnectFailure()
            // after recovery must not keep seeing the pre-recovery error as if it were current.
            lastConnectFailure = null;

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
                    // Use offer() with timeout to prevent lock convoy
                    try {
                        if (host.getConnectionPool().offer(cont, 100, TimeUnit.MILLISECONDS)) {
                            markStatsDirty();
                            con = null; // Don't close, it's now in the pool
                        } else {
                            log.warn("Could not add connection to pool within timeout - closing");
                            try { con.close(); } catch (Exception ignored) {}
                            stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                            markStatsDirty();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        try { con.close(); } catch (Exception ignored) {}
                        stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                        markStatsDirty();
                    }
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
        Host host = hosts.get(normalizeHostKey(h));
        if (host == null) {
            return 0;
        }
        // Include pendingConnectionCreations which tracks connections currently being created
        // This prevents race conditions where multiple threads think they're under the limit
        return host.getBorrowedConnections() + host.getConnectionPool().size() + host.getPendingConnectionCreations();
    }

    // package-private (not private) so tests in this package can exercise it directly without
    // a real connect() - same rationale as handleHelloResult's visibility.
    MongoConnection borrowConnection(String host) throws MorphiumDriverException {
        if (!running) throw new MorphiumDriverException("Driver is shutting down");
        // log.debug("borrowConnection {}", host);
        if (host == null)
            throw new MorphiumDriverException("Cannot connect to host null!");

        // Normalize host key to always include port
        host = normalizeHostKey(host);

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
                waitCounterLock.lock();
                try {
                    waitCounterCondition.signalAll();
                } finally {
                    waitCounterLock.unlock();
                }
            }

            // Computed once, before the retry loop below - not on every iteration/poll. Found
            // via a real run on mongo1/mongo2.fritz.box: a reader thread got stuck forever in
            // here after a failover, with no exception ever thrown. Root cause was two-fold:
            // (1) the deadline used to live *inside* the retry loop, so every stale
            // (disconnected) connection discarded below pushed it back out by another full
            // serverSelectionTimeout; (2) even after hoisting it out, the deadline was only ever
            // *checked* inside the branch that runs when queue.poll() returns null (an empty
            // queue) - if the pool keeps handing back a non-null (but stale) entry on every
            // single poll, that branch never runs at all, so the check is never reached either.
            // Both together made the whole method's wait effectively unbounded despite its
            // contract being "give up after serverSelectionTimeout" - fixed by checking the
            // deadline unconditionally at the top of every iteration, regardless of why the
            // previous one didn't produce a usable connection.
            long deadline = getServerSelectionTimeout() <= 0
                            ? Long.MAX_VALUE
                            : System.currentTimeMillis() + getServerSelectionTimeout();

            while (true) {
                if (System.currentTimeMillis() >= deadline) {
                    bc = null;
                    break;
                }
                if (!hosts.containsKey(host)) {
                    throw new MorphiumDriverException("Host " + host + " was removed while waiting for a connection (failover?)");
                }
                if (!running) {
                    throw new MorphiumDriverException("Driver is shutting down");
                }

                // Poll in slices (rather than for the full remaining budget) so the deadline/
                // eviction/shutdown checks above still run promptly even while waiting.
                bc = queue.poll(100, TimeUnit.MILLISECONDS);
                if (bc == null) {
                    continue;
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
                    continue;
                }

                break;
            }

            if (bc == null) {
                log.error("Connection timeout");
                log.error("Connections to {}: {}", host, getTotalConnectionsToHost(host));
                log.error("WaitingThreads for {}: {}", host, getWaitCounterForHost(host));
                throw new MorphiumDriverException(
                                String.format("Could not get connection to %s in time %dms", host, getServerSelectionTimeout()));
            }

            bc.touch();
            bc.setBorrowedFromHost(host);  // Track the host we borrowed from for correct counter decrement
            ConnectionContainer previous = borrowedConnections.put(bc.getCon().getSourcePort(), bc);
            if (previous != null) {
                // A stale entry with the same sourcePort was overwritten (port reuse after reconnect).
                // Decrement the counter for the previous entry's host to prevent permanent drift.
                String prevHost = previous.getBorrowedFromHost();
                if (prevHost != null) {
                    Host prevH = hosts.get(prevHost);
                    if (prevH != null) {
                        prevH.decrementBorrowedConnections();
                        log.warn("Overwritten borrowed entry detected (sourcePort={}). Previous borrowedFrom={}. Counter corrected.",
                                 bc.getCon().getSourcePort(), prevHost);
                    }
                }
            }
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
            if (!isReplicaSet()) {
                // standalone — no ReadPreference / transaction routing needed
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

            // Force PRIMARY reads shortly after a transaction commit to ensure
            // read-your-writes consistency. On replica sets, secondaries may not
            // have replicated the committed data yet.
            if (type != ReadPreferenceType.PRIMARY && isInReadAfterWriteWindow()) {
                type = ReadPreferenceType.PRIMARY;
            }

            // Force PRIMARY reads for InMemory backend (PoppyDB) to ensure read-your-writes consistency
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
                    String nearestCandidate = fastestHost;
                    if (nearestCandidate != null) {
                        try {
                            return borrowConnection(nearestCandidate);
                        } catch (MorphiumDriverException e) {
                            stats.get(DriverStatsKey.ERRORS).incrementAndGet();
                            log.warn("Could not get connection to fastest host, trying primary", e);
                            // A host we cannot even borrow a connection from is not "fastest".
                            // fastestHost is otherwise only cleared when the heartbeat finally
                            // evicts the host (5 consecutive failures) - observed live on a real
                            // failover: it kept pointing at the faulted ex-primary for ~11s,
                            // taxing EVERY read in that window with a full serverSelectionTimeout
                            // before it could fall through to a healthy node. Clear it here so
                            // only the first read pays; the next successful ping re-elects one.
                            if (nearestCandidate.equals(fastestHost)) {
                                fastestHost = null;
                                fastestTime = 10000;
                            }
                        }
                    }
                    // fall through — NEAREST failed or no fastestHost, try primary next

                case PRIMARY_PREFERRED:
                    // Deliberately NO pool-emptiness precondition here: right after a failover the
                    // freshly-promoted primary's idle pool is typically EMPTY (its connections are
                    // still being created, or all borrowed by concurrent writers) - which is exactly
                    // when this branch matters most. Skipping the healthy primary because of a
                    // momentarily empty pool sent reads into the secondary-only loop below, which
                    // excludes the primary entirely - in a two-data-node RS whose secondary just
                    // died, that loop could then NEVER succeed while writes on the same driver
                    // recovered fine (observed live: readOk frozen for 25s+ while writeOk climbed).
                    // borrowConnection() itself waits deadline-bounded (serverSelectionTimeout) for
                    // the pool to be refilled, which is precisely what PRIMARY_PREFERRED wants.
                    // Snapshot primaryNode: the heartbeat nulls the volatile field on stepdown or
                    // connection error - i.e. exactly while this failover-path code runs - and
                    // hosts.get(null) would throw an NPE that bypasses every MorphiumDriverException
                    // retry-catch on the read path.
                    String preferredPrimary = primaryNode;
                    if (preferredPrimary != null && hosts.get(preferredPrimary) != null) {
                        try {
                            return borrowConnection(preferredPrimary);
                        } catch (MorphiumDriverException e) {
                            stats.get(DriverStatsKey.ERRORS).incrementAndGet();
                            log.warn("Could not get connection to {} trying secondary", preferredPrimary);
                        }
                    }
                    // fall through — primary not available or failed, try secondary

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
                            // "No reachable secondary" must not strand callers whose preference
                            // allows the primary: SECONDARY_PREFERRED - and the fall-throughs from
                            // NEAREST / PRIMARY_PREFERRED that land here - semantically mean
                            // "secondary if available, OTHERWISE primary". Once a full round-robin
                            // wrap over the candidate secondaries has failed (retry > 0), try the
                            // primary instead of hammering dead secondaries for up to
                            // retriesOnNetworkError more wraps (at serverSelectionTimeout each,
                            // that is over a minute inside ONE read call with typical settings -
                            // observed live after a failover: a single countAll() spent ~26s in
                            // this loop retrying the dead ex-primary while the healthy new primary
                            // sat idle, so reads never recovered although writes did). Only a
                            // strict SECONDARY preference keeps excluding the primary.
                            // Same snapshot rationale as the PRIMARY_PREFERRED branch above: the
                            // heartbeat nulls primaryNode concurrently, and hosts.get(null) NPEs
                            // past every retry-catch here.
                            String fallbackPrimary = primaryNode;
                            if (type != ReadPreferenceType.SECONDARY && retry > 0 && fallbackPrimary != null
                                    && hosts.get(fallbackPrimary) != null) {
                                try {
                                    return borrowConnection(fallbackPrimary);
                                } catch (MorphiumDriverException pe) {
                                    stats.get(DriverStatsKey.ERRORS).incrementAndGet();
                                    log.warn("Primary fallback failed too ({}) - continuing secondary retries",
                                             fallbackPrimary, pe);
                                }
                            }

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
            throw e;
        }
    }

    @Override
    public MongoConnection getPrimaryConnection(WriteConcern wc) throws MorphiumDriverException {
        if (primaryNode == null) {
            // Discovery must be able to resume from every state (#304): if the heartbeat is gone,
            // waiting for it below would wait forever - nothing else ever sets primaryNode.
            reviveHeartbeatIfDead();

            // Race-free: connect() returns after scheduling heartbeat, but primary discovery happens async.
            // Also handles short windows during primary failover where primaryNode is cleared temporarily.
            long start = System.currentTimeMillis();
            long timeout = Math.max(getServerSelectionTimeout(), (long) getHeartbeatFrequency() * 2L + 500L);

            if (timeout <= 0) {
                timeout = 1000;
            }

            // If not a replicaset, there is no "primary" concept - use seed host.
            if (!isReplicaSet()) {
                if (getHostSeed().isEmpty()) {
                    throw new MorphiumDriverException("No host seed configured");
                }
                primaryNode = getHostSeed().get(0);
            } else {
                while (primaryNode == null) {
                    if (System.currentTimeMillis() - start > timeout) {
                        throw new MorphiumDriverException("No primary node found - not connected yet?");
                    }

                    // Also while waiting, not just on entry: the heartbeat can die mid-wait, and
                    // then nothing would ever set primaryNode for this caller.
                    reviveHeartbeatIfDead();

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

    /**
     * Restarts the heartbeat if its scheduled task is no longer running. Nothing but the
     * heartbeat sets {@code primaryNode}, so a dead one turns a temporary outage into a
     * permanent one - the client keeps failing with "No primary node found" until it is
     * restarted, which is what #304 reported from production.
     */
    private void reviveHeartbeatIfDead() {
        // Lock-free fast path: this runs on every primary lookup that finds no primary yet, i.e.
        // on every thread at once during a failover - taking the instance monitor (shared with
        // startHeartbeat/removeFromHostSeed) there would serialize exactly the moment that needs
        // to stay parallel. Only a genuinely dead heartbeat is worth synchronizing for.
        ScheduledFuture<?> current = heartbeat;

        if (!running || (current != null && !current.isDone())) {
            return;
        }

        synchronized (this) {
            // Re-check under the lock: another thread may have revived it in the meantime.
            if (!running || (heartbeat != null && !heartbeat.isDone())) {
                return;
            }

            log.warn("Heartbeat was no longer running - restarting topology discovery");
            heartbeat = null;
            startHeartbeat();
        }
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

            // Decrement counter on the ORIGINAL host where connection was borrowed from
            // This prevents counter drift when topology changes during borrow/release.
            // Defensive fallback: if borrowedFromHost is missing (should not happen), try to decrement based on
            // current connectedTo / connectedToHost to avoid counter drift/leaks.
            String borrowedFrom = c.getBorrowedFromHost();
            boolean decremented = false;
            if (borrowedFrom != null) {
                Host originalHost = hosts.get(borrowedFrom);
                if (originalHost != null) {
                    originalHost.decrementBorrowedConnections();
                    decremented = true;
                }
                // else: original host was removed, counter is gone with it - that's fine
            }

            if (!decremented) {
                // Fallback based on where the connection thinks it is connected to.
                // This is less accurate during topology changes, but better than leaking the counter.
                String connectedTo = con.getConnectedTo();
                Host h = connectedTo == null ? null : hosts.get(connectedTo);
                if (h == null) {
                    String connectedToHost = con.getConnectedToHost();
                    h = connectedToHost == null ? null : hosts.get(connectedToHost);
                }
                if (h != null) {
                    h.decrementBorrowedConnections();
                    decremented = true;
                } else {
                    log.warn("Could not decrement borrowed counter for released connection: borrowedFromHost={}, connectedTo={}, connectedToHost={} - counter may drift", borrowedFrom, con.getConnectedTo(), con.getConnectedToHost());
                }
            }

            // Don't return closed/broken connections to the pool — they would cause
            // "Illegal opcode" errors for the next thread that borrows them.
            // Instead, discard and signal ConnectionWaiter to create a replacement.
            if (!con.isConnected()) {
                log.debug("Connection to {} already closed, not returning to pool — signaling for replacement", con.getConnectedTo());
                stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                markStatsDirty();
                // Wake ConnectionWaiter so it creates a replacement connection
                waitCounterLock.lock();
                try {
                    waitCounterCondition.signalAll();
                } finally {
                    waitCounterLock.unlock();
                }
                return;
            }

            // NEVER pool a connection whose sent request still awaits its reply: the next
            // borrower would read the predecessor's answer - THE source of the wire-desync
            // family ("out of sync: expected reply to X, got Y", "Illegal opcode"). Whatever
            // abandoned the request (interrupt, outer timeout between send and read) - the
            // connection is poisoned, close it and let the pool create a fresh one.
            if (con.hasPendingReplies()) {
                String pending = "?";
                boolean expectedAbandon = false;

                if (con instanceof SingleMongoConnection smc) {
                    pending = smc.pendingReplySummary();
                    // a tailable/awaitData teardown legitimately walks away from its in-flight
                    // getMore - close quietly; anything else is an abandoned reply worth a WARN
                    expectedAbandon = smc.pendingRepliesAreOnlyGetMore();
                }

                if (expectedAbandon) {
                    log.debug("Released connection to {} still awaits replies ({}) - closing instead of pooling",
                        con.getConnectedTo(), pending);
                } else {
                    log.warn("Released connection to {} still awaits replies ({}) - closing instead of pooling",
                        con.getConnectedTo(), pending);
                }
                stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                markStatsDirty();

                try {
                    con.close();
                } catch (Exception ignored) {
                }

                return;
            }

            // Don't pool connections that exceeded their lifetime (or idle time) while borrowed —
            // pooling them parks already-expired connections until the heartbeat's expiry sweep
            // runs, which lags behind under load. A borrow burst then keeps the pool far above
            // its per-host minimum for seconds (the testLotsConnectionPool flaky). Closing on
            // release matches what the official MongoDB drivers do.
            long now = System.currentTimeMillis();

            if (c.getCreated() < now - getMaxConnectionLifetime()
                    || c.getLastUsed() < now - getMaxConnectionIdleTime()) {
                log.debug("Connection to {} exceeded lifetime/idle time while borrowed - closing instead of pooling", con.getConnectedTo());
                stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                markStatsDirty();

                try {
                    con.close();
                } catch (Exception ignored) {
                }

                return;
            }

            // Return connection to the pool it currently belongs to (may differ from borrowedFrom after failover)
            if (con.getConnectedTo() != null) {
                Host h = hosts.get(con.getConnectedTo());
                // If not found with host:port, try with just hostname (handles case where
                // hosts map was populated without port numbers)
                if (h == null) {
                    h = hosts.get(con.getConnectedToHost());
                }
                if (h != null) {
                    // Use offer() with timeout instead of add() to prevent lock convoy under high contention
                    try {
                        if (!h.getConnectionPool().offer(c, 100, TimeUnit.MILLISECONDS)) {
                            log.warn("Could not return connection to pool within timeout - closing connection");
                            stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                            try { c.getCon().close(); } catch (Exception ignored) {}
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.warn("Interrupted while returning connection to pool - closing connection");
                        stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                        try { c.getCon().close(); } catch (Exception ignored) {}
                    }
                    markStatsDirty();
                } else {
                    // Host was removed from pool - close connection
                    log.debug("Host {} no longer available, closing connection", con.getConnectedTo());
                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                    markStatsDirty();
                    try {
                        con.close();
                    } catch (Exception ignored) {
                    }
                }
            } else {
                // Connection has no target host - close it
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
                    // Track which host this connection was BORROWED FROM so we can decrement its counter correctly
                    if (connectionContainer != null) {
                        String hostKey = connectionContainer.getBorrowedFromHost();
                        if (hostKey != null) {
                            hostsToDecrement.merge(hostKey, 1, Integer::sum);
                        }
                    }
                }
            }

            // Remove stale entries and decrement only when remove() succeeds,
            // to prevent double-decrement if releaseConnection() handled it concurrently.
            for (int port : sourcePortsToDelete) {
                ConnectionContainer removed = borrowedConnections.remove(port);
                if (removed != null) {
                    String hostKey = removed.getBorrowedFromHost();
                    if (hostKey != null) {
                        Host h = hosts.get(hostKey);
                        if (h == null) {
                            String hostOnly = hostKey.split(":")[0];
                            h = hosts.get(hostOnly);
                        }
                        if (h != null) {
                            h.decrementBorrowedConnections();
                        }
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

    /**
     * The last error seen while trying to establish a connection to any seed host, or
     * {@code null} if none was seen. Populated on both the initial per-seed connect attempts in
     * {@link #connect(String)} and every heartbeat reconnect attempt - so it reflects the most
     * recent failure regardless of replicaset/single-host mode. A caller that finds
     * {@link #isConnected()} still {@code false} after waiting can use this to report the real
     * cause (e.g. a TLS handshake failure) instead of a bare "not connected" message.
     */
    public Throwable getLastConnectFailure() {
        return lastConnectFailure;
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
    public boolean isPoppyDB() {
        return poppyDB;
    }

    @Override
    public boolean isCosmosDB() {
        return cosmosDB;
    }

    /**
     * Detect CosmosDB via hostname patterns and hello handshake signals.
     * Signal 1: Hostname matches a CosmosDB endpoint (Global, China, US Gov)
     * Signal 2: Seed hosts contain CosmosDB patterns
     * Signal 3: setName == "globaldb" + SSL active (fallback heuristic)
     */
    private boolean detectCosmosDB(HelloResult hello, String host) {
        // Signal 1: Connected host is a CosmosDB endpoint
        if (isCosmosDBHost(host)) {
            return true;
        }

        // Signal 2: Any seed host is a CosmosDB endpoint
        for (String seed : getHostSeed()) {
            if (isCosmosDBHost(seed)) {
                return true;
            }
        }

        // Signal 3: setName == "globaldb" with SSL (CosmosDB's default replica set name)
        if ("globaldb".equals(hello.getSetName()) && isUseSSL()) {
            return true;
        }

        return false;
    }

    private static boolean isCosmosDBHost(String hostPort) {
        if (hostPort == null) return false;
        String h = hostPort.split(":")[0].toLowerCase();
        // Azure Global (Commercial Cloud)
        return h.endsWith(".mongo.cosmos.azure.com")
            || h.endsWith(".mongocluster.cosmos.azure.com")
            // Azure China (21Vianet)
            || h.endsWith(".mongo.cosmos.azure.cn")
            || h.endsWith(".mongocluster.cosmos.azure.cn")
            // Azure US Government
            || h.endsWith(".mongo.cosmos.azure.us")
            || h.endsWith(".mongocluster.cosmos.usgovcloudapi.net")
            // Azure Germany (legacy, deprecated but may still exist)
            || h.endsWith(".mongo.cosmos.microsoftazure.de");
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
        // Wake up ConnectionWaiter thread so it can exit its while(running) loop
        try {
            waitCounterLock.lock();
            waitCounterCondition.signalAll();
        } finally {
            waitCounterLock.unlock();
        }
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

        // Per-host check bookkeeping is meaningless once the heartbeat is gone - leaving it
        // behind would make a re-used driver instance skip every host it still lists (#304).
        // Cleared AFTER the executor terminated: a heartbeat cycle already past the cancel
        // still claims entries (hostThreads.put before t.start), so clearing earlier races a
        // final put - the exact flake closeClearsHostBookkeeping caught on the loaded runner.
        hostThreads.clear();

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

        boolean committed = false;
        try {
            var cmd = new CommitTransactionCommand(con).setTxnNumber(ctx.getTxnNumber()).setAutocommit(false)
            .setLsid(ctx.getLsid());
            cmd.execute();
            committed = true;
        } finally {
            clearTransactionContext();
            if (committed) {
                markTransactionCommitted();
            }
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

            mem.stream().filter(d -> d.get("optime") instanceof Map)
               .forEach(d -> {
                   @SuppressWarnings("unchecked")
                   Map<String, Doc> optimeMap = (Map<String, Doc>) d.get("optime");
                   d.put("optime", optimeMap.get("ts"));
               });
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

        // Use borrowedFromHost (not connectedTo) to match how the counter is managed.
        // After topology changes, connectedTo can differ from borrowedFromHost.
        for (var e : borrowedConnections.values()) {
            String host = e.getBorrowedFromHost();
            if (host == null) {
                host = e.getCon().getConnectedTo(); // fallback for legacy
            }
            ret.merge(host, 1, Integer::sum);
        }

        return ret;
    }

    /**
     * Get detailed connection pool statistics for monitoring and debugging.
     * Helps detect counter drift (when borrowed_counter != borrowed_map_size).
     * 
     * @return Map with detailed stats per host:
     *   - {host}.pool_size: connections available in pool
     *   - {host}.borrowed_counter: Host's borrowed counter value
     *   - {host}.borrowed_map_size: actual entries in borrowedConnections map for this host
     *   - {host}.pending_creations: connections currently being created
     *   - {host}.wait_counter: threads waiting for a connection
     *   - borrowed_map_total: total size of borrowedConnections map
     */
    public Map<String, Object> getConnectionPoolDetails() {
        Map<String, Object> ret = new HashMap<>();
        
        // Count borrowed connections per host from the actual map
        // Use borrowedFromHost (not getConnectedTo) to match how the counter is managed
        Map<String, Integer> borrowedMapByHost = new HashMap<>();
        for (var e : borrowedConnections.values()) {
            String host = e.getBorrowedFromHost();
            if (host == null) {
                host = e.getCon().getConnectedTo(); // fallback for legacy
            }
            borrowedMapByHost.merge(host, 1, Integer::sum);
        }
        
        // Add per-host details
        for (var e : hosts.entrySet()) {
            String host = e.getKey();
            Host h = e.getValue();
            ret.put(host + ".pool_size", h.getConnectionPool().size());
            ret.put(host + ".borrowed_counter", h.getBorrowedConnections());
            ret.put(host + ".borrowed_map_size", borrowedMapByHost.getOrDefault(host, 0));
            ret.put(host + ".pending_creations", h.getPendingConnectionCreations());
            ret.put(host + ".wait_counter", h.getWaitCounter());
            
            // Always report counter drift for monitoring (even when 0)
            int counter = h.getBorrowedConnections();
            int mapSize = borrowedMapByHost.getOrDefault(host, 0);
            ret.put(host + ".COUNTER_DRIFT", counter - mapSize);
        }
        
        // Total borrowed map size
        ret.put("borrowed_map_total", borrowedConnections.size());
        
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
        private String borrowedFromHost;  // Track which host this was borrowed from for correct counter decrement

        public ConnectionContainer(SingleMongoConnection con) {
            this.con = con;
            created = System.currentTimeMillis();
            lastUsed = System.currentTimeMillis();
        }

        public String getBorrowedFromHost() {
            return borrowedFromHost;
        }

        public ConnectionContainer setBorrowedFromHost(String borrowedFromHost) {
            this.borrowedFromHost = borrowedFromHost;
            return this;
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
