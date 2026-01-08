package de.caluga.morphium.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.morphium.server.election.ElectionConfig;
import de.caluga.morphium.server.election.ElectionManager;
import de.caluga.morphium.server.election.ElectionNetworkClient;
import de.caluga.morphium.server.netty.MongoCommandHandler;
import de.caluga.morphium.server.netty.MongoWireProtocolDecoder;
import de.caluga.morphium.server.netty.MongoWireProtocolEncoder;
import de.caluga.morphium.server.netty.WatchCursorManager;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Async I/O MongoDB-compatible server using Netty.
 *
 * Event-driven architecture that can handle thousands of concurrent
 * connections with few threads.
 */
public class MorphiumServer {

    private static final Logger log = LoggerFactory.getLogger(MorphiumServer.class);

    // Configuration
    private final int port;
    private final String host;
    private final int maxConnections;
    private final int idleTimeoutSeconds;
    private final int compressorId;

    // Netty components
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private final ChannelGroup allChannels;

    // Server state
    private final InMemoryDriver driver;
    private final WatchCursorManager cursorManager;
    private final AtomicInteger msgId = new AtomicInteger(1000);
    private volatile boolean running = false;

    // Replica set configuration
    private String rsName = "";
    private List<String> hosts = new ArrayList<>();
    private volatile boolean primary = true;
    private volatile String primaryHost;

    // Election configuration
    private boolean electionEnabled = false;
    private ElectionConfig electionConfig = null;
    private ElectionManager electionManager = null;
    private ElectionNetworkClient electionNetworkClient = null;

    // SSL configuration
    private boolean sslEnabled = false;
    private SSLContext sslContext = null;

    // Persistence configuration
    private File dumpDirectory = null;
    private long dumpIntervalMs = 0;
    private java.util.concurrent.ScheduledExecutorService dumpScheduler = null;
    private volatile long lastDumpTime = 0;

    // Replication
    private ReplicationManager replicationManager = null;
    private ReplicationCoordinator replicationCoordinator = null;

    public MorphiumServer(int port, String host, int maxConnections, int idleTimeoutSeconds, int compressorId) {
        this.port = port;
        this.host = host;
        this.maxConnections = maxConnections;
        this.idleTimeoutSeconds = idleTimeoutSeconds;
        this.compressorId = compressorId;
        this.allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        this.driver = new InMemoryDriver();
        this.cursorManager = new WatchCursorManager();
        driver.connect();
        // Enable server mode to prevent internal Morphium instances from shutting down the driver
        driver.setServerMode(true);
    }

    public MorphiumServer(int port, String host, int maxConnections, int idleTimeoutSeconds) {
        this(port, host, maxConnections, idleTimeoutSeconds, OpCompressed.COMPRESSOR_NOOP);
    }

    public MorphiumServer() {
        this(17017, "localhost", 10000, 60, OpCompressed.COMPRESSOR_NOOP);
    }

    /**
     * Start the Netty server.
     */
    public void start() throws Exception {
        if (running) {
            throw new IllegalStateException("Server already running");
        }

        log.info("Starting MorphiumServer on {}:{} (maxConnections={}, idleTimeout={}s)",
                host, port, maxConnections, idleTimeoutSeconds);

        // Configure event loop groups
        // Boss group handles incoming connections
        // Worker group handles I/O for established connections
        int bossThreads = 1;
        int workerThreads = Runtime.getRuntime().availableProcessors() * 2;
        bossGroup = new NioEventLoopGroup(bossThreads);
        workerGroup = new NioEventLoopGroup(workerThreads);

        // Build SSL context if enabled
        io.netty.handler.ssl.SslContext nettySslContext = null;
        if (sslEnabled) {
            nettySslContext = buildSslContext();
        }
        final io.netty.handler.ssl.SslContext finalSslContext = nettySslContext;

        // Configure driver
        driver.setHostSeed(host + ":" + port);
        driver.setReplicaSet(rsName != null && !rsName.isEmpty());
        driver.setReplicaSetName(rsName == null ? "" : rsName);

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        // Check connection limit
                        if (allChannels.size() >= maxConnections) {
                            log.warn("Connection limit reached ({}/{}), rejecting", allChannels.size(), maxConnections);
                            ch.close();
                            return;
                        }

                        ChannelPipeline pipeline = ch.pipeline();

                        // SSL handler (if enabled)
                        if (finalSslContext != null) {
                            pipeline.addLast("ssl", finalSslContext.newHandler(ch.alloc()));
                        }

                        // Idle state handler for connection cleanup
                        pipeline.addLast("idleState", new IdleStateHandler(idleTimeoutSeconds, 0, 0, TimeUnit.SECONDS));
                        pipeline.addLast("idleHandler", new IdleHandler());

                        // Wire protocol handlers
                        pipeline.addLast("decoder", new MongoWireProtocolDecoder());
                        pipeline.addLast("encoder", new MongoWireProtocolEncoder(compressorId));

                        // Command handler - capture current primary state for this connection
                        // Note: primary/primaryHost are volatile and may change during election
                        pipeline.addLast("commandHandler", new MongoCommandHandler(
                                driver, cursorManager, msgId,
                                host, port, rsName, hosts,
                                primary, primaryHost, compressorId,
                                replicationCoordinator, electionManager
                        ));

                        // Track the channel
                        allChannels.add(ch);
                        log.info("New connection accepted (total: {})", allChannels.size());
                    }
                });

        // Bind and start
        ChannelFuture future = bootstrap.bind(host, port).sync();
        serverChannel = future.channel();
        running = true;

        log.info("MorphiumServer started on {}:{} (workers: {})", host, port, workerThreads);

        // Start dump scheduler if configured
        startDumpScheduler();

        // Start election system if enabled
        startElection();

        // Start replication if this is a secondary (for static mode)
        // In election mode, replication is started when leader is discovered
        if (!electionEnabled) {
            startReplication();
        }
    }

    /**
     * Stop the server gracefully.
     */
    public void shutdown() {
        if (!running) {
            return;
        }

        log.info("Shutting down MorphiumServer...");
        running = false;

        // Stop election system
        stopElection();

        // Stop replication
        stopReplication();

        // Stop dump scheduler
        stopDumpScheduler();

        // Final dump
        if (dumpDirectory != null) {
            try {
                log.info("Performing final dump before shutdown...");
                int count = driver.dumpAllToDirectory(dumpDirectory);
                log.info("Final dump completed: {} databases saved", count);
            } catch (Exception e) {
                log.error("Failed to perform final dump: {}", e.getMessage(), e);
            }
        }

        // Shutdown cursor manager
        cursorManager.shutdown();

        // Close all channels
        log.info("Closing {} client connections...", allChannels.size());
        allChannels.close().awaitUninterruptibly(5, TimeUnit.SECONDS);

        // Close server channel
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly(5, TimeUnit.SECONDS);
        }

        // Shutdown event loops
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
        }

        // Shutdown the driver (force shutdown since it's in server mode)
        driver.forceShutdown();

        log.info("MorphiumServer shutdown complete");
    }

    private io.netty.handler.ssl.SslContext buildSslContext() throws Exception {
        if (sslContext != null) {
            // Use provided SSLContext - need to extract key/cert
            // For now, use default
            log.warn("Custom SSLContext not fully supported yet, using self-signed");
        }

        // Build a simple self-signed context for testing
        // In production, you'd provide proper key/cert files
        return SslContextBuilder.forServer(
                getClass().getResourceAsStream("/server.crt"),
                getClass().getResourceAsStream("/server.key")
        ).build();
    }

    /**
     * Handler for idle connections.
     */
    private class IdleHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                log.debug("Connection idle for {}s, closing", idleTimeoutSeconds);
                ctx.close();
            }
            super.userEventTriggered(ctx, evt);
        }
    }

    // Configuration methods

    public void configureReplicaSet(String name, List<String> hostList, Map<String, Integer> priorities) {
        configureReplicaSet(name, hostList, priorities, false, null);
    }

    /**
     * Configure replica set with optional automatic election.
     *
     * @param name        Replica set name
     * @param hostList    List of all hosts in the replica set
     * @param priorities  Priority map for static primary selection (ignored if election enabled)
     * @param enableElection If true, enable automatic leader election
     * @param config      Election configuration (optional, uses defaults if null)
     */
    public void configureReplicaSet(String name, List<String> hostList, Map<String, Integer> priorities,
                                     boolean enableElection, ElectionConfig config) {
        rsName = name == null ? "" : name;
        hosts = hostList == null ? new ArrayList<>() : new ArrayList<>(hostList);
        this.electionEnabled = enableElection;
        this.electionConfig = config != null ? config : new ElectionConfig();

        String myAddress = host + ":" + port;

        if (rsName.isEmpty()) {
            // No replica set - act as standalone primary
            primary = true;
            primaryHost = myAddress;
            electionEnabled = false;
        } else if (hosts.isEmpty()) {
            // Replica set with no hosts list - act as standalone primary
            primary = true;
            primaryHost = myAddress;
            electionEnabled = false;
        } else if (electionEnabled) {
            // Election mode: start as follower, election will determine primary
            primary = false;
            primaryHost = null;

            // Create election manager
            electionManager = new ElectionManager(myAddress, hosts, electionConfig);

            // Set up leadership change callback
            electionManager.setOnLeadershipChange(this::onLeadershipChange);
            electionManager.setOnLeaderDiscovered(this::onLeaderDiscovered);

            // Create network client for inter-node communication
            electionNetworkClient = new ElectionNetworkClient(electionManager);

            log.info("Replica set configured with election: myAddress={}, hosts={}",
                     myAddress, hosts);
        } else {
            // Static mode: determine primary based on priority or first host
            String electedPrimary = hosts.get(0);  // Default to first host

            // If priorities are provided, find the highest priority host
            if (priorities != null && !priorities.isEmpty()) {
                int highestPriority = -1;
                for (String h : hosts) {
                    int prio = priorities.getOrDefault(h, 0);
                    if (prio > highestPriority) {
                        highestPriority = prio;
                        electedPrimary = h;
                    }
                }
            }

            primaryHost = electedPrimary;
            primary = myAddress.equals(electedPrimary);

            log.info("Replica set configured (static): myAddress={}, primary={}, primaryHost={}",
                     myAddress, primary, primaryHost);
        }

        // Initialize replication coordinator for primary nodes in replica sets (static mode only)
        if (!electionEnabled && primary && !rsName.isEmpty() && hosts.size() > 1) {
            replicationCoordinator = new ReplicationCoordinator(hosts.size());
            log.info("Replication coordinator initialized for {} nodes", hosts.size());
        }

        driver.setReplicaSet(!rsName.isEmpty());
        driver.setReplicaSetName(rsName);
        driver.setHostSeed(hosts);
    }

    /**
     * Called when this node's leadership status changes.
     */
    private void onLeadershipChange(boolean isLeader) {
        log.info("Leadership change: {} is now {}", host + ":" + port, isLeader ? "LEADER" : "FOLLOWER");

        if (isLeader) {
            // Becoming leader
            primary = true;
            primaryHost = host + ":" + port;

            // Initialize replication coordinator
            if (replicationCoordinator == null && hosts.size() > 1) {
                replicationCoordinator = new ReplicationCoordinator(hosts.size());
                log.info("Replication coordinator initialized for {} nodes", hosts.size());
            }

            // Stop replication from old primary (if any)
            if (replicationManager != null) {
                replicationManager.stop();
                replicationManager = null;
            }
        } else {
            // Stepping down from leader
            primary = false;

            // Clean up replication coordinator
            replicationCoordinator = null;

            // Start replication from new primary (will be set by onLeaderDiscovered)
        }
    }

    /**
     * Called when a new leader is discovered.
     */
    private void onLeaderDiscovered(String leaderId) {
        log.info("Discovered leader: {}", leaderId);
        primaryHost = leaderId;

        // If we're a follower and not already replicating, start replication
        if (!primary && replicationManager == null && leaderId != null) {
            String[] parts = leaderId.split(":");
            if (parts.length == 2) {
                String leaderHost = parts[0];
                int leaderPort = Integer.parseInt(parts[1]);

                // Start replication from new leader
                replicationManager = new ReplicationManager(driver, leaderHost, leaderPort);
                replicationManager.setMyAddress(host + ":" + port);
                try {
                    replicationManager.start();
                    log.info("Started replication from new leader {}", leaderId);
                } catch (Exception e) {
                    log.error("Failed to start replication from {}: {}", leaderId, e.getMessage());
                }
            }
        }
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public void setDumpDirectory(File dir) {
        this.dumpDirectory = dir;
    }

    public void setDumpIntervalMs(long intervalMs) {
        this.dumpIntervalMs = intervalMs;
    }

    // Persistence methods

    private void startDumpScheduler() {
        if (dumpIntervalMs <= 0 || dumpDirectory == null) {
            return;
        }

        dumpScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "MorphiumServer-DumpScheduler");
            t.setDaemon(true);
            return t;
        });

        dumpScheduler.scheduleAtFixedRate(() -> {
            try {
                int count = driver.dumpAllToDirectory(dumpDirectory);
                lastDumpTime = System.currentTimeMillis();
                if (count > 0) {
                    log.info("Periodic dump: {} databases saved", count);
                }
            } catch (Exception e) {
                log.error("Failed to dump databases: {}", e.getMessage(), e);
            }
        }, dumpIntervalMs, dumpIntervalMs, TimeUnit.MILLISECONDS);

        log.info("Dump scheduler started: interval={}ms", dumpIntervalMs);
    }

    private void stopDumpScheduler() {
        if (dumpScheduler != null) {
            dumpScheduler.shutdown();
            try {
                if (!dumpScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    dumpScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                dumpScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            dumpScheduler = null;
        }
    }

    // Election methods

    private void startElection() {
        if (!electionEnabled || electionManager == null) {
            return;
        }

        log.info("Starting election system");

        // Start network client first (wires up callbacks)
        if (electionNetworkClient != null) {
            electionNetworkClient.start();
        }

        // Start election manager
        electionManager.start();
    }

    private void stopElection() {
        if (electionManager != null) {
            electionManager.stop();
        }
        if (electionNetworkClient != null) {
            electionNetworkClient.stop();
        }
    }

    // Replication methods

    private void startReplication() {
        // Only secondaries need to replicate from primary
        if (primary || primaryHost == null || primaryHost.isEmpty()) {
            log.debug("Not starting replication - this is the primary");
            return;
        }

        try {
            // Parse primary host and port
            String[] parts = primaryHost.split(":");
            String pHost = parts[0];
            int pPort = parts.length > 1 ? Integer.parseInt(parts[1]) : 27017;

            log.info("Starting replication from primary {}:{}", pHost, pPort);

            replicationManager = new ReplicationManager(driver, pHost, pPort);
            // Set this secondary's address for progress reporting
            replicationManager.setMyAddress(host + ":" + port);
            replicationManager.start();

            // Wait for initial sync (up to 30 seconds)
            boolean synced = replicationManager.waitForInitialSync(30, TimeUnit.SECONDS);
            if (synced) {
                log.info("Initial sync complete, secondary is ready");
            } else {
                log.warn("Initial sync did not complete within 30 seconds, continuing anyway");
            }
        } catch (Exception e) {
            log.error("Failed to start replication: {}", e.getMessage(), e);
        }
    }

    private void stopReplication() {
        if (replicationManager != null) {
            replicationManager.stop();
            replicationManager = null;
        }
    }

    public int dumpNow() throws IOException {
        if (dumpDirectory == null) {
            throw new IOException("Dump directory not configured");
        }
        int count = driver.dumpAllToDirectory(dumpDirectory);
        lastDumpTime = System.currentTimeMillis();
        log.info("Dumped {} databases to {}", count, dumpDirectory.getAbsolutePath());
        return count;
    }

    public int restoreFromDump() throws IOException {
        if (dumpDirectory == null) {
            throw new IOException("Dump directory not configured");
        }
        try {
            int count = driver.restoreAllFromDirectory(dumpDirectory);
            log.info("Restored {} databases from {}", count, dumpDirectory.getAbsolutePath());
            return count;
        } catch (org.json.simple.parser.ParseException e) {
            throw new IOException("Failed to parse dump file", e);
        }
    }

    // Status methods

    public boolean isRunning() {
        return running;
    }

    public int getConnectionCount() {
        return allChannels.size();
    }

    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("connections", allChannels.size());
        stats.put("maxConnections", maxConnections);
        stats.put("running", running);
        stats.put("primary", primary);
        stats.put("primaryHost", primaryHost);
        stats.put("electionEnabled", electionEnabled);
        stats.putAll(cursorManager.getStats());
        if (replicationManager != null) {
            stats.put("replication", replicationManager.getStats());
        }
        if (replicationCoordinator != null) {
            stats.put("replicationCoordinator", replicationCoordinator.getStats());
        }
        if (electionManager != null) {
            stats.put("election", electionManager.getStats());
        }
        return stats;
    }

    public ReplicationCoordinator getReplicationCoordinator() {
        return replicationCoordinator;
    }

    public ElectionManager getElectionManager() {
        return electionManager;
    }

    public boolean isElectionEnabled() {
        return electionEnabled;
    }

    public InMemoryDriver getDriver() {
        return driver;
    }

    public boolean isPrimary() {
        return primary;
    }

    public String getPrimaryHost() {
        return primaryHost;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }
}
