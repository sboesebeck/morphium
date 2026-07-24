package de.caluga.poppydb;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.poppydb.election.ElectionConfig;
import de.caluga.poppydb.election.ElectionManager;
import de.caluga.poppydb.election.ElectionNetworkClient;
import de.caluga.poppydb.netty.FindCursorRegistry;
import de.caluga.poppydb.netty.MongoCommandHandler;
import de.caluga.poppydb.netty.MongoWireProtocolDecoder;
import de.caluga.poppydb.netty.MongoWireProtocolEncoder;
import de.caluga.poppydb.netty.WatchCursorManager;
import de.caluga.poppydb.messaging.MessagingOptimizer;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Async I/O MongoDB-compatible server using Netty.
 *
 * Event-driven architecture that can handle thousands of concurrent
 * connections with few threads.
 */
public class PoppyDB {

    private static final Logger log = LoggerFactory.getLogger(PoppyDB.class);

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
    // Per-instance registry of server-side find cursors (and their idle-sweeper) — owned by
    // this PoppyDB instance and handed to every MongoCommandHandler it creates, exactly like
    // cursorManager above. Must stay per-instance (not JVM-static): multiple PoppyDB instances
    // running in one test JVM must not see each other's open find cursors.
    private final FindCursorRegistry findCursorRegistry;
    private final MessagingOptimizer messagingOptimizer;
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
    private boolean authRequired = false;
    private String rootUser = null;
    private String rootPassword = null;

    // Persistence configuration
    private File dumpDirectory = null;
    private long dumpIntervalMs = 0;
    private java.util.concurrent.ScheduledExecutorService dumpScheduler = null;
    private volatile long lastDumpTime = 0;

    // Replication
    // volatile: mutated under synchronized on the election/leadership paths but read unsynchronized
    // from Netty event-loop threads via the isSecondarySyncing() supplier passed to each handler.
    private volatile ReplicationManager replicationManager = null;
    // Held behind an AtomicReference (rather than a plain volatile field copied into each
    // connection at accept time) so every MongoCommandHandler resolves the coordinator live
    // via a Supplier - onLeadershipChange swaps this reference and every existing connection
    // (not just newly accepted ones) immediately observes the new value.
    private final AtomicReference<ReplicationCoordinator> replicationCoordinatorRef = new AtomicReference<>();

    // DevOps command context: live op registry (currentOp/killOp), real connection gauges
    // for serverStatus, and the per-host priorities replSetGetConfig reports.
    private final de.caluga.poppydb.netty.OpRegistry opRegistry = new de.caluga.poppydb.netty.OpRegistry();
    private final java.util.concurrent.atomic.AtomicLong connectionsCreated = new java.util.concurrent.atomic.AtomicLong();
    private volatile Map<String, Integer> hostPriorities;

    public PoppyDB(int port, String host, int maxConnections, int idleTimeoutSeconds, int compressorId) {
        this.port = port;
        this.host = host;
        this.maxConnections = maxConnections;
        this.idleTimeoutSeconds = idleTimeoutSeconds;
        this.compressorId = compressorId;
        this.allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        this.driver = new InMemoryDriver();
        this.cursorManager = new WatchCursorManager();
        this.findCursorRegistry = new FindCursorRegistry();
        this.messagingOptimizer = new MessagingOptimizer(driver);
        // Wire up messaging optimizer with cursor manager for fast-path notifications
        messagingOptimizer.setWatchCursorManager(cursorManager);
        driver.connect();
        // Enable server mode to prevent internal Morphium instances from shutting down the driver
        driver.setServerMode(true);
        // Size the change-event replay buffer for replication resume-after-disconnect: a reconnecting
        // secondary replays events after its last-applied sequence from this buffer instead of doing a
        // full re-sync. Bound: 100_000 events (ring buffer, oldest evicted on overflow).
        driver.setChangeStreamHistoryLimit(REPLICATION_REPLAY_BUFFER_EVENTS);
    }

    /** Primary replay-buffer bound (events) backing replication resume-after-disconnect. */
    static final int REPLICATION_REPLAY_BUFFER_EVENTS = 100_000;

    /**
     * Warn/reject memory watermarks in percent of max heap (100 disables a stage) - see
     * InMemoryDriver.setMemoryWatermarks. Above the reject watermark, document-creating
     * writes are refused with ExceededMemoryLimit (146) instead of running into an OOM
     * that a replica set cannot survive either (replication copies the volume everywhere).
     */
    public void setMemoryWatermarks(int warnPercent, int rejectPercent) {
        driver.setMemoryWatermarks(warnPercent, rejectPercent);
    }

    /**
     * BSON document size limit in bytes, mongod-compatible (default 16MB, 0 = unlimited) -
     * see InMemoryDriver.setMaxBsonObjectSize. Enforced on inserts/stores and on update
     * results (with mongod's 16KB internal margin), answered as BSONObjectTooLarge (10334);
     * hello advertises the configured value so clients enforce it on their side too.
     */
    public void setMaxBsonObjectSize(int maxBsonObjectSize) {
        driver.setMaxBsonObjectSize(maxBsonObjectSize);
    }

    public PoppyDB(int port, String host, int maxConnections, int idleTimeoutSeconds) {
        this(port, host, maxConnections, idleTimeoutSeconds, OpCompressed.COMPRESSOR_NOOP);
    }

    public PoppyDB() {
        this(17017, "localhost", 10000, 300, OpCompressed.COMPRESSOR_NOOP);
    }

    /**
     * Start the Netty server.
     */
    @SuppressWarnings("deprecation")
    public void start() throws Exception {
        if (running) {
            throw new IllegalStateException("Server already running");
        }

        log.info("Starting PoppyDB on {}:{} (maxConnections={}, idleTimeout={}s)",
                host, port, maxConnections, idleTimeoutSeconds);

        // Configure event loop groups
        // Boss group handles incoming connections (can be tuned via system property)
        // Worker group handles I/O for established connections
        int bossThreads = Integer.getInteger("morphiumserver.bossThreads", 2);
        // Default: 4x CPU cores for better load handling under high concurrency
        int defaultWorkers = Runtime.getRuntime().availableProcessors() * 4;
        int workerThreads = Integer.getInteger("morphiumserver.workerThreads", defaultWorkers);
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
                // Increase backlog for high connection rate scenarios
                .option(ChannelOption.SO_BACKLOG, 4096)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                // Increase socket buffer sizes for better throughput (256KB each)
                .childOption(ChannelOption.SO_RCVBUF, 256 * 1024)
                .childOption(ChannelOption.SO_SNDBUF, 256 * 1024)
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

                        // Command handler - capture current primary state for this connection.
                        // Note: primary/primaryHost are volatile and may change during election;
                        // when electionManager is set the handler resolves them live through it
                        // instead. The replication coordinator is always resolved live through
                        // replicationCoordinatorRef::get, never captured, so a leadership change
                        // that happens after this connection was accepted is still observed.
                        pipeline.addLast("commandHandler", new MongoCommandHandler(
                                driver, cursorManager, findCursorRegistry, messagingOptimizer, msgId,
                                host, port, rsName, hosts,
                                primary, primaryHost, compressorId,
                                replicationCoordinatorRef::get, electionManager,
                                PoppyDB.this::isSecondarySyncing
                        ).setAuthRequired(authRequired)
                         .setOpRegistry(opRegistry)
                         .setConnectionCounters(allChannels::size, connectionsCreated::get)
                         .setRsPriorities(hostPriorities)
                         .setDumpNowAction(dumpDirectory == null ? null : PoppyDB.this::dumpNow)
                         .setDumpStatusSupplier(PoppyDB.this::getDumpStatus));

                        // Track the channel
                        allChannels.add(ch);
                        connectionsCreated.incrementAndGet();
                        log.debug("New connection accepted (total: {})", allChannels.size());
                    }
                });

        // Bind and start
        ChannelFuture future = bootstrap.bind(host, port).sync();
        serverChannel = future.channel();
        running = true;

        log.info("PoppyDB started on {}:{} (workers: {})", host, port, workerThreads);

        if (rootUser != null && rootPassword != null) {
            ensureRootUser();
        } else if (authRequired) {
            // users may still exist from a restored dump - but a fresh --auth server without
            // any user would be permanently unreachable (no localhost exception)
            log.warn("auth enforcement is enabled but no root user is configured - "
                + "clients can only authenticate if admin.system.users already contains users");
        }

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

        log.info("Shutting down PoppyDB...");
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

        // Shutdown this instance's find-cursor registry sweeper thread — must happen on every
        // shutdown so sweeper threads don't leak across test instances in the same JVM.
        findCursorRegistry.shutdown();

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

        log.info("PoppyDB shutdown complete");
    }

    @SuppressWarnings("deprecation") // SelfSignedCertificate is deprecated by Netty but is
    // an intentional, WARN-logged test/dev-only fallback here - see below.
    private io.netty.handler.ssl.SslContext buildSslContext() throws Exception {
        if (sslContext != null) {
            // Adapt the caller-provided javax.net.ssl.SSLContext (e.g. built via
            // SslHelper.createServerSslContext(keystorePath, password), as used by
            // PoppyDBCLI's --sslKeystore option) into a Netty SslContext for server use.
            log.info("Using explicitly configured SSLContext for TLS");

            try {
                // Netty's 3-arg JdkSslContext(SSLContext, boolean, ClientAuth) constructor is
                // deprecated in favor of this 8-arg one. This call reproduces the exact
                // defaults the deprecated constructor used internally: no explicit cipher
                // list, IdentityCipherSuiteFilter, the default ALPN negotiator (selected by
                // passing a null ApplicationProtocolConfig), no protocol override, no
                // startTLS - verified against the netty-handler bytecode on the classpath.
                return new JdkSslContext(
                        sslContext,
                        false,
                        null,
                        IdentityCipherSuiteFilter.INSTANCE,
                        (ApplicationProtocolConfig) null,
                        ClientAuth.NONE,
                        null,
                        false
                );
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Configured SSLContext could not be adapted for server-side TLS: " + e.getMessage(), e);
            }
        }

        // No certificate configured: fall back to a freshly generated self-signed
        // certificate so SSL-enabled startup doesn't fail outright. This is NOT suitable
        // for production - configure a real certificate via setSslContext(...) (see
        // docs/poppydb.md, "SSL/TLS Configuration").
        log.warn("SSL enabled but no SSLContext configured - generating a self-signed certificate. " +
                "This is INSECURE and must not be used in production; configure a real certificate " +
                "via setSslContext(...) or PoppyDBCLI's --sslKeystore option.");

        try {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            return SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "SSL is enabled but no SSLContext was configured, and generating a self-signed " +
                    "fallback certificate failed. Configure a certificate via setSslContext(...).", e);
        }
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
     * @param priorities  Priority map for each host (used in both static and election modes)
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

        // A wildcard or alternate bind address (e.g. --bind 0.0.0.0) is not the name this
        // node has in the seed list. Identify it by the unique seed entry carrying our port -
        // otherwise self stays in the election peer list (duplicate rs.status member, votes
        // for itself as a peer) and the priority lookup below misses.
        if (!hosts.isEmpty() && !hosts.contains(myAddress)) {
            List<String> samePortSeeds = hosts.stream().filter(h -> h.endsWith(":" + port)).toList();

            if (samePortSeeds.size() == 1) {
                log.info("Bind address {} is not a replica set seed - using seed {} as member identity",
                         myAddress, samePortSeeds.get(0));
                myAddress = samePortSeeds.get(0);
            } else {
                log.warn("Bind address {} is not in the replica set seed list {} and no unique seed matches "
                         + "port {} - member identity may be wrong (rs.status/election)", myAddress, hosts, port);
            }
        }

        // keep for replSetGetConfig (rs.conf())
        this.hostPriorities = priorities == null ? null : new java.util.HashMap<>(priorities);

        // Set this node's election priority from the priorities map
        // Priorities should be 0-100, where 0 = cannot become primary
        // All nodes in the cluster must use the same priority configuration
        if (priorities != null && !priorities.isEmpty()) {
            int myPriority = priorities.getOrDefault(myAddress, 50);
            // Clamp to valid range
            myPriority = Math.max(0, Math.min(100, myPriority));
            this.electionConfig.setElectionPriority(myPriority);
            log.info("Node {} election priority: {}", myAddress, myPriority);
        }

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

            // Create election manager with priority-aware config
            electionManager = new ElectionManager(myAddress, hosts, electionConfig);

            // Set up leadership change callback
            electionManager.setOnLeadershipChange(this::onLeadershipChange);
            electionManager.setOnLeaderDiscovered(this::onLeaderDiscovered);

            // Replication progress, so the leader can tell whether a higher-priority peer
            // has caught up before handing leadership over to it (priority takeover).
            electionManager.setLocalSequenceSupplier(driver::getChangeStreamSequence);
            electionManager.setPeerSequenceSupplier(peer -> {
                ReplicationCoordinator coordinator = replicationCoordinatorRef.get();
                return coordinator == null ? -1L : coordinator.getAcknowledgedSequence(peer);
            });

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
            replicationCoordinatorRef.set(new ReplicationCoordinator(hosts.size()));
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

            // Initialize replication coordinator (only if not already present)
            if (hosts.size() > 1 && replicationCoordinatorRef.compareAndSet(null, new ReplicationCoordinator(hosts.size()))) {
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
            replicationCoordinatorRef.set(null);

            // Start replication from new primary (will be set by onLeaderDiscovered)
        }
    }

    /**
     * Called when a new leader is discovered.
     */
    private synchronized void onLeaderDiscovered(String leaderId) {
        // Only act on actual leader changes to prevent flapping
        if (leaderId != null && leaderId.equals(primaryHost) && replicationManager != null) {
            return; // same leader, already replicating — nothing to do
        }
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

    /** Enable --auth style enforcement: connections must complete a SCRAM exchange first. */
    public void setAuthRequired(boolean authRequired) {
        this.authRequired = authRequired;
    }

    private void ensureRootUser() {
        try {
            var cmd = new de.caluga.morphium.driver.commands.auth.CreateUserAdminCommand(null)
                .setUserName(rootUser)
                .setPwd(rootPassword);
            cmd.setDb("admin");
            var result = driver.readSingleAnswer(driver.runCommand(cmd));

            if (result != null && Double.valueOf(1.0).equals(result.get("ok"))) {
                log.info("created initial admin user '{}'", rootUser);
            } else if (result != null && Integer.valueOf(51003).equals(result.get("code"))) {
                log.debug("initial admin user '{}' already exists", rootUser);
            } else {
                log.error("could not create initial admin user '{}': {}", rootUser,
                    result == null ? "no result" : result.get("errmsg"));
            }
        } catch (Exception e) {
            log.error("could not create initial admin user '{}'", rootUser, e);
        }
    }

    /**
     * Credentials for an initial admin user, created at startup if absent. Without this,
     * an --auth server would be unreachable (there is no localhost exception).
     */
    public void setRootUser(String user, String password) {
        this.rootUser = user;
        this.rootPassword = password;
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
            Thread t = new Thread(r, "PoppyDB-DumpScheduler");
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

        // Wait for election to complete (either we become leader or discover one)
        // This prevents clients from connecting before primary is known
        waitForElectionResult();
    }

    /**
     * Wait for election to produce a result (either this node becomes leader or a leader is discovered).
     * Times out after 10 seconds to prevent deadlock in case of network issues.
     */
    private void waitForElectionResult() {
        long timeout = 10000; // 10 seconds
        long start = System.currentTimeMillis();
        long pollInterval = 50; // Check every 50ms

        log.info("Waiting for election to complete (timeout: {}ms)...", timeout);

        while (primaryHost == null && running) {
            if (System.currentTimeMillis() - start > timeout) {
                log.warn("Election did not complete within {}ms, proceeding anyway. " +
                        "Clients may see 'no primary' errors initially.", timeout);
                break;
            }

            try {
                Thread.sleep(pollInterval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for election result");
                break;
            }

            // Check if we became leader or found one
            if (electionManager.isLeader()) {
                primary = true;
                primaryHost = host + ":" + port;
                log.info("Election complete: this node is the leader");
                break;
            } else if (electionManager.getCurrentLeader() != null) {
                primaryHost = electionManager.getCurrentLeader();
                log.info("Election complete: leader is {}", primaryHost);
                break;
            }
        }

        if (primaryHost != null) {
            log.info("Election completed, primary is: {}", primaryHost);
        }
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

    /**
     * True when this node is a secondary that is currently (re-)running its initial sync and may
     * therefore hold a half-cleared local database. Resolved live per command by the command
     * handler so it can reject data-plane traffic (RECOVERING) while syncing. A primary has no
     * replication manager, so this is false there.
     */
    private boolean isSecondarySyncing() {
        ReplicationManager rm = replicationManager;
        return rm != null && rm.isSyncing();
    }

    /** Read-only persistence info backing the dumpStatus admin command. */
    public Map<String, Object> getDumpStatus() {
        if (dumpDirectory == null) {
            return de.caluga.morphium.driver.Doc.of("enabled", (Object) false);
        }

        return de.caluga.morphium.driver.Doc.of("enabled", true,
                "dir", dumpDirectory.getAbsolutePath(),
                "intervalMs", dumpIntervalMs,
                "schedulerRunning", dumpScheduler != null && !dumpScheduler.isShutdown(),
                "lastDumpMs", lastDumpTime);
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

    /**
     * Test/monitoring hook: number of currently open server-side find cursors on THIS instance
     * only. Delegates to this instance's own {@link FindCursorRegistry} — see that class'
     * javadoc for why this must never be a JVM-static count shared across PoppyDB instances.
     */
    public int openFindCursors() {
        return findCursorRegistry.openFindCursors();
    }

    /**
     * Test/monitoring hook: number of documents currently retained in memory for a server-side
     * find cursor's bounded window on THIS instance. Returns -1 if no such cursor is open.
     */
    public int retainedFindCursorDocs(long cursorId) {
        return findCursorRegistry.retainedFindCursorDocs(cursorId);
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
        // Query-planning + slow-query counters (Phase C, Task 6) - fullScans/indexHits/indexSorts
        // live on the driver (Phase B1/B2), slowQueries/slowQueriesCollScan/slowQueriesIxscan and
        // the configured threshold were added alongside explain() in this task; nested under
        // "query" rather than flattened so a future driver stats key can't silently collide with
        // one of the top-level keys already used above.
        stats.put("query", driver.getStats());
        if (replicationManager != null) {
            stats.put("replication", replicationManager.getStats());
        }
        ReplicationCoordinator coordinator = replicationCoordinatorRef.get();
        if (coordinator != null) {
            stats.put("replicationCoordinator", coordinator.getStats());
        }
        if (electionManager != null) {
            stats.put("election", electionManager.getStats());
        }
        return stats;
    }

    public ReplicationCoordinator getReplicationCoordinator() {
        return replicationCoordinatorRef.get();
    }

    /** Test hook: the secondary-side replication manager (null on a node that is currently primary). */
    ReplicationManager getReplicationManagerForTest() {
        return replicationManager;
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
