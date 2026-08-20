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

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpCompressed;
import de.caluga.poppydb.config.ConfigException;
import de.caluga.poppydb.config.UserSpec;
import de.caluga.poppydb.config.UsersFileSpec;
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
    // Epoch guard for onLeadershipChange (Finding A hardening): ElectionManager dispatches
    // onLeadershipChange from a 3-thread pool, serialized on the PoppyDB monitor but NOT
    // ordered - two rapid false->true (or true->false) dispatches can acquire the monitor in
    // the wrong order, so a stale body would run its full leader/follower bookkeeping AFTER a
    // newer one already did, undoing it (e.g. clearing replicationCoordinatorRef right after a
    // fresh leader body just set it up). incrementAndGet() is atomic, so whichever wrapper
    // invocation increments LAST always holds the current value at the moment it reaches the
    // synchronized body - by definition nothing newer can exist to make it stale later, so the
    // most recent transition is never starved by this guard, only strictly older ones are.
    private final java.util.concurrent.atomic.AtomicLong leadershipEpoch = new java.util.concurrent.atomic.AtomicLong(0);
    // Guards the epoch-increment + primary-flip pair in applyLeadershipFlip (and the startup
    // poll's mirror write in waitForElectionResult). Without it, the two operations are
    // individually atomic but not jointly: a stale onLeadershipChange dispatch could increment
    // first, get preempted, and write its outdated primary value AFTER a newer transition
    // already wrote the current one - leaving e.g. a demoted leader with primary==true forever,
    // which no-ops startReplicationToLeader/probeReplicationLiveness/retryReplicationStart and
    // silently stops replication. Deliberately NOT the PoppyDB monitor: the flip must stay
    // cheap and must keep happening before the transition body competes for the monitor (see
    // onLeadershipChange).
    private final Object leadershipFlagLock = new Object();

    // Election configuration
    private boolean electionEnabled = false;
    private ElectionConfig electionConfig = null;
    private ElectionManager electionManager = null;
    // Set by the startup restore: false means this node came up with only part of its data
    // (#306 review, P1-2). Kept as a field because the restore runs before the election manager
    // exists, and applied to it as soon as it does.
    private volatile boolean localDataComplete = true;
    private ElectionNetworkClient electionNetworkClient = null;

    // SSL configuration
    private boolean sslEnabled = false;
    private SSLContext sslContext = null;
    // Trust anchor for the RS-internal channel (ElectionNetworkClient/ReplicationManager) when
    // --ssl is on: pinned to this server's own configured certificate, built by PoppyDBCLI via
    // SslHelper.createClientSslContext(sslKeystore, sslKeystorePassword) - see
    // docs/superpowers/specs/2026-08-05-poppydb-rs-internal-auth-tls-design.md. Null means the
    // internal channel stays plaintext even if sslEnabled is true (e.g. the ephemeral
    // self-signed-cert dev fallback, where every node's cert differs and pinning is impossible).
    private SSLContext internalSslContext = null;
    private boolean authRequired = false;
    private String rootUser = null;
    private String rootPassword = null;
    // Users-file bootstrap (--users-file): parsed spec handed in by PoppyDBCLI before start(),
    // applied wherever ensureRootUser runs - non-election start() / leadership hook. volatile:
    // set from the startup thread, read from the election callback threads.
    private volatile UsersFileSpec bootstrapUsers = null;
    /** _id of the version-gate meta document in admin.system.version. */
    static final String USERS_FILE_META_ID = "poppydb.usersFile";

    // Persistence configuration
    private File dumpDirectory = null;
    private long dumpIntervalMs = 0;
    private java.util.concurrent.ScheduledExecutorService dumpScheduler = null;
    private volatile long lastDumpTime = 0;
    // ONE guard for every dump path - the periodic scheduler, the on-demand dumpNow command and
    // the final dump on shutdown (#317). Two dumps running at once would write the same
    // <db>.morphium.gz.tmp files concurrently and rename the interleaved result into place, so a
    // dump never waits for a running one: it is skipped (scheduler, manual) or, at shutdown,
    // waited for with a bound. A Semaphore instead of an AtomicBoolean only for that timed
    // acquire; permits are never added, only released by the holder.
    private final java.util.concurrent.Semaphore dumpGuard = new java.util.concurrent.Semaphore(1);
    // The thread of a running on-demand dump, so shutdown can interrupt one that outstays the
    // wait below instead of leaving it to write into a driver that forceShutdown() has already
    // reset - that would rename an EMPTY but perfectly valid dump over the last good one.
    private volatile Thread dumpThread = null;
    // Set at the start of shutdown(): no new dump may be started from here on, however it is
    // triggered - the driver is about to be reset.
    private volatile boolean shuttingDown = false;
    /** How long shutdown waits for a running dump before giving up on the final dump. */
    long finalDumpWaitMs = 10_000;

    // Replication
    // volatile: mutated under synchronized on the election/leadership paths but read unsynchronized
    // from Netty event-loop threads via the isSecondarySyncing() supplier passed to each handler.
    private volatile ReplicationManager replicationManager = null;
    // Durable carry-over watermark for ReplicationManager#carryOverLastAppliedSequence (2026-08-14
    // review hardening). startReplicationToLeader() reads the predecessor RM's lastAppliedSequence
    // into a purely LOCAL variable before building the replacement - which dies with the attempt
    // if newReplicationManager.start() then throws (real, if narrow: an auth/TLS connect failure).
    // replicationManager stays null in that case, so the retry chain's NEXT
    // startReplicationToLeader() call would otherwise read 0 again, silently making the
    // destructive-resync guard vacuous on the retry. This field persists that value across such
    // retries independent of whether any particular attempt ever successfully starts - updated
    // every time startReplicationToLeader() reads (and stops) a predecessor, so it always reflects
    // the most recently known-good position. volatile: written only under the class monitor
    // (synchronized methods), but read here defensively for the same reason replicationManager is.
    private volatile long lastKnownAppliedSequence = 0;
    // Companion to lastKnownAppliedSequence above (2026-08-14 production-CI fix, I-2): the
    // "host:port" the watermark sequence was actually earned against - see
    // ReplicationManager#carryOverLastAppliedSequence(long, String)'s javadoc for why comparing
    // sequences across a genuine leader change is unsound (production incident: 82 refusal loops
    // over 40+ minutes). Always updated TOGETHER with lastKnownAppliedSequence, from the same
    // predecessor read, so the pair is never inconsistent with each other. null means "no
    // predecessor ever recorded" (cold boot) - carryOverSourceFor()'s null result correctly never
    // matches any real ReplicationManager#getLeaderAddress().
    private volatile String lastKnownAppliedSequenceSource = null;
    // Held behind an AtomicReference (rather than a plain volatile field copied into each
    // connection at accept time) so every MongoCommandHandler resolves the coordinator live
    // via a Supplier - onLeadershipChange swaps this reference and every existing connection
    // (not just newly accepted ones) immediately observes the new value.
    private final AtomicReference<ReplicationCoordinator> replicationCoordinatorRef = new AtomicReference<>();

    // Bounded-backoff retry for a failed startReplicationToLeader() attempt (Finding B
    // hardening) - see scheduleReplicationRetry/retryReplicationStart. Single daemon thread,
    // created lazily (most PoppyDB instances - standalone, static-mode RS, most tests - never
    // fail a replication start and shouldn't pay for a background thread they never use), and
    // shut down in shutdown(). All reads/writes go through synchronized accessors below so the
    // monitor provides the visibility a plain field wouldn't.
    private java.util.concurrent.ScheduledExecutorService replicationRetryScheduler;
    static final long REPLICATION_RETRY_INITIAL_DELAY_MS = 1000;
    static final long REPLICATION_RETRY_MAX_DELAY_MS = 30_000;

    // Post-start liveness probe (closes the gap the exception-based retry above cannot cover):
    // for an unreachable leader, PooledDriver.connect() swallows a single-host connect failure
    // internally and Morphium's constructor returns normally, so ReplicationManager.start() does
    // NOT throw - the RM gets assigned as if live and handleReplicationStartFailure never runs.
    // Every successful start() schedules exactly one of these, ~45s out (comfortably longer than
    // watchLive normally takes to go true on a healthy connection - see ReplicationManager's
    // watch-first design). If by then the SAME ReplicationManager instance is still assigned, this
    // node is still a non-primary follower, and its watch never registered, the connection never
    // actually came up - tear it down and feed the existing backoff retry chain. See
    // probeReplicationLiveness/scheduleReplicationLivenessProbe below.
    static final long REPLICATION_LIVENESS_PROBE_DELAY_MS = 45_000;

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
        // full re-sync. Bounds: 100_000 events AND a byte budget (ring buffer, oldest evicted on
        // overflow of either). The count limit alone does not bound memory - every buffered event
        // retains its full document, so 100k bulk-write events pinned ~4GB on the ACC message bus
        // (incident 2026-08-14, spec 2026-08-14-replay-buffer-byte-budget.md). Trade-off: heavy bulk
        // writes shrink the resume window in wall-clock time, making a secondary re-sync more likely -
        // deliberate (availability over resumability).
        driver.setChangeStreamHistoryLimit(REPLICATION_REPLAY_BUFFER_EVENTS);
        driver.setChangeStreamHistoryByteBudget(REPLICATION_REPLAY_BUFFER_BYTES);
    }

    /** Primary replay-buffer bound (events) backing replication resume-after-disconnect. */
    static final int REPLICATION_REPLAY_BUFFER_EVENTS = 100_000;

    /** Default replay-buffer byte budget (estimated bytes) - overridable via --replay-buffer. */
    static final long REPLICATION_REPLAY_BUFFER_BYTES = 256L * 1024 * 1024;

    /** Default event-queue byte budget (estimated bytes) - overridable via --event-queue-budget. */
    static final long REPLICATION_EVENT_QUEUE_BYTES = 256L * 1024 * 1024;

    // Effective event-queue byte budget, applied to every ReplicationManager this node creates
    // when it (re-)becomes a secondary - the RM is replaced on every leader change, so the value
    // must survive here rather than on any single RM instance.
    private volatile long eventQueueByteBudget = REPLICATION_EVENT_QUEUE_BYTES;

    /**
     * Byte budget for a secondary's replication event queue (estimated bytes, 0 = off) - see
     * ReplicationManager.setEventQueueByteBudget. Unlike the replay buffer's budget this never
     * discards events (they are not applied yet - dropping one would be silent data loss);
     * instead the change-stream reader blocks until the apply side frees budget, exactly like
     * the queue's count capacity. Applied to the current ReplicationManager (if any) and to
     * every one created later.
     */
    public void setEventQueueByteBudget(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("eventQueueByteBudget must be >= 0 (0 = disabled)");
        }

        this.eventQueueByteBudget = bytes;
        ReplicationManager rm = replicationManager;

        if (rm != null) {
            rm.setEventQueueByteBudget(bytes);
        }
    }

    /**
     * Per-cursor byte budget for a watch cursor's buffered, undelivered events (estimated
     * bytes, 0 = off) - see WatchCursorManager.setCursorQueueByteBudget (#321). Overflow kills
     * the cursor, the same policy as the count cap: blocking would stall the writer thread
     * that delivers events in server mode, dropping would silently lose events.
     */
    public void setCursorQueueByteBudget(long bytes) {
        cursorManager.setCursorQueueByteBudget(bytes);
    }

    /** Test hook: the server's cursor manager, e.g. to observe cursor kills (#322 test). */
    de.caluga.poppydb.netty.WatchCursorManager getCursorManagerForTest() {
        return cursorManager;
    }

    /**
     * Replay-buffer byte budget (estimated bytes, 0 = off) - see
     * InMemoryDriver.setChangeStreamHistoryByteBudget. Evicting for bytes has the same
     * window-lost semantics as count overflow: an affected secondary re-syncs.
     */
    public void setReplayBufferByteBudget(long bytes) {
        driver.setChangeStreamHistoryByteBudget(bytes);
    }

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
                         .setDumpNowAction(dumpDirectory == null ? null : PoppyDB.this::triggerDumpNow)
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
            if (!electionEnabled) {
                ensureRootUser();
            } else if (authRequired) {
                // Election-mode bootstrap gap: with --auth on, ElectionNetworkClient's peer RPCs
                // (requestVote/appendEntries) must SCRAM-authenticate before a leader even exists
                // (see startElection() below) - deferring root-user creation to the leadership
                // callback, as secondaries otherwise do, would deadlock the whole RS (no leader
                // can be elected until credentials exist, and credentials are only created by
                // the leader). So every node seeds its own local copy up front.
                // NOTE: "superseded by replication" now actually happens end-to-end: since
                // ReplicationManager wires auth/TLS onto its connection to the primary (see
                // setInternalConnectionSecurity()), a secondary's replication connection to an
                // auth-required primary completes, so the drop+repopulate of admin.system.users
                // this comment describes runs once that secondary syncs - each node's bootstrap
                // copy gets replaced by the primary's authoritative copy. It was always safe even
                // before that wiring landed: every node is given the same rootUser/rootPassword
                // (the "identical config on every node" deployment pattern), so each node's
                // independently-created copy authenticates the same credential regardless of
                // whether/when replication overwrites it.
                ensureRootUser();
            }
            // in election mode without --auth, the leadership callback (below) creates it on the
            // primary only - secondaries receive the user via replication instead of self-creating
            // it, so a resync (which clears and repopulates admin.system.users from the primary)
            // never wipes out a secondary's own root account
        } else if (authRequired) {
            // users may still exist from a restored dump - but a fresh --auth server without
            // any user would be permanently unreachable (no localhost exception)
            log.warn("auth enforcement is enabled but no root user is configured - "
                + "clients can only authenticate if admin.system.users already contains users");
        }

        // Users-file bootstrap (non-election): apply now, while startup can still fail fast -
        // a fatal apply error must abort the start (PoppyDBCLI's startup catch turns it into
        // exit 1). Only the PRIMARY may write users: in election mode the leadership hook
        // applies instead, and a static-mode secondary skips - it receives the result via
        // replication from the static primary.
        if (bootstrapUsers != null && !electionEnabled) {
            if (primary) {
                try {
                    applyBootstrapUsers();
                } catch (ConfigException e) {
                    log.error("users file bootstrap apply failed - aborting startup: {}", e.getMessage());
                    throw e;
                }
            } else {
                log.info("users-file is configured on this node but it is a static-mode secondary "
                    + "(primaryHost={}) - the file is ignored here; it applies only on the primary and "
                    + "this node receives the result via replication", primaryHost);
            }
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
        shuttingDown = true;

        // Stop election system
        stopElection();

        // Stop replication
        stopReplication();

        // Stop the replication-start retry scheduler (Finding B hardening) - must happen on
        // every shutdown so its daemon thread doesn't leak across test instances in the same JVM.
        stopReplicationRetryScheduler();

        // Stop dump scheduler
        stopDumpScheduler();

        // Final dump. The scheduler is already stopped, but an on-demand dumpNow may still be
        // running - wait a bounded time for the guard instead of writing the same files
        // concurrently (#317). If it does not come free, the in-flight dump is writing a state
        // that is seconds old at most, so skipping is the better trade than a corrupted dump.
        if (dumpDirectory != null) {
            boolean acquired = false;

            try {
                acquired = dumpGuard.tryAcquire(finalDumpWaitMs, TimeUnit.MILLISECONDS);

                if (!acquired) {
                    // Abandoning it is not an option: right after this the driver is reset, and a
                    // dump still snapshotting would then write EMPTY databases over the last good
                    // dump files - valid gzip, no data. Interrupting aborts the write inside the
                    // temp file (channel-backed streams close on interrupt), so the previous dump
                    // survives untouched.
                    log.warn("Final dump skipped: another dump is still running after {}ms - "
                             + "interrupting it so it cannot write into the shutdown driver",
                             finalDumpWaitMs);
                    Thread straggler = dumpThread;

                    if (straggler != null) {
                        straggler.interrupt();

                        try {
                            straggler.join(2000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                } else {
                    log.info("Performing final dump before shutdown...");
                    int count = writeDumpFiles();
                    lastDumpTime = System.currentTimeMillis();
                    log.info("Final dump completed: {} databases saved", count);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for the running dump - skipping the final dump");
            } catch (Exception e) {
                log.error("Failed to perform final dump: {}", e.getMessage(), e);
            } finally {
                if (acquired) {
                    dumpGuard.release();
                }
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

            // Raft requires currentTerm/votedFor to survive a restart - a node coming back at
            // term 0 is what turned the ACC rolling upgrade into term churn (#306). A server
            // that persists its data at all should persist this too, so default the state file
            // next to the dump directory unless an operator configured a path explicitly.
            if (electionStatePath != null && !electionStatePath.isBlank()) {
                // An operator decision beats the derivation below - and it is the only way to
                // get persistence at all on a server that keeps no dumps (#316).
                electionConfig.setStatePersistencePath(new File(electionStatePath).getAbsolutePath());
                electionConfig.setPersistState(true);
                log.info("Election state persisted to {}", electionConfig.getStatePersistencePath());
            } else if (dumpDirectory != null && electionConfig.getStatePersistencePath() == null) {
                electionConfig.setStatePersistencePath(
                        new File(dumpDirectory, "election-state.properties").getAbsolutePath());
                electionConfig.setPersistState(true);
                log.info("Election state persisted to {}", electionConfig.getStatePersistencePath());
            } else if (electionConfig.getStatePersistencePath() == null) {
                // Said out loud, because the alternative is a MISSING info line - which looks
                // like one line less of logging, not like a missing guarantee (#316). Raft
                // requires currentTerm/votedFor to be durable: without them a restarted node
                // returns at term 0 and can grant a SECOND vote in a term it already voted in,
                // so two leaders in one term are no longer excluded.
                log.warn("Election is enabled but election state is NOT persisted: no dump directory and no "
                        + "election-state-path configured. currentTerm/votedFor are lost on restart, which "
                        + "allows a node to vote twice in the same term. Set election-state-path (or a dump "
                        + "directory) to fix this.");
            }

            // Create election manager with priority-aware config
            electionManager = new ElectionManager(myAddress, hosts, electionConfig);

            // Set up leadership change callback
            // A node that restored only part of its data must not win an election: restoring
            // does not advance the change-stream sequence, so after a cluster-wide restart every
            // node reports index 0 and the index-based restraint cannot separate an intact node
            // from a gutted one. It would then overwrite the intact peers via their initial sync.
            electionManager.setDataComplete(localDataComplete);
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
     *
     * The externally visible {@link #primary} flag is flipped here, immediately and outside
     * any monitor: clients polling {@code isPrimary()}/hello, and tests asserting on step-down,
     * must not wait behind the (potentially slow, network-touching) ReplicationManager
     * teardown/replace work in {@link #onLeadershipChangeSynchronized}, which serializes on the
     * PoppyDB monitor behind a possibly in-flight {@link #onLeaderDiscovered}
     * (ReplicationManager.start() does a full Morphium client construction + connect - hundreds
     * of ms to seconds, up to a 10s connect timeout). Before the callbacks were serialized
     * (commit 51f82bda) this flip ran unsynchronized and was near-instant; serializing it
     * re-introduced that latency, which is what this early flip undoes without giving up the
     * synchronization the ReplicationManager bookkeeping still needs.
     *
     * Interaction with the guards below, reasoned through explicitly because the flip now runs
     * strictly before the monitor instead of as its first statement:
     * - A stale queued onLeaderDiscovered that runs after this node already became leader: it
     *   now reliably observes primary==true (the flip always happens before
     *   onLeadershipChangeSynchronized(true) is even entered, let alone before any later,
     *   independently scheduled onLeaderDiscovered call), so startReplicationToLeader's
     *   `if (primary || leaderId == null) return;` guard no-ops it. That's strictly better than
     *   before: a leader must never run a ReplicationManager against another node.
     * - A stale queued onLeadershipChange(true) that flips primary=true early while a discovery
     *   is already mid-startReplicationToLeader (having passed the guard just before the flip):
     *   the discovery still finishes constructing/starting its ReplicationManager outside any
     *   monitor, but onLeadershipChangeSynchronized(true) can only acquire the monitor after
     *   that in-flight call releases it, and its leader-body unconditionally stops+nulls
     *   whatever replicationManager it finds - so the stray ReplicationManager the discovery
     *   just started gets torn down right after, not left running against a leader.
     */
    private void onLeadershipChange(boolean isLeader) {
        log.info("Leadership change: {} is now {}", host + ":" + port, isLeader ? "LEADER" : "FOLLOWER");
        // Epoch bump and flag flip happen as ONE atomic unit (applyLeadershipFlip), still
        // before the monitor - see the class-level reasoning in the javadoc above for why the
        // early flip is safe on its own and how it interacts with the synchronized
        // ReplicationManager bookkeeping below.
        long epoch = applyLeadershipFlip(isLeader);
        onLeadershipChangeSynchronized(isLeader, epoch);
    }

    /**
     * Atomically advances {@link #leadershipEpoch} and flips the externally visible
     * {@code primary} flag under {@link #leadershipFlagLock}. Because increment and flip are
     * one critical section, whichever transition runs later in lock order holds the higher
     * epoch AND writes the flag last - a stale dispatch can never overwrite a newer
     * transition's flag (the bug class this replaces: increment-then-unsynchronized-write let
     * a preempted stale true-dispatch re-assert primary==true after a newer false-dispatch,
     * permanently muting startReplicationToLeader and the replication retry/liveness chain on
     * a demoted leader). Package-private so LeadershipEpochGuardTest can hammer it
     * concurrently and assert flag-follows-max-epoch.
     *
     * @return the epoch this transition owns, to be passed to
     *         {@link #onLeadershipChangeSynchronized}
     */
    long applyLeadershipFlip(boolean isLeader) {
        synchronized (leadershipFlagLock) {
            long epoch = leadershipEpoch.incrementAndGet();
            primary = isLeader;
            return epoch;
        }
    }

    /**
     * Synchronized: ElectionManager dispatches both this and onLeaderDiscovered from a
     * multi-thread scheduler pool, so they must serialize on the PoppyDB monitor to avoid
     * interleaving their ReplicationManager teardown/replace logic. The externally visible
     * {@code primary} flag itself is no longer flipped here - see {@link #onLeadershipChange}.
     *
     * {@code epoch} is the value {@link #leadershipEpoch} held right after this transition's
     * wrapper incremented it, captured before this method ever competes for the monitor. If, by
     * the time this body finally acquires the monitor, a NEWER transition's wrapper has already
     * incremented {@link #leadershipEpoch} past {@code epoch}, then that newer body either already
     * ran or is about to - either way it reflects the current {@link #primary}/state truth, and
     * this stale body running its bookkeeping on top would only undo that (Finding A: e.g.
     * clearing {@link #replicationCoordinatorRef} right after a fresh leader body just populated
     * it). No-op in that case for BOTH directions (a stale true-body and a stale false-body are
     * equally capable of clobbering a newer body's work). Package-private (not private) so a
     * test can drive it directly with a deliberately stale epoch.
     */
    synchronized void onLeadershipChangeSynchronized(boolean isLeader, long epoch) {
        if (epoch != leadershipEpoch.get()) {
            log.debug("Ignoring stale leadership transition (epoch {} superseded by {})",
                    epoch, leadershipEpoch.get());
            return;
        }

        if (isLeader) {
            // Becoming leader
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

            // Now that this node has fully assumed primary duties (primary flag flipped,
            // replication coordinator in place, no longer replicating from a stale leader),
            // (re-)create the root user. Idempotent (handles 51003 already-exists) so repeated
            // leadership changes (flapping, priority takeover) never race or double-create -
            // secondaries never self-create root, they only ever receive it via replication.
            if (rootUser != null && rootPassword != null) {
                ensureRootUser();
            }

            // Users-file bootstrap: (re-)apply after primary duties are fully assumed, right
            // after ensureRootUser. Idempotent upsert - repeated leadership changes (flapping,
            // priority takeover) re-run it harmlessly, and the version gate short-circuits
            // when nothing changed. A failure here can only be logged: a running server
            // cannot abort mid-failover.
            if (bootstrapUsers != null) {
                try {
                    applyBootstrapUsers();
                } catch (Exception e) {
                    log.error("users file bootstrap apply failed on the new primary - server keeps running: {}",
                        e.getMessage());
                }
            }
        } else {
            // Stepping down from leader (primary flag already flipped to false by the caller)

            // Clean up replication coordinator
            replicationCoordinatorRef.set(null);

            // Start replication from new primary (will be set by onLeaderDiscovered) - unless
            // ElectionManager already knows one. ElectionManager dispatches this callback and
            // onLeaderDiscovered onto its own multi-thread pool independently: if the discovery
            // task happened to acquire this monitor first (before the `primary = false` flip
            // above ran), it saw primary still true and no-op'd. Since ElectionManager only
            // re-fires onLeaderDiscovered on an actual leader CHANGE, that no-op is permanent -
            // it will never fire again for the same leader, and this node would be stuck not
            // replicating until some unrelated later leader change happened to bail it out.
            // Query the now-current leader here so a raced-out discovery still gets picked up.
            if (electionManager != null) {
                String knownLeader = electionManager.getCurrentLeader();
                if (knownLeader != null && !knownLeader.equals(host + ":" + port)) {
                    startReplicationToLeader(knownLeader);
                }
            }
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
        startReplicationToLeader(leaderId);
    }

    /**
     * (Re-)point replication at {@code leaderId}, tearing down any existing ReplicationManager
     * first. Called from both {@link #onLeaderDiscovered} (follower learns of a new leader via
     * heartbeat - the common case) and {@link #onLeadershipChange} stepping-down-from-leader path
     * (this node just got demoted and needs to catch up on a leader that discovery already raced
     * past - see the comment there). Idempotent for repeated calls: a leaderId equal to the one
     * already being replicated from, a null/malformed id, or a call while this node is itself
     * primary, are all safe no-ops - so it tolerates being invoked redundantly from both paths for
     * the same leader without re-tearing-down a healthy ReplicationManager.
     */
    private synchronized void startReplicationToLeader(String leaderId) {
        startReplicationToLeader(leaderId, REPLICATION_RETRY_INITIAL_DELAY_MS);
    }

    /**
     * Same as {@link #startReplicationToLeader(String)}, but carries the delay to use for the
     * NEXT retry if this attempt also fails (Finding B hardening) - see
     * {@link #scheduleReplicationRetry(long)}/{@link #retryReplicationStart(long)}. The public
     * single-arg entry point always starts a fresh backoff at
     * {@link #REPLICATION_RETRY_INITIAL_DELAY_MS}; only the retry chain itself threads a doubled
     * delay back in here.
     */
    private synchronized void startReplicationToLeader(String leaderId, long delayOnFailureMs) {
        if (!running) {
            // Shutdown already ran (it flips running=false before stopReplication()): a late
            // election/discovery callback must not install and start a fresh ReplicationManager
            // that nothing will ever stop - its daemon threads would hammer the force-shutdown
            // driver for the rest of the JVM (2026-08-06 review finding). stopReplication() is
            // synchronized on the same monitor, so the only two orders are: it tears down what
            // we installed (we held the monitor first), or we no-op here (it ran first).
            return;
        }
        if (primary || leaderId == null) {
            return;
        }
        if (leaderId.equals(primaryHost) && replicationManager != null) {
            return; // same leader, already replicating — nothing to do
        }

        // A ReplicationManager's target host/port is fixed at construction time and never
        // updated, so after a failover the old instance (still replicating from the now-dead
        // former leader) would otherwise retry that dead address forever - it must be torn
        // down and replaced with one pointed at the new leader, not left in place just because
        // it happens to be non-null. A malformed leaderId (practically unreachable) must not
        // tear down a perfectly working existing ReplicationManager, so teardown only happens
        // once we know we have a usable host:port to replace it with.
        String[] parts = leaderId.split(":");
        if (parts.length != 2) {
            return;
        }

        // Carries the predecessor RM's real position into the replacement (2026-08-14
        // empty-node-wipe fix): a fresh ReplicationManager's own lastAppliedSequence starts at 0,
        // and its first watch registration would otherwise unconditionally seed it from whatever
        // the NEW leader reports (recordPrimarySequenceAtRegistration's compareAndSet(0,
        // primarySeq)) - making the destructive-resync guard in startInitialSyncOnce() vacuously
        // pass every time on this path (see ReplicationManager#carryOverLastAppliedSequence's
        // javadoc for the full "why"). Carrying it forward is what lets that guard also protect a
        // leader change, not just a same-address reconnect - defense-in-depth alongside the
        // election-layer empty-vs-data invariant (Tasks 1/2/4).
        ReplicationManager oldReplicationManager = replicationManager;
        if (oldReplicationManager != null) {
            oldReplicationManager.stop();
            replicationManager = null;
        }
        // carryOverSequenceFor()/carryOverSourceFor() are called AFTER stop() returns, not before
        // (2026-08-14 review hardening - TOCTOU): stop() flushes the batch processor's last
        // drained batch before returning, which can still advance lastAppliedSequence past
        // whatever a pre-stop snapshot would have captured. The reference is still valid here -
        // stop() does not invalidate it, only the `replicationManager` field assignment above
        // does - so this reads the predecessor's definitive final position instead of a
        // possibly-stale one.
        long carriedLastAppliedSequence = carryOverSequenceFor(oldReplicationManager);
        // Paired with the sequence above (2026-08-14 production-CI fix, I-2) - see
        // ReplicationManager#carryOverLastAppliedSequence(long, String)'s javadoc: the sequence
        // alone is meaningless without knowing WHICH primary it was earned against, since a
        // leader change is the normal case that makes two RMs' sequence spaces incomparable.
        String carriedSource = carryOverSourceFor(oldReplicationManager);

        // Persist for a possible failed-start retry of THIS attempt (see lastKnownAppliedSequence's
        // javadoc): must happen regardless of whether newReplicationManager.start() below ever
        // succeeds - if it throws, replicationManager stays null and only these two fields (not
        // the oldReplicationManager local, which dies with this method invocation) survive into
        // the next startReplicationToLeader() call the retry chain makes. Always updated together.
        lastKnownAppliedSequence = carriedLastAppliedSequence;
        lastKnownAppliedSequenceSource = carriedSource;

        String leaderHost = parts[0];
        int leaderPort = Integer.parseInt(parts[1]);

        // Start replication from new leader
        ReplicationManager newReplicationManager = new ReplicationManager(driver, leaderHost, leaderPort);
        newReplicationManager.setEventQueueByteBudget(eventQueueByteBudget);
        newReplicationManager.carryOverLastAppliedSequence(carriedLastAppliedSequence, carriedSource);
        newReplicationManager.setInternalConnectionSecurity(
                authRequired, rootUser, rootPassword, sslEnabled ? internalSslContext : null);
        newReplicationManager.setMyAddress(host + ":" + port);
        // Follower-side half of the election log-recency feed (see ElectionManager#updateLogIndex's
        // javadoc): keep our applied replication sequence flowing into ElectionManager so the
        // vote-deny check has real data to compare against instead of a vacuous 0.
        if (electionManager != null) {
            newReplicationManager.setOnLogIndexUpdate((index, term) ->
                    electionManager.updateLogIndex(index, electionManager.getCurrentTerm()));
        }
        // Release of the partial-restore candidacy guard (#306 P1-2 follow-up): the guard used
        // to be lifted only in static-mode startReplication() (after its synchronous
        // waitForInitialSync) - this path, the one election mode actually takes, never lifted
        // it, so a partially-restored node stayed barred from candidacy FOREVER even after a
        // complete authoritative sync; when the primary later died, the cluster stayed without
        // one. The completion hook fires only once an initial sync has COMPLETED and its
        // buffered backlog has drained (see ReplicationManager#maybeFireSyncCompleteNotify) -
        // never on mere replication start, since a guard released early would be no guard at
        // all. The callback is bound to THIS manager instance so a superseded manager firing
        // late cannot release the guard on the strength of a stale sync.
        newReplicationManager.setOnInitialSyncComplete(
                () -> releaseDataCompleteAfterSync(newReplicationManager));
        // Assigned BEFORE start() (#306 review round 2): the sync-complete notification is
        // one-shot (maybeFireSyncCompleteNotify CASes the flag), and on a fast sync (e.g. the
        // consistency shortcut against loopback) the batch tick can fire it before a
        // post-start() assignment lands - releaseDataCompleteAfterSync then sees
        // source != replicationManager, discards the release, and the flag is already
        // consumed: the guard stays stuck until some unrelated resync. The static-mode path
        // (startReplication) already assigns before start() for the same reason.
        replicationManager = newReplicationManager;
        try {
            newReplicationManager.start();
            primaryHost = leaderId;
            log.info("Started replication from new leader {}", leaderId);
            scheduleReplicationLivenessProbe(leaderId, newReplicationManager);
        } catch (Exception e) {
            // Roll the early assignment back before the shared failure handler runs - a
            // dead-but-non-null manager left in the field would trip the same-leader guard
            // above and block every retry until the next leader change (see
            // handleReplicationStartFailure's comment).
            replicationManager = null;
            handleReplicationStartFailure(leaderId, newReplicationManager, e, delayOnFailureMs);
        }
    }

    /**
     * Schedule a single, one-shot {@link #probeReplicationLiveness(String, ReplicationManager)}
     * check ~{@link #REPLICATION_LIVENESS_PROBE_DELAY_MS} after a successful
     * {@code newReplicationManager.start()} - see the field comment on
     * {@link #REPLICATION_LIVENESS_PROBE_DELAY_MS} for why this exists alongside the
     * exception-based {@link #handleReplicationStartFailure} retry. Reuses the same lazily-created
     * scheduler as the retry chain (no second background thread).
     */
    private synchronized void scheduleReplicationLivenessProbe(String leaderId, ReplicationManager probedManager) {
        if (!running) {
            return; // shutting down (or never started) - nothing to probe for
        }

        try {
            replicationRetryScheduler().schedule(() -> probeReplicationLiveness(leaderId, probedManager),
                    REPLICATION_LIVENESS_PROBE_DELAY_MS, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Lost a race with shutdown() shutting the executor down - nothing left to probe for.
            log.debug("Replication liveness probe not scheduled - shutting down");
        }
    }

    /**
     * Fires ~{@link #REPLICATION_LIVENESS_PROBE_DELAY_MS} after a replication start that returned
     * normally (no exception). Distinguishes a healthy-but-silent connection from one that never
     * actually came up (see the {@link #REPLICATION_LIVENESS_PROBE_DELAY_MS} field comment): the
     * dominant real-world case is an unreachable leader, where
     * {@code de.caluga.morphium.driver.wire.PooledDriver#connect()} swallows the single-host
     * connect failure and {@code ReplicationManager.start()} returns as if live.
     *
     * <p>All four guards must hold for the probe to act, otherwise it is a no-op:
     * <ul>
     *   <li>{@code running} - not shutting down;</li>
     *   <li>{@code !primary} - this node hasn't since become leader itself;</li>
     *   <li>{@code replicationManager == probedManager} - the RM this probe was scheduled for is
     *       still the one assigned (not replaced by a newer leadership/discovery transition, and
     *       not already torn down); and</li>
     *   <li>{@code !probedManager.hasWatchEverRegistered()} - the change-stream watch never
     *       registered with the primary at any point since start, which (per
     *       ReplicationManager's watch-first design) is the reliable "never actually connected"
     *       signal. Deliberately NOT the instantaneous {@code isWatchLive()}: that flag drops
     *       between every two watch sessions, so sampling it during a routine reconnect gap
     *       would tear down a connection that did come up.
     * </ul>
     * On all-true, tears the dead RM down, resets {@code primaryHost} (same reasoning as
     * {@link #handleReplicationStartFailure}: a re-discovery of the same leader must not be
     * short-circuited by the same-leader guard), and feeds the existing bounded-backoff retry
     * chain via {@link #scheduleReplicationRetry(long)} - so the retry re-reads the current leader
     * from ElectionManager rather than blindly redialing the address that just proved dead.
     */
    synchronized void probeReplicationLiveness(String leaderId, ReplicationManager probedManager) {
        if (!running || primary) {
            return; // shut down, or promoted to leader meanwhile - nothing to probe
        }
        if (replicationManager != probedManager) {
            return; // superseded by a newer ReplicationManager (or already torn down) - stale probe
        }
        if (probedManager.hasWatchEverRegistered()) {
            return; // healthy: the watch registered (at least once) - the connection came up;
                    // any later watch drop is the watch-retry loop's job, not the probe's
        }

        log.warn("Replication to {} never became live - tearing down and retrying", leaderId);
        replicationManager.stop();
        replicationManager = null;
        primaryHost = null;
        scheduleReplicationRetry(REPLICATION_RETRY_INITIAL_DELAY_MS);
    }

    /**
     * Cleans up after a failed {@code newReplicationManager.start()} and self-schedules a
     * bounded-backoff retry (Finding B hardening). Factored out of
     * {@link #startReplicationToLeader(String, long)}'s catch block, rather than left inline, so
     * a test can drive exactly this logic directly (package-private seam) - forcing the real
     * {@code ReplicationManager.start()} to throw synchronously via network conditions alone
     * isn't reliably possible: for a single-host, non-replica-set-configured target (which is
     * what {@code ReplicationManager} always uses), the underlying Morphium client's
     * {@code PooledDriver.connect()} swallows an unreachable seed internally and defers to its
     * own background heartbeat reconnection instead of throwing, so an RS test that merely
     * leaves the leader's port unbound cannot force this catch block to run - verified
     * empirically (see task-1-report.md).
     */
    synchronized void handleReplicationStartFailure(String leaderId, ReplicationManager failedManager,
                                                      Exception e, long delayOnFailureMs) {
        log.error("Failed to start replication from {}: {}", leaderId, e.getMessage());
        // start() failed partway through; the instance may hold live resources
        // (executors, a connected primaryMorphium) that must be released so we
        // don't leak them. Never assign it to the field: leaving a dead-but-non-null
        // ReplicationManager there would trip the same-leader guard above and block
        // any retry until the next leader change.
        try {
            failedManager.stop();
        } catch (Exception stopException) {
            log.warn("Error stopping failed ReplicationManager for {}: {}", leaderId, stopException.getMessage());
        }
        // Reset primaryHost so a re-discovery of the same leader (e.g. the next
        // heartbeat) doesn't get short-circuited by the same-leader guard and can
        // retry cleanly instead of being stuck until an actual leader change.
        primaryHost = null;
        // Finding B: ElectionManager only re-fires onLeaderDiscovered on an actual leader
        // CHANGE, not on every heartbeat - without this, a follower whose first attempt
        // failed (e.g. the leader's port wasn't listening yet) would stay
        // replication-less forever. Self-schedule a bounded-backoff retry instead.
        scheduleReplicationRetry(delayOnFailureMs);
    }

    /**
     * Schedule a single retry of {@link #retryReplicationStart(long)} after {@code delayMs}
     * (capped at {@link #REPLICATION_RETRY_MAX_DELAY_MS}), unless the node is already shutting
     * down. Lazily creates the retry executor on first use.
     */
    private synchronized void scheduleReplicationRetry(long delayMs) {
        if (!running) {
            return; // shutting down (or never started) - nothing to retry for
        }

        long boundedDelay = Math.min(delayMs, REPLICATION_RETRY_MAX_DELAY_MS);
        log.info("Scheduling replication start retry in {}ms", boundedDelay);

        try {
            replicationRetryScheduler().schedule(() -> retryReplicationStart(boundedDelay),
                    boundedDelay, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Lost a race with shutdown() shutting the executor down between the running-check
            // above and this call - nothing left to retry for either way.
            log.debug("Replication retry not scheduled - shutting down");
        }
    }

    /**
     * Retry body: re-checks every guard {@link #startReplicationToLeader} would anyway (running,
     * not primary, no live ReplicationManager already), then re-reads
     * {@link ElectionManager#getCurrentLeader()} rather than reusing the leader address that
     * failed - the leader may have changed while this retry was backing off (a real failover),
     * and retrying a stale/dead address would just fail again for the wrong reason. On repeated
     * failure, {@link #startReplicationToLeader(String, long)} chains the next retry itself with
     * a doubled delay (capped), via {@link #scheduleReplicationRetry(long)}.
     */
    private synchronized void retryReplicationStart(long lastDelayMs) {
        if (!running || primary || replicationManager != null || electionManager == null) {
            return; // shut down, became primary meanwhile, already replicating, or no election configured
        }

        String leader = electionManager.getCurrentLeader();
        if (leader == null) {
            return; // no leader known at all yet - the eventual onLeaderDiscovered will pick this up
        }

        startReplicationToLeader(leader, Math.min(lastDelayMs * 2, REPLICATION_RETRY_MAX_DELAY_MS));
    }

    /**
     * Lazily creates the single daemon thread backing the replication-start retry chain - see
     * the {@link #replicationRetryScheduler} field comment for why this stays lazy.
     */
    private synchronized java.util.concurrent.ScheduledExecutorService replicationRetryScheduler() {
        if (replicationRetryScheduler == null) {
            replicationRetryScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "PoppyDB-ReplicationRetry-" + host + ":" + port);
                t.setDaemon(true);
                return t;
            });
        }

        return replicationRetryScheduler;
    }

    private void stopReplicationRetryScheduler() {
        // Grab-and-clear under the monitor, but shut down/await OUTSIDE it: retryReplicationStart
        // is itself `synchronized` on this instance, so if we held the monitor across
        // awaitTermination() a currently-queued-or-running retry task could never acquire it to
        // finish - deadlocking this shutdown against its own retry executor.
        java.util.concurrent.ScheduledExecutorService scheduler;
        synchronized (this) {
            scheduler = replicationRetryScheduler;
            replicationRetryScheduler = null;
        }

        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
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
     * Users-file bootstrap spec ({@code --users-file}), handed in by PoppyDBCLI before
     * {@code start()}. Applied by the PRIMARY only, wherever ensureRootUser runs: at
     * {@code start()} for non-election nodes (a fatal apply error aborts startup), in the
     * leadership hook for election-mode primaries (a failure there only logs ERROR).
     */
    public void setBootstrapUsers(UsersFileSpec spec) {
        this.bootstrapUsers = spec;
    }

    /**
     * Applies the users file as an idempotent upsert (users-file task 4, see
     * docs/superpowers/specs/2026-08-04-poppydb-users-file-design.md): per entry
     * {@code createUser} in mongod wire shape; on 51003 already-exists {@code updateUser}
     * (pwd + roles + mechanisms from the file replace the stored state). Version gate: if the
     * file carries a version and the LOCAL meta doc's appliedVersion is already {@code >=} it,
     * the whole apply is skipped (INFO) - that is what makes a straggler node with an old file
     * harmless after a failback. After a fully successful apply of a VERSIONED file the meta
     * doc is upserted through the normal driver write path, so it emits a change event and
     * replicates (admin.system.version is in ReplicationManager.isReplicated's allow-list).
     * Unversioned files always apply and never touch the meta doc.
     *
     * Any entry failure or meta-doc problem surfaces as {@link ConfigException} - the caller
     * decides whether that aborts (non-election startup) or is only logged (leadership hook).
     * No pwd value is ever logged or included in an exception message.
     */
    private void applyBootstrapUsers() {
        UsersFileSpec spec = bootstrapUsers;
        if (spec == null) {
            return;
        }

        if (spec.version() != null) {
            Long applied = readAppliedUsersFileVersion();
            if (applied != null && applied >= spec.version()) {
                log.info("users file version {} already applied (stored appliedVersion {}) - skipping bootstrap apply",
                    spec.version(), applied);
                return;
            }
        }

        List<String> failures = new ArrayList<>();
        for (UserSpec user : spec.users()) {
            String failure = applyBootstrapUser(user);
            if (failure != null) {
                failures.add(failure);
            }
        }
        if (!failures.isEmpty()) {
            throw new ConfigException("users file bootstrap apply failed for " + failures.size() + " of "
                + spec.users().size() + " entries: " + String.join("; ", failures));
        }

        if (spec.version() != null) {
            writeAppliedUsersFileVersion(spec.version());
            log.info("users file applied: {} users upserted, appliedVersion now {}",
                spec.users().size(), spec.version());
        } else {
            log.info("users file applied: {} users upserted (unversioned file - no version gate)",
                spec.users().size());
        }
    }

    /**
     * Upserts ONE users-file entry. Returns a description of the failure, or null on success.
     * The description (and every log line here) names only user and db - never the pwd.
     */
    private String applyBootstrapUser(UserSpec user) {
        String who = user.user() + "@" + user.db();

        try {
            Map<String, Object> create = new LinkedHashMap<>();
            create.put("createUser", user.user());
            create.put("pwd", user.pwd());
            create.put("roles", user.roles()); // mandatory in the wire shape - empty list is fine
            if (!user.mechanisms().isEmpty()) {
                create.put("mechanisms", user.mechanisms());
            }
            Map<String, Object> result = runRawCommand(user.db(), create);

            if (isOk(result)) {
                log.info("users file: created user {}", who);
                return null;
            }
            if (Integer.valueOf(51003).equals(result.get("code"))) {
                Map<String, Object> update = new LinkedHashMap<>();
                update.put("updateUser", user.user());
                update.put("pwd", user.pwd());
                update.put("roles", user.roles());
                if (!user.mechanisms().isEmpty()) {
                    update.put("mechanisms", user.mechanisms());
                }
                Map<String, Object> updResult = runRawCommand(user.db(), update);

                if (isOk(updResult)) {
                    log.info("users file: updated existing user {}", who);
                    return null;
                }
                return who + ": updateUser failed: " + errmsgOf(updResult);
            }
            return who + ": createUser failed: " + errmsgOf(result);
        } catch (Exception e) {
            // e.getMessage() is null for plenty of exceptions (e.g. NPE) - that used to render as
            // "who: null" here, an unusable diagnostic. e.toString() always carries at least the
            // exception's class name. The full throwable (with stack trace) goes to the log so
            // the real cause is not lost even though only the short form goes into the collected
            // failure string.
            log.error("users file: apply failed for {}", who, e);
            return who + ": " + e.toString();
        }
    }

    /** The stored appliedVersion from the LOCAL meta doc in admin.system.version, or null. */
    private Long readAppliedUsersFileVersion() {
        try {
            List<Map<String, Object>> docs = driver.find("admin", "system.version",
                    Doc.of("_id", USERS_FILE_META_ID), null, null, 0, 1);

            if (docs == null || docs.isEmpty()) {
                return null;
            }
            Object v = docs.get(0).get("appliedVersion");
            return v instanceof Number n ? n.longValue() : null;
        } catch (Exception e) {
            // If the gate state cannot be read, applying anyway could roll credentials back -
            // treat it as an apply failure instead of guessing.
            throw new ConfigException("could not read the users-file meta document from admin.system.version: "
                + e.getMessage(), e);
        }
    }

    /**
     * Upserts {@code {_id: "poppydb.usersFile", appliedVersion: N}} into admin.system.version
     * via a generic update-with-upsert through the normal driver write path, so it emits a
     * change event and replicates exactly like any other write.
     */
    private void writeAppliedUsersFileVersion(long version) {
        try {
            Map<String, Object> updateCmd = new LinkedHashMap<>();
            updateCmd.put("update", "system.version");
            updateCmd.put("updates", List.of(Doc.of(
                "q", Doc.of("_id", USERS_FILE_META_ID),
                "u", Doc.of("_id", USERS_FILE_META_ID, "appliedVersion", version),
                "upsert", true, "multi", false)));
            Map<String, Object> result = runRawCommand("admin", updateCmd);

            if (!isOk(result)) {
                throw new ConfigException("could not persist the users-file appliedVersion " + version
                    + " to admin.system.version: " + errmsgOf(result));
            }
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException("could not persist the users-file appliedVersion " + version
                + ": " + e.getMessage(), e);
        }
    }

    /**
     * Runs a raw mongod-wire-shaped command map against the local driver (the shapes
     * InMemoryDriver's GenericCommand path dispatches natively - the same invocation style
     * as {@link #ensureRootUser}).
     */
    private Map<String, Object> runRawCommand(String db, Map<String, Object> cmdMap) throws Exception {
        GenericCommand cmd = new GenericCommand(driver).setCmdData(cmdMap);
        cmd.setDb(db);
        return driver.readSingleAnswer(driver.runCommand(cmd));
    }

    /**
     * True iff the command answer is a genuine success: {@code ok: 1} AND no non-empty
     * {@code writeErrors} array. A generic {@code update} command (as used by
     * {@link #writeAppliedUsersFileVersion}) can answer {@code ok: 1} at the top level while
     * still reporting a per-statement failure via {@code writeErrors} - without this check a
     * failed appliedVersion write would be silently treated as success.
     */
    private static boolean isOk(Map<String, Object> result) {
        if (result == null || !Double.valueOf(1.0).equals(result.get("ok"))) {
            return false;
        }
        Object writeErrors = result.get("writeErrors");
        return !(writeErrors instanceof List<?> errors && !errors.isEmpty());
    }

    private static String errmsgOf(Map<String, Object> result) {
        if (result == null) {
            return "no result";
        }
        Object errmsg = result.get("errmsg");
        Object code = result.get("code");
        return (errmsg == null ? "unknown error" : errmsg.toString())
            + (code == null ? "" : " (code " + code + ")");
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

    /**
     * Trust anchor for the RS-internal channel's outbound connections when SSL is enabled - see
     * {@link #internalSslContext}. Set by {@code PoppyDBCLI} alongside {@link #setSslContext};
     * has no effect unless {@link #setSslEnabled(boolean)} is also true.
     */
    public void setInternalSslContext(SSLContext internalSslContext) {
        this.internalSslContext = internalSslContext;
    }

    /**
     * Explicitly configured location for the election state file (#316). Takes precedence over
     * the dump-directory derivation, and is the only way to get durable currentTerm/votedFor on
     * a server that keeps no dumps. Must be set before configureReplicaSet().
     */
    private String electionStatePath = null;

    public void setElectionStatePath(String path) {
        this.electionStatePath = path;
    }

    /** Test seam: the election-state path this server was configured with (may be null). */
    String getElectionStatePathForTest() {
        return electionStatePath;
    }

    /** Test seam: the ElectionConfig this server was configured with. */
    ElectionConfig getElectionConfigForTest() {
        return electionConfig;
    }

    public void setDumpDirectory(File dir) {
        this.dumpDirectory = dir;

        // Ordering hazard (#306): configureReplicaSet() derives the election-state file path
        // from the dump directory and bakes it into the ElectionManager's config. If the dump
        // directory arrives only afterwards, term/votedFor persistence stays silently disabled
        // - exactly what happened on the customer ACC environment. Make it loud.
        if (dir != null && electionManager != null && electionConfig != null
                && (!electionConfig.isPersistState() || electionConfig.getStatePersistencePath() == null)) {
            log.warn("Dump directory was set AFTER configureReplicaSet() - election state "
                     + "(currentTerm/votedFor) will NOT be persisted. Call setDumpDirectory() before "
                     + "configureReplicaSet(), or set an explicit state persistence path in the ElectionConfig.");
        }
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
            // Skipped, never queued (#317): if a manual dumpNow (or a still-running earlier
            // tick) holds the guard, this tick does nothing - the next one is due anyway.
            if (!dumpGuard.tryAcquire()) {
                log.info("Skipping periodic dump: another dump is already running");
                return;
            }

            try {
                int count = writeDumpFiles();
                lastDumpTime = System.currentTimeMillis();
                if (count > 0) {
                    log.info("Periodic dump: {} databases saved", count);
                }
            } catch (Exception e) {
                log.error("Failed to dump databases: {}", e.getMessage(), e);
            } finally {
                dumpGuard.release();
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
            electionNetworkClient.setInternalConnectionSecurity(
                    authRequired, rootUser, rootPassword, sslEnabled ? internalSslContext : null);
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
                // Mirror write, not a transition: it must not bump the epoch, and it must not
                // be able to overwrite a newer callback-driven flip - so re-check leadership
                // under the same lock applyLeadershipFlip uses. If a stepdown snuck in between
                // the poll above and here, isLeader() is already false (ElectionManager flips
                // its state before dispatching the callback) and we skip; if the stepdown
                // callback is still queued, its own locked flip runs after ours and wins.
                synchronized (leadershipFlagLock) {
                    if (electionManager.isLeader()) {
                        primary = true;
                    }
                }
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
            replicationManager.setEventQueueByteBudget(eventQueueByteBudget);
            replicationManager.setInternalConnectionSecurity(
                    authRequired, rootUser, rootPassword, sslEnabled ? internalSslContext : null);
            // Set this secondary's address for progress reporting
            replicationManager.setMyAddress(host + ":" + port);
            // Same completion hook as the election path (startReplicationToLeader): the
            // partial-restore guard is released by actual sync COMPLETION, wherever and
            // whenever that happens - including a sync that finishes only after the bounded
            // wait below has given up (#306 review, P1-2). Instance-bound like there.
            ReplicationManager staticModeManager = replicationManager;
            staticModeManager.setOnInitialSyncComplete(
                    () -> releaseDataCompleteAfterSync(staticModeManager));
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

    // Synchronized so it serializes with startReplicationToLeader on the PoppyDB monitor: a
    // discovery callback mid-install either finishes first (and this teardown catches its fresh
    // ReplicationManager), or arrives later (and its running-guard no-ops). Without this, a
    // callback already past shutdown()'s running=false flip could install an RM that nothing
    // ever stops.
    private synchronized void stopReplication() {
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

    /**
     * Writes the dump files. The one place that actually touches the driver's dump path, so
     * every trigger (scheduler, dumpNow, shutdown) shares it - and tests can slow it down to
     * exercise the guard. Callers hold {@link #dumpGuard}.
     */
    int writeDumpFiles() throws IOException {
        return driver.dumpAllToDirectory(dumpDirectory);
    }

    /**
     * Dumps all databases synchronously, honoring the shared dump guard (#317).
     *
     * @return the number of databases written, or -1 if nothing was written by this call: either
     *         another dump (periodic or on-demand) was already running, or shutdown has begun
     * @throws IOException if no dump directory is configured, or the dump itself fails
     */
    public int dumpNow() throws IOException {
        if (dumpDirectory == null) {
            throw new IOException("Dump directory not configured");
        }

        // Same gate as triggerDumpNow: once shutdown has begun the driver is about to be reset,
        // and a dump that wins the guard after the final dump released it - but before
        // forceShutdown() - would rename EMPTY databases over the last good dump files. The
        // guard alone does not prevent that; only refusing to start does.
        if (shuttingDown) {
            log.info("Not dumping: shutting down (a final dump runs as part of shutdown)");
            return -1;
        }

        if (!dumpGuard.tryAcquire()) {
            log.info("Not dumping: another dump is already running");
            return -1;
        }

        try {
            int count = writeDumpFiles();
            lastDumpTime = System.currentTimeMillis();
            log.info("Dumped {} databases to {}", count, dumpDirectory.getAbsolutePath());
            return count;
        } finally {
            dumpGuard.release();
        }
    }

    /**
     * Starts a dump in the background and returns immediately - this backs the {@code dumpNow}
     * admin command, which must not keep the client (or the I/O thread) waiting for a
     * potentially large dump (#317).
     *
     * @return true if a dump was started, false if one was already running (nothing was
     *         started; the command answers {@code alreadyRunning}). A missing dump directory is
     *         not a case here: the command is not wired at all without one.
     */
    public boolean triggerDumpNow() {
        if (dumpDirectory == null) {
            return false;
        }

        if (shuttingDown) {
            log.info("dumpNow: shutting down - not starting a dump (a final dump runs on shutdown)");
            return false;
        }

        if (!dumpGuard.tryAcquire()) {
            log.info("dumpNow: a dump is already running, not starting another one");
            return false;
        }

        Thread t = new Thread(() -> {
            try {
                int count = writeDumpFiles();
                lastDumpTime = System.currentTimeMillis();
                log.info("On-demand dump finished: {} databases saved to {}", count,
                         dumpDirectory.getAbsolutePath());
            } catch (Exception e) {
                // Nobody is waiting for this any more - the client got its "started" long ago,
                // so a failure can only be reported here.
                log.error("On-demand dump failed: {}", e.getMessage(), e);
            } finally {
                dumpThread = null;
                dumpGuard.release();
            }
        }, "PoppyDB-DumpNow");
        t.setDaemon(true);
        dumpThread = t;

        try {
            t.start();
        } catch (Throwable e) {
            // The guard must not stay held by a dump that never ran.
            dumpThread = null;
            dumpGuard.release();
            throw e;
        }

        return true;
    }

    /**
     * Restore all databases from the configured dump directory. Broken dump files no longer
     * abort the restore (#306): the driver skips them (logging each on ERROR with stack trace)
     * and always emits a summary line. The returned result exposes restored/total so callers
     * can tell a partial restore from success instead of treating any non-exception as "done".
     */
    public InMemoryDriver.DirectoryRestoreResult restoreFromDump() throws IOException {
        if (dumpDirectory == null) {
            throw new IOException("Dump directory not configured");
        }

        InMemoryDriver.DirectoryRestoreResult result = driver.restoreAllFromDirectoryResult(dumpDirectory);

        // The partial-restore candidacy guard lives HERE, not only in PoppyDBCLI (#306 review
        // round 2): an embedder following docs/poppydb.md (restore, check isComplete(),
        // start()) never calls setLocalDataComplete(false) itself - after a cluster-wide
        // restart the gutted embedded node then reports index 0 like everyone else, wins the
        // election and overwrites the intact peers via their initial sync, the exact
        // empty-node-wipe the guard exists to close. Only ever degrades: a complete restore
        // must not re-set true here, that could lift a guard some other code path dropped.
        if (!result.isComplete()) {
            log.warn("Partial restore ({}/{} databases) - this node will not stand for election "
                    + "until an authoritative sync has completed",
                    result.getRestored(), result.getTotal());
            setLocalDataComplete(false);
        }

        return result;
    }

    // Status methods

    public boolean isRunning() {
        return running;
    }

    /**
     * Records whether the startup restore produced a complete local dataset. A node that is
     * missing databases stays out of candidacy (it still votes) until an authoritative sync
     * completes - otherwise it can become primary after a cluster-wide restart and push its
     * incomplete state onto the intact nodes.
     */
    public void setLocalDataComplete(boolean complete) {
        this.localDataComplete = complete;

        if (electionManager != null) {
            electionManager.setDataComplete(complete);
        }
    }

    /** Whether this node currently considers its local dataset complete (see {@link #setLocalDataComplete}). */
    public boolean isLocalDataComplete() {
        return localDataComplete;
    }

    /**
     * ReplicationManager's initial-sync-completion hook (#306 review, P1-2): an authoritative
     * copy from the primary has fully replaced whatever the local restore produced - snapshot
     * AND the backlog buffered during it (the hook only fires once both are done, see
     * {@code ReplicationManager#maybeFireSyncCompleteNotify}) - so a node held back for an
     * incomplete restore may stand for election again. Idempotent - a resync completing later
     * fires it again, harmlessly.
     *
     * <p>{@code source} is the manager the callback was registered on. A superseded manager
     * must not release the guard (its sync ran against a primary that may no longer lead), so
     * anything other than the CURRENT manager is ignored. Deliberately an unsynchronized
     * volatile read, not the PoppyDB monitor: this runs on the manager's batch thread, and
     * {@code stop()} - which waits on that very thread - is called under the monitor;
     * taking it here could deadlock shutdown. The primary defense against a stale manager is
     * its own running-flag gate in {@code maybeFireSyncCompleteNotify}; this check is the
     * belt to that suspenders.
     */
    private void releaseDataCompleteAfterSync(ReplicationManager source) {
        if (source != replicationManager) {
            log.debug("Ignoring initial-sync completion from a superseded ReplicationManager");
            return;
        }

        if (!localDataComplete) {
            log.info("Initial sync completed (backlog drained) - local data is authoritative again, "
                    + "this node may stand for election");
            setLocalDataComplete(true);
        }
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

    /**
     * The sequence to carry into a replacement {@link ReplicationManager} being started in
     * {@link #startReplicationToLeader(String, long)}: the (already-stopped, but still readable)
     * predecessor's own final position if one was actually stopped this attempt, otherwise the
     * durable {@link #lastKnownAppliedSequence} watermark left behind by a previous attempt -
     * which is exactly what a failed-start retry (predecessor {@code null}, since
     * {@code replicationManager} was already nulled and no live RM survived to hand a value
     * forward) falls back to instead of silently losing the position and reading a vacuous 0.
     *
     * <p>Deliberately pure (reads but never writes {@link #lastKnownAppliedSequence} - the
     * caller persists the result separately) and package-private: lets a test exercise the
     * fallback decision in isolation - including the {@code predecessor == null} branch that in
     * production only a failed {@code newReplicationManager.start()} retry ever reaches - without
     * needing to force a real synchronous {@code start()} throw (which in this driver stack
     * realistically requires an auth/TLS connect mismatch; disproportionate machinery for
     * covering this one fallback decision - see {@code carryOverSequenceFallsBackToPersistedWatermarkWhenNoPredecessor}
     * in {@code ReplicationFailClosedTest}).
     */
    long carryOverSequenceFor(ReplicationManager predecessor) {
        return predecessor != null ? predecessor.getLastAppliedSequence() : lastKnownAppliedSequence;
    }

    /**
     * Companion to {@link #carryOverSequenceFor(ReplicationManager)} (2026-08-14 production-CI
     * fix, I-2): the {@code "host:port"} the sequence returned by that method was actually earned
     * against - a live predecessor's own {@link ReplicationManager#getLeaderAddress()}, or the
     * durable {@link #lastKnownAppliedSequenceSource} watermark on a failed-start retry, mirroring
     * {@code carryOverSequenceFor}'s own fallback exactly (same {@code predecessor} parameter,
     * same null-means-fallback shape) so the two are always read as a matched pair. Passed
     * together into {@link ReplicationManager#carryOverLastAppliedSequence(long, String)}, whose
     * javadoc explains why the sequence is meaningless without this.
     */
    String carryOverSourceFor(ReplicationManager predecessor) {
        return predecessor != null ? predecessor.getLeaderAddress() : lastKnownAppliedSequenceSource;
    }

    /** Test hook: read the durable carry-over watermark (see {@link #lastKnownAppliedSequence}'s javadoc). */
    long getLastKnownAppliedSequenceForTest() {
        return lastKnownAppliedSequence;
    }

    /** Test hook: seed the durable carry-over watermark without going through a real replication attempt. */
    void setLastKnownAppliedSequenceForTest(long sequence) {
        lastKnownAppliedSequence = sequence;
    }

    /** Test hook: read the durable carry-over source watermark (see its field javadoc). */
    String getLastKnownAppliedSequenceSourceForTest() {
        return lastKnownAppliedSequenceSource;
    }

    /** Test hook: seed the durable carry-over source watermark without a real replication attempt. */
    void setLastKnownAppliedSequenceSourceForTest(String source) {
        lastKnownAppliedSequenceSource = source;
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
