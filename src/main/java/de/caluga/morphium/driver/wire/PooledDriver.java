package de.caluga.morphium.driver.wire;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.AggregatorImpl;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.*;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class PooledDriver extends DriverBase {
    public final static String driverName = "PooledDriver";
    private Map<String, List<Connection>> connectionPool;
    private Map<Integer, Connection> borrowedConnections;
    //    private Map<String, AtomicInteger> borrowedConnectionsByCaller = new ConcurrentHashMap<>();
    private Map<DriverStatsKey, AtomicDecimal> stats;
    private long fastestTime = 10000;
    private int idleSleepTime = 5;
    private String fastestHost = "";
    private final Logger log = LoggerFactory.getLogger(PooledDriver.class);
    private String primaryNode;
    private boolean readerRunning = true;
    private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
        private AtomicLong l = new AtomicLong(0);
        @Override
        public Thread newThread(Runnable r) {
            Thread ret = new Thread(r);
            ret.setName("MCon_" + (l.incrementAndGet()));
            ret.setDaemon(true);
            return ret;
        }
        // todo - heartbeat implementation
    });
    private int lastSecondaryNode;
    private Thread readerThread;

    public PooledDriver() {
        connectionPool = new HashMap<>();
        borrowedConnections = Collections.synchronizedMap(new HashMap<>());
        stats = new ConcurrentHashMap<>();

        for (var e : DriverStatsKey.values()) {
            stats.put(e, new AtomicDecimal(0));
        }
    }

    @Override
    public void connect(String replSet) throws MorphiumDriverException {
        int retries = 0;
        boolean connected = false;
        // readerExecutor = new ThreadPoolExecutor(getMinConnectionsPerHost(), getMaxConnectionsPerHost(), getMaxConnectionLifetime(), TimeUnit.MILLISECONDS,
        // new LinkedBlockingQueue<>(getMaxConnectionsPerHost()));
        readerThread = new Thread(()->{
            long start = System.currentTimeMillis();

            while (readerRunning) {
                try {
                    ArrayList<Connection> cons = new ArrayList<>();

                    synchronized (connectionPool) {
                        cons.addAll(borrowedConnections.values());
                        //do we need to check idle connections?
                        // for (var e : connectionPool.entrySet()) {
                        //     cons.addAll(e.getValue());
                        // }
                    }

                    for (Connection c : cons) {
                        if (c.getCon().isDataAvailable()) {
                            c.getCon().readNextMsg();
                        }
                    }

                    //                    int partitionSize = 2;
                    //                    Collection<List<Connection>> partitionedList = IntStream.range(0, cons.size()).boxed()
                    //                    .collect(Collectors.groupingBy(partition->(partition / partitionSize), Collectors.mapping(elementIndex->cons.get(elementIndex), Collectors.toList()))).values();
                    //                    List<Thread> thrs = new ArrayList();
                    //
                    //                    for (List<Connection> lst : partitionedList) {
                    //                        // log.info("Data handling...");
                    //                        // readerExecutor.execute(()->{
                    //                        var t = new Thread(()->{
                    //                            for (Connection c : lst) {
                    //                                if (c.getCon().isDataAvailable()) {
                    //                                    c.getCon().readNextMsg();
                    //                                }
                    //                            }
                    //
                    //                        });
                    //                        t.setDaemon(true);
                    //                        thrs.add(t);
                    //                        t.start();
                    //                        // });
                    //                    }
                    //
                    //                    // while (readerExecutor.getActiveCount() > 0) {
                    //                    //     Thread.yield();
                    //                    // }
                    //                    for (Thread t : thrs) {
                    //                        t.join();
                    //                    }
                    //
                    //                    if (System.currentTimeMillis() - start > 1000) {
                    //                        for (Connection c : cons) {
                    //                            c.getCon().doHouseKeeping();
                    //                        }
                    //
                    //                        start = System.currentTimeMillis();
                    //                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // log.info("Still up...");
                //no sleep?
                Thread.yield();
            }
            log.warn("ReaderThread exiting");

        });
        readerThread.setDaemon(false);
        readerThread.start();

        for (String host : new ArrayList<String>(getHostSeed())) {
            for (int i = 0; i < getMinConnectionsPerHost(); i++) {
                while (true) {
                    try {
                        long start = System.currentTimeMillis();
                        connectToHost(host);
                        //                        log.info("host: "+host+" "+i+"/"+getMaxConnectionsPerHost()+" Connecting took "+(System.currentTimeMillis()-start));
                        connected = true;
                        break;
                    } catch (MorphiumDriverException e) {
                        retries++;

                        if (retries < getRetriesOnNetworkError()) {
                            log.error("Connection failed, retrying...");

                            try {
                                Thread.sleep(getSleepBetweenErrorRetries());
                            } catch (InterruptedException e1) {
                            }
                        } else {
                            log.error("Could not connect to " + host);
                            getHostSeed().remove(host);
                            //throw(e);
                            break;
                        }
                    }
                }
            }
        }

        if (!connected) {
            throw new MorphiumDriverException("Connection failed");
        }

        setReplicaSet(getHostSeed().size() > 1);
        startHeartbeat();
    }

    private void connectToHost(String host) throws MorphiumDriverException {
        String h = getHost(host);
        int port = getPortFromHost(host);
        var con = new SingleMongoConnection();
        var theCon = new Connection(con);

        if (getAuthDb() != null) {
            con.setCredentials(getAuthDb(), getUser(), getPassword());
        }

        synchronized (connectionPool) {
            borrowedConnections.put(con.getSourcePort(), theCon);
        }

        long start = System.currentTimeMillis();
        var hello = con.connect(this, h, port);
        stats.get(DriverStatsKey.CONNECTIONS_OPENED).incrementAndGet();
        long dur = System.currentTimeMillis() - start;

        if (fastestTime > dur) {
            fastestTime = dur;
            fastestHost = host;
        }

        synchronized (connectionPool) {
            borrowedConnections.remove(con.getSourcePort());
            connectionPool.putIfAbsent(host, new CopyOnWriteArrayList<>());
            connectionPool.get(host).add(theCon);
        }

        if (hello.getWritablePrimary()) {
            primaryNode = host;
        }

        handleHello(hello);
        setMaxBsonObjectSize(hello.getMaxBsonObjectSize());
        setMaxMessageSize(hello.getMaxMessageSizeBytes());
        setMaxWriteBatchSize(hello.getMaxWriteBatchSize());
    }

    private ScheduledFuture<?> heartbeat;

    private String getHost(int hostSeedIndex) {
        return getHost(getHostSeed().get(hostSeedIndex));
    }

    private String getHost(String hostPort) {
        if (hostPort == null) {
            return "";
        }

        String h[] = hostPort.split(":");
        return h[0];
    }

    private int getPortFromHost(int hostSeedIdx) {
        return getPortFromHost(getHostSeed().get(hostSeedIdx));
    }

    private int getPortFromHost(String host) {
        String h[] = host.split(":");

        if (h.length == 1) {
            return 27017;
        }

        return Integer.parseInt(h[1]);
    }

    @Override
    public void connect() throws MorphiumDriverException {
        connect(null);
    }

    private void handleHello(HelloResult hello) {
        if (hello.getWritablePrimary() && !hello.getMe().equals(primaryNode)) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Primary failover? %s -> %s", primaryNode, hello.getMe()));
            }

            primaryNode = hello.getMe();
        }

        for (String hst : hello.getHosts()) {
            synchronized (connectionPool) {
                if (!connectionPool.containsKey(hst)) {
                    log.debug("new host needs to be added: " + hst);
                    connectionPool.put(hst, new ArrayList<>());
                }
            }

            if (!getHostSeed().contains(hst)) {
                getHostSeed().add(hst);
            }
        }

        for (String hst : new ArrayList<String>(getHostSeed())) {
            if (!hello.getHosts().contains(hst)) {
                getHostSeed().remove(hst);
            }
        }

        synchronized (connectionPool) {
            for (String k : new ArrayList<>(connectionPool.keySet())) {
                if (!hello.getHosts().contains(k)) {
                    log.warn("Host " + k + " is not part of the replicaset anymore!");
                    getHostSeed().remove(k);
                    List<Connection> lst = connectionPool.remove(k);

                    if (fastestHost.equals(k)) {
                        fastestHost = null;
                        fastestTime = 10000;
                    }

                    for (Connection con : lst) {
                        try {
                            con.getCon().close();
                            stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                        } catch (Exception ex) {
                        }
                    }
                }
            }
        }
    }

    protected synchronized void startHeartbeat() {
        if (heartbeat == null) {
            // log.debug("Starting heartbeat ");
            heartbeat = executor.scheduleWithFixedDelay(()->{
                try {
                    ArrayList<Connection> cons = new ArrayList<>();

                    synchronized (connectionPool) {
                        cons.addAll(borrowedConnections.values());

                        //do we need to check idle connections?
                        for (var e : connectionPool.entrySet()) {
                            cons.addAll(e.getValue());
                        }
                    }

                    for (Connection c : cons) {
                        c.getCon().doHouseKeeping();
                    }

                    // log.debug("heartbeat running");
                    for (var hst : new ArrayList<String>(getHostSeed())) {
                        Connection c = null;

                        synchronized (connectionPool) {
                            if (connectionPool.get(hst) == null || connectionPool.get(hst).size() == 0) {
                                continue;
                            }

                            c = connectionPool.get(hst).remove(0);
                        }

                        // checking max lifetime
                        if (c == null) {
                            continue;
                        }

                        if (System.currentTimeMillis() - c.getCreated() > getMaxConnectionLifetime()) {
                            try {
                                // max lifetime exceeded
                                log.debug("Lifetime exceeded...host: " + hst);
                                c.getCon().close();
                                stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                            } catch (Exception ex) {
                            }
                        } else if (System.currentTimeMillis() - c.getLastUsed() > getMaxConnectionIdleTime()) {
                            log.debug("Unused connection to " + hst + " closed");
                            c.getCon().close();
                            stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                        }

                        if (c.getCon().isConnected()) {
                            borrowedConnections.put(c.getCon().getSourcePort(), c); //otherwise no answers
                            HelloCommand h = new HelloCommand(c.getCon()).setHelloOk(true).setIncludeClient(false);

                            try {
                                long start = System.currentTimeMillis();
                                var hello = h.execute();
                                long dur = System.currentTimeMillis() - start;

                                if (dur < fastestTime) {
                                    fastestTime = dur;
                                    fastestHost = hst;
                                }

                                if (hello != null && hello.getWritablePrimary()) {
                                    handleHello(hello);
                                } else {
                                    // log.info("Hello from secondary");
                                }

                                synchronized (connectionPool) {
                                    borrowedConnections.remove(c.getCon().getSourcePort());
                                    connectionPool.get(hst).add(c);
                                }
                            } catch (MorphiumDriverException ex) {
                                if (!ex.getMessage().contains("closed")) {
                                    log.error("Error talking to " + hst, ex);

                                    try {
                                        c.getCon().close();
                                        stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                    } catch (Exception exc) {
                                        // swallow - something was broken before already!
                                    }
                                }
                            }
                        }

                        while (getTotalConnectionsToHost(hst) < getMinConnectionsPerHost()) {
                            // need to add connections!
                            //                            log.info("need to add connections: "+getTotalConnectionsToHost(e.getKey())+"<"+getMinConnections()+"/"+getMinConnectionsPerHost());
                            try {
                                connectToHost(hst);
                            } catch (MorphiumDriverException ex) {
                                log.error("Could not fill connection pool for " + hst, ex);
                                getHostSeed().remove(hst); //removing from hostSeeed

                                synchronized (connectionPool) {
                                    if (connectionPool.get(hst).size() == 0) {
                                        connectionPool.remove(hst);
                                    }
                                }

                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("Error during heartbeat", e);
                }
            }, getHeartbeatFrequency(), getHeartbeatFrequency(), TimeUnit.MILLISECONDS);
        } else {
            log.debug("Heartbeat already scheduled...");
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
                con.release();
            }
        }
    }

    private int getTotalConnectionsToHost(String h) {
        int borrowed = 0;
        Collection<Connection> values = new ArrayList<>(borrowedConnections.values());

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

    private synchronized MongoConnection borrowConnection(String host) throws MorphiumDriverException {
        // var st=Thread.currentThread().getStackTrace();
        // int n = 3;
        // String s = st[n].getClassName() + "." + st[n].getMethodName() ;
        //// log.info("Borrowing connection to: " + s+ ":" + st[n].getLineNumber());
        // this.borrowedConnectionsByCaller.putIfAbsent(s,new AtomicInteger(0));
        // this.borrowedConnectionsByCaller.get(s).incrementAndGet();
        Connection c = null;
        //        long start = System.currentTimeMillis();

        //        while (connectionPool.get(host).size()==0 && getTotalConnectionsToHost(host) >= getMaxConnectionsPerHost()) {
        //            if ((System.currentTimeMillis() - start) % 1000 == 0) {
        //                log.warn(String.format("Connection pool exceeded for host %s (in use: %d, max: %d)- need to wait", host, getTotalConnectionsToHost(host), getMaxConnectionsPerHost()));
        //            }
        //
        //            if (System.currentTimeMillis() - start > getMaxWaitTime()) {
        //                log.error(String.format("Could not get connection from pool to %s - timeout %d", host, getMaxWaitTime()));
        //                throw new MorphiumDriverException("Could not get connection from pool in time - max connections per host exceeded");
        //            }
        //
        //            try {
        //                Thread.sleep(100);
        //            } catch (InterruptedException e) {
        //            }
        //        }

        while (true) {
            if (connectionPool.get(host) == null || connectionPool.get(host).size() == 0) {
                // if too many connections were already borrowed, wait for some to return
                long start = System.currentTimeMillis();

                while (getTotalConnectionsToHost(host) > getMaxConnectionsPerHost()) {
                    if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                        log.error("maxwaitTime exceeded while waiting for a connection - connectionpool exceeded! " + getMaxWaitTime() + "ms");
                        throw new MorphiumDriverException("Could not get connection in time: " + getMaxWaitTime() + "ms");
                    }

                    //                            log.debug("finally got connection..." + getTotalConnectionsToHost(host));
                    c = connectionPool.get(host).remove(0);

                    if (c != null) {
                        synchronized (connectionPool) {
                            borrowedConnections.put(c.getCon().getSourcePort(), c);
                        }

                        stats.get(DriverStatsKey.CONNECTIONS_BORROWED).incrementAndGet();
                        return c.getCon();
                    }

                    try {
                        Thread.sleep(idleSleepTime);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }

                String h = getHost(host);
                int port = getPortFromHost(host);
                var con = new SingleMongoConnection();

                if (getAuthDb() != null) {
                    con.setCredentials(getAuthDb(), getUser(), getPassword());
                }

                c = new Connection(con);
                int fakePort = 64000;

                synchronized (connectionPool) {
                    while (borrowedConnections.containsKey(fakePort)) {
                        fakePort++;
                    }

                    borrowedConnections.put(fakePort, c);
                }

                var hello = con.connect(this, h, port);

                if (hello == null) {
                    throw new MorphiumDriverException("Could not create connection");
                }
                synchronized(connectionPool){
                    borrowedConnections.remove(fakePort);
                    borrowedConnections.put(con.getSourcePort(),c);
                }

                stats.get(DriverStatsKey.CONNECTIONS_OPENED).incrementAndGet();
                return con;
            } else {
                synchronized (connectionPool) {
                    if (connectionPool.get(host).size() != 0) {
                        try {
                            c = connectionPool.get(host).remove(0);
                        } catch (Exception e) {
                        }

                        while (c == null) {
                            //                            log.error("Got null from pool?!?!?");
                            if (connectionPool.get(host).size() == 0) {
                                break;
                            }

                            c = connectionPool.get(host).remove(0);
                        }

                        break;
                    }
                }
            }
        }

        if (c == null) {
            log.error("Could not get connection!?!?");
            return null;
        }

        synchronized (connectionPool) {
            borrowedConnections.put(c.getCon().getSourcePort(), c);
        }

        stats.get(DriverStatsKey.CONNECTIONS_BORROWED).incrementAndGet();
        return c.getCon();
    }

    @Override
    public MongoConnection getReadConnection(ReadPreference rp) {
        try {
            if (getHostSeed().size() == 1 || !isReplicaSet()) {
                // no replicaset
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
                return borrowConnection(primaryNode);

            case NEAREST:

                // check fastest answer time
                if (fastestHost != null) {
                    try {
                        return borrowConnection(fastestHost);
                    } catch (MorphiumDriverException e) {
                        log.warn("Could not get connection to fastest host, trying primary", e);
                    }
                }

            case PRIMARY_PREFERRED:
                synchronized (connectionPool) {
                    if (connectionPool.get(primaryNode).size() != 0) {
                        try {
                            return borrowConnection(primaryNode);
                        } catch (MorphiumDriverException e) {
                            log.warn("Could not get connection to " + primaryNode + " trying secondary");
                        }
                    }
                }

            case SECONDARY_PREFERRED:
            case SECONDARY:
                int retry = 0;

                while (true) {
                    // round-robin
                    if (lastSecondaryNode >= getHostSeed().size()) {
                        lastSecondaryNode = 0;
                        retry++;
                    }

                    if (getHostSeed().get(lastSecondaryNode).equals(primaryNode)) {
                        lastSecondaryNode++;

                        if (lastSecondaryNode > getHostSeed().size()) {
                            lastSecondaryNode = 0;
                            retry++;
                        }
                    }

                    String host = getHostSeed().get(lastSecondaryNode++);

                    try {
                        return borrowConnection(host);
                    } catch (MorphiumDriverException e) {
                        if (retry > getRetriesOnNetworkError()) {
                            log.error("Could not get Connection - abort");
                            throw(e);
                        }

                        log.warn(String.format("could not get connection to secondary node '%s'- trying other replicaset node", host));
                        getHostSeed().remove(lastSecondaryNode - 1);                                                                     //removing node - heartbeat should add it again...

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
                for (Connection c : new ArrayList<>(connectionPool.get(k))) { // avoid concurrendModification
                    if (c.getCon() == con) {
                        connectionPool.get(k).remove(c);
                        return;
                    }
                }
            }
        }
    }

    public Map<Integer, Connection> getBorrowedConnections() {
        return borrowedConnections;
    }

    @Override
    public void releaseConnection(MongoConnection con) {
        //        var st = Thread.currentThread().getStackTrace();
        //        int n = 3;
        //        String s = st[n].getClassName() + "." + st[n].getMethodName();
        //
        // log.info("releasing connection from: " + s + ":" + st[n].getLineNumber());
        //        if (borrowedConnectionsByCaller.containsKey(s)) {
        //            borrowedConnectionsByCaller.get(s).decrementAndGet();
        //        }
        if (con == null) {
            log.error("Returning null connection????");
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
                // log.debug("Returning not borrowed connection!?!?");
                if (con.isConnected()) {
                    c = new Connection((SingleMongoConnection) con);
                } else {
                    return;
                }
            }

            if (con.getConnectedTo() != null) {
                synchronized (connectionPool) {
                    connectionPool.putIfAbsent(con.getConnectedTo(), new CopyOnWriteArrayList<>());
                    connectionPool.get(con.getConnectedTo()).add(0, c);
                }
            }
        }

        stats.get(DriverStatsKey.CONNECTIONS_RELEASED).incrementAndGet();
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
    public <T, R> Aggregator<T, R> createAggregator(Morphium morphium, Class<? extends T> type, Class<? extends R> resultType) {
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
        // if (connection != null)
        // connection.close();
        readerRunning = false;

        if (heartbeat != null) {
            heartbeat.cancel(true);
        }

        heartbeat = null;

        try {
            readerThread.join();
        } catch (InterruptedException e) {
        }

        synchronized (connectionPool) {
            for (var e : new ArrayList<>(connectionPool.entrySet())) {
                for (var c : new ArrayList<>(e.getValue())) {
                    try {
                        c.getCon().close();
                        stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                    } catch (Exception ex) {
                    }
                }

                connectionPool.get(e.getKey()).clear();
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
        log.debug("killed cursor");
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
        con.release();
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
            // getPrimaryConnection(null).sendCommand(Doc.of("abortTransaction", 1,
            // "txnNumber", ctx.getTxnNumber(), "autocommit", false, "lsid", Doc.of("id",
            // ctx.getLsid(), "$db", "admin")));
        } finally {
            con.release();
            clearTransactionContext();
        }
    }
    //
    // @Override
    // public SingleElementResult runCommandSingleResult(SingleResultCommand cmd)
    // throws MorphiumDriverException {
    // var start = System.currentTimeMillis();
    // var m = cmd.asMap();
    // var msg = getConnection().sendCommand(m);
    // var res = getConnection().readSingleAnswer(msg);
    // return new
    // SingleElementResult().setResult(res).setDuration(System.currentTimeMillis() -
    // start)
    // .setServer(getConnection().getConnectedTo()).setMetadata(cmd.getMetaData());
    // }

    // @Override
    // public CursorResult runCommand(MultiResultCommand cmd) throws
    // MorphiumDriverException {
    // var start = System.currentTimeMillis();
    // var msg = getConnection().sendCommand(cmd.asMap());
    // return new CursorResult().setCursor(getConnection().getAnswerFor(msg,
    // getDefaultBatchSize()))
    // .setMessageId(msg).setDuration(System.currentTimeMillis() - start)
    // .setServer(getConnection().getConnectedTo()).setMetadata(cmd.getMetaData());
    // }

    // @Override
    // public ListResult runCommandList(MultiResultCommand cmd) throws
    // MorphiumDriverException {
    // var start = System.currentTimeMillis();
    // var msg = getConnection().sendCommand(cmd.asMap());
    // return new ListResult().setResult(getConnection().readAnswerFor(msg))
    // .setMessageId(msg).setDuration(System.currentTimeMillis() - start)
    // .setServer(getConnection().getConnectedTo()).setMetadata(cmd.getMetaData());
    // }

    // @Override
    // public Map<String, Object> runCommand(String db, Map<String, Object> cmd)
    // throws MorphiumDriverException {
    // cmd.put("$db", db);
    // var start = System.currentTimeMillis();
    // var msg = getConnection().sendCommand(cmd);
    // return getConnection().readSingleAnswer(msg);
    // }

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
            mem.stream().filter(d->d.get("optime") instanceof Map).forEach(d->d.put("optime", ((Map<String, Doc>) d.get("optime")).get("ts")));
            return result;
        } finally {
            if (con != null) {
                con.release();
            }
        }
    }

    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        MongoConnection con = null;

        try {
            con = getPrimaryConnection(null);
            return new DbStatsCommand(con).setDb(db).execute();
        } finally {
            if (con != null) {
                con.release();
            }
        }
    }

    @Override
    public Map<String, Object> getCollStats(String db, String coll) throws MorphiumDriverException {
        CollStatsCommand cmd = new CollStatsCommand(this).setColl(coll).setDb(db);
        return cmd.execute();
    }

    public List<Map<String, Object>> currentOp(int threshold) throws MorphiumDriverException {
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

    private List<Map<String, Object>> readBatches(MongoConnection connection, int waitingfor, int batchSize) throws MorphiumDriverException {
        List<Map<String, Object>> ret = new ArrayList<>();
        Map<String, Object> doc;
        String db = null;
        String coll = null;

        while (true) {
            OpMsg reply = connection.getReplyFor(waitingfor, getMaxWaitTime());

            if (reply.getResponseTo() != waitingfor) {
                log.error("Wrong answer - waiting for " + waitingfor + " but got " + reply.getResponseTo());
                log.error("Document: " + Utils.toJsonString(reply.getFirstDoc()));
                continue;
            }

            // replies.remove(i);
            @SuppressWarnings("unchecked")
            Map<String, Object> cursor = (Map<String, Object>) reply.getFirstDoc().get("cursor");

            if (cursor == null) {
                // trying result
                if (reply.getFirstDoc().get("result") != null) {
                    // noinspection unchecked
                    return (List<Map<String, Object>>) reply.getFirstDoc().get("result");
                }

                if (reply.getFirstDoc().containsKey("results")) {
                    return (List<Map<String, Object>>) reply.getFirstDoc().get("results");
                }

                throw new MorphiumDriverException("Mongo Error: " + reply.getFirstDoc().get("codeName") + " - " + reply.getFirstDoc().get("errmsg"));
            }

            if (db == null) {
                // getting ns
                String[] namespace = cursor.get("ns").toString().split("\\.");
                db = namespace[0];

                if (namespace.length > 1) {
                    coll = namespace[1];
                }
            }

            if (cursor.get("firstBatch") != null) {
                // noinspection unchecked
                ret.addAll((List) cursor.get("firstBatch"));
            } else if (cursor.get("nextBatch") != null) {
                // noinspection unchecked
                ret.addAll((List) cursor.get("nextBatch"));
            }

            if (((Long) cursor.get("id")) != 0) {
                // there is more! Sending getMore!
                OpMsg q = new OpMsg();
                q.setFirstDoc(Doc.of("getMore", (Object) cursor.get("id")).add("$db", db).add("batchSize", batchSize));

                if (coll != null) {
                    q.getFirstDoc().put("collection", coll);
                }

                q.setMessageId(getNextId());
                waitingfor = q.getMessageId();
                // connection.sendQuery(q);
            } else {
                break;
            }
        }

        return ret;
    }

    public Map<String, Object> getDbStats(String db, boolean withStorage) throws MorphiumDriverException {
        return new NetworkCallHelper<Map<String, Object>>().doCall(()->{
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

    // @Override
    // public RunCommandResult sendCommand(String db, Map<String, Object> cmd)
    // throws MorphiumDriverException {
    // OpMsg q = new OpMsg();
    // cmd.put("$db", db);
    // q.setMessageId(getNextId());
    // q.setFirstDoc(Doc.of(cmd));
    //
    // OpMsg rep = null;
    // long start = System.currentTimeMillis();
    // connection.sendQuery(q);
    // return new RunCommandResult().setMessageId(q.getMessageId())
    // .setDuration(System.currentTimeMillis() - start)
    // .setServer(connection.getConnectedTo());
    // }
    //
    // private Map<String, Object> getSingleDocAndKillCursor(OpMsg msg) throws
    // MorphiumDriverException {
    // if (!msg.hasCursor()) return null;
    // Map<String, Object> cursor = (Map<String, Object>)
    // msg.getFirstDoc().get("cursor");
    // Map<String, Object> ret = null;
    // if (cursor.containsKey("firstBatch")) {
    // ret = (Map<String, Object>) cursor.get("firstBatch");
    // } else {
    // ret = (Map<String, Object>) cursor.get("nextBatch");
    // }
    // String[] namespace = cursor.get("ns").toString().split("\\.");
    // killCursors(namespace[0], namespace[1], (Long) cursor.get("id"));
    // return ret;
    // }

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

    private List<Map<String, Object>> getCollectionInfo(String db, String collection) throws MorphiumDriverException {
        // noinspection unchecked
        return new NetworkCallHelper<List<Map<String, Object>>>().doCall(()->{
            var con = getPrimaryConnection(null);
            ListCollectionsCommand cmd = new ListCollectionsCommand(con);
            cmd.setDb(db);
            cmd.setFilter(Doc.of("name", collection));
            var ret = cmd.execute();
            con.release();
            return ret;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public synchronized Map<String, Integer> getNumConnectionsByHost() {
        Map<String, Integer> ret = new HashMap<>();

        synchronized (connectionPool) {
            for (var e : connectionPool.entrySet()) {
                ret.put(e.getKey(), e.getValue().size());
            }
        }

        for (var e : borrowedConnections.values()) {
            ret.put(e.getCon().getConnectedTo(), ret.get(e.getCon().getConnectedTo()).intValue() + 1);
        }

        return ret;
    }

    @Override
    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        List<Map<String, Object>> lst = getCollectionInfo(db, coll);

        try {
            if (!lst.isEmpty() && lst.get(0).get("name").equals(coll)) {
                Object capped = ((Map) lst.get(0).get("options")).get("capped");
                return capped != null && capped.equals(true);
            }
        } catch (Exception e) {
            log.error("Error", e);
        }

        return false;
    }

    private class Connection {
        private SingleMongoConnection con;
        private long created;
        private long lastUsed;

        public Connection(SingleMongoConnection con) {
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

        public Connection setCon(SingleMongoConnection con) {
            this.con = con;
            return this;
        }

        public long getCreated() {
            return created;
        }

        public Connection setCreated(long created) {
            this.created = created;
            return this;
        }

        public long getLastUsed() {
            return lastUsed;
        }

        public Connection setLastUsed(long lastUsed) {
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
                                   settings.getConnection().release();
                               } else if (r instanceof UpdateBulkRequest) {
                                   UpdateBulkRequest up = (UpdateBulkRequest) r;
                                   UpdateMongoCommand upCmd = new UpdateMongoCommand(getPrimaryConnection(null));
                                   upCmd.setColl(collection).setDb(db).setUpdates(Arrays.asList(Doc.of("q", up.getQuery(), "u", up.getCmd(), "upsert", up.isUpsert(), "multi", up.isMultiple())));
                                   upCmd.execute();
                                   upCmd.getConnection().release();
                               } else if (r instanceof DeleteBulkRequest) {
                                   DeleteBulkRequest dbr = ((DeleteBulkRequest) r);
                                   DeleteMongoCommand del = new DeleteMongoCommand(getPrimaryConnection(null));
                                   del.setColl(collection).setDb(db).setDeletes(Arrays.asList(Doc.of("q", dbr.getQuery(), "limit", dbr.isMultiple() ? 0 : 1)));
                                   del.execute();
                                   del.getConnection().release();
                               } else {
                                   throw new RuntimeException("Unknown operation " + r.getClass().getName());
                               }
                           }
                       } catch (MorphiumDriverException e) {
                           log.error("Got exception: ", e);
                       }

                       return new Doc();
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
        //        for (var e : borrowedConnectionsByCaller.entrySet()) {
        //            log.debug("Caller: " + e.getKey() + " -> " + e.getValue());
        //        }
        //
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
        // m.put(DriverStatsKey.CONNECTIONS_IN_USE,stats.get(DriverStatsKey.CONNECTIONS_BORROWED).doubleValue()-stats.get(DriverStatsKey.CONNECTIONS_RELEASED).doubleValue());
        return m;
    }

}
