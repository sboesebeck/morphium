package de.caluga.morphium.driver.wire;

import de.caluga.morphium.Morphium;
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
    private Map<String, List<ConnectionContainer>> connectionPool;
    private Map<Integer, ConnectionContainer> borrowedConnections;
    private Map<DriverStatsKey, AtomicDecimal> stats;
    private long fastestTime = 10000;
    private int idleSleepTime = 5;
    private String fastestHost = "";
    private final Logger log = LoggerFactory.getLogger(PooledDriver.class);
    private String primaryNode;
    private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
        private AtomicLong l = new AtomicLong(0);
        @Override
        public Thread newThread(Runnable r) {
            Thread ret = new Thread(r);
            ret.setName("MCon_" + (l.incrementAndGet()));
            ret.setDaemon(true);
            return ret;
        }
    });

    private int lastSecondaryNode;

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

        for (String host : new ArrayList<String>(getHostSeed())) {
            for (int i = 0; i < getMinConnectionsPerHost(); i++) {
                while (true) {
                    try {
                        connectToHost(host);
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
        var theCon = new ConnectionContainer(con);

        if (getAuthDb() != null) {
            con.setCredentials(getAuthDb(), getUser(), getPassword());
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

    private String getHost(String hostPort) {
        if (hostPort == null) {
            return "";
        }

        String h[] = hostPort.split(":");
        return h[0];
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
        if (hello.getWritablePrimary() && hello.getMe() != null && !hello.getMe().equals(primaryNode)) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Primary failover? %s -> %s", primaryNode, hello.getMe()));
            }

            primaryNode = hello.getMe();
        }

        if (hello.getHosts() != null) {
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
                        List<ConnectionContainer> lst = connectionPool.remove(k);

                        if (fastestHost.equals(k)) {
                            fastestHost = null;
                            fastestTime = 10000;
                        }

                        for (ConnectionContainer con : lst) {
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
    }

    protected synchronized void startHeartbeat() {
        if (heartbeat == null) {
            heartbeat = executor.scheduleWithFixedDelay(()-> {
                try {
                    // log.debug("heartbeat running");
                    for (var hst : new ArrayList<String>(getHostSeed())) {
                        for (int i = 0; i < getMinConnectionsPerHost(); i++) {
                            ConnectionContainer c = null;

                            synchronized (connectionPool) {
                                if (connectionPool.get(hst) != null && !connectionPool.get(hst).isEmpty()) {
                                    c = connectionPool.get(hst).remove(0);
                                }
                            }

                            if (c != null) {
                                // checking max lifetime
                                if (System.currentTimeMillis() - c.getCreated() > getMaxConnectionLifetime()) {
                                    log.debug("Lifetime exceeded...host: " + hst);
                                    c.getCon().close();
                                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                } else if (System.currentTimeMillis() - c.getLastUsed() > getMaxConnectionIdleTime()) {
                                    log.debug("Unused connection to " + hst + " closed");
                                    c.getCon().close();
                                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                } else {
                                    synchronized (connectionPool) {
                                        connectionPool.get(hst).add(c);
                                    }
                                }
                            }
                        }

                        HelloCommand h = null;

                        try {
                            MongoConnection con = null;

                            if (getMaxConnectionsPerHost() <= getTotalConnectionsToHost(hst)) {
                                con = new SingleMongoConnection();

                                try {
                                    con.connect(this, getHost(hst), getPortFromHost(hst));
                                } catch (Exception e) {
                                    log.error("Could not create connection to host " + hst);

                                    for (var connectionToClose : connectionPool.get(hst)) {
                                        connectionToClose.getCon().close();;
                                    }

                                    continue;
                                }
                            } else {
                                con = borrowConnection(hst);
                            }

                            h = new HelloCommand(con).setHelloOk(true).setIncludeClient(false);
                            long start = System.currentTimeMillis();
                            var hello = h.execute();
                            h.releaseConnection();
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
                        } catch (Throwable ex) {
                            if (ex.getMessage() != null && !ex.getMessage().contains("closed")) {
                                log.error("Error talking to " + hst, ex);

                                try {
                                    c.getCon().close();
                                    stats.get(DriverStatsKey.CONNECTIONS_CLOSED).incrementAndGet();
                                } catch (Exception exc) {
                                    // swallow - something was broken before already!
                                }
                            }
                        } finally {
                            if (h != null)
                                h.releaseConnection();
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
            }, 0, getHeartbeatFrequency(), TimeUnit.MILLISECONDS);
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

    private  MongoConnection borrowConnection(String host) throws MorphiumDriverException {
        ConnectionContainer c = null;

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
                    synchronized (connectionPool) {
                        if (connectionPool.get(host).size() != 0) {
                            c = connectionPool.get(host).remove(0);
                        }
                    }

                    if (c != null) {
                        synchronized (connectionPool) {
                            borrowedConnections.put(c.getCon().getSourcePort(), c);
                        }

                        stats.get(DriverStatsKey.CONNECTIONS_BORROWED).incrementAndGet();
                        // log.info("Borrowing connection sourceport " + c.getCon().getSourcePort());
                        c.touch();
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

                c = new ConnectionContainer(con);
                var hello = con.connect(this, h, port);

                if (hello == null) {
                    throw new MorphiumDriverException("Could not create connection");
                }

                synchronized (connectionPool) {
                    borrowedConnections.put(con.getSourcePort(), c);
                }

                stats.get(DriverStatsKey.CONNECTIONS_OPENED).incrementAndGet();
                // log.info("Borrowing new connection sourceport " + c.getCon().getSourcePort());
                c.touch();
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
            if (borrowedConnections.containsKey(c.getCon().getSourcePort())) {
                log.error("Re-borrowing same connection?!?!?! recursing...!");
                return borrowConnection(host);
            }

            borrowedConnections.put(c.getCon().getSourcePort(), c);
            // log.info("Borrowing connection sourceport " + c.getCon().getSourcePort());
        }

        stats.get(DriverStatsKey.CONNECTIONS_BORROWED).incrementAndGet();
        c.touch();
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
                            getHostSeed().remove(lastSecondaryNode -
                                1);                                                                                                                                                //removing node - heartbeat should add it again...

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
    public synchronized MongoConnection getPrimaryConnection(WriteConcern wc) throws MorphiumDriverException {
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
            return new HashMap(borrowedConnections);
        }
    }

    @Override
    public void releaseConnection(MongoConnection con) {
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
                    con.close();
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
            mem.stream().filter(d->d.get("optime") instanceof Map).forEach(d->d.put("optime", ((Map<String, Doc>) d.get("optime")).get("ts")));
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


    public Map<String, Object> getDbStats(String db, boolean withStorage) throws MorphiumDriverException {
        return new NetworkCallHelper<Map<String, Object>>().doCall(()-> {
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

    private List<Map<String, Object>> getCollectionInfo(String db, String collection) throws MorphiumDriverException {
        // noinspection unchecked
        return new NetworkCallHelper<List<Map<String, Object>>>().doCall(()-> {
            var con = getReadConnection(null);
            ListCollectionsCommand cmd = new ListCollectionsCommand(con);
            cmd.setDb(db);
            cmd.setFilter(Doc.of("name", collection));
            var ret = cmd.execute();
            // cmd.releaseConnection();
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
        return m;
    }

}
