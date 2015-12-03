package de.caluga.morphium.driver.meta;

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.singleconnect.DriverBase;
import de.caluga.morphium.driver.singleconnect.ThreaddedBase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Stephan Bösebeck
 * Date: 02.12.15
 * Time: 23:56
 * <p>
 * TODO: Add documentation here
 */
public class MetaDriver extends DriverBase {
    private Logger log = new Logger(MetaDriver.class);
    private volatile static long seq;
    private volatile Map<String, List<Connection>> driverPool = new ConcurrentHashMap<>();
    private String currentMaster;

    @Override
    public void connect() throws MorphiumDriverException {

    }

    @Override
    public void connect(String replicasetName) throws MorphiumDriverException {

    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public void close() throws MorphiumDriverException {

    }

    @Override
    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> getOps(long threshold) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        return null;
    }

    @Override
    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference rp, Map<String, Object> findMetaData) throws MorphiumDriverException {
        return null;
    }

    @Override
    public long count(String db, String collection, Map<String, Object> query, ReadPreference rp) throws MorphiumDriverException {
        return 0;
    }

    @Override
    public void insert(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {

    }

    @Override
    public void store(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {

    }

    @Override
    public Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> op, boolean multiple, boolean upsert, WriteConcern wc) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> delete(String db, String collection, Map<String, Object> query, boolean multiple, WriteConcern wc) throws MorphiumDriverException {
        return null;
    }

    @Override
    public void drop(String db, String collection, WriteConcern wc) throws MorphiumDriverException {

    }

    @Override
    public void drop(String db, WriteConcern wc) throws MorphiumDriverException {

    }

    @Override
    public boolean exists(String db) throws MorphiumDriverException {
        return false;
    }

    @Override
    public List<Object> distinct(String db, String collection, String field, Map<String, Object> filter) throws MorphiumDriverException {
        return null;
    }

    @Override
    public boolean exists(String db, String collection) throws MorphiumDriverException {
        return false;
    }

    @Override
    public List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException {
        return null;
    }

    @Override
    public List<String> getCollectionNames(String db) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> group(String db, String coll, Map<String, Object> query, Map<String, Object> initial, String jsReduce, String jsFinalize, ReadPreference rp, String... keys) {
        return null;
    }

    @Override
    public List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, ReadPreference readPreference) throws MorphiumDriverException {
        return null;
    }

    @Override
    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        return false;
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return null;
    }

    @Override
    public void createIndex(String db, String collection, Map<String, Object> index, Map<String, Object> options) throws MorphiumDriverException {

    }

    private DriverBase createDriver(String host) throws MorphiumDriverException {
        DriverBase d = new ThreaddedBase();
        d.setHostSeed(host); //only connecting to one host
        d.setSocketKeepAlive(isSocketKeepAlive());
        d.setLocalThreshold(getLocalThreshold());
        d.setMaxWaitTime(getMaxWaitTime());
        d.setReplicaSetName(getReplicaSetName());
        d.setDefaultW(getDefaultW());
        d.setDefaultReadPreference(getDefaultReadPreference());
        d.setRetriesOnNetworkError(getRetriesOnNetworkError());
        d.setSleepBetweenErrorRetries(getSleepBetweenErrorRetries());
        d.connect(getReplicaSetName());
        return d;
    }

    private Connection getMasterConnection() throws MorphiumDriverNetworkException {
        long start = System.currentTimeMillis();
        while (currentMaster == null) {
            if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                throw new MorphiumDriverNetworkException("could not get Master!");
            }
            Thread.yield();

        }
        List<Connection> masterConnections = driverPool.get(currentMaster);
        for (int i = 0; i < masterConnections.size(); i++) {
            if (masterConnections.get(i).isInUse()) continue;

        }
        return null;
    }

    private Connection getConnection(ReadPreference rp) {
        return null;
    }

    private void freeConnection(Connection c) {

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

        public Connection(DriverBase dr) throws MorphiumDriverException {
            this.d = dr;
            lru = System.currentTimeMillis();
            created = lru;
            synchronized (Connection.class) {
                id = ++seq;
            }

            new Thread() {
                public void run() {
                    while (d.isConnected()) {
                        Map<String, Object> reply = null;
                        try {
                            long start = System.currentTimeMillis();
                            reply = d.runCommand("local", Utils.getMap("isMaster", true));
                            answerTime = System.currentTimeMillis() - start;
                        } catch (MorphiumDriverException e) {
                            log.error("Error with connection - exiting", e);
                            ok = false;
                            try {
                                d.close();
                            } catch (MorphiumDriverException e1) {
                            }
                            return;
                        }
                        if (reply.get("ismaster").equals(true)) {
                            //got master connection...
                            master = true;
                            currentMaster = getHostSeed()[0];
                        } else {
                            master = false;
                        }

                        try {
                            sleep(getHeartbeatFrequency());
                        } catch (InterruptedException e) {
                        }
                    }
                }
            }.start();

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
            if (!(o instanceof Connection)) return false;

            Connection that = (Connection) o;

            return id == that.id;

        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }
}
