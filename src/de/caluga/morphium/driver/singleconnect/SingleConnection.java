package de.caluga.morphium.driver.singleconnect;/**
 * Created by stephan on 30.11.15.
 */

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.mongodb.Maximums;
import de.caluga.morphium.driver.wireprotocol.OpQuery;
import de.caluga.morphium.driver.wireprotocol.OpReply;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;

/**
 * TODO: Add Documentation here
 **/
public class SingleConnection implements MorphiumDriver {

    private Logger log = new Logger(SingleConnection.class);
    private Socket s;
    private OutputStream out;
    private InputStream in;

    private Vector<OpReply> replies = new Vector<>();
    private volatile int rqid = 10000;

    @Override
    public void setCredentials(String db, String login, char[] pwd) {

    }

    @Override
    public boolean isReplicaset() {
        return false;
    }

    @Override
    public void setReplicaset(boolean replicaset) {

    }

    @Override
    public String[] getCredentials(String db) {
        return new String[0];
    }

    @Override
    public boolean isDefaultFsync() {
        return false;
    }

    @Override
    public String[] getHostSeed() {
        return new String[0];
    }

    @Override
    public int getMaxConnectionsPerHost() {
        return 0;
    }

    @Override
    public int getMinConnectionsPerHost() {
        return 0;
    }

    @Override
    public int getMaxConnectionLifetime() {
        return 0;
    }

    @Override
    public int getMaxConnectionIdleTime() {
        return 0;
    }

    @Override
    public int getSocketTimeout() {
        return 0;
    }

    @Override
    public int getConnectionTimeout() {
        return 0;
    }

    @Override
    public int getDefaultW() {
        return 0;
    }

    @Override
    public int getMaxBlockintThreadMultiplier() {
        return 0;
    }

    @Override
    public int getHeartbeatFrequency() {
        return 0;
    }

    @Override
    public void setHeartbeatSocketTimeout(int heartbeatSocketTimeout) {

    }

    @Override
    public void setUseSSL(boolean useSSL) {

    }

    @Override
    public void setHeartbeatFrequency(int heartbeatFrequency) {

    }

    @Override
    public void setWriteTimeout(int writeTimeout) {

    }

    @Override
    public void setDefaultBatchSize(int defaultBatchSize) {

    }

    @Override
    public void setCredentials(Map<String, String[]> credentials) {

    }

    @Override
    public int getHeartbeatSocketTimeout() {
        return 0;
    }

    @Override
    public boolean isUseSSL() {
        return false;
    }

    @Override
    public boolean isDefaultJ() {
        return false;
    }

    @Override
    public int getWriteTimeout() {
        return 0;
    }

    @Override
    public int getLocalThreshold() {
        return 0;
    }

    @Override
    public void setHostSeed(String... host) {

    }

    @Override
    public void setMaxConnectionsPerHost(int mx) {

    }

    @Override
    public void setMinConnectionsPerHost(int mx) {

    }

    @Override
    public void setMaxConnectionLifetime(int timeout) {

    }

    @Override
    public void setMaxConnectionIdleTime(int time) {

    }

    @Override
    public void setSocketTimeout(int timeout) {

    }

    @Override
    public void setConnectionTimeout(int timeout) {

    }

    @Override
    public void setDefaultW(int w) {

    }

    @Override
    public void setMaxBlockingThreadMultiplier(int m) {

    }

    @Override
    public void heartBeatFrequency(int t) {

    }

    @Override
    public void heartBeatSocketTimeout(int t) {

    }

    @Override
    public void useSsl(boolean ssl) {

    }

    @Override
    public void connect() throws MorphiumDriverException {
        try {
            s = new Socket("localhost", 27017);

            out = s.getOutputStream();
            in = s.getInputStream();

            //Reader
            new Thread() {
                public void run() {
                    byte[] inBuffer = new byte[4];
                    while (true) {
                        int numRead = 0;
                        try {

                            numRead = in.read(inBuffer, 0, 4);
                            int size = OpReply.readInt(inBuffer, 0);
                            byte buf[] = new byte[size];
                            buf[0] = inBuffer[0];
                            buf[1] = inBuffer[1];
                            buf[2] = inBuffer[2];
                            buf[3] = inBuffer[3];

                            numRead = in.read(buf, 4, size - 4);
                            OpReply reply = new OpReply();
                            try {
                                reply.parse(buf);
                                replies.add(reply);
                            } catch (Exception e) {
                                log.error("Could not read");
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }.start();
        } catch (IOException e) {
            throw new MorphiumDriverNetworkException("connection failed", e);
        }
    }

    @Override
    public void setDefaultReadPreference(ReadPreference rp) {

    }

    @Override
    public void connect(String replicasetName) throws MorphiumDriverException {
        connect();

    }


    @Override
    public Maximums getMaximums() {
        return null;
    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public void setDefaultJ(boolean j) {

    }

    @Override
    public void setDefaultWriteTimeout(int wt) {

    }

    @Override
    public void setLocalThreshold(int thr) {

    }

    @Override
    public void setDefaultFsync(boolean j) {

    }

    @Override
    public void setRetriesOnNetworkError(int r) {

    }

    @Override
    public int getRetriesOnNetworkError() {
        return 0;
    }

    @Override
    public void setSleepBetweenErrorRetries(int s) {

    }

    @Override
    public int getSleepBetweenErrorRetries() {
        return 0;
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

        OpQuery q = new OpQuery();
        q.setDb(db);
        q.setColl("$cmd");
        q.setLimit(1);
        q.setSkip(0);
        q.setReqId(++rqid);

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("find", collection);
        if (limit > 0)
            doc.put("limit", limit);
        doc.put("skip", skip);
        if (query.size() != 0)
            doc.put("filter", query);
        if (sort == null) sort = new HashMap<>();
        doc.put("sort", sort);

        q.setDoc(doc);
        q.setFlags(0);
        q.setInReplyTo(0);

        sendQuery(q);

        boolean wait = true;
        OpReply reply = null;
        int waitingfor = q.getReqId();
        List<Map<String, Object>> ret = new ArrayList<>();
        while (wait) {
            for (int i = 0; i < replies.size(); i++) {
                if (replies.get(i).getInReplyTo() == waitingfor) {
                    reply = replies.get(i);

                    Map<String, Object> cursor = (Map<String, Object>) reply.getDocuments().get(0).get("cursor");
                    if (cursor.get("firstBatch") != null) {
                        ret.addAll((List) cursor.get("firstBatch"));
                    } else if (cursor.get("nextBatch") != null) {
                        ret.addAll((List) cursor.get("nextBatch"));
                    }
                    if (((Long) cursor.get("id")) != 0) {
                        System.out.println("getting next batch for cursor " + cursor.get("id"));
                        //there is more! Sending getMore!
                        q = new OpQuery();
                        q.setColl("$cmd");
                        q.setDb(db);
                        q.setReqId(++rqid);
                        q.setSkip(0);
                        q.setLimit(1);
                        doc = new LinkedHashMap<>();
                        doc.put("getMore", cursor.get("id"));
                        doc.put("collection", collection);
                        doc.put("batchSize", batchSize);
                        q.setDoc(doc);
                        waitingfor = q.getReqId();
                        sendQuery(q);
                    } else {
                        wait = false;
                        break;
                    }
                }
            }
        }


        return ret;
    }

    private void sendQuery(OpQuery q) {
        try {
            out.write(q.bytes());
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long count(String db, String collection, Map<String, Object> query, ReadPreference rp) throws MorphiumDriverException {
        return 0;
    }

    @Override
    public void insert(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        int idx = 0;
        for (Map<String, Object> o : objs) {
            if (o.get("_id") == null) o.put("_id", new MorphiumId());
        }

        while (idx < objs.size()) {
            OpQuery op = new OpQuery();
            op.setInReplyTo(0);
            op.setReqId(++rqid);
            op.setDb(db);
            op.setColl("$cmd");
            HashMap<String, Object> map = new LinkedHashMap<>();
            map.put("insert", collection);

            List<Map<String, Object>> docs = new ArrayList<>();
            for (int i = idx; i < idx + 1000 && i < objs.size(); i++) {
                docs.add(objs.get(i));
            }
            idx += docs.size();
            map.put("documents", docs);
            map.put("ordered", false);
            map.put("writeConcern", new HashMap<String, Object>());
            op.setDoc(map);

            sendQuery(op);
        }
    }

    @Override
    public void store(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        OpQuery op = new OpQuery();
        op.setInReplyTo(0);
        op.setReqId(++rqid);
        op.setDb(db);
        op.setColl("$cmd");
        HashMap<String, Object> map = new LinkedHashMap<>();
        map.put("update", collection);
        List<Map<String, Object>> update = new ArrayList<>();
        for (Map<String, Object> o : objs) {
            if (o.get("_id") == null) o.put("_id", new MorphiumId());
            HashMap<String, Object> set = new HashMap<>();
            set.put("$set", o);
            update.add(set);
        }
        map.put("updates", update);
        map.put("ordered", false);
        map.put("writeConcern", new HashMap<String, Object>());
        op.setDoc(map);

        sendQuery(op);
    }

    @Override
    public Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> ops, boolean multiple, boolean upsert, WriteConcern wc) throws MorphiumDriverException {
        OpQuery op = new OpQuery();
        op.setInReplyTo(0);
        op.setReqId(++rqid);
        op.setDb(db);
        op.setColl("$cmd");
        HashMap<String, Object> map = new LinkedHashMap<>();
        map.put("update", collection);
        map.put("updates", ops);
        map.put("ordered", false);
        map.put("writeConcern", new HashMap<String, Object>());
        op.setDoc(map);

        sendQuery(op);
        return null;
    }

    @Override
    public Map<String, Object> delete(String db, String collection, Map<String, Object> query, boolean multiple, WriteConcern wc) throws MorphiumDriverException {
        OpQuery op = new OpQuery();
        op.setInReplyTo(0);
        op.setReqId(++rqid);
        op.setDb(db);
        op.setColl("$cmd");
        HashMap<String, Object> map = new LinkedHashMap<>();
        map.put("delete", collection);
        List<Map<String, Object>> cmd = new ArrayList<>();
        cmd.add(query);
        map.put("deletes", cmd);
        map.put("ordered", false);
        map.put("writeConcern", new HashMap<String, Object>());
        op.setDoc(map);

        sendQuery(op);
        return null;
    }

    @Override
    public void drop(String db, String collection, WriteConcern wc) throws MorphiumDriverException {
        OpQuery op = new OpQuery();
        op.setInReplyTo(0);
        op.setReqId(++rqid);

        op.setDb(db);
        op.setColl("$cmd");
        HashMap<String, Object> map = new LinkedHashMap<>();
        map.put("drop", collection);
        op.setDoc(map);
        sendQuery(op);

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
    public boolean isSocketKeepAlive() {
        return false;
    }

    @Override
    public void setSocketKeepAlive(boolean socketKeepAlive) {

    }

    @Override
    public int getHeartbeatConnectTimeout() {
        return 0;
    }

    @Override
    public void setHeartbeatConnectTimeout(int heartbeatConnectTimeout) {

    }

    @Override
    public int getMaxWaitTime() {
        return 0;
    }

    @Override
    public void setMaxWaitTime(int maxWaitTime) {

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
}
