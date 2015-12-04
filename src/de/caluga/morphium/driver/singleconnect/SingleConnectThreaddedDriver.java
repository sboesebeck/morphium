package de.caluga.morphium.driver.singleconnect;/**
 * Created by stephan on 30.11.15.
 */

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.wireprotocol.OpQuery;
import de.caluga.morphium.driver.wireprotocol.OpReply;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;
import java.util.stream.Collectors;

/**
 * TODO: Add Documentation here
 **/
public class SingleConnectThreaddedDriver extends DriverBase {

    private Logger log = new Logger(SingleConnectThreaddedDriver.class);
    private Socket s;
    private OutputStream out;
    private InputStream in;

    private Vector<OpReply> replies = new Vector<>();



    private void reconnect() {
        try {
            out.close();
            in.close();
            s.close();
            connect();
        } catch (Exception e) {
            s = null;
            in = null;
            out = null;
            e.printStackTrace();
        }
    }

    @Override
    public void connect(String replSet) throws MorphiumDriverException {
        try {
            s = new Socket("localhost", 27017);
            s.setKeepAlive(isSocketKeepAlive());
            s.setSoTimeout(getSocketTimeout());
            out = s.getOutputStream();
            in = s.getInputStream();


            //Reader
            new Thread() {
                public void run() {
                    byte[] inBuffer = new byte[16];
                    while (s != null && s.isConnected()) {
                        int numRead = 0;
                        try {

                            numRead = in.read(inBuffer, 0, 16);
                            int size = OpReply.readInt(inBuffer, 0);
                            if (size == 0) {
                                log.info("Error - null size!");
                                reconnect();
                                break;

                            }
                            if (size > 16 * 1024 * 1024) {
                                log.info("Error - size too big! " + size);
                                continue;
                            }
                            int opcode = OpReply.readInt(inBuffer, 12);
                            if (opcode != 1) {
                                log.info("illegal opcode! " + opcode);
                                continue;
                            }
                            byte buf[] = new byte[size];
                            for (int i = 0; i < 16; i++) {
                                buf[i] = inBuffer[i];
                            }

                            numRead = in.read(buf, 16, size - 16);
                            OpReply reply = new OpReply();
                            try {
                                reply.parse(buf);
                                if (!reply.getDocuments().get(0).get("ok").equals(1)) {
                                    if (reply.getDocuments().get(0).get("code") != null) {
                                        log.info("Error " + reply.getDocuments().get(0).get("code"));
                                        log.info("Error " + reply.getDocuments().get(0).get("errmsg"));
                                    }
                                }
                                replies.add(reply);
                            } catch (Exception e) {
                                log.error("Could not read");
                            }
                        } catch (IOException e) {
//                            e.printStackTrace();
                            break; //probably best!
                        }
                    }
                    log.info("reply-thread terminated!");
                }
            }.start();
            try {
                Map<String, Object> result = runCommand("local", Utils.getMap("isMaster", true));
                log.info("Got result");
                if (!result.get("ismaster").equals(true)) {
                    close();
                    throw new RuntimeException("Cannot run with secondary connection only!");
                }
                setReplicaSetName((String) result.get("setName"));
                if (replSet != null && !replSet.equals(getReplicaSetName())) {
                    throw new MorphiumDriverException("Replicaset name is wrong - connected to " + getReplicaSetName() + " should be " + replSet);
                }
                //"maxBsonObjectSize" : 16777216,
//                "maxMessageSizeBytes" : 48000000,
//                        "maxWriteBatchSize" : 1000,
                setMaxBsonObjectSize(((Integer) result.get("maxBsonObjectSize")).intValue());
                setMaxMessageSize(((Integer) result.get("maxMessageSizeBytes")).intValue());
                setMaxWriteBatchSize(((Integer) result.get("maxWriteBatchSize")).intValue());

            } catch (MorphiumDriverException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            throw new MorphiumDriverNetworkException("connection failed", e);
        }
    }

    @Override
    public void connect() throws MorphiumDriverException {
        connect(null);

    }



    @Override
    public boolean isConnected() {
        return s != null && s.isConnected();
    }


    @Override
    public void close() throws MorphiumDriverException {
        try {
            s.close();
            s = null;
            out.close();
            in.close();
        } catch (Exception e) {
        }
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
        return new NetworkCallHelper().doCall(new MorphiumDriverOperation() {
            @Override
            public Map<String, Object> execute() {
                OpQuery q = new OpQuery();
                q.setDb(db);
                q.setColl("$cmd");
                q.setLimit(1);
                q.setSkip(0);
                q.setReqId(getNextId());

                Map<String, Object> doc = new LinkedHashMap<>();
                doc.putAll(cmd);
                q.setDoc(doc);
                q.setFlags(0);
                q.setInReplyTo(0);

                sendQuery(q);
                OpReply rep = null;
                try {
                    rep = waitForReply(db, null, null, q.getReqId());
                } catch (MorphiumDriverException e) {
                    e.printStackTrace();
                }
                if (rep == null || rep.getDocuments() == null) return null;
                return rep.getDocuments().get(0);
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());

    }

    @Override
    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> s, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference rp, Map<String, Object> findMetaData) throws MorphiumDriverException {
        if (s == null) s = new HashMap<>();
        final Map<String, Integer> sort = s;
        return (List<Map<String, Object>>) new NetworkCallHelper().doCall(new MorphiumDriverOperation() {
            @Override
            public Map<String, Object> execute() throws MorphiumDriverNetworkException {
                OpQuery q = new OpQuery();
                q.setDb(db);
                q.setColl("$cmd");
                q.setLimit(1);
                q.setSkip(0);
                q.setReqId(getNextId());

                Map<String, Object> doc = new LinkedHashMap<>();
                doc.put("find", collection);
                if (limit > 0)
                    doc.put("limit", limit);
                doc.put("skip", skip);
                if (query.size() != 0)
                    doc.put("filter", query);
                doc.put("sort", sort);

                q.setDoc(doc);
                q.setFlags(0);
                q.setInReplyTo(0);

                sendQuery(q);

                boolean wait = true;
                OpReply reply = null;
                int waitingfor = q.getReqId();
                List<Map<String, Object>> ret = readBatches(waitingfor, db, collection, batchSize);
                return Utils.getMap("values", ret);
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("values");

    }

    private List<Map<String, Object>> readBatches(int waitingfor, String db, String collection, int batchSize) throws MorphiumDriverNetworkException {
        List<Map<String, Object>> ret = new ArrayList<>();
        OpReply reply;
        OpQuery q;
        Map<String, Object> doc;
        long start = System.currentTimeMillis();
        boolean wait = true;
        while (wait) {
            for (int i = 0; i < replies.size(); i++) {
                if (replies.get(i).getInReplyTo() == waitingfor) {
                    reply = replies.get(i);
//                    replies.remove(i);
                    Map<String, Object> cursor = (Map<String, Object>) reply.getDocuments().get(0).get("cursor");
                    if (cursor.get("firstBatch") != null) {
                        ret.addAll((List) cursor.get("firstBatch"));
                    } else if (cursor.get("nextBatch") != null) {
                        ret.addAll((List) cursor.get("nextBatch"));
                    }
                    if (((Long) cursor.get("id")) != 0) {
//                        log.info("getting next batch for cursor " + cursor.get("id"));
                        //there is more! Sending getMore!
                        //there is more! Sending getMore!
                        q = new OpQuery();
                        q.setColl("$cmd");
                        q.setDb(db);
                        q.setReqId(getNextId());
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
            if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                throw new MorphiumDriverNetworkException("Did not get message in time", null);
            }
        }
        return ret;
    }

    private synchronized void sendQuery(OpQuery q) {
        try {
            out.write(q.bytes());
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long count(String db, String collection, Map<String, Object> query, ReadPreference rp) throws MorphiumDriverException {
        OpQuery q = new OpQuery();
        q.setDb(db);
        q.setColl("$cmd");
        q.setLimit(1);
        q.setSkip(0);
        q.setReqId(getNextId());

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("count", collection);
        doc.put("query", query);
        q.setDoc(doc);
        q.setFlags(0);
        q.setInReplyTo(0);

        sendQuery(q);
        OpReply rep = waitForReply(db, collection, query, q.getReqId());
        return (Integer) rep.getDocuments().get(0).get("n");
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
            op.setReqId(getNextId());
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
            waitForReply(db, collection, null, op.getReqId());
        }
    }

    @Override
    public void store(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        List<Map<String, Object>> toInsert = new ArrayList<>();
        OpQuery op = new OpQuery();
        op.setInReplyTo(0);
        op.setReqId(getNextId());
        op.setDb(db);
        op.setColl("$cmd");
        HashMap<String, Object> map = new LinkedHashMap<>();
        map.put("update", collection);
        List<Map<String, Object>> update = new ArrayList<>();
        for (Map<String, Object> o : objs) {
            if (o.get("_id") == null) {
                toInsert.add(o);
            } else {
                HashMap<String, Object> set = new HashMap<>();
                set.put("$set", o);
                set.put("upsert", true);
                set.put("multiple", false);
                update.add(set);
            }
        }
        map.put("updates", update);
        map.put("ordered", false);
        map.put("writeConcern", new HashMap<String, Object>());
        op.setDoc(map);

        if (update.size() != 0) {
            sendQuery(op);

            OpReply reply = waitForReply(db, collection, null, op.getReqId());
        }
        insert(db, collection, toInsert, wc);

    }

    @Override
    public Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> ops, boolean multiple, boolean upsert, WriteConcern wc) throws MorphiumDriverException {
        OpQuery op = new OpQuery();
        op.setInReplyTo(0);
        op.setReqId(getNextId());
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
        op.setColl("$cmd");
        op.setDb(db);
        op.setReqId(getNextId());
        op.setLimit(-1);

        Map<String, Object> o = new LinkedHashMap<>();
        o.put("delete", collection);
        o.put("ordered", false);
        Map<String, Object> wrc = new LinkedHashMap<>();
        wrc.put("w", 1);
        wrc.put("wtimeout", 1000);
        wrc.put("fsync", false);
        wrc.put("j", true);
        o.put("writeConcern", wrc);

        Map<String, Object> q = new LinkedHashMap<>();
        q.put("q", query);
        q.put("limit", 0);
        List<Map<String, Object>> del = new ArrayList<>();
        del.add(q);

        o.put("deletes", del);
        op.setDoc(o);

        sendQuery(op);

        OpReply reply = null;
        int waitingfor = op.getReqId();
        if (wc == null || wc.getW() == 0) {
            reply = waitForReply(db, collection, query, waitingfor);
        }
        return null;
    }

    private OpReply waitForReply(String db, String collection, Map<String, Object> query, int waitingfor) throws MorphiumDriverException {
        OpReply reply = null;
        boolean wait = true;
        long start = System.currentTimeMillis();
        while (wait) {
            for (int i = 0; i < replies.size(); i++) {
                reply = replies.get(i);
//                replies.remove(i);
                if (reply.getInReplyTo() == waitingfor) {
                    if (!reply.getDocuments().get(0).get("ok").equals(1) && !reply.getDocuments().get(0).get("ok").equals(1.0)) {
                        Object code = reply.getDocuments().get(0).get("code");
                        Object errmsg = reply.getDocuments().get(0).get("errmsg");
                        throw new MorphiumDriverException("Operation failed - error: " + code + "= " + errmsg, null, collection, db, query);
                    } else {
                        //got OK message
//                        log.info("ok");
                    }
                    wait = false;
                }
            }
            if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                throw new MorphiumDriverNetworkException("could not get message " + waitingfor + " in time (" + getMaxWaitTime() + "ms)", null);
            }
            Thread.yield();
        }
        return reply;
    }

    @Override
    public void drop(String db, String collection, WriteConcern wc) throws MorphiumDriverException {
        OpQuery op = new OpQuery();
        op.setInReplyTo(0);
        op.setReqId(getNextId());

        op.setDb(db);
        op.setColl("$cmd");
        HashMap<String, Object> map = new LinkedHashMap<>();
        map.put("drop", collection);
        op.setDoc(map);
        sendQuery(op);
        try {
            waitForReply(db, collection, null, op.getReqId());
        } catch (Exception e) {
            log.error("Drop failed! " + e.getMessage());
        }
    }

    @Override
    public void drop(String db, WriteConcern wc) throws MorphiumDriverException {
        OpQuery op = new OpQuery();
        op.setInReplyTo(0);
        op.setReqId(getNextId());

        op.setDb(db);
        op.setColl("$cmd");
        HashMap<String, Object> map = new LinkedHashMap<>();
        map.put("drop", 1);
        op.setDoc(map);
        sendQuery(op);
        try {
            waitForReply(db, null, null, op.getReqId());
        } catch (Exception e) {
            log.error("Drop failed! " + e.getMessage());
        }
    }

    @Override
    public boolean exists(String db) throws MorphiumDriverException {
        return false;
    }

    @Override
    public List<Object> distinct(String db, String collection, String field, Map<String, Object> filter, ReadPreference rp) throws MorphiumDriverException {
        return null;
    }

    @Override
    public boolean exists(String db, String collection) throws MorphiumDriverException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("listCollections", 1);
        OpQuery q = new OpQuery();
        q.setDb(db);
        q.setColl("$cmd");
        q.setLimit(1);
        q.setSkip(0);
        q.setReqId(getNextId());

        q.setDoc(cmd);
        q.setFlags(0);
        q.setInReplyTo(0);

        sendQuery(q);

        List<Map<String, Object>> ret = readBatches(q.getReqId(), db, null, getMaxWriteBatchSize());
        for (Map<String, Object> c : ret) {
            if (c.get("name").equals(collection)) return true;
        }
        return false;
    }

    @Override
    public List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException {
        return null;
    }

    @Override
    public List<String> getCollectionNames(String db) throws MorphiumDriverException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("listCollections", 1);
        OpQuery q = new OpQuery();
        q.setDb(db);
        q.setColl("$cmd");
        q.setLimit(1);
        q.setSkip(0);
        q.setReqId(getNextId());

        q.setDoc(cmd);
        q.setFlags(0);
        q.setInReplyTo(0);

        sendQuery(q);

        List<Map<String, Object>> ret = readBatches(q.getReqId(), db, null, getMaxWriteBatchSize());
        return ret.stream().map(c -> (String) c.get("name")).collect(Collectors.toList());
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
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("createIndexes", collection);
        List<Map<String, Object>> lst = new ArrayList<>();
        Map<String, Object> idx = new HashMap<>();
        idx.put("key", index);
        String key = "";
        for (String k : index.keySet()) {
            key += k;
            key += "-";
            key += index.get(k);
        }

        idx.put("name", "idx_" + key);
        if (options != null)
            idx.putAll(options);
        lst.add(idx);
        cmd.put("indexes", lst);
        runCommand(db, cmd);

    }
}
