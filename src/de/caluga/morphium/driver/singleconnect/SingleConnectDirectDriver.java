package de.caluga.morphium.driver.singleconnect;

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
 * User: Stephan Bösebeck
 * Date: 02.12.15
 * Time: 23:47
 * <p>
 * connects to one node only!
 */
public class SingleConnectDirectDriver extends DriverBase {

    private final Logger log = new Logger(SingleConnectThreaddedDriver.class);
    private Socket s;
    private OutputStream out;
    private InputStream in;

    //    private Vector<OpReply> replies = new Vector<>();


    private void reconnect() {
        try {
            if (out != null) {
                out.close();
            }
            if (in != null) {
                in.close();
            }
            if (s != null) {
                s.close();
            }
            connect();
        } catch (Exception e) {
            s = null;
            in = null;
            out = null;
            log.fatal("Could not reconnect!", e);
        }
    }

    @Override
    public void connect(String replSet) throws MorphiumDriverException {
        try {
            String host = getHostSeed()[0];
            String h[] = host.split(":");
            int port = 27017;
            if (h.length > 1) {
                port = Integer.parseInt(h[1]);
            }
            s = new Socket(h[0], port);
            out = s.getOutputStream();
            in = s.getInputStream();


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
                setMaxBsonObjectSize((Integer) result.get("maxBsonObjectSize"));
                setMaxMessageSize((Integer) result.get("maxMessageSizeBytes"));
                setMaxWriteBatchSize((Integer) result.get("maxWriteBatchSize"));

            } catch (MorphiumDriverException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            throw new MorphiumDriverNetworkException("connection failed", e);
        }
    }

    private synchronized OpReply getReply() throws MorphiumDriverNetworkException {
        byte[] inBuffer = new byte[16];
        int numRead;
        try {

            numRead = in.read(inBuffer, 0, 16);
            while (numRead < 16) {
                numRead += in.read(inBuffer, numRead, 16 - numRead);
            }
            int size = OpReply.readInt(inBuffer, 0);
            if (size == 0) {
                log.info("Error - null size!");
                throw new MorphiumDriverNetworkException("Got garbage message!");
            }
            if (size > getMaxMessageSize()) {
                log.error("Error - size too big! " + size);
                throw new MorphiumDriverNetworkException("Got garbage message - size too big!");
            }
            int opcode = OpReply.readInt(inBuffer, 12);
            if (opcode != 1) {
                log.info("illegal opcode! " + opcode);
                throw new MorphiumDriverNetworkException("Illegal opcode should be 1 but is " + opcode);
            }
            byte buf[] = new byte[size];
            System.arraycopy(inBuffer, 0, buf, 0, 16);

            numRead = in.read(buf, 16, size - 16);
            while (numRead < size - 16) {
                numRead += in.read(buf, 16 + numRead, size - 16 - numRead);
            }
            OpReply reply = new OpReply();
            try {
                reply.parse(buf);
                if (!reply.getDocuments().get(0).get("ok").equals(1)) {
                    if (reply.getDocuments().get(0).get("code") != null) {
                        log.info("Error " + reply.getDocuments().get(0).get("code"));
                        log.info("Error " + reply.getDocuments().get(0).get("errmsg"));
                    }
                }
                return reply;
            } catch (Exception e) {
                log.error("Could not read");
                throw new MorphiumDriverNetworkException("could not read from socket", e);
            }
        } catch (IOException e) {
            throw new MorphiumDriverNetworkException("could not read from socket", e);
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
        //noinspection EmptyCatchBlock
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
        return new NetworkCallHelper().doCall(() -> {
            Map<String, Object> ret = runCommand("admin", Utils.getMap("replSetGetStatus", 1));
            @SuppressWarnings("unchecked") List<Map<String, Object>> mem = (List) ret.get("members");
            if (mem == null) {
                return null;
            }
            //noinspection unchecked
            mem.stream().filter(d -> d.get("optime") instanceof Map).forEach(d -> d.put("optime", ((Map<String, Map<String, Object>>) d.get("optime")).get("ts")));
            return ret;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        return runCommand(db, Utils.getMap("dbstats", 1));
    }

    @Override
    public Map<String, Object> getOps(long threshold) throws MorphiumDriverException {
        return null;
    }

    @Override
    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
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

            OpReply rep = null;
            synchronized (SingleConnectDirectDriver.this) {
                sendQuery(q);
                try {
                    rep = waitForReply(db, null, null, q.getReqId());
                } catch (MorphiumDriverException e) {
                    e.printStackTrace();
                }
            }
            if (rep == null || rep.getDocuments() == null) {
                return null;
            }
            return rep.getDocuments().get(0);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());

    }

    @Override
    public MorphiumCursor initIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, Map<String, Object> findMetaData) throws MorphiumDriverException {
        if (sort == null) {
            sort = new HashMap<>();
        }
        OpQuery q = new OpQuery();
        q.setDb(db);
        q.setColl("$cmd");
        q.setLimit(1);
        q.setSkip(0);
        q.setReqId(getNextId());

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("find", collection);
        if (limit > 0) {
            doc.put("limit", limit);
        }
        doc.put("skip", skip);
        if (!query.isEmpty()) {
            doc.put("filter", query);
        }
        doc.put("sort", sort);
        doc.put("batchSize", batchSize);

        q.setDoc(doc);
        q.setFlags(0);
        q.setInReplyTo(0);

        OpReply reply;
        synchronized (SingleConnectDirectDriver.this) {
            sendQuery(q);

            int waitingfor = q.getReqId();
            reply = getReply();
            if (reply.getInReplyTo() != waitingfor) {
                throw new MorphiumDriverNetworkException("Got wrong answser. Request: " + waitingfor + " got answer for " + reply.getInReplyTo());
            }

        }

        MorphiumCursor crs = new MorphiumCursor();
        @SuppressWarnings("unchecked") Map<String, Object> cursor = (Map<String, Object>) reply.getDocuments().get(0).get("cursor");
        if (cursor != null && cursor.get("id") != null) {
            crs.setCursorId((Long) cursor.get("id"));
        }

        if (cursor != null) {
            if (cursor.get("firstBatch") != null) {
                //noinspection unchecked
                crs.setBatch((List) cursor.get("firstBatch"));
            } else if (cursor.get("nextBatch") != null) {
                //noinspection unchecked
                crs.setBatch((List) cursor.get("nextBatch"));
            }
        }

        SingleConnectCursor internalCursorData = new SingleConnectCursor(this);
        internalCursorData.setBatchSize(batchSize);
        internalCursorData.setCollection(collection);
        internalCursorData.setDb(db);
        //noinspection unchecked
        crs.setInternalCursorObject(internalCursorData);
        return crs;


    }

    @Override
    public MorphiumCursor nextIteration(MorphiumCursor crs) throws MorphiumDriverException {
        OpReply reply;
        OpQuery q;
        long cursorId = crs.getCursorId();
        SingleConnectCursor internalCursorData = (SingleConnectCursor) crs.getInternalCursorObject();

        if (cursorId == 0) {
            return null;
        }
        synchronized (SingleConnectDirectDriver.this) {
            q = new OpQuery();
            q.setColl("$cmd");
            q.setDb(internalCursorData.getDb());
            q.setReqId(getNextId());
            q.setSkip(0);
            q.setLimit(1);
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("getMore", cursorId);
            doc.put("collection", internalCursorData.getCollection());
            doc.put("batchSize", internalCursorData.getBatchSize());
            q.setDoc(doc);

            sendQuery(q);
            reply = getReply();
        }
        crs = new MorphiumCursor();
        //noinspection unchecked
        crs.setInternalCursorObject(internalCursorData);
        @SuppressWarnings("unchecked") Map<String, Object> cursor = (Map<String, Object>) reply.getDocuments().get(0).get("cursor");
        if (cursor == null) {
            //cursor not found
            throw new MorphiumDriverException("Iteration failed! Error: " + reply.getDocuments().get(0).get("code") + "  Message: " + reply.getDocuments().get(0).get("errmsg"));
        }
        if (cursor.get("id") != null) {
            crs.setCursorId((Long) cursor.get("id"));
        }
        if (cursor.get("firstBatch") != null) {
            //noinspection unchecked
            crs.setBatch((List) cursor.get("firstBatch"));
        } else if (cursor.get("nextBatch") != null) {
            //noinspection unchecked
            crs.setBatch((List) cursor.get("nextBatch"));
        }

        return crs;
    }

    @Override
    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        if (crs == null) {
            return;
        }
        SingleConnectCursor internalCursor = (SingleConnectCursor) crs.getInternalCursorObject();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("killCursors", internalCursor.getCollection());
        List<Long> cursors = new ArrayList<>();
        cursors.add(crs.getCursorId());
        m.put("cursors", cursors);

    }

    @Override
    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> s, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference rp, Map<String, Object> findMetaData) throws MorphiumDriverException {
        if (s == null) {
            s = new HashMap<>();
        }
        final Map<String, Integer> sort = s;
        //noinspection unchecked
        return (List<Map<String, Object>>) new NetworkCallHelper().doCall(() -> {
            OpQuery q = new OpQuery();
            q.setDb(db);
            q.setColl("$cmd");
            q.setLimit(1);
            q.setSkip(0);
            q.setReqId(getNextId());

            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("find", collection);
            if (limit > 0) {
                doc.put("limit", limit);
            }
            doc.put("skip", skip);
            if (!query.isEmpty()) {
                doc.put("filter", query);
            }
            doc.put("sort", sort);

            q.setDoc(doc);
            q.setFlags(0);
            q.setInReplyTo(0);

            List<Map<String, Object>> ret = null;
            synchronized (SingleConnectDirectDriver.this) {
                sendQuery(q);

                OpReply reply = null;
                int waitingfor = q.getReqId();
                ret = readBatches(waitingfor, db, collection, batchSize);
            }
            return Utils.getMap("values", ret);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("values");

    }


    private List<Map<String, Object>> readBatches(int waitingfor, String db, String collection, int batchSize) throws MorphiumDriverException {
        List<Map<String, Object>> ret = new ArrayList<>();
        OpReply reply;
        OpQuery q;
        Map<String, Object> doc;
        synchronized (SingleConnectDirectDriver.this) {
            while (true) {
                reply = getReply();
                if (reply.getInReplyTo() != waitingfor) {
                    throw new MorphiumDriverNetworkException("Wrong answer - waiting for " + waitingfor + " but got " + reply.getInReplyTo());
                }
                //                    replies.remove(i);
                @SuppressWarnings("unchecked") Map<String, Object> cursor = (Map<String, Object>) reply.getDocuments().get(0).get("cursor");
                if (cursor == null) {
                    //trying result
                    if (reply.getDocuments().get(0).get("result") != null) {
                        //noinspection unchecked
                        return (List<Map<String, Object>>) reply.getDocuments().get(0).get("result");
                    }
                    throw new MorphiumDriverException("did not get any data, cursor == null!");
                }
                if (cursor.get("firstBatch") != null) {
                    //noinspection unchecked
                    ret.addAll((List) cursor.get("firstBatch"));
                } else if (cursor.get("nextBatch") != null) {
                    //noinspection unchecked
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
                    break;
                }
            }
        }
        return ret;
    }

    private void sendQuery(OpQuery q) throws MorphiumDriverException {
        boolean retry = true;
        long start = System.currentTimeMillis();
        while (retry) {
            try {
                if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                    throw new MorphiumDriverException("Could not send message! Timeout!");
                }
                out.write(q.bytes());
                out.flush();
                retry = false;
            } catch (IOException e) {
                log.error("Error sending request - reconnecting", e);
                reconnect();

            }
        }
    }

    @Override
    public long count(String db, String collection, Map<String, Object> query, ReadPreference rp) throws MorphiumDriverException {
        Map<String, Object> ret = new NetworkCallHelper().doCall(() -> {
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

            OpReply rep = null;
            synchronized (SingleConnectDirectDriver.this) {
                sendQuery(q);
                rep = waitForReply(db, collection, query, q.getReqId());
            }
            Integer n = (Integer) rep.getDocuments().get(0).get("n");
            return Utils.getMap("count", n == null ? 0 : n);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
        return ((int) ret.get("count"));
    }

    @Override
    public void insert(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            int idx = 0;
            objs.forEach(o -> o.putIfAbsent("_id", new MorphiumId()));

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

                synchronized (SingleConnectDirectDriver.this) {
                    sendQuery(op);
                    waitForReply(db, collection, null, op.getReqId());
                }
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public void store(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            List<Map<String, Object>> toInsert = new ArrayList<>();
            List<Map<String, Object>> toUpdate = new ArrayList<>();
            List<Map<String, Object>> update = new ArrayList<>();
            for (Map<String, Object> o : objs) {
                if (o.get("_id") == null) {
                    toInsert.add(o);
                } else {
                    toUpdate.add(o);
                }
            }
            List<Map<String, Object>> updateCmd = new ArrayList<>();
            for (Map<String, Object> obj : toUpdate) {
                Map<String, Object> up = new HashMap<>();
                up.put("q", Utils.getMap("_id", obj.get("_id")));
                up.put("u", obj);
                up.put("upsert", true);
                up.put("multi", false);

                updateCmd.add(up);
            }
            if (!updateCmd.isEmpty()) {
                update(db, collection, updateCmd, false, wc);
            }

            if (!toInsert.isEmpty()) {
                insert(db, collection, toInsert, wc);
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());

    }

    @Override
    public Map<String, Object> update(String db, String collection, Map<String, Object> query, Map<String, Object> ops, boolean multiple, boolean upsert, WriteConcern wc) throws MorphiumDriverException {
        List<Map<String, Object>> opsLst = new ArrayList<>();
        Map<String, Object> up = new HashMap<>();
        up.put("q", query);
        up.put("u", ops);
        up.put("upsert", upsert);
        up.put("multi", multiple);
        opsLst.add(up);
        return update(db, collection, opsLst, false, wc);
    }

    public Map<String, Object> update(String db, String collection, List<Map<String, Object>> updateCommand, boolean ordered, WriteConcern wc) throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            OpQuery op = new OpQuery();
            op.setInReplyTo(0);
            op.setReqId(getNextId());
            op.setDb(db);
            op.setColl("$cmd");
            HashMap<String, Object> map = new LinkedHashMap<>();
            map.put("update", collection);
            map.put("updates", updateCommand);
            map.put("ordered", false);
            map.put("writeConcern", new HashMap<String, Object>());
            op.setDoc(map);
            synchronized (SingleConnectDirectDriver.this) {
                sendQuery(op);
                if (wc != null) {
                    OpReply res = waitForReply(db, collection, null, op.getReqId());
                    return res.getDocuments().get(0);
                }
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public Map<String, Object> delete(String db, String collection, Map<String, Object> query,
                                      boolean multiple, WriteConcern wc) throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
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


            synchronized (SingleConnectDirectDriver.this) {
                sendQuery(op);

                OpReply reply = null;
                int waitingfor = op.getReqId();
                //        if (wc == null || wc.getW() == 0) {
                reply = waitForReply(db, collection, query, waitingfor);
                //        }
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private OpReply waitForReply(String db, String collection, Map<String, Object> query, int waitingfor) throws MorphiumDriverException {
        OpReply reply;
        reply = getReply();
        //                replies.remove(i);
        if (reply.getInReplyTo() == waitingfor) {
            if (!reply.getDocuments().get(0).get("ok").equals(1) && !reply.getDocuments().get(0).get("ok").equals(1.0)) {
                Object code = reply.getDocuments().get(0).get("code");
                Object errmsg = reply.getDocuments().get(0).get("errmsg");
                //                throw new MorphiumDriverException("Operation failed - error: " + code + " - " + errmsg, null, collection, db, query);
                MorphiumDriverException mde = new MorphiumDriverException("Operation failed on " + getHostSeed()[0] + " - error: " + code + " - " + errmsg, null, collection, db, query);
                mde.setMongoCode(code);
                mde.setMongoReason(errmsg);

                throw mde;

            } else {
                //got OK message
                //                        log.info("ok");
            }
        }

        return reply;
    }

    @Override
    public void drop(String db, String collection, WriteConcern wc) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            OpQuery op = new OpQuery();
            op.setInReplyTo(0);
            op.setReqId(getNextId());

            op.setDb(db);
            op.setColl("$cmd");
            HashMap<String, Object> map = new LinkedHashMap<>();
            map.put("drop", collection);
            op.setDoc(map);
            synchronized (SingleConnectDirectDriver.this) {
                sendQuery(op);
                try {
                    waitForReply(db, collection, null, op.getReqId());
                } catch (Exception e) {
                    log.error("Drop failed! " + e.getMessage());
                }
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public void drop(String db, WriteConcern wc) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            OpQuery op = new OpQuery();
            op.setInReplyTo(0);
            op.setReqId(getNextId());

            op.setDb(db);
            op.setColl("$cmd");
            HashMap<String, Object> map = new LinkedHashMap<>();
            map.put("drop", 1);
            op.setDoc(map);
            synchronized (SingleConnectDirectDriver.this) {
                sendQuery(op);
                try {
                    waitForReply(db, null, null, op.getReqId());
                } catch (Exception e) {
                    log.error("Drop failed! " + e.getMessage());
                }
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public boolean exists(String db) throws MorphiumDriverException {
        //noinspection EmptyCatchBlock
        try {
            getDBStats(db);
            return true;
        } catch (MorphiumDriverException e) {
        }
        return false;
    }

    @Override
    public List<Object> distinct(String db, String collection, String field, Map<String, Object> filter, ReadPreference rp) throws MorphiumDriverException {
        Map<String, Object> ret = new NetworkCallHelper().doCall(() -> {
            OpQuery op = new OpQuery();
            op.setColl("$cmd");
            op.setLimit(1);
            op.setReqId(getNextId());
            op.setSkip(0);

            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("distinct", collection);
            cmd.put("field", field);
            cmd.put("query", filter);
            op.setDoc(cmd);

            synchronized (SingleConnectDirectDriver.this) {
                sendQuery(op);
                //noinspection EmptyCatchBlock
                try {
                    OpReply res = waitForReply(db, null, null, op.getReqId());
                    log.fatal("Need to implement distinct");
                } catch (Exception e) {

                }
            }

            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
        //noinspection unchecked
        return (List<Object>) ret.get("result");
    }

    @Override
    public boolean exists(String db, String collection) throws MorphiumDriverException {
        List<Map<String, Object>> ret = getCollectionInfo(db, collection);
        for (Map<String, Object> c : ret) {
            if (c.get("name").equals(collection)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException {
        //noinspection unchecked
        return (List<Map<String, Object>>) new NetworkCallHelper().doCall(() -> {
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("listIndexes", 1);
            OpQuery q = new OpQuery();
            q.setDb(db);
            q.setColl("$cmd");
            q.setLimit(1);
            q.setSkip(0);
            q.setReqId(getNextId());

            q.setDoc(cmd);
            q.setFlags(0);
            q.setInReplyTo(0);

            List<Map<String, Object>> ret;
            synchronized (SingleConnectDirectDriver.this) {
                sendQuery(q);

                ret = readBatches(q.getReqId(), db, null, getMaxWriteBatchSize());
            }
            return Utils.getMap("result", ret);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("result");
    }

    @Override
    public List<String> getCollectionNames(String db) throws MorphiumDriverException {
        List<Map<String, Object>> ret = getCollectionInfo(db, null);
        return ret.stream().map(c -> (String) c.get("name")).collect(Collectors.toList());
    }

    private List<Map<String, Object>> getCollectionInfo(String db, String collection) throws MorphiumDriverException {
        //noinspection unchecked
        return (List<Map<String, Object>>) new NetworkCallHelper().doCall(() -> {
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("listCollections", 1);
            OpQuery q = new OpQuery();
            q.setDb(db);
            q.setColl("$cmd");
            q.setLimit(1);
            q.setSkip(0);
            q.setReqId(getNextId());

            if (collection != null) {
                cmd.put("filter", Utils.getMap("name", collection));
            }
            q.setDoc(cmd);
            q.setFlags(0);
            q.setInReplyTo(0);

            List<Map<String, Object>> ret;
            synchronized (SingleConnectDirectDriver.this) {
                sendQuery(q);

                ret = readBatches(q.getReqId(), db, null, getMaxWriteBatchSize());
            }
            return Utils.getMap("result", ret);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("result");
    }

    @Override
    public Map<String, Object> group(String db, String coll, Map<String, Object> query, Map<String, Object> initial, String jsReduce, String jsFinalize, ReadPreference rp, String... keys) throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            OpQuery q = new OpQuery();
            q.setDb(db);
            q.setColl("$cmd");
            q.setReqId(getNextId());
            q.setSkip(0);
            q.setLimit(1);

            @SuppressWarnings("MismatchedQueryAndUpdateOfCollection") Map<String, Object> cmd = new LinkedHashMap<>();
            Map<String, Object> map = Utils.getMap("ns", coll);

            Map<String, Object> key = new HashMap<>();
            for (String k : keys) key.put(k, 1);
            map.put("key", key);
            if (jsReduce != null) {
                map.put("$reduce", jsReduce);
            }

            if (jsFinalize != null) {
                map.put("finalize", jsFinalize);
            }
            if (initial != null) {
                map.put("initial", initial);
            }
            if (query != null) {
                map.put("cond", query);
            }

            cmd.put("group", map);

            synchronized (SingleConnectDirectDriver.this) {
                try {
                    sendQuery(q);
                } catch (MorphiumDriverException e) {
                    log.error("Sending of message failed: ", e);
                    return null;
                }
                //noinspection EmptyCatchBlock
                try {
                    OpReply res = waitForReply(db, coll, query, q.getReqId());
                } catch (MorphiumDriverException e) {

                }
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, ReadPreference readPreference) throws MorphiumDriverException {
        //noinspection unchecked
        return (List<Map<String, Object>>) new NetworkCallHelper().doCall(() -> {
            OpQuery q = new OpQuery();
            q.setDb(db);
            q.setColl("$cmd");
            q.setReqId(getNextId());
            q.setSkip(0);
            q.setLimit(1);

            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("aggregate", collection);
            doc.put("pipeline", pipeline);
            doc.put("explain", explain);
            doc.put("allowDiskUse", allowDiskUse);

            q.setDoc(doc);

            synchronized (SingleConnectDirectDriver.this) {
                sendQuery(q);
                List<Map<String, Object>> lst = readBatches(q.getReqId(), db, collection, getMaxWriteBatchSize());
                return Utils.getMap("result", lst);
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("result");
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
            log.error(e);
        }
        return false;
    }

    @Override
    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return null;
    }

    @Override
    public void createIndex(String db, String collection, Map<String, Object> index, Map<String, Object> options) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("createIndexes", collection);
            List<Map<String, Object>> lst = new ArrayList<>();
            Map<String, Object> idx = new HashMap<>();
            idx.put("key", index);
            StringBuilder stringBuilder = new StringBuilder();
            for (String k : index.keySet()) {
                stringBuilder.append(k);
                stringBuilder.append("-");
                stringBuilder.append(index.get(k));
            }

            idx.put("name", "idx_" + stringBuilder.toString());
            if (options != null) {
                idx.putAll(options);
            }
            lst.add(idx);
            cmd.put("indexes", lst);
            runCommand(db, cmd);
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());

    }


}
