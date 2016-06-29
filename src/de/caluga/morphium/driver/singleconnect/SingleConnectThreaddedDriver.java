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
 * connects to one node only, creates a thread for reading data from mongo. Is  theradsafe.
 **/
public class SingleConnectThreaddedDriver extends DriverBase {

    private final List<OpReply> replies = Collections.synchronizedList(new ArrayList<>());
    private final Logger log = new Logger(SingleConnectThreaddedDriver.class);
    private Socket s;
    private OutputStream out;
    private InputStream in;
    private volatile int waitingForReply = 0;


    private void reconnect() throws MorphiumDriverException {
        try {
            out.close();
            in.close();
            s.close();

        } catch (Exception e) {
            s = null;
            in = null;
            out = null;
            //            e.printStackTrace();
        }
        connect();
    }

    @Override
    public void connect(String replSet) throws MorphiumDriverException {
        if (s != null && s.isConnected() && !s.isClosed()) {
            log.error("Already connected! not reconnecting!");
            return;
        }
        try {
            String host = getHostSeed()[0];
            String h[] = host.split(":");
            int port = 27017;
            if (h.length > 1) {
                port = Integer.parseInt(h[1]);
            }
            s = new Socket(h[0], port);
            s.setKeepAlive(isSocketKeepAlive());
            s.setSoTimeout(getSocketTimeout());

            out = s.getOutputStream();
            in = s.getInputStream();


            //Reader
            Thread reader = new Thread() {
                @Override
                public void run() {
                    byte[] inBuffer = new byte[16];
                    int errorcount = 0;
                    while (s != null && s.isConnected()) {
                        int numRead;
                        try {

                            numRead = in.read(inBuffer, 0, 16);
                            while (numRead < 16) {
                                numRead += in.read(inBuffer, numRead, 16 - numRead);
                            }

                            int size = OpReply.readInt(inBuffer, 0);
                            //                            log.info("Got size: " + Utils.getHex(size));
                            if (size == 0) {
                                log.error("Error - null size! closing connection");
                                System.exit(1);
                                break;

                            }
                            if (size > 16 * 1024 * 1024) {
                                log.error("Error - size too big! " + size);
                                System.exit(1);
                                break;
                            }
                            int opcode = OpReply.readInt(inBuffer, 12);
                            if (opcode != 1) {
                                log.error("illegal opcode! " + opcode);
                                System.exit(1);
                                break;
                            }
                            byte buf[] = new byte[size];
                            System.arraycopy(inBuffer, 0, buf, 0, 16);

                            numRead = in.read(buf, 16, size - 16);
                            while (numRead < size - 16) {
                                numRead += in.read(buf, 16 + numRead, size - numRead - 16);
                            }
                            //                            log.info("Read:        " + Utils.getHex(numRead + 16));
                            OpReply reply = new OpReply();
                            try {
                                reply.parse(buf);
                                if (reply.getDocuments() == null || reply.getDocuments().isEmpty()) {
                                    log.error("did not get any data... slowing down");
                                    errorcount++;
                                    if (errorcount > 10) {
                                        log.error("Could not recover... exiting!");
                                        //noinspection EmptyCatchBlock
                                        try {
                                            close();
                                            break;
                                        } catch (MorphiumDriverException e) {
                                        }
                                    }
                                    Thread.sleep(500);
                                    continue;
                                }
                                if (reply.getDocuments() == null || reply.getDocuments().isEmpty()) {
                                    log.error("did not get a valid reply!");
                                    Thread.sleep(500);
                                    continue;
                                }
                                if (reply.getDocuments().get(0).get("ok") == null) {
                                    log.error("Weird result! " + reply.getInReplyTo());
                                    log.error(Utils.toJsonString(reply.getDocuments().get(0)));
                                    System.exit(1);
                                    continue;
                                } else if (!reply.getDocuments().get(0).get("ok").equals(1)) {
                                    if (reply.getDocuments().get(0).get("code") != null) {
                                        log.debug("Error " + reply.getDocuments().get(0).get("code"));
                                        log.debug("Error " + reply.getDocuments().get(0).get("errmsg"));
                                    }
                                }
                                synchronized (replies) {
                                    replies.add(reply);
                                    ArrayList<OpReply> toRemove = replies.stream().filter(r -> System.currentTimeMillis() - r.timestamp > getHeartbeatSocketTimeout()).collect(Collectors.toCollection(ArrayList::new));
                                    toRemove.forEach(replies::remove);
                                }
                            } catch (Exception e) {
                                log.error("Could not read", e);
                            }
                        } catch (IOException e) {
                            //here will be a timeout exception everytime the socket timeout is reachts
                            //                            e.printStackTrace();
                            break; //probably best!
                        }
                    }
                    //noinspection EmptyCatchBlock
                    try {
                        close();
                        //                                    connect(replSet);
                    } catch (Exception e) {
                    }
                    log.debug("reply-thread terminated!");
                }
            };
            reader.setDaemon(true);
            reader.start();
            try {
                Map<String, Object> result = runCommand("local", Utils.getMap("isMaster", true));
                //                log.info("Got result");
                if (result == null) {
                    log.fatal("Could not run ismaster!!!! result is null");
                    throw new RuntimeException("Connect failed!");
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
            out.close();
            in.close();
        } catch (Exception e) {
        } finally {
            s = null;
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
            q.setInReplyTo(0);

            OpReply rep = null;
            sendQuery(q);
            try {
                rep = waitForReply(db, null, null, q.getReqId());
            } catch (MorphiumDriverException e) {
                e.printStackTrace();
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
        sendQuery(q);

        int waitingfor = q.getReqId();
        reply = getReply(waitingfor);
        if (reply.getInReplyTo() != waitingfor) {
            throw new MorphiumDriverNetworkException("Got wrong answser. Request: " + waitingfor + " got answer for " + reply.getInReplyTo());
        }


        MorphiumCursor crs = new MorphiumCursor();
        @SuppressWarnings("unchecked") Map<String, Object> cursor = (Map<String, Object>) reply.getDocuments().get(0).get("cursor");
        if (cursor != null && cursor.get("id") != null) {
            crs.setCursorId((Long) cursor.get("id"));
        }
        if (cursor == null) {
            return null;
        }
        if (cursor.get("firstBatch") != null) {
            //noinspection unchecked
            crs.setBatch((List) cursor.get("firstBatch"));
        } else if (cursor.get("nextBatch") != null) {
            //noinspection unchecked
            crs.setBatch((List) cursor.get("nextBatch"));
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
        reply = getReply(q.getReqId());
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
        runCommand(internalCursor.getDb(), m);
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
            if (projection != null) {
                doc.put("projection", projection);
            }
            doc.put("sort", sort);

            q.setDoc(doc);
            q.setInReplyTo(0);

            List<Map<String, Object>> ret = null;
            sendQuery(q);

            OpReply reply = null;
            int waitingfor = q.getReqId();
            ret = readBatches(waitingfor, db, collection, batchSize);
            return Utils.getMap("values", ret);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("values");

    }

    private List<Map<String, Object>> readBatches(int waitingfor, String db, String collection, int batchSize) throws MorphiumDriverException {
        List<Map<String, Object>> ret = new ArrayList<>();
        OpReply reply;
        OpQuery q;
        Map<String, Object> doc;
        while (true) {
            reply = getReply(waitingfor);
            if (reply.getInReplyTo() != waitingfor) {
                throw new MorphiumDriverNetworkException("Wrong answer - waiting for " + waitingfor + " but got " + reply.getInReplyTo());
            }
            @SuppressWarnings("unchecked") Map<String, Object> cursor = (Map<String, Object>) reply.getDocuments().get(0).get("cursor");
            if (cursor == null) {
                //trying result
                if (reply.getDocuments().get(0).get("result") != null) {
                    //noinspection unchecked
                    return (List<Map<String, Object>>) reply.getDocuments().get(0).get("result");
                }
                log.error("did not get cursor. Data: " + Utils.toJsonString(reply.getDocuments().get(0)));
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
        return ret;
    }

    private OpReply getReply(int waitingfor) throws MorphiumDriverException {
        waitingForReply++;
        try {
            long start = System.currentTimeMillis();
            while (true) {
                synchronized (replies) {
                    for (int i = 0; i < replies.size(); i++) {
                        if (replies.get(i).getInReplyTo() == waitingfor) {
                            //                        if (!reply.getDocuments().get(0).get("ok").equals(1)) {
                            //                            if (reply.getDocuments().get(0).get("code") != null) {
                            //                                log.info("Error " + reply.getDocuments().get(0).get("code"));
                            //                                log.info("Error " + reply.getDocuments().get(0).get("errmsg"));
                            //                            }
                            //                        }
                            return replies.remove(i);
                        }
                    }
                    if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                        throw new MorphiumDriverNetworkException("could not get reply in time");
                    }
                }
                Thread.yield();
            }
        } finally {
            waitingForReply--;
        }
    }

    private void sendQuery(OpQuery q) throws MorphiumDriverException {
        boolean retry = true;
        if (q.getDb() == null) {
            throw new IllegalArgumentException("cannot send command without db");
        }
        if (isSlaveOk()) {
            q.setFlags(4);
        }
        long start = System.currentTimeMillis();
        while (retry) {
            if (s == null || !s.isConnected()) {
                log.debug("Not connected - reconnecting");
                connect();
            }
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

            q.setInReplyTo(0);

            OpReply rep = null;
            sendQuery(q);
            rep = waitForReply(db, collection, query, q.getReqId());
            Integer n = (Integer) rep.getDocuments().get(0).get("n");
            return Utils.getMap("count", n == null ? 0 : n);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
        return ((int) ret.get("count"));
    }

    @Override
    public void insert(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            int idx = 0;
            for (Map<String, Object> o : objs) {
                o.putIfAbsent("_id", new MorphiumId());
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
            int idx = 0;
            for (int i = idx; i < updateCommand.size() - idx; i += getMaxWriteBatchSize()) {
                int end = idx + getMaxWriteBatchSize();
                if (end > updateCommand.size()) {
                    end = updateCommand.size();
                }
                OpQuery op = new OpQuery();
                op.setInReplyTo(0);
                op.setReqId(getNextId());
                op.setDb(db);
                op.setColl("$cmd");

                HashMap<String, Object> map = new LinkedHashMap<>();
                map.put("update", collection);
                map.put("updates", updateCommand.subList(idx, end));
                map.put("ordered", false);
                map.put("writeConcern", new HashMap<String, Object>());
                op.setDoc(map);
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


            sendQuery(op);

            OpReply reply = null;
            int waitingfor = op.getReqId();
            //        if (wc == null || wc.getW() == 0) {
            reply = waitForReply(db, collection, query, waitingfor);
            //        }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    private OpReply waitForReply(String db, String collection, Map<String, Object> query, int waitingfor) throws MorphiumDriverException {
        OpReply reply;
        reply = getReply(waitingfor);
        if (!reply.getDocuments().get(0).get("ok").equals(1) && !reply.getDocuments().get(0).get("ok").equals(1.0)) {
            Object code = reply.getDocuments().get(0).get("code");
            Object errmsg = reply.getDocuments().get(0).get("errmsg");
            MorphiumDriverException mde = new MorphiumDriverException("Operation failed on " + getHostSeed()[0] + " - error: " + code + " - " + errmsg, null, collection, db, query);
            mde.setMongoCode(code);
            mde.setMongoReason(errmsg);

            throw mde;
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
            sendQuery(op);
            try {
                waitForReply(db, collection, null, op.getReqId());
            } catch (Exception e) {
                if (e instanceof MorphiumDriverException && e.getMessage().contains("ns not found")) {
                    log.debug("Drop failed, non existent collection");
                } else {
                    log.debug("Drop failed! " + e.getMessage(), e);
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
            sendQuery(op);
            try {
                waitForReply(db, null, null, op.getReqId());
            } catch (Exception e) {
                log.error("Drop failed! " + e.getMessage());
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
            op.setDb(db);

            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("distinct", collection);
            cmd.put("key", field);
            cmd.put("query", filter);
            op.setDoc(cmd);

            sendQuery(op);
            try {
                OpReply res = waitForReply(db, null, null, op.getReqId());
                return Utils.getMap("result", res.getDocuments().get(0).get("values"));
            } catch (Exception e) {
                log.fatal("did not get result", e);
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
            cmd.put("listIndexes", collection);
            OpQuery q = new OpQuery();
            q.setDb(db);
            q.setColl("$cmd");
            q.setLimit(1);
            q.setSkip(0);
            q.setReqId(getNextId());

            q.setDoc(cmd);

            q.setInReplyTo(0);

            List<Map<String, Object>> ret;
            sendQuery(q);

            ret = readBatches(q.getReqId(), db, null, getMaxWriteBatchSize());
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

            q.setInReplyTo(0);

            List<Map<String, Object>> ret;
            sendQuery(q);

            ret = readBatches(q.getReqId(), db, null, getMaxWriteBatchSize());
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

            sendQuery(q);
            List<Map<String, Object>> lst = readBatches(q.getReqId(), db, collection, getMaxWriteBatchSize());
            return Utils.getMap("result", lst);
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
