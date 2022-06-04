package de.caluga.morphium.driver.sync;

import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionPoolListener;
import de.caluga.morphium.Collation;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.commands.*;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import de.caluga.morphium.driver.wireprotocol.WireProtocolMessage;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
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
public class SynchronousMongoConnection extends DriverBase {

    private final Logger log = LoggerFactory.getLogger(SynchronousMongoConnection.class);
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
            log.error("Could not reconnect!", e);
        }
    }


    public void connect(String replSet) {
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
                Doc result = runCommand("local", Doc.of("hello", true));
                //log.info("Got result");
                if (!result.get("secondary").equals(false)) {
                    close();
                    throw new RuntimeException("Cannot run with secondary connection only!");
                }
                if (replSet != null) {
                    setReplicaSetName((String) result.get("setName"));
                    if (replSet != null && !replSet.equals(getReplicaSetName())) {
                        throw new MorphiumDriverException("Replicaset name is wrong - connected to " + getReplicaSetName() + " should be " + replSet);
                    }
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
            throw new RuntimeException("connection failed", e);
        }
    }


    protected OpMsg getReply(int waitingFor, int timeout) throws MorphiumDriverException {
        return getReply();
    }

    private synchronized OpMsg getReply() throws MorphiumDriverNetworkException {
        return (OpMsg) WireProtocolMessage.parseFromStream(in);
    }


    @Override
    public String getName() {
        return "SingleSyncConnection";
    }

    @Override
    public int getBuildNumber() {
        return 0;
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public void setConnectionUrl(String connectionUrl) {

    }

    @Override
    public void connect() {
        connect(null);
    }


    public boolean isConnected() {
        return s != null && s.isConnected();
    }

    @Override
    public void disconnect() {
        try {
            in.close();
            out.close();
            s.close();
        } catch (IOException e) {
            //throw new RuntimeException(e);
        }
    }

    @Override
    public List<Doc> aggregate(AggregateCmdSettings settings) throws MorphiumDriverException {
        //noinspection unchecked
        return (List<Doc>) new NetworkCallHelper().doCall(() -> {
            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());

            Doc doc = settings.asMap("aggregate");
            doc.put("cursor", Doc.of("batchSize", getDefaultBatchSize()));

            q.setFirstDoc(doc);

            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);
                List<Doc> lst = readBatches(q.getMessageId(), settings.getDb(), settings.getColl(), getMaxWriteBatchSize());
                return Doc.of("result", lst);
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("result");
    }

    @Override
    public long count(CountCmdSettings settings) throws MorphiumDriverException {
        Doc ret = new NetworkCallHelper().doCall(() -> {
            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());

            Doc doc = settings.asMap("count");
            q.setFirstDoc(doc);
            q.setFlags(0);
            q.setResponseTo(0);

            OpMsg rep;
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);
                rep = waitForReply(settings.getDb(), settings.getColl(), q.getMessageId());
            }
            Integer n = (Integer) rep.getFirstDoc().get("n");
            return Doc.of("count", n == null ? 0 : n);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
        return ((int) ret.get("count"));
    }

    @Override
    public void watch(WatchCmdSettings settings) {

    }

    @Override
    public List<Object> distinct(DistinctCmdSettings settings) throws MorphiumDriverException {
        Doc ret = new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setMessageId(getNextId());

            Doc cmd = settings.asMap("distinct");
            op.setFirstDoc(cmd);

            synchronized (SynchronousMongoConnection.this) {
                sendQuery(op);
                //noinspection EmptyCatchBlock
                try {
                    OpMsg res = waitForReply(settings.getDb(), null, op.getMessageId());
                    log.error("Need to implement distinct");
                } catch (Exception e) {

                }
            }

            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
        //noinspection unchecked
        return (List<Object>) ret.get("result");
    }

    @Override
    public List<Doc> mapReduce(MapReduceSettings settings) {
        return null;
    }

    @Override
    public int delete(DeleteCmdSettings settings) throws MorphiumDriverException {
        Doc d = new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setMessageId(getNextId());

            Doc o = settings.asMap("delete");
            op.setFirstDoc(o);


            synchronized (SynchronousMongoConnection.this) {
                sendQuery(op);

                int waitingfor = op.getMessageId();
                //        if (wc == null || wc.getW() == 0) {
                OpMsg reply = waitForReply(settings.getDb(), settings.getColl(), waitingfor);
                return reply.getFirstDoc();
                //        }
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());

        if (d.containsKey("n")) {
            return (int) d.get("n");
        }
        return 0;
    }

    @Override
    public List<Doc> find(FindCmdSettings settings) throws MorphiumDriverException {

        //noinspection unchecked
        return (List<Doc>) new NetworkCallHelper().doCall(() -> {

            List<Doc> ret;

            OpMsg q = new OpMsg();

            q.setFirstDoc(settings.asMap("find"));
            q.setMessageId(getNextId());
            q.setFlags(0);
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);

                int waitingfor = q.getMessageId();
                ret = readBatches(waitingfor, settings.getDb(), settings.getColl(), settings.getBatchSize() == null ? 100 : settings.getBatchSize());
            }
            return Doc.of("values", ret);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("values");
    }

    @Override
    public List<Doc> findAndModify(FindAndModifyCmdSettings settings) {
        return null;
    }

    @Override
    public void insert(InsertCmdSettings settings) {

    }

    @Override
    public Doc update(UpdateCmdSettings settings) throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setResponseTo(0);
            op.setMessageId(getNextId());
            Doc map = settings.asMap("update");
            op.setFirstDoc(map);
//            WriteConcern lwc = wc;
//            if (lwc == null) lwc = WriteConcern.getWc(0, false, false, 0);
//            map.put("writeConcern", lwc.toMongoWriteConcern().asDocument());
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(op);
                OpMsg res = waitForReply(settings.getDb(), settings.getColl(), op.getMessageId());
                return res.getFirstDoc();
            }
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }

    @Override
    public Doc drop(DropCmdSettings settings) {
        return null;
    }

    @Override
    public Doc dropDatabase(DropCmdSettings settings) {
        return null;
    }

    @Override
    public int clearCollection(ClearCollectionSettings settings) throws MorphiumDriverException {
        DeleteCmdSettings del = new DeleteCmdSettings();
        del.addDelete(Doc.of("q", new HashMap<>(), "limit", 0));
        del.setDb(settings.getDb());
        del.setColl(settings.getColl());
        del.setOrdered(false);
        del.setComment(settings.getComment());
        return delete(del);
    }


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


    public Doc getReplsetStatus() throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            Doc ret = runCommand("admin", Doc.of("replSetGetStatus", 1));
            @SuppressWarnings("unchecked") List<Doc> mem = (List) ret.get("members");
            if (mem == null) {
                return null;
            }
            //noinspection unchecked
            mem.stream().filter(d -> d.get("optime") instanceof Map).forEach(d -> d.put("optime", ((Map<String, Doc>) d.get("optime")).get("ts")));
            return ret;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }


    public Doc getDBStats(String db) throws MorphiumDriverException {
        return runCommand(db, Doc.of("dbstats", 1));
    }


    public Doc getCollStats(String db, String coll) throws MorphiumDriverException {
        return null;
    }


    public Doc getOps(long threshold) throws MorphiumDriverException {
        return null;
    }


    public Doc runCommand(String db, Doc cmd) throws MorphiumDriverException {
        return new NetworkCallHelper().doCall(() -> {
            OpMsg q = new OpMsg();
            cmd.put("$db", db);
            q.setMessageId(getNextId());
            q.setFirstDoc(cmd);

            OpMsg rep = null;
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);
                try {
                    rep = waitForReply(db, null, q.getMessageId());
                } catch (MorphiumDriverException e) {
                    e.printStackTrace();
                }
            }
            if (rep == null || rep.getFirstDoc() == null) {
                return null;
            }
            return rep.getFirstDoc();
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());

    }


    public MorphiumCursor initAggregationIteration(String db, String collection, List<Doc> aggregationPipeline, ReadPreference readPreference, Collation collation, int batchSize, Doc findMetaData) throws MorphiumDriverException {
        return null;
    }


    public MorphiumCursor initIteration(String db, String collection, Doc query, Map<String, Integer> sort, Doc projection, int skip, int limit, int batchSize, ReadPreference readPreference, Collation collation, Doc findMetaData) throws MorphiumDriverException {
        if (sort == null) {
            sort = new HashMap<>();
        }
        OpMsg q = new OpMsg();
        q.setMessageId(getNextId());
        Doc doc = new Doc();
        doc.put("$db", db);
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

        q.setFirstDoc(doc);
        q.setFlags(0);
        q.setResponseTo(0);

        OpMsg reply;
        synchronized (SynchronousMongoConnection.this) {
            sendQuery(q);

            int waitingfor = q.getMessageId();
            reply = getReply();
            if (reply.getResponseTo() != waitingfor) {
                throw new MorphiumDriverNetworkException("Got wrong answser. Request: " + waitingfor + " got answer for " + reply.getResponseTo());
            }

        }

        MorphiumCursor crs = new MorphiumCursor();
        @SuppressWarnings("unchecked") Doc cursor = (Doc) reply.getFirstDoc().get("cursor");
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

        SynchronousConnectCursor internalCursorData = new SynchronousConnectCursor(this);
        internalCursorData.setBatchSize(batchSize);
        internalCursorData.setCollection(collection);
        internalCursorData.setDb(db);
        //noinspection unchecked
        crs.setInternalCursorObject(internalCursorData);
        return crs;


    }

    /**
     * watch for changes in all databases
     * this is a synchronous call which blocks until the C
     *
     * @param maxWait
     * @param fullDocumentOnUpdate
     * @param pipeline
     * @param cb
     * @throws MorphiumDriverException
     */
    public void watch(int maxWait, boolean fullDocumentOnUpdate, List<Doc> pipeline, DriverTailableIterationCallback cb) throws MorphiumDriverException {

    }

    /**
     * watch for changes in the given database
     *
     * @param db
     * @param maxWait
     * @param fullDocumentOnUpdate
     * @param pipeline
     * @param cb
     * @throws MorphiumDriverException
     */

    public void watch(String db, int maxWait, boolean fullDocumentOnUpdate, List<Doc> pipeline, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        OpMsg msg = new OpMsg();
        msg.setMessageId(getNextId());
        Doc cmd = Doc.of("aggregate", (Object) 1).add("pipeline", pipeline)
                .add("$db", db);
        msg.setFirstDoc(cmd);
        sendQuery(msg);
        OpMsg reply = waitForReply(db, null, msg.getMessageId());


    }

    /**
     * watch for changes in the given db and collection
     *
     * @param db
     * @param collection
     * @param maxWait
     * @param fullDocumentOnUpdate
     * @param pipeline
     * @param cb
     * @throws MorphiumDriverException
     */

    public void watch(String db, String collection, int maxWait, boolean fullDocumentOnUpdate, List<Doc> pipeline, int batchsize, DriverTailableIterationCallback cb) throws MorphiumDriverException {
        if (maxWait <= 0) maxWait = getReadTimeout();
        OpMsg startMsg = new OpMsg();
        startMsg.setMessageId(getNextId());
        ArrayList<Doc> localPipeline = new ArrayList<>();
        localPipeline.add(Doc.of("$changeStream", new HashMap<>()));
        if (pipeline != null && !pipeline.isEmpty()) localPipeline.addAll(pipeline);
        Doc cmd = Doc.of("aggregate", (Object) collection).add("pipeline", localPipeline)
                .add("cursor", Doc.of("batchSize", batchsize))  //getDefaultBatchSize()
                .add("$db", db);
        startMsg.setFirstDoc(cmd);
        long start = System.currentTimeMillis();
        sendQuery(startMsg);

        OpMsg msg = startMsg;
        while (true) {
            OpMsg reply = waitForReply(db, collection, msg.getMessageId());
            log.info("got answer for watch!");
            Doc cursor = (Doc) reply.getFirstDoc().get("cursor");
            if (cursor == null) throw new MorphiumDriverException("Could not watch - cursor is null");
            log.debug("CursorID:" + cursor.get("id").toString());
            long cursorId = Long.parseLong(cursor.get("id").toString());

            List<Doc> result = (List<Doc>) cursor.get("firstBatch");
            if (result == null) {
                result = (List<Doc>) cursor.get("nextBatch");
            }
            if (result != null) {
                for (Doc o : result) {
                    cb.incomingData(o, System.currentTimeMillis() - start);
                }
            }
            if (!cb.isContinued()) {
                killCursors(db, collection, cursorId);
                break;
            }
            if (cursorId != 0) {
                msg = new OpMsg();
                msg.setMessageId(getNextId());

                Doc doc = new Doc();
                doc.put("getMore", cursorId);
                doc.put("collection", collection);
                doc.put("batchSize", 1);   //getDefaultBatchSize());
                doc.put("maxTimeMS", maxWait);
                doc.put("$db", db);
                msg.setFirstDoc(doc);
                sendQuery(msg);
                log.debug("sent getmore....");
            } else {
                log.debug("Cursor exhausted, restarting");
                msg = startMsg;
                msg.setMessageId(getNextId());
                sendQuery(msg);

            }
        }
    }


    public MorphiumCursor nextIteration(MorphiumCursor crs) throws MorphiumDriverException {

        long cursorId = crs.getCursorId();
        SynchronousConnectCursor internalCursorData = (SynchronousConnectCursor) crs.getInternalCursorObject();

        if (cursorId == 0) {
            return null;
        }
        OpMsg reply;
        synchronized (SynchronousMongoConnection.this) {
            OpMsg q = new OpMsg();

            q.setFirstDoc(Doc.of("getMore", (Object) cursorId)
                    .add("$db", internalCursorData.getDb())
                    .add("collection", internalCursorData.getCollection())
                    .add("batchSize", internalCursorData.getBatchSize()
                    ));
            q.setMessageId(getNextId());
            sendQuery(q);
            reply = getReply();
        }
        crs = new MorphiumCursor();
        //noinspection unchecked
        crs.setInternalCursorObject(internalCursorData);
        @SuppressWarnings("unchecked") Doc cursor = (Doc) reply.getFirstDoc().get("cursor");
        if (cursor == null) {
            //cursor not found
            throw new MorphiumDriverException("Iteration failed! Error: " + reply.getFirstDoc().get("code") + "  Message: " + reply.getFirstDoc().get("errmsg"));
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


    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        if (crs == null) {
            return;
        }
        SynchronousConnectCursor internalCursor = (SynchronousConnectCursor) crs.getInternalCursorObject();
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection") Doc m = new Doc();
        m.put("killCursors", internalCursor.getCollection());
        List<Long> cursors = new ArrayList<>();
        cursors.add(crs.getCursorId());
        m.put("cursors", cursors);

    }


    private List<Doc> readBatches(int waitingfor, String db, String collection, int batchSize) throws MorphiumDriverException {
        List<Doc> ret = new ArrayList<>();

        Doc doc;
        synchronized (SynchronousMongoConnection.this) {
            while (true) {
                OpMsg reply = getReply();
                if (reply.getResponseTo() != waitingfor) {
                    log.error("Wrong answer - waiting for " + waitingfor + " but got " + reply.getResponseTo());
                    log.error("Document: " + Utils.toJsonString(reply.getFirstDoc()));
                    continue;
                }
                //                    replies.remove(i);
                @SuppressWarnings("unchecked") Doc cursor = (Doc) reply.getFirstDoc().get("cursor");
                if (cursor == null) {
                    //trying result
                    if (reply.getFirstDoc().get("result") != null) {
                        //noinspection unchecked
                        return (List<Doc>) reply.getFirstDoc().get("result");
                    }
                    throw new MorphiumDriverException("Mongo Error: " + reply.getFirstDoc().get("codeName") + " - " + reply.getFirstDoc().get("errmsg"));
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
                    OpMsg q = new OpMsg();
                    q.setFirstDoc(Doc.of("getMore", (Object) cursor.get("id"))
                            .add("$db", db)
                            .add("collection", collection)
                            .add("batchSize", batchSize)
                    );
                    q.setMessageId(getNextId());
                    waitingfor = q.getMessageId();
                    sendQuery(q);
                } else {
                    break;
                }
            }
        }
        return ret;
    }


    protected void sendQuery(OpMsg q) throws MorphiumDriverException {
        boolean retry = true;
        long start = System.currentTimeMillis();
        while (retry) {
            try {
                if (System.currentTimeMillis() - start > getMaxWaitTime()) {
                    throw new MorphiumDriverException("Could not send message! Timeout!");
                }
                //q.setFlags(4); //slave ok
                out.write(q.bytes());
                out.flush();
                retry = false;
            } catch (IOException e) {
                log.error("Error sending request - reconnecting", e);
                reconnect();

            }
        }
    }


    public OpMsg sendAndWaitForReply(OpMsg q) throws MorphiumDriverException {
        sendQuery(q);
        return getReply(q.getMessageId(), getMaxWaitTime());
    }


    public long estimatedDocumentCount(String db, String collection, ReadPreference rp) {

        return 0;
    }


    public void insert(String db, String collection, List<Doc> objs, WriteConcern wc) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            int idx = 0;
            objs.forEach(o -> o.putIfAbsent("_id", new MorphiumId()));

            while (idx < objs.size()) {
                OpMsg op = new OpMsg();
                op.setResponseTo(0);
                op.setMessageId(getNextId());
                Doc map = new Doc();
                map.put("insert", collection);

                List<Doc> docs = new ArrayList<>();
                for (int i = idx; i < idx + 1000 && i < objs.size(); i++) {
                    docs.add(objs.get(i));
                }
                idx += docs.size();
                map.put("documents", docs);
                map.put("$db", db);
                map.put("ordered", false);
                map.put("writeConcern", new Doc());
                op.setFirstDoc(map);

                synchronized (SynchronousMongoConnection.this) {
                    sendQuery(op);
                    waitForReply(db, collection, op.getMessageId());
                }
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }


    public Doc store(String db, String collection, List<Doc> objs, WriteConcern wc) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {

            List<Doc> opsLst = new ArrayList<>();
            for (Doc o : objs) {
                o.putIfAbsent("_id", new ObjectId());
                Doc up = new Doc();
                up.put("q", Doc.of("_id", o.get("_id")));
                up.put("u", o);
                up.put("upsert", true);
                up.put("multi", false);
                up.put("collation", null);
                //up.put("arrayFilters",list of arrayfilters)
                //up.put("hint",indexInfo);
                //up.put("c",variablesDocument);
                opsLst.add(up);
            }

            return update(new UpdateCmdSettings().setDb(db).setColl(collection).setUpdates(opsLst));
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
        return null;
    }


//    public Doc update(String db, String collection, Doc query, Doc ops, boolean multiple, boolean upsert, Collation collation, WriteConcern wc) throws MorphiumDriverException {
//        List<Doc> opsLst = new ArrayList<>();
//        Doc up = new HashMap<>();
//        up.put("q", query);
//        up.put("u", ops);
//       //up.put("sort",sort);
//        up.put("upsert", upsert);
//        up.put("multi", multiple);
//        up.put("collation", collation != null ? collation.toQueryObject() : null);
//        //up.put("arrayFilters",list of arrayfilters)
//        //up.put("hint",indexInfo);
//        //up.put("c",variablesDocument);
//        opsLst.add(up);
//        return update(db, collection, opsLst, false, wc);
//    }


    @SuppressWarnings("StatementWithEmptyBody")
    private OpMsg waitForReply(String db, String collection, int waitingfor) throws MorphiumDriverException {
        OpMsg reply;
        reply = getReply();
        //                replies.remove(i);
        if (reply.getResponseTo() == waitingfor) {
            if (!reply.getFirstDoc().get("ok").equals(1) && !reply.getFirstDoc().get("ok").equals(1.0)) {
                Object code = reply.getFirstDoc().get("code");
                Object errmsg = reply.getFirstDoc().get("errmsg");
                //                throw new MorphiumDriverException("Operation failed - error: " + code + " - " + errmsg, null, collection, db, query);
                MorphiumDriverException mde = new MorphiumDriverException("Operation failed on " + getHostSeed()[0] + " - error: " + code + " - " + errmsg, null, collection, db, null);
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


    public void drop(String db, String collection, WriteConcern wc) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setResponseTo(0);
            op.setMessageId(getNextId());

            Doc map = new Doc();
            map.put("drop", collection);
            map.put("$db", db);
            op.setFirstDoc(map);
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(op);
                try {
                    waitForReply(db, collection, op.getMessageId());
                } catch (Exception e) {
                    log.warn("Drop failed! " + e.getMessage());
                }
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }


    public void drop(String db, WriteConcern wc) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            OpMsg op = new OpMsg();
            op.setResponseTo(0);
            op.setMessageId(getNextId());

            Doc map = new Doc();
            map.put("drop", 1);
            map.put("$db", db);
            op.setFirstDoc(map);
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(op);
                try {
                    waitForReply(db, null, op.getMessageId());
                } catch (Exception e) {
                    log.error("Drop failed! " + e.getMessage());
                }
            }
            return null;
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries());
    }


    public boolean exists(String db) throws MorphiumDriverException {
        //noinspection EmptyCatchBlock
        try {
            getDBStats(db);
            return true;
        } catch (MorphiumDriverException e) {
        }
        return false;
    }


    public boolean exists(String db, String collection) throws MorphiumDriverException {
        List<Doc> ret = getCollectionInfo(db, collection);
        for (Doc c : ret) {
            if (c.get("name").equals(collection)) {
                return true;
            }
        }
        return false;
    }


    public List<Doc> getIndexes(String db, String collection) throws MorphiumDriverException {
        //noinspection unchecked
        return (List<Doc>) new NetworkCallHelper().doCall(() -> {
            Doc cmd = new Doc();
            cmd.put("listIndexes", 1);
            cmd.put("$db", db);
            cmd.put("collection", collection);
            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());

            q.setFirstDoc(cmd);
            q.setFlags(0);
            q.setResponseTo(0);

            List<Doc> ret;
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);

                ret = readBatches(q.getMessageId(), db, null, getMaxWriteBatchSize());
            }
            return Doc.of("result", ret);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("result");
    }


    public List<String> getCollectionNames(String db) throws MorphiumDriverException {
        List<Doc> ret = getCollectionInfo(db, null);
        return ret.stream().map(c -> (String) c.get("name")).collect(Collectors.toList());
    }


    public Doc findAndOneAndDelete(String db, String col, Doc query, Map<String, Integer> sort, Collation collation) throws MorphiumDriverException {
        OpMsg msg = new OpMsg();
        msg.setMessageId(getNextId());
        Doc cmd = Doc.of("findAndModify", (Object) col)
                .add("query", query)
                .add("sort", sort)
                .add("collation", collation != null ? collation.toQueryObject() : null)
                .add("new", true)
                .add("$db", db);
        msg.setFirstDoc(cmd);
        msg.setFlags(0);
        msg.setResponseTo(0);

        OpMsg reply = sendAndWaitForReply(msg);

        return reply.getFirstDoc();
    }


    public Doc findAndOneAndUpdate(String db, String col, Doc query, Doc update, Map<String, Integer> sort, Collation collation) throws MorphiumDriverException {
        return null;
    }


    public Doc findAndOneAndReplace(String db, String col, Doc query, Doc replacement, Map<String, Integer> sort, Collation collation) throws MorphiumDriverException {
        return null;
    }

    private List<Doc> getCollectionInfo(String db, String collection) throws MorphiumDriverException {
        //noinspection unchecked
        return (List<Doc>) new NetworkCallHelper().doCall(() -> {
            Doc cmd = new Doc();
            cmd.put("listCollections", 1);
            OpMsg q = new OpMsg();
            q.setMessageId(getNextId());

            if (collection != null) {
                cmd.put("filter", Doc.of("name", collection));
            }
            cmd.put("$db", db);
            q.setFirstDoc(cmd);
            q.setFlags(0);
            q.setResponseTo(0);

            List<Doc> ret;
            synchronized (SynchronousMongoConnection.this) {
                sendQuery(q);

                ret = readBatches(q.getMessageId(), db, null, getMaxWriteBatchSize());
            }
            return Doc.of("result", ret);
        }, getRetriesOnNetworkError(), getSleepBetweenErrorRetries()).get("result");
    }


    public int getServerSelectionTimeout() {
        return 0;
    }


    public void setServerSelectionTimeout(int serverSelectionTimeout) {

    }


    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        List<Doc> lst = getCollectionInfo(db, coll);
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


    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return null;
    }


    public void createIndex(String db, String collection, Doc index, Doc options) throws MorphiumDriverException {
        new NetworkCallHelper().doCall(() -> {
            Doc cmd = new Doc();
            cmd.put("createIndexes", collection);
            List<Doc> lst = new ArrayList<>();
            Doc idx = new Doc();
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


    public List<Doc> mapReduce(String db, String collection, String mapping, String reducing) throws MorphiumDriverException {
        return null;
    }


    public List<Doc> mapReduce(String db, String collection, String mapping, String reducing, Doc query) throws MorphiumDriverException {
        return null;
    }


    public List<Doc> mapReduce(String db, String collection, String mapping, String reducing, Doc query, Doc sorting, Collation collation) throws MorphiumDriverException {
        return null;
    }


    public void addCommandListener(CommandListener cmd) {

    }


    public void removeCommandListener(CommandListener cmd) {

    }


    public void addClusterListener(ClusterListener cl) {

    }


    public void removeClusterListener(ClusterListener cl) {

    }


    public void addConnectionPoolListener(ConnectionPoolListener cpl) {

    }


    public void removeConnectionPoolListener(ConnectionPoolListener cpl) {

    }


    public void startTransaction() {

    }


    public void commitTransaction() {

    }


    public MorphiumTransactionContext getTransactionContext() {
        return null;
    }


    public void setTransactionContext(MorphiumTransactionContext ctx) {

    }


    public void abortTransaction() {

    }


    public SSLContext getSslContext() {
        return null;
    }


    public void setSslContext(SSLContext sslContext) {

    }


    public boolean isSslInvalidHostNameAllowed() {
        return false;
    }


    public void setSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {

    }
}
