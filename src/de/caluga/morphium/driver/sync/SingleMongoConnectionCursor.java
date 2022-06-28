package de.caluga.morphium.driver.sync;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 23.03.16
 * Time: 16:13
 * <p>
 * Cursor implementation for the singleconnect drivers
 */

public class SingleMongoConnectionCursor extends MorphiumCursor {

    private final boolean multithreaddedAccess;
    private final DriverBase driver;
    private Logger log = LoggerFactory.getLogger(SingleMongoConnectionCursor.class);
    private int internalIndex = 0;
    private int index = 0;

    public SingleMongoConnectionCursor(DriverBase drv, int batchSize, boolean multithreaddedAccess, OpMsg reply) throws MorphiumDriverException {
        this.driver = drv;
        this.multithreaddedAccess = multithreaddedAccess;
        Long cursorId = null;
        @SuppressWarnings("unchecked") Doc cursor = (Doc) reply.getFirstDoc().get("cursor");
        if (cursor == null) {
            throw new MorphiumDriverException("No cursor returned: " + reply.getFirstDoc().get("code") + "  Message: " + reply.getFirstDoc().get("errmsg"));
        }
        if (cursor.get("id") != null) {
            cursorId = (Long) cursor.get("id");
        }
        setCursorId(cursorId);
        String[] ns = ((String) cursor.get("ns")).split("\\.");
        setDb(ns[0]); //collection name
        if (ns.length > 1) setCollection(ns[1]); //collection name
        List<Map<String, Object>> firstBatch;
        if (cursor.get("firstBatch") != null) {
            //noinspection unchecked
            setBatch((List) cursor.get("firstBatch"));
        } else if (cursor.get("nextBatch") != null) {
            //noinspection unchecked
            setBatch((List) cursor.get("nextBatch"));
            log.error("First Cursor init returned NEXT BATCH?!?!?");
        } else {
            log.warn("No result returned");
            setBatch(Collections.emptyList());
        }

        setBatchSize(batchSize);

    }
//    public SynchronousConnectCursor(DriverBase drv, long id,String db, String collection, int batchSize,List<Map<String,Object>> firstBatch , boolean multithreadded) {
//        this.driver = drv;
//        this.multithreaddedAccess=multithreadded;
//        setBatch(firstBatch);
//        this.setCursorId(id);
//        setBatchSize(batchSize);
//        setCollection(collection);
//        setDb(db);
//    }


    @Override
    public Iterator<Map<String, Object>> iterator() {
        return this;
    }

    @Override
    public int getCursor() {
        return index;
    }

    @Override
    public boolean hasNext() {
        //end of stream
        if (getBatch() == null || getBatch().isEmpty()) {
            return false;
        }
        //internal index points within batch
        if (internalIndex < getBatch().size()) {
            return true;
        }
        if (getCursorId() != 0) {
            // next batch
            try {
                getNextIteration();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            }

            if (getBatch() == null || getBatch().isEmpty()) return false;
            return true;
        }
        return false;
    }

    private void getNextIteration() throws MorphiumDriverException {
        if (multithreaddedAccess) {
            setBatch(Collections.synchronizedList(nextIteration()));
        } else {
            setBatch(nextIteration());
        }
        internalIndex = 0;
    }

    @Override
    public Map<String, Object> next() {
        if (getBatch() == null || getBatch().isEmpty()) return null;
        if (getBatch().size() <= internalIndex) {
            try {
                getNextIteration();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            }
            if (getBatch() == null || getBatch().isEmpty()) return null;
        }
        index++;
        return getBatch().get(internalIndex++);
    }

    @Override
    public void close() {
        try {
            getDriver().closeIteration(this);
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int available() {
        return getBatch().size() - internalIndex;
    }

    public DriverBase getDriver() {
        return driver;
    }


    private List<Map<String, Object>> nextIteration() throws MorphiumDriverException {
        if (getCursorId() == 0) {
            //end of stream
            return null;
        }
        OpMsg reply;
        synchronized (SingleMongoConnectionCursor.this) {
            OpMsg q = new OpMsg();

            q.setFirstDoc(Doc.of("getMore", (Object) getCursorId())
                    .add("$db", getDb())
                    .add("collection", getCollection())
                    .add("batchSize", getBatchSize()
                    ));
            q.setMessageId(driver.getNextId());
            driver.sendQuery(q);
            reply = driver.getReplyFor(q.getMessageId(), driver.getMaxWaitTime());
        }

        //noinspection unchecked

        @SuppressWarnings("unchecked") Doc cursor = (Doc) reply.getFirstDoc().get("cursor");
        if (cursor == null) {
            //cursor not found
            throw new MorphiumDriverException("Iteration failed! Error: " + reply.getFirstDoc().get("code") + "  Message: " + reply.getFirstDoc().get("errmsg"));
        }
        //just a sanity check
        if (cursor.get("id") != null) {
            if (!cursor.get("id").equals(getCursorId())) {
                if (cursor.get("id").equals(0L)) {
                    //end of data
                    setCursorId(0);
                } else {
                    throw new MorphiumDriverException("Cursor ID changed!? was " + getCursorId() + " we got:" + cursor.get("id"));
                }
            }
        }

        if (cursor.get("firstBatch") != null) {

            log.warn("NEXT Iteration got first batch!?!?!?");
            //noinspection unchecked
            return (List) cursor.get("firstBatch");
        } else if (cursor.get("nextBatch") != null) {
            //noinspection unchecked
            return (List) cursor.get("nextBatch");
        }
        throw new MorphiumDriverException("Cursor did not contain data! " + reply.getFirstDoc().get("code") + "  Message: " + reply.getFirstDoc().get("errmsg"));


    }

    @Override
    public List<Map<String, Object>> getAll() throws MorphiumDriverException {
        List<Map<String, Object>> ret = new ArrayList<>();
        while (hasNext()) {
            ret.addAll(getBatch());
            internalIndex = getBatch().size();
        }
        return ret;
    }

    @Override
    public void ahead(int jump) throws MorphiumDriverException {
        internalIndex += jump;
        index += jump;
        while (getBatch() != null && internalIndex >= getBatch().size()) {
            int diff = internalIndex - getBatch().size();
            getNextIteration();
            internalIndex = diff;


        }
    }

    @Override
    public void back(int jump) throws MorphiumDriverException {
        internalIndex -= (jump);
        index -= (jump);
        if (internalIndex < 0) {
            throw new IllegalArgumentException("cannot jump back over batch boundaries!");
        }
    }
}
