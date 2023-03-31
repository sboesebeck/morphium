package de.caluga.morphium.driver.wire;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.GetMoreMongoCommand;
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
    private MongoConnection connection;
    private Logger log = LoggerFactory.getLogger(SingleMongoConnectionCursor.class);
    private int internalIndex = 0;
    private int index = 0;

    public SingleMongoConnectionCursor(MongoConnection drv, int batchSize, boolean multithreaddedAccess, OpMsg reply) throws MorphiumDriverException {
        this.connection = drv;
        this.multithreaddedAccess = multithreaddedAccess;
        Long cursorId = null;
        @SuppressWarnings("unchecked")
        Doc cursor = (Doc) reply.getFirstDoc().get("cursor");

        if (cursor == null) {
            throw new MorphiumDriverException("No cursor returned: " + reply.getFirstDoc().get("code") + "  Message: " + reply.getFirstDoc().get("errmsg"));
        }

        if (cursor.get("id") != null) {
            cursorId = (Long) cursor.get("id");
        }

        setCursorId(cursorId);
        String[] ns = ((String) cursor.get("ns")).split("\\.");
        setDb(ns[0]); //collection name

        if (ns.length > 1) {
            setCollection(ns[1]);    //collection name
        }

        if (cursor.get("firstBatch") != null) {
            //noinspection unchecked
            setBatch((List) cursor.get("firstBatch"));
        } else if (cursor.get("nextBatch") != null) {
            //noinspection unchecked
            setBatch((List) cursor.get("nextBatch"));
            //log.error("First Cursor init returned NEXT BATCH?!?!?");
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
    public synchronized boolean hasNext() {
        //end of stream
        if (getBatch() == null || getBatch().isEmpty()) {
            if (connection != null) {
                connection.release();
                connection = null;
            }
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

            if (getBatch() == null || getBatch().isEmpty()) {
                if (connection != null) {
                    connection.release();
                    connection = null;
                }

                return false;
            }

            return true;
        }

        if (connection != null) {
            connection.release();
            connection = null;
        }

        return false;
    }

    private void getNextIteration() throws MorphiumDriverException {
        var batch = nextIteration();
        internalIndex = 0;

        if (multithreaddedAccess && batch != null) {
            setBatch(Collections.synchronizedList(batch));
        } else {
            setBatch(batch);
        }
    }

    @Override
    public synchronized Map<String, Object> next() {
        if (getBatch() == null || getBatch().isEmpty()) {
            return null;
        }

        if (getBatch().size() <= internalIndex) {
            try {
                getNextIteration();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            }

            if (getBatch() == null || getBatch().isEmpty()) {
                return null;
            }
        }

        index++;
        return getBatch().get(internalIndex++);
    }

    @Override
    public synchronized void close() {
        if (getConnection() == null) {
            return;
        }

        try {
            getConnection().closeIteration(this);

            if (connection != null) {
                connection.release();
                connection = null;
            }
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int available() {
        return getBatch().size() - internalIndex;
    }

    public MongoConnection getConnection() {
        return connection;
    }

    private List<Map<String, Object>> nextIteration() throws MorphiumDriverException {
        if (getCursorId() == 0) {
            //end of stream
            return null;
        }

        MorphiumCursor reply;
        GetMoreMongoCommand more = new GetMoreMongoCommand(connection).setCursorId(getCursorId()).setDb(getDb()).setColl(getCollection()).setBatchSize(getBatchSize());
        // try {
        reply = more.execute();
        setCursorId(reply.getCursorId()); //setting 0 if end of iteration

        if (getCursorId() == 0L) {
            if (connection != null) {
                connection.release();
                connection = null;
            }
        }

        return reply.getBatch();
        // } catch (MorphiumDriverException e) {
        //     log.error("Error ", e);
        // }
        // return null;
    }

    @Override
    public synchronized List<Map<String, Object>> getAll() throws MorphiumDriverException {
        List<Map<String, Object>> ret = new ArrayList<>();

        while (hasNext()) {
            ret.addAll(getBatch());
            internalIndex = getBatch().size();
        }

        return ret;
    }

    @Override
    public synchronized void ahead(int jump) throws MorphiumDriverException {
        internalIndex += jump;
        index += jump;

        while (getBatch() != null && internalIndex >= getBatch().size()) {
            int diff = internalIndex - getBatch().size();
            getNextIteration();
            internalIndex = diff;
        }
    }

    @Override
    public synchronized void back(int jump) throws MorphiumDriverException {
        internalIndex -= (jump);
        index -= (jump);

        if (internalIndex < 0) {
            throw new IllegalArgumentException("cannot jump back over batch boundaries!");
        }
    }
}
