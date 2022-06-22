package de.caluga.morphium.query;

import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 25.03.16
 * Time: 22:33
 * <p>
 * iterating over huge collections using the mongodb internal cursor
 */
public class QueryIterator<T> implements MorphiumIterator<T>, Iterator<T>, Iterable<T> {

    private final Logger log = LoggerFactory.getLogger(QueryIterator.class);
    private Query<T> query;
    private MorphiumCursor currentBatch = null;

    private int cursor = 0;
    private int cursorExternal = 0;
    private boolean multithreadded;
    private int windowSize = -1;


    public int getWindowSize() {
        if (query == null) {
            return 0;
        }
        if (windowSize <= 0) {
            windowSize = query.getMorphium().getConfig().getCursorBatchSize();
        }
        return windowSize;
    }


    public void setWindowSize(int sz) {
        windowSize = sz;
    }


    public Query<T> getQuery() {
        return query;
    }


    public void setQuery(Query<T> q) {
            query = q.clone();
    }


    public int getCurrentBufferSize() {
        return currentBatch.getBatch().size();
    }


    public List<T> getCurrentBuffer() {
        return null;
    }


    public long getCount() {
        return query.countAll();
    }


    public int getCursor() {
        return cursorExternal;
    }


    public void ahead(int jump) {
        cursor += jump;
        cursorExternal += jump;
        while (cursor >= currentBatch.getBatch().size()) {
            int diff = cursor - currentBatch.getBatch().size();
            cursor = currentBatch.getBatch().size() - 1;

            next();
            cursor += diff;
        }
    }


    public void back(int jump) {
        cursor -= jump;
        cursorExternal -= jump;
        if (cursor < 0) {
            throw new IllegalArgumentException("cannot jump back over batch boundaries!");
        }
    }


    public boolean isMultithreaddedAccess() {
        return multithreadded;
    }


    public void setMultithreaddedAccess(boolean mu) {
        multithreadded = mu;
    }


    public Iterator<T> iterator() {
        return this;
    }


    public boolean hasNext() {
        if (multithreadded) {
            synchronized (this) {
                return doHasNext();
            }
        }
        return doHasNext();
    }

    private boolean doHasNext() {
        if (currentBatch != null && currentBatch.getBatch() != null) {
            if (currentBatch.getBatch().size() <= cursor) {
                close();
                return false;
            }
            return true;
        }
        if (currentBatch == null && cursorExternal == 0) {
            try {
                //noinspection unchecked
                currentBatch = query.getMorphium().getDriver().initIteration(query.getFindCmdSettings());
            } catch (MorphiumDriverException e) {
                log.error("error during fetching first batch", e);
            }
            return doHasNext();
        }
        close();
        return false;
    }


    public Map<String, Object> nextMap() {
        if (multithreadded) {
            synchronized (this) {
                return doNextMap();
            }
        } else {
            return doNextMap();
        }
    }

    @SuppressWarnings({"ConstantConditions", "CommentedOutCode"})
    public Map<String, Object> doNextMap() {
        if (currentBatch == null && !hasNext()) {
            return null;
        }
        Map<String, Object> ret = currentBatch.getBatch().get(cursor);
        //T unmarshall = query.getMorphium().getMapper().deserialize(query.getType(), currentBatch.getBatch().get(cursor));
        //query.getMorphium().firePostLoadEvent(unmarshall);
//        try {
            if (currentBatch == null && cursorExternal == 0) {
                //noinspection unchecked
//                currentBatch = query.getMorphium().getDriver().initIteration(query.getMorphium().getConfig().getDatabase(), query.getCollectionName(), query.toQueryObject(), query.getSort(), query.getFieldListForQuery(), query.getSkip(), query.getLimit(), getWindowSize(), query.getMorphium().getReadPreferenceForClass(query.getType()), query.getCollation(), null);
                cursor = 0;
            } else if (currentBatch != null && cursor + 1 < currentBatch.getBatch().size()) {
                cursor++;
            } else if (currentBatch != null && cursor + 1 == currentBatch.getBatch().size()) {
                //noinspection unchecked
//                currentBatch = query.getMorphium().getDriver().nextIteration(currentBatch);
                cursor = 0;
            } else {
                cursor++;
            }
        if (multithreadded && currentBatch != null && currentBatch.getBatch() != null) {
            currentBatch.setBatch(Collections.synchronizedList(currentBatch.getBatch()));
        }

//        } catch (MorphiumDriverException e) {
//            log.error("Got error during iteration...", e);
//        }
        cursorExternal++;

        return ret;
    }


    public T next() {
        if (multithreadded) {
            synchronized (this) {
                return doNext();
            }
        }
        return doNext();
    }

    public T doNext() {

        if (currentBatch == null || !hasNext()) {
            return null;
        }
        if (currentBatch.getBatch().size() <= cursor) {
            return null;
        }
        T unmarshall = null;
        if (query.getType() != null) {
            unmarshall = query.getMorphium().getMapper().deserialize(query.getType(), currentBatch.getBatch().get(cursor));
        } else {
            unmarshall = (T) currentBatch.getBatch().get(cursor);
        }
        query.getMorphium().firePostLoadEvent(unmarshall);
//        try {
            if (currentBatch == null && cursorExternal == 0) {
                //noinspection unchecked
                if (multithreadded) {
//                    synchronized (this) {
//                        currentBatch = query.getMorphium().getDriver().initIteration(query.getMorphium().getConfig().getDatabase(), query.getCollectionName(), query.toQueryObject(), query.getSort(), query.getFieldListForQuery(), query.getSkip(), query.getLimit(), getWindowSize(), query.getMorphium().getReadPreferenceForClass(query.getType()), query.getCollation(), null);
//                    }
                } else {
//                    currentBatch = query.getMorphium().getDriver().initIteration(query.getMorphium().getConfig().getDatabase(), query.getCollectionName(), query.toQueryObject(), query.getSort(), query.getFieldListForQuery(), query.getSkip(), query.getLimit(), getWindowSize(), query.getMorphium().getReadPreferenceForClass(query.getType()), query.getCollation(), null);
                }
                cursor = 0;
            } else if (currentBatch != null && cursor + 1 < currentBatch.getBatch().size()) {
                cursor++;
            } else if (currentBatch != null && cursor + 1 == currentBatch.getBatch().size()) {
                //noinspection unchecked
                if (multithreadded) {
//                    synchronized (this) {
//                        currentBatch = query.getMorphium().getDriver().nextIteration(currentBatch);
//                    }
                } else {
//                    currentBatch = query.getMorphium().getDriver().nextIteration(currentBatch);
                }

                cursor = 0;
            } else {
                cursor++;
            }
        if (multithreadded && currentBatch != null && currentBatch.getBatch() != null) {
            currentBatch.setBatch(Collections.synchronizedList(currentBatch.getBatch()));
        }

//        } catch (MorphiumDriverException e) {
//            log.error("Got error during iteration...", e);
//        }
        cursorExternal++;

        return unmarshall;
    }


    public void close() {
//        try {
//            query.getMorphium().getDriver().closeIteration(currentBatch);
//        } catch (MorphiumDriverException e) {
//            //swallow
//        }
    }
}
