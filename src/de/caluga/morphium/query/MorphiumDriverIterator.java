package de.caluga.morphium.query;

import de.caluga.morphium.Logger;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 25.03.16
 * Time: 22:33
 * <p>
 * iterating over huge collections using the mongodb internal cursor
 */
public class MorphiumDriverIterator<T> implements MorphiumIterator<T> {

    private Query<T> query;
    private Logger log = new Logger(MorphiumDriverIterator.class);

    private MorphiumCursor<T> currentBatch = null;

    private int cursor = 0;
    private int cursorExternal = 0;
    private boolean multithreadded;
    private int windowSize = -1;

    @Override
    public void setWindowSize(int sz) {
        windowSize = sz;
    }

    @Override
    public int getWindowSize() {
        if (query == null) return 0;
        if (windowSize <= 0)
            windowSize = query.getMorphium().getConfig().getCursorBatchSize();
        return windowSize;
    }

    @Override
    public void setQuery(Query<T> q) {
        try {
            query = q.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Query<T> getQuery() {
        return query;
    }

    @Override
    public int getCurrentBufferSize() {
        return currentBatch.getBatch().size();
    }

    @Override
    public List<T> getCurrentBuffer() {
        return null;
    }

    @Override
    public long getCount() {
        return 0;
    }

    @Override
    public int getCursor() {
        return cursorExternal;
    }

    @Override
    public void ahead(int jump) {
        cursor += jump;
        cursorExternal += jump;
        while (cursor >= currentBatch.getBatch().size()) {
            int diff = cursor - currentBatch.getBatch().size();
            cursor = currentBatch.getBatch().size() - 1;

            if (!hasNext()) return;
            cursor += diff;
        }
    }

    @Override
    public void back(int jump) {
        cursor -= jump;
        cursorExternal -= jump;
        if (cursor < 0) throw new IllegalArgumentException("cannot jumb back over batch boundaries!");
    }

    @Override
    public void setNumberOfPrefetchWindows(int n) {
        throw new IllegalArgumentException("not possible");
    }

    @Override
    public int getNumberOfAvailableThreads() {
        return 1;
    }


    @Override
    public int getNumberOfThreads() {
        return 1;
    }

    @Override
    public boolean isMultithreaddedAccess() {
        return multithreadded;
    }

    @Override
    public void setMultithreaddedAccess(boolean mu) {
        multithreadded = mu;
    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        if (multithreadded) {
            synchronized (this) {
                return doHasNext();
            }
        }
        return doHasNext();
    }

    private boolean doHasNext() {
        try {
            int size = currentBatch.getBatch().size();
            if (currentBatch == null) {
                currentBatch = query.getMorphium().getDriver().initIteration(query.getMorphium().getConfig().getDatabase(), query.getCollectionName(), query.toQueryObject(), query.getSort(), query.getFieldListForQuery(), query.getSkip(), query.getLimit(), getWindowSize(), query.getMorphium().getReadPreferenceForClass(query.getType()), null);
                cursor = 0;
                cursorExternal++;
            } else if (currentBatch != null && cursor + 1 < size) {
                cursor++;
                cursorExternal++;
                return true;
            } else if (currentBatch != null && cursor + 1 == size) {
                currentBatch = query.getMorphium().getDriver().nextIteration(currentBatch);
                cursor = 0;
                cursorExternal++;
            }
            if (multithreadded && currentBatch != null && currentBatch.getBatch() != null)
                currentBatch.setBatch(Collections.synchronizedList(currentBatch.getBatch()));
            if (currentBatch != null && currentBatch.getBatch() != null && size > 0)
                return true;
        } catch (MorphiumDriverException e) {
            log.error("Got error during iteration...", e);
        }
        return false;
    }

    @Override
    public T next() {
        if (currentBatch != null) {
            T unmarshall = query.getMorphium().getMapper().unmarshall(query.getType(), currentBatch.getBatch().get(cursor));
            query.getMorphium().firePostLoadEvent(unmarshall);
            return unmarshall;
        }
        return null;
    }
}
