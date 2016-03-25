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
 * TODO: Add documentation here
 */
public class MorphiumDriverIterator<T> implements MorphiumIterator<T> {

    private Query<T> query;
    private Logger log = new Logger(MorphiumDriverIterator.class);

    private MorphiumCursor<T> currentBatch = null;

    private int cursor = 0;
    private boolean multithreadded;

    @Override
    public void setWindowSize(int sz) {
        log.error("Cannot set window size - buffer is determined by driver");
    }

    @Override
    public int getWindowSize() {
        if (query == null) return 0;
        return query.getMorphium().getConfig().getCursorBatchSize();
    }

    @Override
    public void setQuery(Query<T> q) {
        try {
            query = q.clone();
        } catch (CloneNotSupportedException e) {
            //TODO: Implement Handling
            throw new RuntimeException(e);
        }
    }

    @Override
    public Query<T> getQuery() {
        return query;
    }

    @Override
    public int getCurrentBufferSize() {
        return 0;
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
        return 0;
    }

    @Override
    public void ahead(int jump) {

    }

    @Override
    public void back(int jump) {

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
            if (currentBatch == null) {
                currentBatch = query.getMorphium().getDriver().initIteration(query.getMorphium().getConfig().getDatabase(), query.getCollectionName(), query.toQueryObject(), query.getSort(), query.getFieldListForQuery(), query.getSkip(), query.getLimit(), query.getMorphium().getConfig().getCursorBatchSize(), query.getMorphium().getReadPreferenceForClass(query.getType()), null);
                cursor = 0;

            } else if (currentBatch != null && cursor + 1 < currentBatch.getBatch().size()) {
                cursor++;
                return true;
            } else if (currentBatch != null && cursor + 1 == currentBatch.getBatch().size()) {
                currentBatch = query.getMorphium().getDriver().nextIteration(currentBatch);
                cursor = 0;
            }
            if (multithreadded && currentBatch != null && currentBatch.getBatch() != null)
                currentBatch.setBatch(Collections.synchronizedList(currentBatch.getBatch()));
            if (currentBatch != null && currentBatch.getBatch() != null && currentBatch.getBatch().size() > 0)
                return true;
        } catch (MorphiumDriverException e) {
            log.error("Got error during iteration...", e);
        }
        return false;
    }

    @Override
    public T next() {
        if (currentBatch != null)
            return query.getMorphium().getMapper().unmarshall(query.getType(), currentBatch.getBatch().get(cursor));
        return null;
    }
}
