package de.caluga.morphium.aggregation;

import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * User: Stephan Bösebeck
 * Date: 25.03.16
 * Time: 22:33
 * <p>
 * iterating over huge collections using the mongodb internal cursor
 */
public class AggregationIterator<T, R> implements MorphiumAggregationIterator<T, R> {

    private final Logger log = LoggerFactory.getLogger(AggregationIterator.class);
    private MorphiumCursor currentBatch;

    private int cursor = 0;
    private int cursorExternal = 0;
    private boolean multithreadded;
    private int windowSize = -1;
    private Aggregator<T, R> aggregator;

    @Override
    public int getWindowSize() {

        if (windowSize <= 0) {
            windowSize = aggregator.getMorphium().getConfig().getCursorBatchSize();
        }
        return windowSize;
    }

    @Override
    public void setWindowSize(int sz) {
        windowSize = sz;
    }

    @Override
    public int getCurrentBufferSize() {
        if (currentBatch == null || currentBatch.getBatch() == null) return 0;
        return currentBatch.getBatch().size();
    }

    @Override
    public List<R> getCurrentBuffer() {
        return null;
    }

    @Override
    public long getCount() {
        return aggregator.getCount();
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

            next();
            cursor += diff;
        }
    }

    @Override
    public void back(int jump) {
        cursor -= jump;
        cursorExternal -= jump;
        if (cursor < 0) {
            throw new IllegalArgumentException("cannot jumb back over batch boundaries!");
        }
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
    public Iterator<R> iterator() {
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
        if (currentBatch != null && currentBatch.getBatch() != null && currentBatch.getBatch().size() > cursor) {
            return true;
        }
        if (currentBatch == null && cursorExternal == 0) {
            try {
                //noinspection unchecked
                currentBatch = aggregator.getMorphium().getDriver().initAggregationIteration(aggregator.getMorphium().getConfig().getDatabase(), aggregator.getCollectionName(), aggregator.getPipeline(), aggregator.getMorphium().getReadPreferenceForClass(aggregator.getSearchType()), aggregator.getCollation(), getWindowSize(), null);
            } catch (MorphiumDriverException e) {
                log.error("error during fetching first batch", e);
            }
            return doHasNext();
        }
        return false;
    }

    @Override
    public R next() {
        if (currentBatch == null && !hasNext()) {
            return null;
        }
        R unmarshall = null;
        if (aggregator.getResultType().equals(Map.class)) {
            //noinspection unchecked
            unmarshall = (R) currentBatch.getBatch().get(cursor);
        } else {
            unmarshall = aggregator.getMorphium().getMapper().deserialize(aggregator.getResultType(), currentBatch.getBatch().get(cursor));
        }
        aggregator.getMorphium().firePostLoadEvent(unmarshall);
        try {
            if (currentBatch == null && cursorExternal == 0) {
                //noinspection unchecked
                currentBatch = aggregator.getMorphium().getDriver().initAggregationIteration(aggregator.getMorphium().getConfig().getDatabase(), aggregator.getCollectionName(), aggregator.getPipeline(), aggregator.getMorphium().getReadPreferenceForClass(aggregator.getSearchType()), aggregator.getCollation(), getWindowSize(), null);
                cursor = 0;
            } else if (currentBatch != null && cursor + 1 < currentBatch.getBatch().size()) {
                cursor++;
            } else if (currentBatch != null && cursor + 1 == currentBatch.getBatch().size()) {
                //noinspection unchecked
                currentBatch = aggregator.getMorphium().getDriver().nextIteration(currentBatch);
                cursor = 0;
            } else {
                cursor++;
            }
            if (multithreadded && currentBatch != null && currentBatch.getBatch() != null) {
                currentBatch.setBatch(Collections.synchronizedList(currentBatch.getBatch()));
            }

        } catch (MorphiumDriverException e) {
            log.error("Got error during iteration...", e);
        }
        cursorExternal++;

        return unmarshall;
    }

    @Override
    public Aggregator<T, R> getAggregator() {
        return aggregator;
    }

    @Override
    public void setAggregator(Aggregator<T, R> aggregator) {
        this.aggregator = aggregator;
    }
}
