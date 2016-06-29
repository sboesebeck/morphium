package de.caluga.morphium.query;

/**
 * Created by stephan on 03.06.15.
 */

import de.caluga.morphium.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This class was introduced with V2.2.21BETA7. The Prefetching iterator had some issues useing the skip functionality of mongodb (causing timeouts of some kind)
 * After researching the only solution was to remove the prefetching functionality and usage of Skip as teh default behavior
 * the DefaultMorphiumIterator uses the sort paramter to determin the last values a window and would issue an additional query for it when reading the next.
 * This is a bit slower but more reliable than using skip.
 *
 * @param <T>
 * @Author: Stephan Bösebeck (sb@caluga.de)
 */
public class DefaultMorphiumIterator<T> implements MorphiumIterator<T> {

    private final Logger log = new Logger(DefaultMorphiumIterator.class);
    private int windowSize = 1;
    private Query<T> theQuery;
    private List<T> buffer;
    private int cursor = 0;
    private long count = 0;
    private Map<String, Object> lastValues = new HashMap<>();
    private long limit;

    @Override
    public Iterator<T> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return (cursor < count) && (cursor < limit);
    }

    @Override
    public T next() {
        if (isMultithreaddedAccess()) {
            synchronized (this) {
                return donext();
            }
        } else {
            return donext();
        }
    }

    private T donext() {
        if (cursor > count || cursor > limit) {
            return null;
        }
        int idx = cursor % windowSize;
        if (buffer == null || idx == 0) {
            if (theQuery.getSort() == null || theQuery.getSort().isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("No sort parameter given - sorting by _id");
                }
                theQuery.sort("_id"); //always sort with id field if no sort is given
            }

            Query<T> q = getLimitedQuery(theQuery);
            if (count - cursor < windowSize) {
                q.limit((int) (count - cursor));
            } else if (limit - cursor < windowSize) {
                q.limit((int) (limit - cursor));
            } else {
                q.limit(windowSize);
            }
            buffer = q.asList();
            if (log.isDebugEnabled()) {
                log.debug("Reached window boundary - read in: " + buffer.size() + " limit:" + windowSize + " pos: " + cursor);
            }
            if (buffer == null || buffer.isEmpty()) {
                log.fatal("Buffer is empty!?!?!?! cursor: " + cursor + " cnt: " + count + " window: " + windowSize + " query: " + q.toQueryObject().toString());
            } else {
                updateLastValues(q, buffer);
            }

        }
        if (buffer != null) {
            cursor++;
            if (idx >= buffer.size()) {
                log.debug("Trying to read past end of buffer - automatic deletion?");
                return null;
            }
            return buffer.get(idx);
        } else {
            return null;
        }
    }

    private void updateLastValues(Query<T> q, List<T> buffer) {
        try {
            Map<String, Integer> sort = q.getSort();
            for (Map.Entry<String, Integer> e : sort.entrySet()) {
                lastValues.put(e.getKey(), q.getMorphium().getARHelper().getValue(buffer.get(buffer.size() - 1), e.getKey()));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Query<T> getLimitedQuery(Query<T> q) {
        try {
            Map<String, Integer> sort = q.getSort();
            Query<T> ret = q.clone();
            for (Map.Entry<String, Integer> e : sort.entrySet()) {
                if (lastValues.get(e.getKey()) == null) {
                    continue;
                }
                if (e.getValue().equals(-1)) {
                    ret.f(e.getKey()).lt(lastValues.get(e.getKey()));
                } else {
                    ret.f(e.getKey()).gt(lastValues.get(e.getKey()));
                }
            }

            return ret;
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex); //should never happen
        }

    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove is not possible on MorphiumIterators");
    }

    @Override
    public int getWindowSize() {
        return windowSize;
    }

    @Override
    public void setWindowSize(int sz) {
        windowSize = sz;
    }

    @Override
    public Query<T> getQuery() {
        return theQuery;
    }

    @Override
    public void setQuery(Query<T> q) {
        try {
            theQuery = q.clone();
        } catch (CloneNotSupportedException ignored) {
        }
        count = theQuery.countAll();
        limit = theQuery.getLimit();
        if (limit <= 0) {
            limit = count;
        }
    }

    @Override
    public int getCurrentBufferSize() {
        if (buffer == null) {
            return 0;
        }
        return buffer.size();
    }

    @Override
    public List<T> getCurrentBuffer() {
        return buffer;
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public int getCursor() {
        return cursor;
    }

    @Override
    public void ahead(int jump) {
        //end of buffer index
        //        if ((cursor / windowSize) * windowSize + windowSize <= cursor + jump) {
        //            if (log.isDebugEnabled()) {
        //                log.debug("Would jump over boundary - resetting buffer");
        //            }
        //            buffer = null;
        //        }
        //        cursor += jump;
        for (int i = 0; i < jump && hasNext(); i++) next();
    }

    @Override
    public void back(int jump) {
        int newCursor = cursor - jump;
        lastValues.clear();
        cursor = 0;
        buffer = null;
        if (newCursor < 0) {
            return;
        }
        for (int i = 0; i < newCursor && hasNext(); i++) next();

    }

    @Override
    public void setNumberOfPrefetchWindows(int n) {
        throw new IllegalArgumentException("prefetch not possible in Default iterator");
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
        return lastValues instanceof ConcurrentHashMap;
    }

    @Override
    public void setMultithreaddedAccess(boolean mu) {
        lastValues = new ConcurrentHashMap<>(lastValues);
    }


}

