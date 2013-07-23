package de.caluga.morphium.query;

import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Stack;

/**
 * User: Stephan Bösebeck
 * Date: 23.11.12
 * Time: 11:40
 * <p/>
 * This implementation of the Iterable Interface maxe paging on db side easier.
 * This iterator read WINDOWSIZE objects from Mongo and holds them in memory, until iterated over them.
 */
public class MorphiumIteratorImpl<T> implements MorphiumIterator<T> {
    private int windowSize = 1;

    private Query<T> theQuery;
    private List<T> buffer;
    private Stack<List<T>> prefechBuffers;
    private int cursor = 0;
    private long count = 0;

    private Logger log = Logger.getLogger(MorphiumIterator.class);
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
        if (cursor > count || cursor > limit) {
            return null;
        }
        int idx = cursor % windowSize;
        if (buffer == null || idx == 0) {
            int skp = (cursor / windowSize) * windowSize;
            theQuery.skip(skp); //sounds strange, but is necessary for Jump / backs
            if (count - cursor < windowSize) {
                theQuery.limit((int) (count - cursor));
            } else if (limit - cursor < windowSize) {
                theQuery.limit((int) (limit - cursor));
            } else {
                theQuery.limit(windowSize);
            }
            if (log.isDebugEnabled()) {
                log.debug("Reached window boundary - reading in: skip:" + skp + " limit:" + windowSize);
            }
            if (theQuery.getSort() == null || theQuery.getSort().isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("No sort parameter given - sorting by _id");
                }
                theQuery.sort("_id"); //always sort with id field if no sort is given
            }
            buffer = theQuery.asList();
        }
        cursor++;
        return buffer.get(idx);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove is not possible on MorphiumIterators");
    }

    @Override
    public void setWindowSize(int sz) {
        windowSize = sz;
    }

    @Override
    public int getWindowSize() {
        return windowSize;
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
    public Query<T> getQuery() {
        return theQuery;
    }

    @Override
    public int getCurrentBufferSize() {
        if (buffer == null) return 0;
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
        if ((cursor / windowSize) * windowSize + windowSize <= cursor + jump) {
            if (log.isDebugEnabled()) {
                log.debug("Would jump over boundary - resetting buffer");
            }
            buffer = null;
        }
        cursor += jump;
    }

    @Override
    public void back(int jump) {
        //begin of buffer index
        if ((cursor / windowSize * windowSize) > cursor - jump) {
            if (log.isDebugEnabled()) {
                log.debug("Would jump before boundary - resetting buffer");
            }
            buffer = null;
        }
        cursor -= jump;
    }


}
