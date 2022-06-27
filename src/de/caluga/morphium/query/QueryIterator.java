package de.caluga.morphium.query;

import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
    private MorphiumCursor cursor = null;

    private int windowSize = 0;

    public int getWindowSize() {
        return windowSize;
    }

    public QueryIterator<T> setWindowSize(int windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    public Query<T> getQuery() {
        return query;
    }


    public void setQuery(Query<T> q) {
        query = q.clone();
    }


    public long getCount() {
        return query.countAll();
    }


    public void ahead(int jump) {
        try {
            getMongoCursor().ahead(jump);
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }


    public void back(int jump) {
        try {
            getMongoCursor().back(jump);
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int available() {
        return getMongoCursor().available();
    }

    public Iterator<T> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        try {
            return getMongoCursor().hasNext();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Object> nextMap() {
        try {
            return getMongoCursor().next();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T next() {

        try {
            return query.getMorphium().getMapper().deserialize(query.getType(), getMongoCursor().next());
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<T> getCurrentBuffer() {
        List<T> ret = new ArrayList<>();
        for (Map<String, Object> o : getMongoCursor().getBatch()) {
            ret.add(query.getMorphium().getMapper().deserialize(query.getType(), o));
        }
        return ret;
    }

    @Override
    public void close() {
        try {
            getMongoCursor().close();
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getCursor() {
        return getMongoCursor().getCursor();
    }

    private MorphiumCursor getMongoCursor() {
        if (cursor == null) {
            try {
                var cmd = query.getFindCmd();
                if (windowSize != 0) {
                    cmd.setBatchSize(windowSize);
                }
                cursor = cmd.executeIterable();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            }
        }
        return cursor;
    }
}
