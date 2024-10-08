package de.caluga.morphium.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.annotations.lifecycle.Lifecycle;
import de.caluga.morphium.annotations.lifecycle.PostLoad;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;

/**
 * User: Stephan Bösebeck
 * Date: 25.03.16
 * Time: 22:33
 * <p>
 * iterating over huge collections using the mongodb internal cursor
 */
public class QueryIterator<T> implements MorphiumIterator<T>, Iterator<T> {

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
            if (getMongoCursor() != null && getMongoCursor().getConnection() != null)
                getMongoCursor().getConnection().close();

            throw new RuntimeException(e);
        }
    }

    public void back(int jump) {
        try {
            getMongoCursor().back(jump);
        } catch (MorphiumDriverException e) {
            if (getMongoCursor() != null && getMongoCursor().getConnection() != null)
                getMongoCursor().getConnection().close();

            throw new RuntimeException(e);
        }
    }

    @Override
    public int available() {
        try {
            return getMongoCursor().available();
        } catch (Exception e) {
            if (getMongoCursor() != null && getMongoCursor().getConnection() != null)
                getMongoCursor().getConnection().close();

            throw new RuntimeException(e);
        }
    }

    public Iterator<T> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        try {
            var ret = getMongoCursor().hasNext();

            if (!ret) {
                if (getMongoCursor() != null && getMongoCursor().getConnection() != null)
                    getMongoCursor().getConnection().close();
            }

            return ret;
        } catch (Exception e) {
            if (getMongoCursor() != null && getMongoCursor().getConnection() != null)
                getMongoCursor().getConnection().close();

            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Object> nextMap() {
        try {
            return getMongoCursor().next();
        } catch (Exception e) {
            if (getMongoCursor() != null && getMongoCursor().getConnection() != null)
                getMongoCursor().getConnection().close();

            throw new RuntimeException(e);
        }
    }

    @Override
    public T next() {
        try {
            if (query.getType() == null) {
                return (T) getMongoCursor().next();
            }

            var ret = query.getMorphium().getMapper().deserialize(query.getType(), getMongoCursor().next());

            if (query.getMorphium().getARHelper().isAnnotationPresentInHierarchy(query.getType(), Lifecycle.class)) {
                query.getMorphium().getARHelper().callLifecycleMethod(PostLoad.class, ret);
            }

            return ret;
        } catch (Exception e) {
            if (getMongoCursor() != null && getMongoCursor().getConnection() != null)
                getMongoCursor().getConnection().close();

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
        getMongoCursor().close();
    }

    @Override
    public int getCursor() {
        return getMongoCursor().getCursor();
    }

    public MorphiumCursor getMongoCursor() {
        if (cursor == null) {
            var cmd = query.getFindCmd();

            try {
                if (windowSize != 0) {
                    cmd.setBatchSize(windowSize);
                }

                cursor = cmd.executeIterable(windowSize);
            } catch (MorphiumDriverException e) {
                cmd.releaseConnection();
                throw new RuntimeException(e);
            }
        }

        return cursor;
    }
}
