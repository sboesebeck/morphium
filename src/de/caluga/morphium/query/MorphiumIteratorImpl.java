package de.caluga.morphium.query;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    private Container<T>[] prefetchBuffers;
    private int cursor = 0;
    private long count = 0;

    private Logger log = Logger.getLogger(MorphiumIterator.class);
    private long limit;
    private int prefetchWindows = 1;


    private final ArrayBlockingQueue<Runnable> workQueue;
    private ExecutorService executorService;


    public MorphiumIteratorImpl() {
        workQueue = new ArrayBlockingQueue<>(100);
        executorService = new ThreadPoolExecutor(10, 100, 10000, TimeUnit.MILLISECONDS, workQueue);

    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return (cursor < count) && (cursor < limit);
    }


    private List<T> getBuffer(int windowNumber) {
        int skp = windowNumber * windowSize;
        Query q = theQuery.q();
        q.skip(skp); //sounds strange, but is necessary for Jump / backs
        q.limit(windowSize);
        if (q.getSort() == null || q.getSort().isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("No sort parameter given - sorting by _id");
            }
            q.sort("_id"); //always sort with id field if no sort is given
        }
        return q.asList();
    }

    @Override
    public T next() {
        if (cursor > count || cursor > limit) {
            return null;
        }

        if (prefetchBuffers == null) {
            //first iteration
            prefetchBuffers = new Container[prefetchWindows];
            prefetchBuffers[0] = new Container<T>();
            prefetchBuffers[0].setData(getBuffer(0));

            for (int i = 1; i < prefetchWindows; i++) {
                final Container<T> c = new Container<>();
                prefetchBuffers[i] = c;
                final int idx = i;
                while (workQueue.remainingCapacity() < 5) {
                    Thread.yield();
                }
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        if (idx * windowSize <= limit && idx * windowSize <= count) {
                            c.setData(getBuffer(idx));
                        }
                    }
                });
            }


        }


        if (cursor + 1 > windowSize) {
            //removing first
            for (int i = 1; i < prefetchWindows; i++) {
                prefetchBuffers[i - 1] = prefetchBuffers[i];
            }


            prefetchBuffers[prefetchWindows - 1] = new Container<T>();

            //add new one in background...

            while (workQueue.remainingCapacity() < 5) {
                Thread.yield();
            }
            int numOfThreads = workQueue.size();
            final int win = cursor / windowSize + prefetchWindows - 1;
            executorService.submit(new Runnable() {
                public void run() {
                    prefetchBuffers[prefetchWindows - 1].setData(getBuffer(win));
                }
            });


        }
        if (prefetchBuffers[0] == null || prefetchBuffers[0].getData() == null) {
            while (workQueue.size() > 0 && !(prefetchBuffers[0].getData() != null)) {
//                log.info("Waiting for threads to finish...");
                Thread.yield();
            }
            if (prefetchBuffers[0] == null || prefetchBuffers[0].getData() == null) {
                return null;
            }

        }
        if (prefetchBuffers[0].getData() == null) return null;
        cursor++;
        T ret = prefetchBuffers[0].getData().get(cursor % windowSize);
        return ret;
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
        if (prefetchBuffers == null) return 0;
        if (prefetchBuffers[0] == null || prefetchBuffers[0].getData() == null) return 0;

        int cnt = 0;
        for (Container<T> buffer : prefetchBuffers) {
            if (buffer.getData() == null) continue;
            cnt += buffer.getData().size();
        }
        return cnt;
    }

    @Override
    public List<T> getCurrentBuffer() {
        if (prefetchBuffers == null || prefetchBuffers[0] == null || prefetchBuffers[0].getData() == null)
            return new ArrayList<>();
        return prefetchBuffers[0].getData();
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
            prefetchBuffers = null;
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
            prefetchBuffers = null;
        }
        cursor -= jump;
    }

    @Override
    public void setNumberOfPrefetchWindows(int n) {
        this.prefetchWindows = n;
    }


    private class Container<T> {
        private List<T> data;

        public List<T> getData() {
            return data;
        }

        public void setData(List<T> data) {
            this.data = data;
        }
    }
}
