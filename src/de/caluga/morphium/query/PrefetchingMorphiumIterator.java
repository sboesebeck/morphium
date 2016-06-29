package de.caluga.morphium.query;

import de.caluga.morphium.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 23.11.12
 * Time: 11:40
 * <p/>
 * This implementation of the Iterable Interface maxe paging on db side easier.
 * This iterator read WINDOWSIZE objects from Mongo and holds them in memory, until iterated over them.
 */
public class PrefetchingMorphiumIterator<T> implements MorphiumIterator<T> {
    private final Logger log = new Logger(MorphiumIterator.class);
    private int windowSize = 1;
    private Query<T> theQuery;
    private Container<T>[] prefetchBuffers;
    private int cursor = 0;
    private long count = 0;
    private long limit;
    private int prefetchWindows = 2;

    private boolean multithreaddedAccess = false;


    public PrefetchingMorphiumIterator() {
        log.warn("Prefetching Iterator is relaying on skip-functionality of mongo which can cause problems in some cases - use DefaultMorphiumIterator instead");
        //        workQueue = new ArrayBlockingQueue<>(1000, true);
        //        executorService = new ThreadPoolExecutor(10, 100, 1000, TimeUnit.MILLISECONDS, workQueue);

    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return (cursor < count) && (cursor < limit);
    }


    private List getBuffer(int windowNumber) {
        try {
            int skp = windowNumber * windowSize;
            //            System.out.println("Getting buffer win: " + windowNumber + " skip: " + skp + " windowSize: " + windowSize+" Count: "+count+"   limit: "+limit);
            Query q;

            q = theQuery.clone();

            q.skip(skp); //sounds strange, but is necessary for Jump / backs
            q.limit(windowSize);
            if (q.getSort() == null || q.getSort().isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("No sort parameter given - sorting by _id");
                }
                q.sort("_id"); //always sort with id field if no sort is given
            }
            List list = q.asList();
            if (list.isEmpty()) {
                log.error("No results?");
            }
            return list;
        } catch (CloneNotSupportedException e) {
            log.fatal("CLONE FAILED!?!?!?!?");
            return new ArrayList<>();
        }
    }

    @Override
    public T next() {
        if (multithreaddedAccess) {
            synchronized (this) {
                return doNext();
            }
        } else {
            return doNext();
        }
    }


    private T doNext() {
        if (cursor > count || cursor > limit) {
            return null;
        }

        if (prefetchBuffers == null) {
            //first iteration
            //noinspection unchecked
            prefetchBuffers = new Container[prefetchWindows];
            prefetchBuffers[0] = new Container<>();
            //noinspection unchecked
            prefetchBuffers[0].setData(getBuffer(cursor / windowSize));

            for (int i = cursor / windowSize + 1; i < prefetchWindows; i++) {
                final Container<T> c = new Container<>();
                prefetchBuffers[i] = c;
                final int idx = i;
                //                while (executorService.getQueue().remainingCapacity() < 100) {
                //                    Thread.yield();
                //                }
                Runnable cmd = () -> {
                    if (idx * windowSize <= limit && idx * windowSize <= count) {
                        //noinspection unchecked
                        c.setData(getBuffer(idx));
                    }
                };

                boolean queued = false;
                while (!queued) {
                    try {
                        theQuery.getMorphium().queueTask(cmd);
                        queued = true;
                    } catch (Throwable ignored) {

                    }

                }
            }
        }


        while (prefetchBuffers[0].getData() == null) {
            Thread.yield();
        }
        T ret;
        if (prefetchBuffers[0].getData().isEmpty()) {
            ret = null;
        } else {
            ret = prefetchBuffers[0].getData().get(cursor % windowSize);
        }
        if ((cursor % windowSize) + 1 >= windowSize) {
            //removing first
            System.arraycopy(prefetchBuffers, 1, prefetchBuffers, 0, prefetchWindows - 1);

            prefetchBuffers[prefetchWindows - 1] = new Container<>();
            final int win = cursor / windowSize + prefetchWindows;
            cursor++;
            if (win * windowSize < count) {
                //add new one in background...
                final Container<T> container = prefetchBuffers[prefetchWindows - 1];

                theQuery.getMorphium().queueTask(() -> {
                    //                        System.out.println("Executing..." + win + " / " + cursor + " / " + executorService.getActiveCount() + " / queue: " + executorService.getQueue().size());
                    //noinspection unchecked
                    container.setData(getBuffer(win));
                });
            }
        } else {
            cursor++;
        }

        return ret;
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
        if (prefetchBuffers == null) {
            return 0;
        }
        if (prefetchBuffers[0] == null || prefetchBuffers[0].getData() == null) {
            return 0;
        }

        int cnt = 0;
        for (Container<T> buffer : prefetchBuffers) {
            if (buffer.getData() == null) {
                continue;
            }
            cnt += buffer.getData().size();
        }
        return cnt;
    }

    @Override
    public List<T> getCurrentBuffer() {
        if (prefetchBuffers == null || prefetchBuffers[0] == null || prefetchBuffers[0].getData() == null) {
            return new ArrayList<>();
        }
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
        if (multithreaddedAccess) {
            synchronized (this) {
                doAhead(jump);
            }
        } else {
            doAhead(jump);
        }
    }

    private void doAhead(int jump) {
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
        if (multithreaddedAccess) {
            synchronized (this) {
                doBack(jump);
            }
        } else {
            doBack(jump);
        }

    }

    private void doBack(int jump) {
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

    @Override
    public int getNumberOfAvailableThreads() {
        return theQuery.getMorphium().getNumberOfAvailableThreads();
        //        executorService.
        //        return workQueue.remainingCapacity();

    }


    @Override
    public int getNumberOfThreads() {
        //        return workQueue.size();
        return theQuery.getMorphium().getActiveThreads();
    }


    @Override
    public boolean isMultithreaddedAccess() {
        return multithreaddedAccess;
    }

    @Override
    public void setMultithreaddedAccess(boolean mu) {
        multithreaddedAccess = mu;
    }

    @SuppressWarnings("TypeParameterHidesVisibleType")
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
