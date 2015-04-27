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
public class MorphiumIteratorImpl<T> implements MorphiumIterator<T> {
    private int windowSize = 1;

    private Query<T> theQuery;
    private Container<T>[] prefetchBuffers;
    private int cursor = 0;
    private long count = 0;

    private Logger log = new Logger(MorphiumIterator.class);
    private long limit;
    private int prefetchWindows = 1;

    private boolean multithreaddedAccess = false;



    public MorphiumIteratorImpl() {
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


    private List<T> getBuffer(int windowNumber) {
        try {
            int skp = windowNumber * windowSize;
//            System.out.println("Getting buffer win: " + windowNumber + " skip: " + skp + " windowSize: " + windowSize+" Count: "+count+"   limit: "+limit);
            Query q = null;

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
            if (list.size() == 0) {
                System.err.println("No results?");
            }
            if (list == null) {
                System.err.println("Error: no result!?!?!");
                return new ArrayList<>();
            }
            return list;
        } catch (CloneNotSupportedException e) {
            System.out.println("CLONE FAILED!?!?!?!?");
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
            prefetchBuffers = new Container[prefetchWindows];
            prefetchBuffers[0] = new Container<T>();
            prefetchBuffers[0].setData(getBuffer(0));

            for (int i = 1; i < prefetchWindows; i++) {
                final Container<T> c = new Container<>();
                prefetchBuffers[i] = c;
                final int idx = i;
//                while (executorService.getQueue().remainingCapacity() < 100) {
//                    Thread.yield();
//                }
                Runnable cmd = new Runnable() {
                    @Override
                    public void run() {
                        if (idx * windowSize <= limit && idx * windowSize <= count) {
                            c.setData(getBuffer(idx));
                        }
                    }
                };

                boolean queued = false;
                while (!queued) {
                    try {
                        theQuery.getMorphium().queueTask(cmd);
                        queued = true;
                    } catch (Throwable e) {

                    }

                }
            }
        }


        while (prefetchBuffers[0].getData() == null) {
            Thread.yield();
        }
        T ret;
        if (prefetchBuffers[0].getData().size() == 0) {
            ret = null;
        } else {
            ret = prefetchBuffers[0].getData().get(cursor % windowSize);
        }
        if ((cursor % windowSize) + 1 >= windowSize) {
            //removing first
            for (int i = 1; i < prefetchWindows; i++) {
                prefetchBuffers[i - 1] = prefetchBuffers[i];
            }

            prefetchBuffers[prefetchWindows - 1] = new Container<T>();
            final int win = cursor / windowSize + prefetchWindows;
            cursor++;
            if (win * windowSize < count) {
                //add new one in background...
                final Container<T> container = prefetchBuffers[prefetchWindows - 1];

                theQuery.getMorphium().queueTask(new Runnable() {
                    public void run() {
//                        System.out.println("Executing..." + win + " / " + cursor + " / " + executorService.getActiveCount() + " / queue: " + executorService.getQueue().size());
                        container.setData(getBuffer(win));
                    }
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
