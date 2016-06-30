package de.caluga.morphium.query;/**
 * Created by stephan on 04.04.16.
 */

import de.caluga.morphium.Logger;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 * iterating over huge collections using the db interal cursor. This iterator does create a thread reading the data
 **/
public class PrefetchingDriverIterator<T> implements MorphiumIterator<T> {
    private final Logger log = new de.caluga.morphium.Logger(PrefetchingDriverIterator.class);
    private long lastAccess = System.currentTimeMillis();
    private List<List<T>> prefetchBuffer; //each entry is one buffer
    private Query<T> query;
    private int batchsize;
    private MorphiumCursor cursor;
    private int numPrefetchBuffers;
    private volatile int cursorPos;
    private boolean startedAlready = false;

    public PrefetchingDriverIterator() {
        prefetchBuffer = new CopyOnWriteArrayList<>();//Collections.synchronizedList(new ArrayList<>());
    }

    @SuppressWarnings("unused")
    public List<List<T>> getPrefetchBuffer() {
        checkAndUpdateLastAccess();
        return prefetchBuffer;
    }

    @SuppressWarnings("unused")
    public void setPrefetchBuffer(List<List<T>> prefetchBuffer) {
        checkAndUpdateLastAccess();
        this.prefetchBuffer = prefetchBuffer;
    }

    @Override
    public int getWindowSize() {
        checkAndUpdateLastAccess();

        return batchsize;
    }

    @Override
    public void setWindowSize(int sz) {
        checkAndUpdateLastAccess();
        this.batchsize = sz;
    }

    @Override
    public Query<T> getQuery() {
        checkAndUpdateLastAccess();
        return query;
    }

    @Override
    public void setQuery(Query<T> q) {
        checkAndUpdateLastAccess();

        query = q;
    }

    @Override
    public int getCurrentBufferSize() {
        checkAndUpdateLastAccess();
        return prefetchBuffer.size();
    }

    @Override
    public List<T> getCurrentBuffer() {
        checkAndUpdateLastAccess();
        return prefetchBuffer.get(0);
    }

    @Override
    public long getCount() {
        checkAndUpdateLastAccess();
        return query.countAll();
    }

    @Override
    public int getCursor() {
        checkAndUpdateLastAccess();
        return cursorPos;
    }

    @Override
    public void ahead(int jump) {
        for (int i = 0; i < jump; i++)
            next();
    }

    @Override
    public void back(int jump) {
        if (jump < cursorPos % getWindowSize()) {
            cursorPos -= jump;
        } else {
            throw new IllegalArgumentException("Cannot jump back past window boundaries");
        }
    }

    @Override
    public void setNumberOfPrefetchWindows(int n) {
        checkAndUpdateLastAccess();

        if (n <= 1) {
            n = 2;
            log.error("Prefetching only makes sense with at least 2 prefetchwindows... setting to 2");
        }
        numPrefetchBuffers = n;
    }

    @Override
    public int getNumberOfAvailableThreads() {
        checkAndUpdateLastAccess();
        return numPrefetchBuffers;
    }

    @Override
    public int getNumberOfThreads() {
        checkAndUpdateLastAccess();
        return 0;
    }

    @Override
    public boolean isMultithreaddedAccess() {
        checkAndUpdateLastAccess();
        return true;
    }

    @Override
    public void setMultithreaddedAccess(boolean mu) {
        //always true
        checkAndUpdateLastAccess();

    }

    @Override
    public Iterator<T> iterator() {
        checkAndUpdateLastAccess();
        return this;
    }

    @Override
    public boolean hasNext() {
        checkAndUpdateLastAccess();

        if (cursor == null && !startedAlready) {
            startedAlready = true;
            //startup
            try {
                cursor = query.getMorphium().getDriver().initIteration(query.getMorphium().getConfig().getDatabase(), query.getCollectionName(), query.toQueryObject(), query.getSort(), query.getFieldListForQuery(), query.getSkip(), query.getLimit(), batchsize, query.getMorphium().getReadPreferenceForClass(query.getType()), null);
                if (cursor == null) {
                    return false;
                }
                if (cursor.getBatch() == null) {
                    return false;
                }
                //Starting background process for filling buffer
                prefetchBuffer.add(getBatch(cursor));
                startPrefetch();

                if (!prefetchBuffer.get(0).isEmpty()) {
                    return true;
                }

            } catch (MorphiumDriverException e) {
                e.printStackTrace();
            }

        }
        while (prefetchBuffer.size() <= 1 && cursor != null) {
            Thread.yield(); //for end of data detection
        }
        if (prefetchBuffer.isEmpty() && cursor == null) {
            return false;
        }
        //end of results
        return !(cursorPos % getWindowSize() == 0 && prefetchBuffer.size() == 1 && cursor == null) && (cursorPos % getWindowSize() < prefetchBuffer.get(0).size());

    }

    private List<T> getBatch(MorphiumCursor crs) {
        @SuppressWarnings("unchecked") List<Map<String, Object>> batch = crs.getBatch();
        List<T> ret = new ArrayList<>();
        if (batch == null) {
            return ret;
        }
        for (Map<String, Object> obj : batch) {
            T unmarshall = query.getMorphium().getMapper().unmarshall(query.getType(), obj);

            ret.add(unmarshall);
        }
        query.getMorphium().firePostLoad(ret);
        return ret;
    }

    private void startPrefetch() {
        query.getMorphium().queueTask(() -> {
            log.info("Starting prefetching...");
            while (cursor != null) {
                while (prefetchBuffer.size() >= numPrefetchBuffers && cursor != null) //noinspection EmptyCatchBlock
                {
                    try {
                        //Busy wait for buffer to be processed
                        int socketTimeout = query.getMorphium().getConfig().getSocketTimeout();
                        if (socketTimeout > 0 && System.currentTimeMillis() - lastAccess > socketTimeout) {
                            log.error("Cursor timeout... closing");
                            try {
                                query.getMorphium().getDriver().closeIteration(cursor);
                            } catch (MorphiumDriverException e) {
                                //e.printStackTrace(); Swallow it, as it is probably timedout anyway
                            }
                            cursor = null;
                            return;
                        }
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        log.debug("got interrupted - ignore");
                    }
                }
                while (prefetchBuffer.size() < numPrefetchBuffers) {
                    try {
                        if (cursor == null) {
                            break;
                        }
                        MorphiumCursor crs = query.getMorphium().getDriver().nextIteration(cursor);
                        if (crs == null || crs.getBatch() == null || crs.getBatch().isEmpty()) {
                            cursor = null;
                            break;
                        }
                        prefetchBuffer.add(getBatch(crs));
                        cursor = crs;
                    } catch (MorphiumDriverException e) {
                        cursor = null;
                        e.printStackTrace();
                        break;
                    }
                }
            }
            log.info("Prefetch finished");
        });
    }

    @Override
    public T next() {
        checkAndUpdateLastAccess();
        if (cursor == null && !startedAlready) {
            if (!hasNext()) {
                return null;
            }
        }
        if (cursorPos != 0 && cursorPos % getWindowSize() == 0) {
            prefetchBuffer.remove(0);
        }
        while (prefetchBuffer.isEmpty() && cursor != null) {
            Thread.yield();
        }
        if (prefetchBuffer.isEmpty()) {
            log.error("Prefetchbuffer is empty!");
            return null;
        }
        return prefetchBuffer.get(0).get(cursorPos++ % getWindowSize());
    }

    private void checkAndUpdateLastAccess() {
        if (query == null) {
            return;
        }
        if (System.currentTimeMillis() - lastAccess > query.getMorphium().getConfig().getMaxWaitTime()) {
            throw new RuntimeException("Cursor timeout - max wait time reached");
        }
        lastAccess = System.currentTimeMillis();
    }
}