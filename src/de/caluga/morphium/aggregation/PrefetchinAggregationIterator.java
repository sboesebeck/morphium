package de.caluga.morphium.aggregation;/**
 * Created by stephan on 04.04.16.
 */

import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.query.MorphiumQueryIterator;
import de.caluga.morphium.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 * iterating over huge collections using the db interal cursor. This iterator does create a thread reading the data
 **/
public class PrefetchinAggregationIterator<T, R> implements MorphiumAggregationIterator<T, R> {
    private final Logger log = LoggerFactory.getLogger(PrefetchinAggregationIterator.class);
    private long lastAccess = System.currentTimeMillis();
    private List<List<R>> prefetchBuffer; //each entry is one buffer
    private int batchsize;
    private MorphiumCursor cursor;
    private int numPrefetchBuffers;
    private volatile int cursorPos;
    private boolean startedAlready = false;
    private Aggregator<T, R> aggregator;

    public PrefetchinAggregationIterator() {
        prefetchBuffer = new CopyOnWriteArrayList<>();//Collections.synchronizedList(new ArrayList<>());
    }

    @SuppressWarnings("unused")
    public List<List<R>> getPrefetchBuffer() {
        checkAndUpdateLastAccess();
        return prefetchBuffer;
    }

    @SuppressWarnings("unused")
    public void setPrefetchBuffer(List<List<R>> prefetchBuffer) {
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
    public Aggregator<T, R> getAggregator() {
        return aggregator;
    }

    @Override
    public void setAggregator(Aggregator<T, R> aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    public int getCurrentBufferSize() {
        checkAndUpdateLastAccess();
        return prefetchBuffer.size();
    }

    @Override
    public List<R> getCurrentBuffer() {
        checkAndUpdateLastAccess();
        return prefetchBuffer.get(0);
    }

    @Override
    public long getCount() {
        checkAndUpdateLastAccess();
        return 0;
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
            //noinspection NonAtomicOperationOnVolatileField
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
    public Iterator<R> iterator() {
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
                cursor = aggregator.getMorphium().getDriver().initAggregationIteration(aggregator.getMorphium().getConfig().getDatabase(), aggregator.getCollectionName(), aggregator.getPipeline(), aggregator.getMorphium().getReadPreferenceForClass(aggregator.getSearchType()), aggregator.getCollation(), getWindowSize(), null);
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
        if (prefetchBuffer.isEmpty()) {
            return false;
        }
        //end of results
        return !(cursorPos % getWindowSize() == 0 && prefetchBuffer.size() == 1 && cursor == null) && (cursorPos % getWindowSize() < prefetchBuffer.get(0).size());

    }

    private List<R> getBatch(MorphiumCursor crs) {
        @SuppressWarnings("unchecked") List<Map<String, Object>> batch = crs.getBatch();
        List<R> ret = new ArrayList<>();
        if (batch == null) {
            return ret;
        }
        for (Map<String, Object> obj : batch) {
            R unmarshall = aggregator.getMorphium().getMapper().deserialize(aggregator.getResultType(), obj);

            ret.add(unmarshall);
        }
        aggregator.getMorphium().firePostLoad(ret);
        return ret;
    }

    private void startPrefetch() {
        aggregator.getMorphium().queueTask(() -> {
            log.info("Starting prefetching...");
            while (cursor != null) {
                while (prefetchBuffer.size() >= numPrefetchBuffers && cursor != null) //noinspection EmptyCatchBlock
                {
                    try {
                        //Busy wait for buffer to be processed
                        int waitTime = aggregator.getMorphium().getConfig().getMaxWaitTime();
                        if (waitTime > 0 && System.currentTimeMillis() - lastAccess > waitTime) {
                            log.error("Cursor timeout... closing");
                            try {
                                aggregator.getMorphium().getDriver().closeIteration(cursor);
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
                        MorphiumCursor crs = aggregator.getMorphium().getDriver().nextIteration(cursor);
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
    public R next() {
        checkAndUpdateLastAccess();

        if (cursor == null && !startedAlready) {
            if (!hasNext()) {
                return null;
            }
        }
        if (prefetchBuffer.isEmpty()) {
            log.error("Prefetchbuffer is empty!");
            return null;
        }
        if (cursorPos != 0 && cursorPos % getWindowSize() == 0) {
            prefetchBuffer.remove(0);
        }
        while (prefetchBuffer.isEmpty() && cursor != null) {
            Thread.yield();
        }
        //noinspection NonAtomicOperationOnVolatileField
        return prefetchBuffer.get(0).get(cursorPos++ % getWindowSize());
    }

    private void checkAndUpdateLastAccess() {
//        if (query == null) {
//            return;
//        }
//        long l = System.currentTimeMillis() - lastAccess;
//        if (l > query.getMorphium().getConfig().getMaxWaitTime()) {
//            throw new RuntimeException("Cursor timeout - max wait time of " + query.getMorphium().getConfig().getMaxWaitTime() + "ms reached (duration is " + l + ")");
//        }
        lastAccess = System.currentTimeMillis();
    }
}