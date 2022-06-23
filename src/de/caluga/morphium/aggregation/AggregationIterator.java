package de.caluga.morphium.aggregation;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.AggregateMongoCommand;
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
    private Aggregator<T, R> aggregator;


    @Override
    public int available() {
        if (currentBatch == null || currentBatch.getBatch() == null) return 0;
        return currentBatch.getBatch().size();
    }

    @Override
    public List<R> getCurrentBuffer() {
        return null;
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
    public void close() {

    }

    @Override
    public Map<String, Object> nextMap() {
        return null;
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
                currentBatch = aggregator.getMorphium().getDriver().initAggregationIteration(getAggregateCmdSettings());
            } catch (MorphiumDriverException e) {
                log.error("error during fetching first batch", e);
            }
            return doHasNext();
        }
        return false;
    }

    private AggregateMongoCommand getAggregateCmdSettings() {
        AggregateMongoCommand settings = new AggregateMongoCommand();
        settings.setDb(aggregator.getMorphium().getConfig().getDatabase())
                .setColl(aggregator.getCollectionName())
                .setPipeline(Doc.convertToDocList(aggregator.getPipeline()))
                .setExplain(aggregator.isExplain())
                .setReadPreference(aggregator.getMorphium().getReadPreferenceForClass(aggregator.getSearchType()))
                .setAllowDiskUse(aggregator.isUseDisk());
        if (aggregator.getCollation() != null) settings.setCollation(Doc.of(aggregator.getCollation().toQueryObject()));
        //TODO .setReadConcern(morphium.getReadPreferenceForC)
        return settings;
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

                currentBatch = aggregator.getMorphium().getDriver().initAggregationIteration(getAggregateCmdSettings());
                cursor = 0;
            } else if (currentBatch != null && cursor + 1 < currentBatch.getBatch().size()) {
                cursor++;
            } else if (currentBatch != null && cursor + 1 == currentBatch.getBatch().size()) {
                //noinspection unchecked
                //currentBatch = aggregator.getMorphium().getDriver().nextIteration(currentBatch);
                cursor = 0;
            } else {
                cursor++;
            }
            if (multithreadded && currentBatch != null && currentBatch.getBatch() != null) {
                currentBatch.setBatch(Collections.synchronizedList(currentBatch.getBatch()));
            }

        } catch (Exception e) {
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
