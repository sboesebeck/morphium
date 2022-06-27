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
    private boolean multithreadded;
    private Aggregator<T, R> aggregator;

    private MorphiumCursor cursor = null;


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

    public Iterator<R> iterator() {
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
    public R next() {
        try {
            if (Map.class.isAssignableFrom(aggregator.getResultType())) {
                return (R) getMongoCursor().next();
            }
            return aggregator.getMorphium().getMapper().deserialize(aggregator.getResultType(), getMongoCursor().next());
        } catch (MorphiumDriverException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<R> getCurrentBuffer() {
        List<R> ret = new ArrayList<>();
        for (Map<String, Object> o : getMongoCursor().getBatch()) {
            if (Map.class.isAssignableFrom(aggregator.getResultType())) {
                ret.add((R) o);
            } else {
                ret.add(aggregator.getMorphium().getMapper().deserialize(aggregator.getResultType(), o));
            }
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

    private AggregateMongoCommand getAggregateCmd() {
        AggregateMongoCommand settings = new AggregateMongoCommand(aggregator.getMorphium().getDriver());
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
    public Aggregator<T, R> getAggregator() {
        return aggregator;
    }

    @Override
    public void setAggregator(Aggregator<T, R> aggregator) {
        this.aggregator = aggregator;
    }

    private MorphiumCursor getMongoCursor() {
        if (cursor == null) {
            try {
                cursor = aggregator.getAggregateCmd().executeIterable();
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            }
        }
        return cursor;
    }
}
