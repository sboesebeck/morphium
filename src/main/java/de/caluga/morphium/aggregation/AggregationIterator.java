package de.caluga.morphium.aggregation;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumCursor;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.AggregateMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
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
public class AggregationIterator<T, R> implements MorphiumAggregationIterator<T, R> {

    private final Logger log = LoggerFactory.getLogger(AggregationIterator.class);
    private boolean multithreadded;
    private Aggregator<T, R> aggregator;

    private MorphiumCursor cursor = null;


    public void ahead(int jump) {
        try {
            getMongoCursor().ahead(jump);
        } catch (MorphiumDriverException e) {
            getMongoCursor().close();
            throw new RuntimeException(e);
        }
    }


    public void back(int jump) {
        try {
            getMongoCursor().back(jump);
        } catch (MorphiumDriverException e) {
            getMongoCursor().close();
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
        return getMongoCursor().hasNext();
    }

    @Override
    public Map<String, Object> nextMap() {
        return getMongoCursor().next();
    }

    @Override
    public R next() {
        if (Map.class.isAssignableFrom(aggregator.getResultType())) {
            return (R) getMongoCursor().next();
        }

        return aggregator.getMorphium().getMapper().deserialize(aggregator.getResultType(), getMongoCursor().next());
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
        getMongoCursor().close();
    }

    @Override
    public int getCursor() {
        return getMongoCursor().getCursor();
    }

    //private AggregateMongoCommand getAggregateCmd() {
    //    MongoConnection readConnection = aggregator.getMorphium().getDriver().getReadConnection(null);
    //    AggregateMongoCommand settings = new AggregateMongoCommand(readConnection);
    //    settings.setDb(aggregator.getMorphium().getConfig().getDatabase())
    //            .setColl(aggregator.getCollectionName())
    //            .setPipeline(aggregator.getPipeline())
    //            .setExplain(aggregator.isExplain())
    //            .setReadPreference(aggregator.getMorphium().getReadPreferenceForClass(aggregator.getSearchType()))
    //            .setAllowDiskUse(aggregator.isUseDisk());
    //    if (aggregator.getCollation() != null) settings.setCollation(Doc.of(aggregator.getCollation().toQueryObject()));
    //    //TODO .setReadConcern(morphium.getReadPreferenceForC)
    //    aggregator.getMorphium().getDriver().releaseConnection(readConnection);
    //    return settings;
    //}
    //
    //
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
                cursor = aggregator.getAggregateCmd().executeIterable(aggregator.getMorphium().getConfig().getCursorBatchSize());
            } catch (MorphiumDriverException e) {
                throw new RuntimeException(e);
            }
        }

        return cursor;
    }
}
