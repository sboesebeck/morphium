package de.caluga.morphium.aggregation;

import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.query.MorphiumIterator;

public interface MorphiumAggregationIterator<T, R> extends MorphiumIterator<R> {

    Aggregator<T, R> getAggregator();

    void setAggregator(Aggregator<T, R> aggregator);
}
