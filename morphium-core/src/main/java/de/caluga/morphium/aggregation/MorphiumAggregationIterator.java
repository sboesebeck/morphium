package de.caluga.morphium.aggregation;

import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.query.MorphiumIterator;

/**
 * Iterator for aggregation results.
 * @param <T> the search type
 * @param <R> the result type
 */
public interface MorphiumAggregationIterator<T, R> extends MorphiumIterator<R> {

    /** Returns the aggregator associated with this iterator.
     * @return the aggregator */
    Aggregator<T, R> getAggregator();

    /** Sets the aggregator for this iterator.
     * @param aggregator the aggregator to set */
    void setAggregator(Aggregator<T, R> aggregator);
}
