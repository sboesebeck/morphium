package de.caluga.morphium.aggregation;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 11:07
 * <p/>
 */
@SuppressWarnings("UnusedDeclaration")
public interface AggregatorFactory {
    Class<? extends Aggregator> getAggregatorClass();

    void setAggregatorClass(Class<? extends Aggregator> AggregatorImpl);

    <T, R> Aggregator<T, R> createAggregator(Class<? extends T> type, Class<? extends R> resultType);
}


