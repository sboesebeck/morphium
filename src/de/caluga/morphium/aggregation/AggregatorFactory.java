package de.caluga.morphium.aggregation;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 11:07
 * <p/>
 */
public interface AggregatorFactory {
    public Class<? extends Aggregator> getAggregatorClass();

    public void setAggregatorClass(Class<? extends Aggregator> AggregatorImpl);

    public <T, R> Aggregator<T, R> createAggregator(Class<? extends T> type, Class<? extends R> resultType);
}


