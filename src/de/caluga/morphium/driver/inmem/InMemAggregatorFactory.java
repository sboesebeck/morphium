package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.aggregation.Aggregator;
import de.caluga.morphium.aggregation.AggregatorFactory;

public class InMemAggregatorFactory implements AggregatorFactory {
    private Class<? extends Aggregator> inMemAggregator;

    @Override
    public Class<? extends Aggregator> getAggregatorClass() {
        return inMemAggregator;
    }

    @Override
    public void setAggregatorClass(Class<? extends Aggregator> aggregatorImpl) {
        inMemAggregator = aggregatorImpl;
    }

    @Override
    public <T, R> Aggregator<T, R> createAggregator(Class<? extends T> type, Class<? extends R> resultType) {
        try {
            Aggregator agg = inMemAggregator.newInstance();
            //noinspection unchecked
            agg.setSearchType(type);
            //noinspection unchecked
            agg.setResultType(resultType);
            //noinspection unchecked
            return agg;
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }
}
