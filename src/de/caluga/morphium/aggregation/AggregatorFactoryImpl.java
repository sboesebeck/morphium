package de.caluga.morphium.aggregation;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 11:13
 * <p/>
 * TODO: Add documentation here
 */
public class AggregatorFactoryImpl implements AggregatorFactory {
    private Class<? extends Aggregator> aggregatorClass;

    public AggregatorFactoryImpl(Class<? extends Aggregator> qi) {
        aggregatorClass = qi;
    }

    @Override
    public Class<? extends Aggregator> getAggregatorClass() {
        return aggregatorClass;
    }

    @Override
    public void setAggregatorClass(Class<? extends Aggregator> AggregatorImpl) {
        this.aggregatorClass = AggregatorImpl;
    }

    @Override
    public <T, R> Aggregator<T, R> createAggregator(Class<T> type, Class<R> resultType) {
        try {
            Aggregator<T, R> a = (Aggregator<T, R>) aggregatorClass.newInstance();
            a.setSearchType(type);
            a.setResultType(resultType);
            return a;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }
}
