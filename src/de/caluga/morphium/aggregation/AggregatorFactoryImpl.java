package de.caluga.morphium.aggregation;

/**
 * User: Stephan Bösebeck
 * Date: 31.08.12
 * Time: 11:13
 * <p/>
 */
public class AggregatorFactoryImpl implements AggregatorFactory {
    private Class<? extends Aggregator> aggregatorClass;

    @SuppressWarnings("unused")
    public AggregatorFactoryImpl() {
    }

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

    @SuppressWarnings("unchecked")
    @Override
    public <T, R> Aggregator<T, R> createAggregator(Class<? extends T> type, Class<? extends R> resultType) {
        try {
            Aggregator<T, R> a = (Aggregator<T, R>) aggregatorClass.newInstance();
            a.setSearchType(type);
            a.setResultType(resultType);
            return a;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }
}
