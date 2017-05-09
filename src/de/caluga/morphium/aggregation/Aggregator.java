package de.caluga.morphium.aggregation;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.query.Query;

import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 16:23
 * <p/>
 * Aggregator Framework:
 * represents the aggregator of Mongo 2.2.x. and above
 * $project
 * $match
 * $limit
 * $skip
 * $unwind
 * $group
 * $sort
 * $geoNear - implementation still missing
 */
public interface Aggregator<T, R> {

    @SuppressWarnings("unused")
    Morphium getMorphium();

    void setMorphium(Morphium m);

    Class<? extends T> getSearchType();

    void setSearchType(Class<? extends T> type);

    Class<? extends R> getResultType();

    void setResultType(Class<? extends R> type);

    Aggregator<T, R> project(Map<String, Object> m);  //field -> other field, field -> 0,1

    Aggregator<T, R> project(String... m);    //field:1

    Aggregator<T, R> match(Query<T> q);

    /**
     * use matchSubQuery if you prepared som mid-result in your aggregation you need to do an additional match for
     * Do not use it for the base query!
     *
     * @param q
     * @return aggregator
     */
    @SuppressWarnings("unused")
    Aggregator<T, R> matchSubQuery(Query<?> q);

    Aggregator<T, R> limit(int num);

    @SuppressWarnings("unused")
    Aggregator<T, R> skip(int num);

    @SuppressWarnings("unused")
    Aggregator<T, R> unwind(String listField);

    Aggregator<T, R> sort(String... prefixed);

    @SuppressWarnings("unused")
    Aggregator<T, R> sort(Map<String, Integer> sort);

    String getCollectionName();

    @SuppressWarnings("unused")
    void setCollectionName(String cn);

    Group<T, R> group(Map<String, Object> id);

    @SuppressWarnings("unused")
    Group<T, R> groupSubObj(Map<String, String> idSubObject);

    Group<T, R> group(String id);

    List<Map<String, Object>> toAggregationList();

    void addOperator(Map<String, Object> o);

    List<R> aggregate();

    @SuppressWarnings("unused")
    void aggregate(AsyncOperationCallback<R> callback);

    boolean isExplain();

    @SuppressWarnings("unused")
    void setExplain(boolean explain);

    boolean isUseDisk();

    @SuppressWarnings("unused")
    void setUseDisk(boolean useDisk);

}
