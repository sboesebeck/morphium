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

    void setMorphium(Morphium m);

    Morphium getMorphium();

    void setSearchType(Class<? extends T> type);

    Class<? extends T> getSearchType();

    void setResultType(Class<? extends R> type);

    Class<? extends R> getResultType();

    Aggregator<T, R> project(Map<String, Object> m);  //field -> other field, field -> 0,1

    Aggregator<T, R> project(String... m);    //field:1

    Aggregator<T, R> match(Query q);

    Aggregator<T, R> limit(int num);

    Aggregator<T, R> skip(int num);

    Aggregator<T, R> unwind(String listField);

    Aggregator<T, R> sort(String... prefixed);

    Aggregator<T, R> sort(Map<String, Integer> sort);

    void setCollectionName(String cn);

    String getCollectionName();

    Group<T, R> group(Map<String, Object> id);

    Group<T, R> groupSubObj(Map<String, String> idSubObject);

    Group<T, R> group(String id);

    List<Map<String, Object>> toAggregationList();

    void addOperator(Map<String, Object> o);

    List<R> aggregate();

    void aggregate(AsyncOperationCallback<R> callback);

    void setExplain(boolean explain);

    void setUseDisk(boolean useDisk);

    boolean isExplain();

    boolean isUseDisk();

}
