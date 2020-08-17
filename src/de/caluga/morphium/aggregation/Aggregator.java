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

    @SuppressWarnings("unused")
    Aggregator<T, R> addFields(Map<String, Object> m);  //field -> other field, field -> 0,1

    Aggregator<T, R> project(String... m);    //field:1

    Aggregator<T, R> match(Query<T> q);

    Aggregator<T, R> count(String fld);

    Aggregator<T, R> count(Enum fld);

    Aggregator<T, R> bucket(Map<String, Object> param);

    Aggregator<T, R> bucketAuto(Map<String, Object> param);

    Aggregator<T, R> collStats(Map<String, Object> param);

    Aggregator<T, R> currentOp(Map<String, Object> param);

    Aggregator<T, R> facet(Map<String, Object> param);

    Aggregator<T, R> geoNear(Map<String, Object> param);

    Aggregator<T, R> graphLookup(Map<String, Object> param);

    Aggregator<T, R> indexStats(Map<String, Object> param);

    Aggregator<T, R> listLocalSessions(Map<String, Object> param);

    Aggregator<T, R> listSessions(Map<String, Object> param);

    Aggregator<T, R> lookup(Map<String, Object> param);

    Aggregator<T, R> match(Map<String, Object> param);

    Aggregator<T, R> merge(Map<String, Object> param);

    Aggregator<T, R> out(Map<String, Object> param);

    Aggregator<T, R> planCacheStats(Map<String, Object> param);

    Aggregator<T, R> redact(Map<String, Object> param);

    Aggregator<T, R> replaceRoot(Map<String, Object> param);

    Aggregator<T, R> replaceWith(Map<String, Object> param);

    Aggregator<T, R> sample(Map<String, Object> param);

    Aggregator<T, R> set(Map<String, Object> param);

    Aggregator<T, R> sortByCount(Map<String, Object> param);

    Aggregator<T, R> unionWith(Map<String, Object> param);

    Aggregator<T, R> unset(List<String> field);

    Aggregator<T, R> unset(String... param);

    Aggregator<T, R> unset(Enum... field);


    Aggregator<T, R> genericStage(String stageName, Object param);


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
