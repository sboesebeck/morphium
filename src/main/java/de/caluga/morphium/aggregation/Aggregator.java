package de.caluga.morphium.aggregation;

import de.caluga.morphium.Collation;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.AggregateMongoCommand;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.query.Query;

import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 16:23
 * <p>
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
@SuppressWarnings("rawtypes")
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

    Aggregator<T, R> project(String fld, Expr e);

    Aggregator<T, R> match(Query<T> q);

    Aggregator<T, R> matchSubQuery(Query<?> q);

    Aggregator<T, R> match(Expr q);

    Aggregator<T, R> count(String fld);

    Aggregator<T, R> count(Enum fld);

    Aggregator<T, R> bucket(Expr groupBy, List<Expr> boundaries, Expr preset, Map<String, Expr> output);

    Aggregator<T, R> bucketAuto(Expr groupBy, int numBuckets, Map<String, Expr> output, BucketGranularity granularity);

    /**
     * @param latencyHistograms: if null, no latency stats
     * @param scale:             if null, no storageStats
     * @param count
     * @param queryExecStats
     * @return
     */

    Aggregator<T, R> collStats(Boolean latencyHistograms, Double scale, boolean count, boolean queryExecStats);

    Aggregator<T, R> currentOp(boolean allUsers, boolean idleConnections, boolean idleCursors, boolean idleSessions, boolean localOps);

    Aggregator<T, R> facetExpr(Map<String, Expr> param);

    Aggregator<T, R> facet(Map<String, Aggregator> pipeline);

    Aggregator<T, R> geoNear(Map<GeoNearFields, Object> param);

    Aggregator<T, R> graphLookup(Class<?> fromType, Expr startWith, Enum connectFromField, Enum connectToField, String as, Integer maxDepth, String depthField, Query restrictSearchWithMatch);

    Aggregator<T, R> graphLookup(Class<?> fromType, Expr startWith, String connectFromField, String connectToField, String as, Integer maxDepth, String depthField, Query restrictSearchWithMatch);

    Aggregator<T, R> graphLookup(String fromCollection, Expr startWith, String connectFromField, String connectToField, String as, Integer maxDepth, String depthField, Query restrictSearchWithMatch);

    Aggregator<T, R> indexStats();

    Aggregator<T, R> listLocalSessions();

    Aggregator<T, R> listLocalSessionsAllUsers();

    Aggregator<T, R> listLocalSessions(List<String> users, List<String> dbs);

    Aggregator<T, R> listSessions();

    Aggregator<T, R> listSessionsAllUsers();

    Aggregator<T, R> listSessions(List<String> users, List<String> dbs);

    Aggregator<T, R> collation(Collation collation);

    Collation getCollation();


    Aggregator<T, R> lookup(Class fromType, Enum localField, Enum foreignField, String outputArray, List<Expr> pipeline, Map<String, Expr> let);

    Aggregator<T, R> lookup(String fromCollection, String localField, String foreignField, String outputArray, List<Expr> pipeline, Map<String, Expr> let);

    Aggregator<T, R> merge(String intoDb, String intoCollection, MergeActionWhenMatched matchAction, MergeActionWhenNotMatched notMatchedAction, String... onFields);

    Aggregator<T, R> merge(String intoCollection, MergeActionWhenMatched matchAction, MergeActionWhenNotMatched notMatchedAction, String... onFields);

    Aggregator<T, R> merge(String intoCollection, Map<String, Expr> let, List<Map<String, Expr>> machedPipeline, MergeActionWhenNotMatched notMatchedAction, String... onFields);

    Aggregator<T, R> merge(Class<?> intoCollection, Map<String, Expr> let, List<Map<String, Expr>> machedPipeline, MergeActionWhenMatched matchAction, MergeActionWhenNotMatched notMatchedAction, String... onFields);

    Aggregator<T, R> merge(String intoDb, String intoCollection);

    Aggregator<T, R> merge(Class<?> intoCollection);

    Aggregator<T, R> merge(String intoCollection);

    Aggregator<T, R> out(String collection);

    Aggregator<T, R> out(Class<?> type);

    Aggregator<T, R> out(String db, String collection);

    Aggregator<T, R> planCacheStats(Map<String, Object> param);

    /**
     * see https://docs.mongodb.com/manual/reference/operator/aggregation/redact/
     *
     * @param expr
     * @return
     */
    Aggregator<T, R> redact(Expr expr);

    Aggregator<T, R> replaceRoot(Expr newRoot);

    /**
     * Replaces the input document with the specified document. The operation replaces all existing fields
     * in the input document, including the _id field. With $replaceWith, you can promote an embedded document
     * to the top-level. You can also specify a new document as the replacement.
     *
     * @param replacement
     * @return
     */

    Aggregator<T, R> replaceWith(Expr replacement);

    /**
     * Randomly selects the specified number of documents from its input.
     *
     * @param sampleSize
     * @return
     */
    Aggregator<T, R> sample(int sampleSize);

    Aggregator<T, R> set(Map<String, Expr> param);

    Aggregator<T, R> sortByCount(Expr countBy);

    Aggregator<T, R> unionWith(String collection);

    Aggregator<T, R> unionWith(Aggregator aggregator);

    Aggregator<T, R> unset(List<String> field);

    Aggregator<T, R> unset(String... param);

    Aggregator<T, R> unset(Enum... field);


    Aggregator<T, R> genericStage(String stageName, Object param);


    Aggregator<T, R> limit(int num);

    @SuppressWarnings("unused")
    Aggregator<T, R> skip(int num);

    @SuppressWarnings("unused")
    Aggregator<T, R> unwind(Expr listField);

    Aggregator<T, R> unwind(String listField);

    Aggregator<T, R> sort(String... prefixed);

    @SuppressWarnings("unused")
    Aggregator<T, R> sort(Map<String, Integer> sort);

    String getCollectionName();

    @SuppressWarnings("unused")
    Aggregator<T,R> setCollectionName(String cn);

    Group<T, R> group(Map<String, Object> id);

    Group<T, R> group(String id);

    Group<T, R> group(Expr id);

    List<Map<String, Object>> getPipeline();

    void addOperator(Map<String, Object> o);

    List<R> aggregate();

    long getCount();

    MorphiumAggregationIterator<T, R> aggregateIterable();

    @SuppressWarnings("unused")
    void aggregate(AsyncOperationCallback<R> callback);

    List<Map<String, Object>> aggregateMap();

    @SuppressWarnings("unused")
    void aggregateMap(AsyncOperationCallback<Map<String, Object>> callback);

    boolean isExplain();

    @SuppressWarnings("unused")
    void setExplain(boolean explain);

    boolean isUseDisk();

    @SuppressWarnings("unused")
    void setUseDisk(boolean useDisk);

    Map<String,Object> explain() throws MorphiumDriverException;
    Map<String,Object> explain(ExplainVerbosity verbosity) throws MorphiumDriverException;

    enum GeoNearFields {
        near,
        distanceField,
        spherical,
        maxDistance,
        query,
        distanceMultiplier,
        includeLocs,
        uniqueDocs,
        minDistance,
        key,
    }


    @SuppressWarnings("SameParameterValue")
    enum BucketGranularity {
        R5,
        R10,
        R20,
        R40,
        R80,
        E6,
        E12,
        E24,
        E48,
        E96,
        E192,
        POWERSOF2,
        SERIES_125("1-2-5");

        private final String value;

        BucketGranularity() {
            value = name();
        }

        BucketGranularity(String name) {
            value = name;
        }

        public String getValue() {
            return value;
        }


    }

    public AggregateMongoCommand getAggregateCmd();

    enum MergeActionWhenMatched {
        replace, keepExisting, merge, fail,
    }

    enum MergeActionWhenNotMatched {
        insert, discard, fail,
    }

}
