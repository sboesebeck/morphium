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
 * </p>
 *
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 16:23
 *
 * @param <T> search type
 * @param <R> result type
 */
@SuppressWarnings("rawtypes")
public interface Aggregator<T, R> {

    /** Returns the morphium instance.
     * @return the morphium instance */
    @SuppressWarnings("unused")
    Morphium getMorphium();

    /** Sets the morphium instance.
     * @param m the morphium instance */
    void setMorphium(Morphium m);

    /** Returns the search type for this aggregation.
     * @return the search type */
    Class<? extends T> getSearchType();

    /** Sets the search type for this aggregation.
     * @param type the search type */
    void setSearchType(Class<? extends T> type);

    /** Returns the result type for this aggregation.
     * @return the result type */
    Class<? extends R> getResultType();

    /** Sets the result type for this aggregation.
     * @param type the result type */
    void setResultType(Class<? extends R> type);

    /** Adds a $project stage using a map of field expressions.
     * @param m map of field names to 0/1 or other field references
     * @return the aggregator */
    Aggregator<T, R> project(Map<String, Object> m);  //field -> other field, field -> 0,1

    /** Adds a $addFields stage to the pipeline.
     * @param m map of field names to values or other field references
     * @return the aggregator */
    @SuppressWarnings("unused")
    Aggregator<T, R> addFields(Map<String, Object> m);  //field -> other field, field -> 0,1

    /** Adds a $project stage with the given field names (included with value 1).
     * @param m the field names to include
     * @return the aggregator */
    Aggregator<T, R> project(String... m);    //field:1

    /** Adds a $project stage with a single field and expression.
     * @param fld the field name
     * @param e the expression
     * @return the aggregator */
    Aggregator<T, R> project(String fld, Expr e);

    /** Adds a $match stage using a Query.
     * @param q the query to match
     * @return the aggregator */
    Aggregator<T, R> match(Query<T> q);

    /** Adds a $match stage using a sub-query of a different type.
     * @param q the sub-query to match
     * @return the aggregator */
    Aggregator<T, R> matchSubQuery(Query<?> q);

    /** Adds a $match stage using an expression.
     * @param q the match expression
     * @return the aggregator */
    Aggregator<T, R> match(Expr q);

    /** Adds a $count stage using a field name.
     * @param fld the output field name for the count
     * @return the aggregator */
    Aggregator<T, R> count(String fld);

    /** Adds a $count stage using an enum field.
     * @param fld the output field enum for the count
     * @return the aggregator */
    Aggregator<T, R> count(Enum fld);

    /** Adds a $bucket stage to the pipeline.
     * @param groupBy the expression to group by
     * @param boundaries the bucket boundaries
     * @param preset the default value for documents not matching any boundary
     * @param output the output fields
     * @return the aggregator */
    Aggregator<T, R> bucket(Expr groupBy, List<Expr> boundaries, Expr preset, Map<String, Expr> output);

    /** Adds a $bucketAuto stage to the pipeline.
     * @param groupBy the expression to group by
     * @param numBuckets the number of buckets
     * @param output the output fields
     * @param granularity the preferred number series for bucket boundaries
     * @return the aggregator */
    Aggregator<T, R> bucketAuto(Expr groupBy, int numBuckets, Map<String, Expr> output, BucketGranularity granularity);

    /**
     * Adds a $collStats stage to the pipeline.
     * @param latencyHistograms if null, no latency stats are included
     * @param scale if null, no storageStats are included
     * @param count whether to include document count
     * @param queryExecStats whether to include query execution stats
     * @return the aggregator
     */
    Aggregator<T, R> collStats(Boolean latencyHistograms, Double scale, boolean count, boolean queryExecStats);

    /** Adds a $currentOp stage to the pipeline.
     * @param allUsers whether to include all users
     * @param idleConnections whether to include idle connections
     * @param idleCursors whether to include idle cursors
     * @param idleSessions whether to include idle sessions
     * @param localOps whether to include only local operations
     * @return the aggregator */
    Aggregator<T, R> currentOp(boolean allUsers, boolean idleConnections, boolean idleCursors, boolean idleSessions, boolean localOps);

    /** Adds a $facet stage using expressions.
     * @param param the facet output field map
     * @return the aggregator */
    Aggregator<T, R> facetExpr(Map<String, Expr> param);

    /** Adds a $facet stage using sub-pipelines.
     * @param pipeline map of output field names to sub-aggregators
     * @return the aggregator */
    Aggregator<T, R> facet(Map<String, Aggregator> pipeline);

    /** Adds a $geoNear stage to the pipeline.
     * @param param the geoNear parameters
     * @return the aggregator */
    Aggregator<T, R> geoNear(Map<GeoNearFields, Object> param);

    /** Adds a $graphLookup stage using enum field references.
     * @param fromType the collection type to search
     * @param startWith expression for the starting value
     * @param connectFromField the field to connect from
     * @param connectToField the field to connect to
     * @param as the output array field name
     * @param maxDepth maximum recursion depth
     * @param depthField field name to store depth
     * @param restrictSearchWithMatch optional filter query
     * @return the aggregator */
    Aggregator<T, R> graphLookup(Class<?> fromType, Expr startWith, Enum connectFromField, Enum connectToField, String as, Integer maxDepth, String depthField, Query restrictSearchWithMatch);

    /** Adds a $graphLookup stage using string field references.
     * @param fromType the collection type to search
     * @param startWith expression for the starting value
     * @param connectFromField the field to connect from
     * @param connectToField the field to connect to
     * @param as the output array field name
     * @param maxDepth maximum recursion depth
     * @param depthField field name to store depth
     * @param restrictSearchWithMatch optional filter query
     * @return the aggregator */
    Aggregator<T, R> graphLookup(Class<?> fromType, Expr startWith, String connectFromField, String connectToField, String as, Integer maxDepth, String depthField, Query restrictSearchWithMatch);

    /** Adds a $graphLookup stage using a collection name and string fields.
     * @param fromCollection the collection name to search
     * @param startWith expression for the starting value
     * @param connectFromField the field to connect from
     * @param connectToField the field to connect to
     * @param as the output array field name
     * @param maxDepth maximum recursion depth
     * @param depthField field name to store depth
     * @param restrictSearchWithMatch optional filter query
     * @return the aggregator */
    Aggregator<T, R> graphLookup(String fromCollection, Expr startWith, String connectFromField, String connectToField, String as, Integer maxDepth, String depthField, Query restrictSearchWithMatch);

    /** Adds a $indexStats stage to the pipeline.
     * @return the aggregator */
    Aggregator<T, R> indexStats();

    /** Adds a $listLocalSessions stage for the current user.
     * @return the aggregator */
    Aggregator<T, R> listLocalSessions();

    /** Adds a $listLocalSessions stage for all users.
     * @return the aggregator */
    Aggregator<T, R> listLocalSessionsAllUsers();

    /** Adds a $listLocalSessions stage filtered by users and databases.
     * @param users list of user names to filter
     * @param dbs list of database names to filter
     * @return the aggregator */
    Aggregator<T, R> listLocalSessions(List<String> users, List<String> dbs);

    /** Adds a $listSessions stage for the current user.
     * @return the aggregator */
    Aggregator<T, R> listSessions();

    /** Adds a $listSessions stage for all users.
     * @return the aggregator */
    Aggregator<T, R> listSessionsAllUsers();

    /** Adds a $listSessions stage filtered by users and databases.
     * @param users list of user names to filter
     * @param dbs list of database names to filter
     * @return the aggregator */
    Aggregator<T, R> listSessions(List<String> users, List<String> dbs);

    /** Sets the collation for this aggregation.
     * @param collation the collation to use
     * @return the aggregator */
    Aggregator<T, R> collation(Collation collation);

    /** Returns the collation for this aggregation.
     * @return the collation */
    Collation getCollation();


    /** Adds a $lookup stage using enum field references.
     * @param fromType the type to join
     * @param localField the local field enum
     * @param foreignField the foreign field enum
     * @param outputArray the output array field name
     * @param pipeline optional sub-pipeline
     * @param let optional let variables
     * @return the aggregator */
    Aggregator<T, R> lookup(Class fromType, Enum localField, Enum foreignField, String outputArray, List<Expr> pipeline, Map<String, Expr> let);

    /** Adds a $lookup stage using a collection name and string fields.
     * @param fromCollection the collection name to join
     * @param localField the local field name
     * @param foreignField the foreign field name
     * @param outputArray the output array field name
     * @param pipeline optional sub-pipeline
     * @param let optional let variables
     * @return the aggregator */
    Aggregator<T, R> lookup(String fromCollection, String localField, String foreignField, String outputArray, List<Expr> pipeline, Map<String, Expr> let);

    /** Adds a $merge stage to a database and collection.
     * @param intoDb the target database
     * @param intoCollection the target collection
     * @param matchAction the action when a match is found
     * @param notMatchedAction the action when no match is found
     * @param onFields the fields to match on
     * @return the aggregator */
    Aggregator<T, R> merge(String intoDb, String intoCollection, MergeActionWhenMatched matchAction, MergeActionWhenNotMatched notMatchedAction, String... onFields);

    /** Adds a $merge stage to a collection.
     * @param intoCollection the target collection
     * @param matchAction the action when a match is found
     * @param notMatchedAction the action when no match is found
     * @param onFields the fields to match on
     * @return the aggregator */
    Aggregator<T, R> merge(String intoCollection, MergeActionWhenMatched matchAction, MergeActionWhenNotMatched notMatchedAction, String... onFields);

    /** Adds a $merge stage with let variables and a matched pipeline.
     * @param intoCollection the target collection
     * @param let the let variables
     * @param machedPipeline the pipeline to run on matched documents
     * @param notMatchedAction the action when no match is found
     * @param onFields the fields to match on
     * @return the aggregator */
    Aggregator<T, R> merge(String intoCollection, Map<String, Expr> let, List<Map<String, Expr>> machedPipeline, MergeActionWhenNotMatched notMatchedAction, String... onFields);

    /** Adds a $merge stage using a class type for the target collection.
     * @param intoCollection the target collection type
     * @param let the let variables
     * @param machedPipeline the pipeline to run on matched documents
     * @param matchAction the action when a match is found
     * @param notMatchedAction the action when no match is found
     * @param onFields the fields to match on
     * @return the aggregator */
    Aggregator<T, R> merge(Class<?> intoCollection, Map<String, Expr> let, List<Map<String, Expr>> machedPipeline, MergeActionWhenMatched matchAction, MergeActionWhenNotMatched notMatchedAction, String... onFields);

    /** Adds a $merge stage to a database and collection (no field matching).
     * @param intoDb the target database
     * @param intoCollection the target collection
     * @return the aggregator */
    Aggregator<T, R> merge(String intoDb, String intoCollection);

    /** Adds a $merge stage using a class type.
     * @param intoCollection the target collection type
     * @return the aggregator */
    Aggregator<T, R> merge(Class<?> intoCollection);

    /** Adds a $merge stage to a collection.
     * @param intoCollection the target collection name
     * @return the aggregator */
    Aggregator<T, R> merge(String intoCollection);

    /** Adds an $out stage writing to the given collection.
     * @param collection the output collection name
     * @return the aggregator */
    Aggregator<T, R> out(String collection);

    /** Adds an $out stage writing to the collection of the given type.
     * @param type the output collection type
     * @return the aggregator */
    Aggregator<T, R> out(Class<?> type);

    /** Adds an $out stage writing to a collection in a specific database.
     * @param db the target database
     * @param collection the output collection name
     * @return the aggregator */
    Aggregator<T, R> out(String db, String collection);

    /** Adds a $planCacheStats stage to the pipeline.
     * @param param additional parameters
     * @return the aggregator */
    Aggregator<T, R> planCacheStats(Map<String, Object> param);

    /**
     * Adds a $redact stage to the pipeline. See https://docs.mongodb.com/manual/reference/operator/aggregation/redact/
     *
     * @param expr the redact expression
     * @return the aggregator
     */
    Aggregator<T, R> redact(Expr expr);

    /** Replaces the input document with the specified new root expression.
     * @param newRoot the expression for the new root document
     * @return the aggregator */
    Aggregator<T, R> replaceRoot(Expr newRoot);

    /**
     * Replaces the input document with the specified document. The operation replaces all existing fields
     * in the input document, including the _id field. With $replaceWith, you can promote an embedded document
     * to the top-level. You can also specify a new document as the replacement.
     *
     * @param replacement the replacement expression
     * @return the aggregator
     */
    Aggregator<T, R> replaceWith(Expr replacement);

    /**
     * Randomly selects the specified number of documents from its input.
     *
     * @param sampleSize the number of documents to randomly select
     * @return the aggregator
     */
    Aggregator<T, R> sample(int sampleSize);

    /** Adds a $set stage with the given field expressions.
     * @param param map of field names to set expressions
     * @return the aggregator */
    Aggregator<T, R> set(Map<String, Expr> param);

    /** Adds a $sortByCount stage.
     * @param countBy the expression to count by
     * @return the aggregator */
    Aggregator<T, R> sortByCount(Expr countBy);

    /** Adds a $unionWith stage using a collection name.
     * @param collection the collection to union with
     * @return the aggregator */
    Aggregator<T, R> unionWith(String collection);

    /** Adds a $unionWith stage using another aggregator.
     * @param aggregator the aggregator to union with
     * @return the aggregator */
    Aggregator<T, R> unionWith(Aggregator aggregator);

    /** Adds an $unset stage for the given list of fields.
     * @param field list of field names to unset
     * @return the aggregator */
    Aggregator<T, R> unset(List<String> field);

    /** Adds an $unset stage for the given field names.
     * @param param field names to unset
     * @return the aggregator */
    Aggregator<T, R> unset(String... param);

    /** Adds an $unset stage for the given enum fields.
     * @param field enum fields to unset
     * @return the aggregator */
    Aggregator<T, R> unset(Enum... field);

    /** Adds a generic stage to the pipeline.
     * @param stageName the stage name
     * @param param the stage parameter
     * @return the aggregator */
    Aggregator<T, R> genericStage(String stageName, Object param);

    /** Adds a $limit stage.
     * @param num the maximum number of documents to return
     * @return the aggregator */
    Aggregator<T, R> limit(int num);

    /** Adds a $skip stage.
     * @param num the number of documents to skip
     * @return the aggregator */
    @SuppressWarnings("unused")
    Aggregator<T, R> skip(int num);

    /** Adds an $unwind stage using an expression.
     * @param listField the array field expression to unwind
     * @return the aggregator */
    @SuppressWarnings("unused")
    Aggregator<T, R> unwind(Expr listField);

    /** Adds an $unwind stage using a field name.
     * @param listField the array field name to unwind
     * @return the aggregator */
    Aggregator<T, R> unwind(String listField);

    /** Adds a $sort stage using prefixed field names (e.g. "+field" or "-field").
     * @param prefixed the sort field names with prefix
     * @return the aggregator */
    Aggregator<T, R> sort(String... prefixed);

    /** Adds a $sort stage using a map of field names to sort directions.
     * @param sort map of field names to 1 (ascending) or -1 (descending)
     * @return the aggregator */
    @SuppressWarnings("unused")
    Aggregator<T, R> sort(Map<String, Integer> sort);

    /** Returns the collection name for this aggregation.
     * @return the collection name */
    String getCollectionName();

    /** Sets the collection name for this aggregation.
     * @param cn the collection name
     * @return the aggregator */
    @SuppressWarnings("unused")
    Aggregator<T,R> setCollectionName(String cn);

    /** Adds a $group stage using a map as the id expression.
     * @param id the group id expression map
     * @return the group stage */
    Group<T, R> group(Map<String, Object> id);

    /** Adds a $group stage using a field name as the id.
     * @param id the group id field name
     * @return the group stage */
    Group<T, R> group(String id);

    /** Adds a $group stage using an Expr as the id.
     * @param id the group id expression
     * @return the group stage */
    Group<T, R> group(Expr id);

    /** Returns the current pipeline stages.
     * @return list of pipeline stage maps */
    List<Map<String, Object>> getPipeline();

    /** Adds a raw operator stage to the pipeline.
     * @param o the operator stage as a map */
    void addOperator(Map<String, Object> o);

    /** Executes the aggregation and returns the result list.
     * @return list of results */
    List<R> aggregate();

    /** Returns the number of result documents from the aggregation.
     * @return the document count */
    long getCount();

    /** Executes the aggregation and returns an iterable cursor.
     * @return an aggregation iterator */
    MorphiumAggregationIterator<T, R> aggregateIterable();

    /** Executes the aggregation asynchronously.
     * @param callback the callback to invoke when done */
    @SuppressWarnings("unused")
    void aggregate(AsyncOperationCallback<R> callback);

    /** Executes the aggregation and returns raw map results.
     * @return list of result maps */
    List<Map<String, Object>> aggregateMap();

    /** Executes the aggregation as raw maps asynchronously.
     * @param callback the callback to invoke when done */
    @SuppressWarnings("unused")
    void aggregateMap(AsyncOperationCallback<Map<String, Object>> callback);

    /** Returns whether explain mode is enabled.
     * @return true if explain is enabled */
    boolean isExplain();

    /** Sets explain mode.
     * @param explain true to enable explain mode */
    @SuppressWarnings("unused")
    void setExplain(boolean explain);

    /** Returns whether disk use is allowed.
     * @return true if disk use is allowed */
    boolean isUseDisk();

    /** Sets whether disk use is allowed.
     * @param useDisk true to allow disk use */
    @SuppressWarnings("unused")
    void setUseDisk(boolean useDisk);

    /** Returns the explain output for this aggregation.
     * @return the explain document
     * @throws MorphiumDriverException in case of error */
    Map<String,Object> explain() throws MorphiumDriverException;

    /** Returns the explain output for this aggregation with the given verbosity.
     * @param verbosity the verbosity level
     * @return the explain document
     * @throws MorphiumDriverException in case of error */
    Map<String,Object> explain(ExplainVerbosity verbosity) throws MorphiumDriverException;

    /** Fields used in the $geoNear aggregation stage. */
    enum GeoNearFields {
        /** The point for which to find the closest documents. */
        near,
        /** The output field that contains the calculated distance. */
        distanceField,
        /** Whether to use spherical geometry. */
        spherical,
        /** The maximum distance from the center point. */
        maxDistance,
        /** An optional query to limit the results. */
        query,
        /** A factor to multiply all distances by. */
        distanceMultiplier,
        /** Whether to include a field with the location used to calculate the distance. */
        includeLocs,
        /** Whether to return a unique document for each unique key value. */
        uniqueDocs,
        /** The minimum distance from the center point. */
        minDistance,
        /** The key to use for the geospatial index. */
        key,
    }


    /** Preferred number series for bucket boundaries used in $bucketAuto. */
    @SuppressWarnings("SameParameterValue")
    enum BucketGranularity {
        /** Renard series R5. */ R5,
        /** Renard series R10. */ R10,
        /** Renard series R20. */ R20,
        /** Renard series R40. */ R40,
        /** Renard series R80. */ R80,
        /** E-series E6. */ E6,
        /** E-series E12. */ E12,
        /** E-series E24. */ E24,
        /** E-series E48. */ E48,
        /** E-series E96. */ E96,
        /** E-series E192. */ E192,
        /** Powers of 2. */ POWERSOF2,
        /** 1-2-5 series. */ SERIES_125("1-2-5");

        private final String value;

        /** Creates a granularity constant using its name as value. */
        BucketGranularity() {
            value = name();
        }

        /** Creates a granularity constant with a custom value string.
         * @param name the custom value string */
        BucketGranularity(String name) {
            value = name;
        }

        /** Returns the string value for this granularity constant.
         * @return the granularity value string */
        public String getValue() {
            return value;
        }


    }

    /** Returns the underlying AggregateMongoCommand for this aggregation.
     * @return the aggregate command */
    public AggregateMongoCommand getAggregateCmd();

    /** Action to take when a $merge stage finds a matching document. */
    enum MergeActionWhenMatched {
        /** Replace the existing document. */ replace,
        /** Keep the existing document unchanged. */ keepExisting,
        /** Merge the fields of the matching documents. */ merge,
        /** Stop and fail with an error. */ fail,
    }

    /** Action to take when a $merge stage does not find a matching document. */
    enum MergeActionWhenNotMatched {
        /** Insert the document as a new document. */ insert,
        /** Discard the document without inserting. */ discard,
        /** Stop and fail with an error. */ fail,
    }

}
