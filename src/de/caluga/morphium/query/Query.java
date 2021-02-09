package de.caluga.morphium.query;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.Collation;
import de.caluga.morphium.FilterExpression;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.aggregation.Expr;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.async.AsyncOperationCallback;
import org.json.simple.parser.ParseException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 11:29
 * <p/>
 * usage:
 * <code>
 * Query<UncachedObject> q= Morphium.get().createQueryFor(UncachedObject.class);
 * q=q.f("counter").lt(15).f("counter").gt(10);
 * </code>
 * Or
 * <code>
 * q.or(q.q().f("counter").eq(15),q.q().f("counter").eq(22));
 * </code>
 * AND is the default!
 */
@SuppressWarnings({"UnusedDeclaration", "unchecked"})
public interface Query<T> extends Cloneable {
    /**
     * set the where string for this query - where-String needs to be valid java script! Errors will only be shown in MongoD-Log!
     *
     * @param wh where string
     * @return where wuery
     */
    Query<T> where(String wh);

    /**
     * Get a field. F may be the name as it is in mongo db or the variable name in java...
     *
     * @param f field
     * @return the field implementation
     */
    MongoField<T> f(String f);

    /**
     * returns the serveraddress the query was executed on
     *
     * @return the serveraddress the query was executed on, null if not executed yet
     */
    String getServer();

    /**
     * use this db instead of the default one
     *
     * @param db
     */
    void overrideDB(String db);

    Collation getCollation();

    Query<T> setCollation(Collation c);

    /**
     * return the DB name the query is going to be executed on or was executed on
     *
     * @return
     */
    String getDB();

    /**
     * same as f(field.name())
     *
     * @param field field
     * @return mongo field
     */
    MongoField<T> f(Enum field);

    /**
     * concatenate those queries with or
     *
     * @param q query
     */
    Query<T> or(Query<T>... q);

    /**
     * concatenate those queries with or
     *
     * @param q query
     */
    Query<T> or(List<Query<T>> q);

    /**
     * not or
     *
     * @param q query
     */
    Query<T> nor(Query<T>... q);

    /**
     * limit the number of entries in result
     *
     * @param i - limit
     * @return the query
     */
    Query<T> limit(int i);

    /**
     * skip the first entries in result
     *
     * @param i skip
     * @return the query
     */
    Query<T> skip(int i);

    /**
     * set an order - Key: FieldName (java or Mongo-Name), Value: Integer: -1 reverse, 1 standard
     *
     * @param n - sort
     * @return the query
     */
    Query<T> sort(Map<String, Integer> n);

    Query<T> sortEnum(Map<Enum, Integer> n);

    /**
     * set order by prefixing field names with - for reverse ordering (+ or nothing default)
     *
     * @param prefixedString sort
     * @return the query
     */
    Query<T> sort(String... prefixedString);

    Query<T> sort(Enum... naturalOrder);

    /**
     * count all results in query - does not take limit or skip into account
     *
     * @return number
     */

    long countAll();  //not taking limit and skip into account!

    void countAll(AsyncOperationCallback<T> callback);

    /**
     * needed for creation of the query representation tree
     *
     * @param e expression
     */
    @SuppressWarnings("UnusedReturnValue")
    Query<T> addChild(FilterExpression e);

    /**
     * create a db object from this query and all of it's child nodes
     *
     * @return query object
     */
    Map<String, Object> toQueryObject();

    Query<T> expr(Expr exp);

    Query<T> matchesJsonSchema(Map<String, Object> schemaDef);

    Query<T> matchesJsonSchema(String schemaDef) throws ParseException;

    /**
     * what type this query is for
     *
     * @return class
     */
    Class<? extends T> getType();

    /**
     * what type to use
     *
     * @param type type
     */
    void setType(Class<? extends T> type);

    /**
     * the result as list
     *
     * @return list
     */
    List<T> asList();

    void asList(AsyncOperationCallback<T> callback);

    /**
     * create an iterator / iterable for this query, default windowSize (10), prefetch windows 1
     */
    MorphiumQueryIterator<T> asIterable();

    MorphiumQueryIterator<T> asIterable(int windowSize, Class<? extends MorphiumQueryIterator<T>> it);

    MorphiumQueryIterator<T> asIterable(MorphiumQueryIterator<T> ret);

    /**
     * create an iterator / iterable for this query, sets window size (how many objects should be read from DB)
     * prefetch number is 1 in this case
     */
    MorphiumQueryIterator<T> asIterable(int windowSize);

    /**
     * create an iterator / iterable for this query, sets window size (how many entities are read en block) and how many windows of this size will be prefechted...
     *
     * @param windowSize
     * @param prefixWindows
     * @return
     */

    MorphiumQueryIterator<T> asIterable(int windowSize, int prefixWindows);


    void tail(int bufferSize, int maxWait, AsyncOperationCallback<T> cb);

    /**
     * get only 1 result (first one in result list)
     *
     * @return entity
     */
    T get();

    void get(AsyncOperationCallback<T> callback);

    /**
     * only return the IDs of objects (useful if objects are really large)
     *
     * @return list of Ids, type R
     */
    <R> List<R> idList();

    void idList(AsyncOperationCallback<T> callback);

    /**
     * create a new empty query for the same type using the same mapper as this
     *
     * @return query
     */
    Query<T> q();

    /**
     * bound directly to mong
     * no field name tanslations done!
     *
     * @param query
     * @return
     */
    @Deprecated
    List<T> complexQuery(Map<String, Object> query);

    AnnotationAndReflectionHelper getARHelper();

    void setARHelper(AnnotationAndReflectionHelper ar);

    Query<T> rawQuery(Map<String, Object> query);

    /**
     * just sends the given query to the MongoDBDriver and masrhalls objects as listed
     * ignores all other query settings!!!!!
     * bound directly to mong
     * no field name tanslations done!
     * deprecated, use rawQuery instead
     *
     * @param query - query to be sent
     * @param skip  - amount to skip
     * @param limit - maximium number of results
     * @return list of objects matching query
     */
    @Deprecated
    List<T> complexQuery(Map<String, Object> query, Map<String, Integer> sort, int skip, int limit);

    @Deprecated
    List<T> complexQuery(Map<String, Object> query, String sort, int skip, int limit);

    /**
     * deprecated, use rawQuery and standard count() methods
     *
     * @param query
     * @return
     */
    @Deprecated
    long complexQueryCount(Map<String, Object> query);

    /**
     * same as copmplexQuery(query,0,1).get(0);
     * deprecated user rawQuery().get()
     *
     * @param query - query
     * @return type
     */

    @Deprecated
    T complexQueryOne(Map<String, Object> query);

    /**
     * deprecated use rawQuery().get();
     *
     * @param query
     * @param sort
     * @param skip
     * @return
     */
    @Deprecated
    T complexQueryOne(Map<String, Object> query, Map<String, Integer> sort, int skip);


    /**
     * deprecated, user rawQuery().get()
     *
     * @param query
     * @param sort
     * @return
     */
    @Deprecated
    T complexQueryOne(Map<String, Object> query, Map<String, Integer> sort);

    int getLimit();

    int getSkip();

    Map<String, Integer> getSort();

    @SuppressWarnings("RedundantThrows")
    Query<T> clone() throws CloneNotSupportedException;

    ReadPreferenceLevel getReadPreferenceLevel();

    void setReadPreferenceLevel(ReadPreferenceLevel readPreferenceLevel);

    String getWhere();

    Morphium getMorphium();

    void setMorphium(Morphium m);

    void setExecutor(ThreadPoolExecutor executor);


    @Deprecated
    void getById(Object id, AsyncOperationCallback<T> callback);

    /**
     * deprecatedc - use morphium.findById() instead.
     *
     * @param id
     * @return
     */
    @Deprecated
    T getById(Object id);

    void set(String field, Object value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void set(Enum field, Object value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void setEnum(Map<Enum, Object> map, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void set(Map<String, Object> map, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void set(String field, Object value, AsyncOperationCallback<T> cb);

    void set(Enum field, Object value, AsyncOperationCallback<T> cb);

    void setEnum(Map<Enum, Object> map, AsyncOperationCallback<T> cb);

    void set(Map<String, Object> map, AsyncOperationCallback<T> cb);

    void set(String field, Object value, boolean upsert, boolean multiple);

    void set(Enum field, Object value, boolean upsert, boolean multiple);

    void setEnum(Map<Enum, Object> map, boolean upsert, boolean multiple);

    void set(Map<String, Object> map, boolean upsert, boolean multiple);

    void set(String field, Object value);

    void set(Enum field, Object value);

    void setEnum(Map<Enum, Object> map);

    void set(Map<String, Object> map);

    void push(String field, Object value);

    void push(Enum field, Object value);

    void push(String field, Object value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void push(Enum field, Object value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void pushAll(String field, List value);

    void pushAll(Enum field, List value);

    void pushAll(String field, List value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void pushAll(Enum field, List value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void pullAll(String field, List value);

    void pullAll(Enum field, List value);

    void pullAll(String field, List value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void pullAll(Enum field, List value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void pull(String field, Object value);

    void pull(Enum field, Object value);

    void pull(String field, Object value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void pull(Enum field, Object value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void inc(String field, Integer value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void inc(String field, Integer value, boolean upsert, boolean multiple);

    void inc(Enum field, Integer value, boolean upsert, boolean multiple);

    void inc(String field, Integer value);

    void inc(Enum field, Integer value);

    void inc(String field, Double value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void inc(String field, Double value, boolean upsert, boolean multiple);

    void inc(Enum field, Double value, boolean upsert, boolean multiple);

    void inc(String field, Double value);

    void inc(Enum field, Double value);

    void inc(String field, Long value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void inc(String field, Long value, boolean upsert, boolean multiple);

    void inc(Enum field, Long value, boolean upsert, boolean multiple);

    void inc(String field, Long value);

    void inc(Enum field, Long value);

    void inc(String field, Number value, boolean upsert, boolean multiple, AsyncOperationCallback<T> cb);

    void inc(String field, Number value, boolean upsert, boolean multiple);

    void inc(Enum field, Number value, boolean upsert, boolean multiple);

    void inc(String field, Number value);

    void inc(Enum field, Number value);

    void dec(String field, Integer value, boolean upsert, boolean multiple);

    void dec(Enum field, Integer value, boolean upsert, boolean multiple);

    void dec(String field, Integer value);

    void dec(Enum field, Integer value);

    void dec(String field, Double value, boolean upsert, boolean multiple);

    void dec(Enum field, Double value, boolean upsert, boolean multiple);

    void dec(String field, Double value);

    void dec(Enum field, Double value);

    void dec(String field, Long value, boolean upsert, boolean multiple);

    void dec(Enum field, Long value, boolean upsert, boolean multiple);

    void dec(String field, Long value);

    void dec(Enum field, Long value);

    void dec(String field, Number value, boolean upsert, boolean multiple);

    void dec(Enum field, Number value, boolean upsert, boolean multiple);

    void dec(String field, Number value);

    void dec(Enum field, Number value);

    int getNumberOfPendingRequests();

    String getCollectionName();

    /**
     * use a different collection name for the query
     *
     * @param n
     */
    Query<T> setCollectionName(String n);

    @Deprecated
    List<T> textSearch(String... texts);

    @Deprecated
    List<T> textSearch(TextSearchLanguages lang, String... texts);

    MongoField<T> f(Enum... f);

    MongoField<T> f(String... f);

    void delete();

    T findOneAndDelete();

    T findOneAndUpdate(Map<String, Object> update);

    T findOneAndUpdateEnums(Map<Enum, Object> update);

    boolean isAutoValuesEnabled();

    @SuppressWarnings("UnusedReturnValue")
    Query<T> setAutoValuesEnabled(boolean autoValues);

    String[] getTags();

    Query<T> addTag(String name, String value);

    Query<T> disableAutoValues();

    Query<T> enableAutoValues();


    Query<T> text(String... text);

    Query<T> text(TextSearchLanguages lang, String... text);

    Query<T> text(TextSearchLanguages lang, boolean caseSensitive, boolean diacriticSensitive, String... text);

    Query<T> text(String metaScoreField, TextSearchLanguages lang, boolean caseSensitive, boolean diacriticSensitive, String... text);

    Query<T> text(String metaScoreField, TextSearchLanguages lang, String... text);

    Query<T> setProjection(String... fl);

    Query<T> addProjection(String f);

    Query<T> addProjection(String f, String projectOperator);

    Query<T> addProjection(Enum f, String projectOperator);

    Query<T> setProjection(Enum... fl);

    Query<T> addProjection(Enum f);

    Query<T> hideFieldInProjection(String f);

    Query<T> hideFieldInProjection(Enum f);


    Map<String, Object> getFieldListForQuery();

    List distinct(String field);


    enum TextSearchLanguages {
        danish,
        dutch,
        english,
        finnish,
        french,
        german,
        hungarian,
        italian,
        norwegian,
        portuguese,
        romanian,
        russian,
        spanish,
        swedish,
        turkish,
        mongo_default,
        none,

    }
}
