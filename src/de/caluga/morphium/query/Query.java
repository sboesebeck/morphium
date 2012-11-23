package de.caluga.morphium.query;

import com.mongodb.DBObject;
import de.caluga.morphium.FilterExpression;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

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
public interface Query<T> extends Cloneable {
    /**
     * set the where string for this query - where-String needs to be valid java script! Errors will only be shown in MongoD-Log!
     *
     * @param wh
     * @return
     */
    public Query<T> where(String wh);

    /**
     * Get a field. F may be the name as it is in mongo db or the variable name in java...
     *
     * @param f
     * @return
     */
    public MongoField f(String f);


    /**
     * same as f(field.name())
     *
     * @param field
     * @return
     */
    public MongoField f(Enum field);

    /**
     * concatenate those queries with or
     *
     * @param q
     */
    public void or(Query<T>... q);


    /**
     * not or
     *
     * @param q
     */
    public void nor(Query<T>... q);

    /**
     * limit the number of entries in result
     *
     * @param i
     * @return
     */
    public Query<T> limit(int i);

    /**
     * skip the first entries in result
     *
     * @param i
     * @return
     */
    public Query<T> skip(int i);

    /**
     * set an order - Key: FieldName (java or Mongo-Name), Value: Integer: -1 reverse, 1 standard
     *
     * @param n
     * @return
     */
    public Query<T> sort(Map<String, Integer> n);

    /**
     * set order by prefixing field names with - for reverse ordering (+ or nothing default)
     *
     * @param prefixedString
     * @return
     */
    public Query<T> sort(String... prefixedString);

    public Query<T> sort(Enum... naturalOrder);

    /**
     * set an order - Key: FieldName (java or Mongo-Name), Value: Integer: -1 reverse, 1 standard
     * deprectaed, use sort instead (more like the maongodb-commands)... used for backward compatibility.
     *
     * @param n
     * @return
     * @deprecated
     */
    public Query<T> order(Map<String, Integer> n);

    /**
     * set order by prefixing field names with - for reverse ordering (+ or nothing default)
     * deprectaed, use sort instead (more like the maongodb-commands)... used for backward compatibility.
     *
     * @param prefixedString
     * @return
     * @deprecated
     */
    public Query<T> order(String... prefixedString);

    /**
     * count all results in query - does not take limit or skip into account
     *
     * @return
     */
    public long countAll();  //not taking limit and skip into account!

    /**
     * needed for creation of the query representation tree
     *
     * @param e
     */
    public void addChild(FilterExpression e);

    /**
     * create a db object from this query and all of it's child nodes
     *
     * @return
     */
    public DBObject toQueryObject();

    /**
     * what type this query is for
     *
     * @return
     */
    public Class<T> getType();

    /**
     * the result as list
     *
     * @return
     */
    public List<T> asList();

    /**
     * create an iterator / iterable for this query, default windowSize (10)
     */
    public MorphiumIterator<T> asIterable();


    /**
     * create an iterator / iterable for this query, sets window size (how many objects should be read from DB)
     */
    public MorphiumIterator<T> asIterable(int windowSize);


    /**
     * get only 1 result (first one in result list)
     *
     * @return
     */
    public T get();

    /**
     * returns one object that matches to id
     *
     * @param id - should be the Unique ID
     * @return - the object, if found, null otherwise
     */
    public T getById(ObjectId id);

    /**
     * only return the IDs of objects (useful if objects are really large)
     *
     * @return
     */
    public List<ObjectId> idList();

    /**
     * what type to use
     *
     * @param type
     */
    public void setType(Class<T> type);

    /**
     * create a new empty query for the same type using the same mapper as this
     *
     * @return
     */
    public Query<T> q();

    public List<T> complexQuery(DBObject query);

    /**
     * just sends the given query to the MongoDBDriver and masrhalls objects as listed
     * ignores all other query settings!!!!!
     *
     * @param query - query to be sent
     * @param skip  - amount to skip
     * @param limit - maximium number of results
     * @return list of objects matching query
     */
    public List<T> complexQuery(DBObject query, Map<String, Integer> sort, int skip, int limit);

    public List<T> complexQuery(DBObject query, String sort, int skip, int limit);

    /**
     * same as copmplexQuery(query,0,1).get(0);
     *
     * @param query
     * @return
     */
    public T complexQueryOne(DBObject query);

    public T complexQueryOne(DBObject query, Map<String, Integer> sort, int skip);

    public T complexQueryOne(DBObject query, Map<String, Integer> sort);

    public int getLimit();

    public int getSkip();

    public Map<String, Integer> getOrder();

    public Query<T> clone() throws CloneNotSupportedException;

    public ReadPreferenceLevel getReadPreferenceLevel();

    public void setReadPreferenceLevel(ReadPreferenceLevel readPreferenceLevel);

    public String getWhere();

    public Morphium getMorphium();

    public void setMorphium(Morphium m);
}
