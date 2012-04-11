package de.caluga.morphium;

import com.mongodb.DBObject;
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
     * concatenate those queries with or
     *
     * @param q
     */
    public void or(Query<T>... q);

    /**
     * negate the query
     *
     * @param q
     */
    public void not(Query<T> q);

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
    public Query<T> order(Map<String, Integer> n);

    /**
     * set order by prefixing field names with - for reverse ordering (+ or nothing default)
     *
     * @param prefixedString
     * @return
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
     * set the object Mapper this query object should use. By default this is set by Morphium-Class
     *
     * @param mapper
     */
    void setMapper(ObjectMapper mapper);

    /**
     * get the current mapper implmenentation
     *
     * @return
     */
    public ObjectMapper getMapper();

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

    public int getLimit();

    public int getSkip();

    public Map<String, Integer> getOrder();

    public Query<T> clone();
}
