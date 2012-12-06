package de.caluga.morphium;

import de.caluga.morphium.query.Query;

import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 14:39
 * <p/>
 * Interface for all morphium write accesses. Override for own use and set to MorphiumConfig
 *
 * @see MorphiumConfig
 */
public interface Writer {
    /**
     * Stores the object, should be an entity
     *
     * @param o
     */
    public void store(Object o);

    /**
     * stores the given list of objects, should be entities or embedded
     *
     * @param lst
     */
    public void store(List lst);

    /**
     * update an object using fields specified
     *
     * @param ent
     * @param fields
     */
    public void storeUsingFields(Object ent, String... fields);

    /**
     * changes an object in DB
     *
     * @param toSet
     * @param field
     * @param value
     */
    void set(Object toSet, String field, Object value);

    /**
     * will change an entry in mongodb-collection corresponding to given class object
     * if query is too complex, upsert might not work!
     * Upsert should consist of single and-queries, which will be used to generate the object to create, unless
     * it already exists. look at Mongodb-query documentation as well
     *
     * @param query            - query to specify which objects should be set
     * @param values           - map fieldName->Value, which values are to be set!
     * @param insertIfNotExist - insert, if it does not exist (query needs to be simple!)
     * @param multiple         - update several documents, if false, only first hit will be updated
     */
    void set(Query<?> query, Map<String, Object> values, boolean insertIfNotExist, boolean multiple);

    void inc(Query<?> query, String field, int amount, boolean insertIfNotExist, boolean multiple);

    /**
     * Increases a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toInc.id},{$inc:{field:amount}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toInc:  object to set the value in (or better - the corresponding entry in mongo)
     * @param field:  the field to change
     * @param amount: the value to set
     */
    void inc(Object toInc, String field, int amount);

    void setMorphium(Morphium m);

    void delete(List lst);

    void delete(Object o);

    /**
     * deletes all objects matching the given query
     *
     * @param q
     */
    void delete(Query q);

    void pushPull(boolean push, Query<?> query, String field, Object value, boolean insertIfNotExist, boolean multiple);

    void pushPullAll(boolean push, Query<?> query, String field, List<?> value, boolean insertIfNotExist, boolean multiple);

    /**
     * Un-setting a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toSet.id},{$unset:{field:1}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toSet: object to set the value in (or better - the corresponding entry in mongo)
     * @param field: field to remove from document
     */
    void unset(Object toSet, String field);
}
