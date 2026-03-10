package de.caluga.morphium.writer;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.driver.commands.ExplainCommand.ExplainVerbosity;
import de.caluga.morphium.query.Query;

import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 30.08.12
 * Time: 14:39
 * <p>
 * Interface for all morphium write accesses. Override for own use and set to MorphiumConfig
 *
 * @see de.caluga.morphium.MorphiumConfig
 */
public interface MorphiumWriter {
    /**
     * Stores the object, should be an entity
     *
     * @param o - entity
     */
    <T> void store(T o, String collection, AsyncOperationCallback<T> callback);

    <T> void insert(T o, String collection, AsyncOperationCallback<T> callback);

    /**
     * stores the given list of objects, should be entities or embedded.
     *
     * @param lst - to store
     */
    <T> void store(List<T> lst, AsyncOperationCallback<T> callback);

    <T> void insert(List<T> o, AsyncOperationCallback<T> callback);


    <T> void store(List<T> lst, String collectionName, AsyncOperationCallback<T> callback);

    <T> void insert(List<T> lst, String collectionName, AsyncOperationCallback<T> callback);
    /**
     * update an object using fields specified
     *
     * @param ent    entity
     * @param fields - fields
     */
    <T> void updateUsingFields(T ent, String collection, AsyncOperationCallback<T> callback, String... fields);

    /**
     * changes an object in DB AND in Memory...
     * the Object toSet WILL be modified!
     *
     * @param toSet   entity to set values in both in mongo and in memory
     * @param values: contains name/values to set to the given object!
     */

    <T> void set(T toSet, String collection, Map<String, Object> values, boolean upsert, AsyncOperationCallback<T> callback);

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
    <T> Map<String,Object> set(Query<T> query, Map<String, Object> values, boolean insertIfNotExist, boolean multiple, AsyncOperationCallback<T> callback);


    <T> Map<String,Object> inc(Query<T> query, String field, Number amount, boolean insertIfNotExist, boolean multiple, AsyncOperationCallback<T> callback);

    /**
     * Increases a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toInc.id},{$inc:{field:amount}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toInc:  object to set the value in (or better - the corresponding entry in mongo)
     * @param field:  the field to change
     * @param amount: the value to set
     */
    <T> void inc(T toInc, String collection, String field, Number amount, AsyncOperationCallback<T> callback);

    void setMorphium(Morphium m);

    <T> void remove(List<T> lst, AsyncOperationCallback<T> callback);

    @SuppressWarnings("unused")
    <T> Map<String,Object> remove(Query<T> q, boolean multiple, AsyncOperationCallback<T> callback);

    <T> Map<String,Object> explainRemove(ExplainVerbosity verbosity,T o, String Collection );
    <T> void remove(T o, String collection, AsyncOperationCallback<T> callback);

    /**
     * deletes all objects matching the given query
     *
     * @param q the query
     */
    <T> Map<String,Object> remove(Query<T> q, AsyncOperationCallback<T> callback);
    <T> Map<String,Object> explainRemove(ExplainVerbosity verbosity, Query<T> q);

    <T> Map<String,Object> pushPull(MorphiumStorageListener.UpdateTypes type, Query<T> query, String field, Object value, boolean insertIfNotExist, boolean multiple, AsyncOperationCallback<T> callback);

    <T> Map<String,Object> pushPullAll(MorphiumStorageListener.UpdateTypes type, Query<T> query, String field, List<?> value, boolean insertIfNotExist, boolean multiple, AsyncOperationCallback<T> callback);

    /**
     * Un-setting a value in an existing mongo collection entry - no reading necessary. Object is altered in place
     * db.collection.update({"_id":toSet.id},{$unset:{field:1}}
     * <b>attention</b>: this alteres the given object toSet in a similar way
     *
     * @param toSet: object to set the value in (or better - the corresponding entry in mongo)
     * @param field: field to remove from document
     */
    <T> void unset(T toSet, String collection, String field, AsyncOperationCallback<T> callback);

    @SuppressWarnings("unused")
    <T> void pop(T obj, String collection, String field, boolean first, AsyncOperationCallback<T> callback);

    @SuppressWarnings("unused")
    <T> Map<String,Object> unset(Query<T> query, String field, boolean multiple, AsyncOperationCallback<T> callback);

    <T> Map<String,Object> unset(Query<T> query, AsyncOperationCallback<T> callback, boolean multiple, String... fields);

    <T> Map<String,Object> unset(Query<T> query, AsyncOperationCallback<T> callback, boolean multiple, Enum... fields);

    <T> void dropCollection(Class<T> cls, String collection, AsyncOperationCallback<T> callback);

    <T> void createIndex(Class<T> cls, String collection, IndexDescription idx, AsyncOperationCallback<T> callback);

    int writeBufferCount();


    void flush();

    void flush(Class type);

    void setMaximumQueingTries(int n);

    void setPauseBetweenTries(int p);

    <T> Map<String,Object> inc(Query<T> query, Map<String, Number> fieldsToInc, boolean insertIfNotExist, boolean multiple, AsyncOperationCallback<T> callback);

    /**
     * information about closing of morphium and all connections
     */
    void close();


}
