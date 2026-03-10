package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.DAO;
import de.caluga.morphium.Morphium;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 17.05.12
 * Time: 15:11
 * <p>
 */
public class UncachedObjectDAO extends DAO<UncachedObject> {
    public enum Field {
        counter, value, mongo_id
    }


    public UncachedObjectDAO(Morphium m) {
        super(m, UncachedObject.class);
    }


    public List<UncachedObject> getAll() {
        return getQuery().asList();
    }


    public List<UncachedObject> findByField(Field f, Object value) {
        return getQuery().f(f).eq(value).asList();
    }


}
