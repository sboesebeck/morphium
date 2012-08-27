package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.Query;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 27.08.12
 * Time: 11:17
 * <p/>
 * TODO: Add documentation here
 */
public class WhereTest extends MongoTest {

    @Test
    public void testWhere() throws Exception {
        super.createUncachedObjects(100);

        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q = q.where("rs.slaveOk(); db.uncached_object.count({count:{$lt:10}}) > 0 && db.uncached_object.find({ _id: this._id }).count()>0");
        q.setReadPreferenceLevel(ReadPreferenceLevel.ALL_NODES);
        q.get();

    }

}
