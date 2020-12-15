package de.caluga.test.mongo.suite;

import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 27.08.12
 * Time: 11:17
 * <p/>
 */
public class WhereTest extends MorphiumTestBase {

    @Test
    public void testWhere() throws Exception {
        createUncachedObjects(100);
        Thread.sleep(500);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.where("this.counter > 15");
//        q.f("counter").ne(0);
//
//        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
//        assert(q.countAll()==85):"Count wrong: "+q.countAll();
//
//        Driver dr=(Driver)morphium.getDriver();
//        MongoCollection coll = dr.getCollection(morphium.getConfig().getDatabase(), q.getCollectionName());
//        coll.find(new BasicDBObject("$where","counter>15"));
        List<UncachedObject> lst = q.asList();
        assert (lst.size() == 85);
        assert (q.countAll() == 85);

    }

}
