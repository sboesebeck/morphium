package de.caluga.test.mongo.suite;

import com.mongodb.DBObject;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.Query;
import org.junit.Test;

/**
 * User: Stpehan Bösebeck
 * Date: 26.03.12
 * Time: 16:18
 * <p/>
 * TODO: Add documentation here
 */
public class QueryImplTest extends MongoTest {

    @Test
    public void testQuery() {

        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);


        q.or(q.q().f("counter").lte(15),
                q.q().f("counter").gte(10),
                q.q().f("counter").lt(15).f("counter").gt(10).f("value").eq("hallo").f("value").ne("test")
        );
        DBObject dbObject = q.toQueryObject();
        assert (dbObject != null) : "DBObject created is null?";

        String str = dbObject.toString();
        assert (str != null) : "ToString is NULL?!?!?";

        System.out.println("Query: " + str);


        q = q.q(); //new query
        q = q.f("counter").mod(10, 5);
        dbObject = q.toQueryObject();
        str = dbObject.toString();
        assert (str != null) : "ToString is NULL?!?!?";

        System.out.println("Query: " + str);
    }
}
