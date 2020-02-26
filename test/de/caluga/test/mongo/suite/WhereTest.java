package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 27.08.12
 * Time: 11:17
 * <p/>
 */
public class WhereTest extends MorphiumTestBase {

    @Test
    public void testWhere() {
        super.createUncachedObjects(100);

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.where("this.count > 0");
        q.setReadPreferenceLevel(ReadPreferenceLevel.NEAREST);
        q.get();

    }

}
