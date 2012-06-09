package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.Entity;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 10.06.12
 * Time: 00:02
 * <p/>
 * TODO: Add documentation here
 */
public class HierarchyTest extends MongoTest {

    public static class SubClass extends UncachedObject {
        private String additionalProperty;

        public String getAdditionalProperty() {
            return additionalProperty;
        }

        public void setAdditionalProperty(String additionalProperty) {
            this.additionalProperty = additionalProperty;
        }
    }

    @Test
    public void testHierarchy() throws Exception {
        assert (MorphiumSingleton.get().isAnnotationPresentInHierarchy(SubClass.class, Entity.class)) : "hierarchy not found";
        String n = MorphiumSingleton.get().getConfig().getMapper().getCollectionName(de.caluga.test.mongo.suite.HierarchyTest.SubClass.class);
        assert (!n.equals("uncached_object")) : "Wrong collection name!";
    }


}
