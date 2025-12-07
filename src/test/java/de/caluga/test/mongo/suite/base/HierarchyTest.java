package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.AnnotationAndReflectionHelper;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.ObjectMapperImpl;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * User: Stephan Bösebeck
 * Date: 10.06.12
 * Time: 00:02
 * <p>
 */
@Tag("core")
public class HierarchyTest extends MorphiumTestBase {

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
    public void testHierarchy() {
        assert (new AnnotationAndReflectionHelper(true).isAnnotationPresentInHierarchy(SubClass.class, Entity.class)) : "hierarchy not found";
        String n = new ObjectMapperImpl().getCollectionName(HierarchyTest.SubClass.class);
        assert (!n.equals("uncached_object")) : "Wrong collection name!";
    }


}
