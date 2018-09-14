package de.caluga.test.mongo.suite;

import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.ComplexObject;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 10.05.12
 * Time: 13:05
 * <p/>
 */
public class CollectionMappingTest extends MongoTest {
    @Test
    public void collectionMappingTest() {
        String n = morphium.getMapper().getCollectionName(CachedObject.class);
        assert (n.equals("cached_object")) : "Collection wrong";
        n = morphium.getMapper().getCollectionName(ComplexObject.class);
        assert (n.equals("ComplexObject")) : "Collection wrong";

    }
}
