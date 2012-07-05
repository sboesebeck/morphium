package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 10.05.12
 * Time: 13:05
 * <p/>
 * TODO: Add documentation here
 */
public class CollectionMappingTest extends MongoTest {
    @Test
    public void collectionMappingTest() throws Exception {
        String n = MorphiumSingleton.get().getConfig().getMapper().getCollectionName(CachedObject.class);
        assert (n.equals("cached_object")) : "Collection wrong";
        n = MorphiumSingleton.get().getConfig().getMapper().getCollectionName(ComplexObject.class);
        assert (n.equals("ComplexObject")) : "Collection wrong";

    }
}
