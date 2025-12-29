package de.caluga.test.mongo.suite.base;

import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.ComplexObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

/**
 * User: Stephan Bösebeck
 * Date: 10.05.12
 * Time: 13:05
 * <p>
 */
@Tag("core")
public class CollectionMappingTest extends MultiDriverTestBase {
    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void collectionMappingTest(Morphium morphium) {
        String n = morphium.getMapper().getCollectionName(CachedObject.class);
        assert (n.equals("cached_object")) : "Collection wrong";
        n = morphium.getMapper().getCollectionName(ComplexObject.class);
        assert (n.equals("ComplexObject")) : "Collection wrong";

    }
}
