package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 29.04.14
 * Time: 13:24
 * To change this template use File | Settings | File Templates.
 */
@Tag("core")
public class DistinctTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void distinctTest(Morphium morphium) {
        try (morphium) {
            createUncachedObjects(morphium, 100);

            List lst = morphium.createQueryFor(UncachedObject.class).distinct("counter");
            assert (lst.size() == 100);
            lst = morphium.createQueryFor(UncachedObject.class).distinct("str_value");
            assert (lst.size() == 1);
        }
    }
//
//    @Test
//    public void distinctTestInMemory() throws Exception {
//        morphiumInMemeory.dropCollection(UncachedObject.class);
//        createUncachedObjectsInMemory(100);
//
//        List lst = morphiumInMemeory.createQueryFor(UncachedObject.class).distinct("counter");
//        assert (lst.size() == 100);
//        lst = morphiumInMemeory.createQueryFor(UncachedObject.class).distinct("value");
//        assert (lst.size() == 1);
//    }
}
