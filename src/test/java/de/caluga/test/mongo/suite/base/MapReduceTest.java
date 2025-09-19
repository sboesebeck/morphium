package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Created by stephan on 28.07.16.
 */
@Tag("core")
public class MapReduceTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void doSimpleMRTest(Morphium m) throws Exception {
        try (m) {

            m.dropCollection(UncachedObject.class);
            TestUtils.waitForConditionToBecomeTrue(1000, "Deletion of collection failed", ()-> {
                try {
                    return !m.exists(m.getDatabase(), m.getMapper().getCollectionName(UncachedObject.class));
                } catch (MorphiumDriverException e) {
                }
                return false;
            });
            createUncachedObjects(m, 10);
            List<UncachedObject> result = m.mapReduce(UncachedObject.class, "function(){emit(this.counter%2==0,this);}",
                                          "function (key,values){var ret={_id:ObjectId(), str_value:\"\", counter:0}; if (key==true) {ret.str_value=\"even\";} else { ret.str_value=\"odd\";} for (var i=0; i<values.length;i++){ret.counter=ret.counter+values[i].counter;}return ret;}");
            assertEquals(2, result.size());
            boolean odd = false;
            boolean even = false;

            for (UncachedObject r : result) {
                if (r.getStrValue().equals("odd")) {
                    odd = true;
                }

                if (r.getStrValue().equals("even")) {
                    even = true;
                }

                assert(r.getCounter() > 0);
            }

            assert(odd);
            assert(even);
        }
    }
}
