package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 13.07.12
 * Time: 08:53
 * <p/>
 */
public class DistinctGroupTest extends MultiDriverTestBase {
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void distinctTest(Morphium morphium) throws Exception {
        try (morphium) {
            List<UncachedObject> lst = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                UncachedObject uc = new UncachedObject();
                uc.setCounter(i % 3);
                uc.setStrValue("Value " + (i % 2));
                lst.add(uc);
            }
            morphium.storeList(lst);
            Thread.sleep(500);
            List values = morphium.distinct("counter", UncachedObject.class);
            assert (values.size() == 3) : "Size wrong: " + values.size();
            for (Object o : values) {
                log.info("counter: " + o.toString());
            }
            values = morphium.distinct("str_value", UncachedObject.class);
            assert (values.size() == 2) : "Size wrong: " + values.size();
            for (Object o : values) {
                log.info("Value: " + o.toString());
            }
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void distinctTestWithTransaction(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(UncachedObject.class);
            List<UncachedObject> lst = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                UncachedObject uc = new UncachedObject();
                uc.setCounter(i % 3);
                uc.setStrValue("dv " + (i % 2));
                lst.add(uc);
            }
            morphium.storeList(lst);
            morphium.startTransaction();
            createCachedObjects(morphium, 2);
            Thread.sleep(500);
            List values = morphium.distinct("counter", UncachedObject.class);
            assert (values.size() == 3) : "Size wrong: " + values.size();
            for (Object o : values) {
                log.info("counter: " + o.toString());
            }
            values = morphium.distinct("strValue", UncachedObject.class);
            assert (values.size() == 2) : "Size wrong: " + values.size();
            for (Object o : values) {
                log.info("Value: " + o.toString());
            }
            morphium.commitTransaction();
        }
    }

}
