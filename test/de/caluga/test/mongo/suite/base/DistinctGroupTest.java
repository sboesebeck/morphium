package de.caluga.test.mongo.suite.base;

import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 13.07.12
 * Time: 08:53
 * <p/>
 */
public class DistinctGroupTest extends MorphiumTestBase {
    @Test
    public void distinctTest() throws Exception {
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


    @Test
    public void distinctTestWithTransaction() throws Exception {
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
        createCachedObjects(2);
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
