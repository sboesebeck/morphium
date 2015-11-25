package de.caluga.test.mongo.suite;

import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 13.07.12
 * Time: 08:53
 * <p/>
 */
public class DistinctGroupTest extends MongoTest {
    @Test
    public void distinctTest() throws Exception {
        List<UncachedObject> lst = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i % 3);
            uc.setValue("Value " + (i % 2));
            lst.add(uc);
        }
        morphium.storeList(lst);
        Thread.sleep(500);
        List values = morphium.distinct("counter", UncachedObject.class);
        assert (values.size() == 3) : "Size wrong: " + values.size();
        for (Object o : values) {
            log.info("counter: " + o.toString());
        }
        values = morphium.distinct("value", UncachedObject.class);
        assert (values.size() == 2) : "Size wrong: " + values.size();
        for (Object o : values) {
            log.info("Value: " + o.toString());
        }
    }

    @Test
    public void groupTest() throws Exception {
        createUncachedObjects(100);
        HashMap<String, Object> initial = new HashMap<>();
        initial.put("count", 0);
        initial.put("sum", 0);
        Map<String, Object> ret = morphium.group(morphium.createQueryFor(UncachedObject.class), initial,
                "data.count++; data.sum+=obj.counter;", "data.avg=data.sum/data.count;");
        log.info("got DBObject: " + ret);

    }
}
