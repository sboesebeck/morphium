package de.caluga.test.mongo.suite;

import com.mongodb.DBObject;
import de.caluga.morphium.MorphiumSingleton;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 13.07.12
 * Time: 08:53
 * <p/>
 */
public class DistinctGroupTest extends MongoTest {
    @Test
    public void distinctTest() throws Exception {
        List<UncachedObject> lst = new ArrayList<UncachedObject>();
        for (int i = 0; i < 100; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i % 3);
            uc.setValue("Value " + (i % 2));
            lst.add(uc);
        }
        MorphiumSingleton.get().storeList(lst);

        List values = MorphiumSingleton.get().distinct("counter", UncachedObject.class);
        assert (values.size() == 3) : "Size wrong: " + values.size();
        for (Object o : values) {
            log.info("counter: " + o.toString());
        }
        values = MorphiumSingleton.get().distinct("value", UncachedObject.class);
        assert (values.size() == 2) : "Size wrong: " + values.size();
        for (Object o : values) {
            log.info("Value: " + o.toString());
        }
    }

    @Test
    public void groupTest() throws Exception {
        createUncachedObjects(100);
        HashMap<String, Object> initial = new HashMap<String, Object>();
        initial.put("count", 0);
        initial.put("sum", 0);
        DBObject ret = MorphiumSingleton.get().group(MorphiumSingleton.get().createQueryFor(UncachedObject.class), initial,
                "data.count++; data.sum+=obj.counter;", "data.avg=data.sum/data.count;");
        log.info("got DBObject: " + ret);

    }
}
