package de.caluga.test.mongo.suite;

import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 11.05.12
 * Time: 23:18
 * <p/>
 */
public class SortingTest extends MongoTest {
    private void prepare() {
        log.info("Writing 5000 random elements...");
        List<UncachedObject> lst = new ArrayList<>(5000);
        for (int i = 0; i < 5000; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setValue("Random value");
            uc.setCounter((int) (Math.random() * 6000));
            lst.add(uc);
        }

        UncachedObject uc = new UncachedObject();
        uc.setValue("Random value");
        uc.setCounter(-1);
        lst.add(uc);
        uc = new UncachedObject();
        uc.setValue("Random value");
        uc.setCounter(7599);
        lst.add(uc);
        log.info("Sending bulk write...");
        morphium.storeList(lst);
        log.info("Wrote it... waiting for batch to be stored");
        while (morphium.createQueryFor(UncachedObject.class).countAll() < 5002) {
            log.info("Waiting...");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {

            }
        }
    }

    @Test
    public void sortTestDescending() throws Exception {
        prepare();
        log.info("Sorting objects...");
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("value").eq("Random value");
        q = q.sort("-counter");
        long start = System.currentTimeMillis();
        List<UncachedObject> lst = q.asList();
        long dur = System.currentTimeMillis() - start;
        log.info("Got list in: " + dur + "ms");
        int lastValue = 8888;

        for (UncachedObject u : lst) {
            assert (lastValue >= u.getCounter()) : "Counter not smaller, last: " + lastValue + " now:" + u.getCounter();
            lastValue = u.getCounter();
        }
        assert (lastValue == -1) : "Last value wrong: " + lastValue;


        q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("value").eq("Random value");
        Map<String, Integer> order = new HashMap<>();
        order.put("counter", -1);
        q = q.sort(order);

        lst = q.asList();
        lastValue = 8888;

        for (UncachedObject u : lst) {
            assert (lastValue >= u.getCounter()) : "Counter not smaller, last: " + lastValue + " now:" + u.getCounter();
            lastValue = u.getCounter();
        }
        assert (lastValue == -1) : "Last value wrong: " + lastValue;

    }


    @Test
    public void sortTestAscending() throws Exception {
        prepare();

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("value").eq("Random value");
        q = q.sort("counter");

        List<UncachedObject> lst = q.asList();
        int lastValue = -1;

        for (UncachedObject u : lst) {
            assert (lastValue <= u.getCounter()) : "Counter not greater, last: " + lastValue + " now:" + u.getCounter();
            lastValue = u.getCounter();
        }
        assert (lastValue == 7599) : "Last value wrong: " + lastValue;


        q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("value").eq("Random value");
        Map<String, Integer> order = new HashMap<>();
        order.put("counter", 1);
        q = q.sort(order);

        lst = q.asList();
        lastValue = -1;

        for (UncachedObject u : lst) {
            assert (lastValue <= u.getCounter()) : "Counter not smaller, last: " + lastValue + " now:" + u.getCounter();
            lastValue = u.getCounter();
        }
        assert (lastValue == 7599) : "Last value wrong: " + lastValue;

    }


    @Test
    public void sortTestLimit() throws Exception {
        prepare();

        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("value").eq("Random value");
        q = q.sort("counter");
        q.limit(1);
        List<UncachedObject> lst = q.asList();
        assert (lst.size() == 1) : "List size wrong: " + lst.size();
        assert (lst.get(0).getCounter() == -1) : "Smalest value wrong, should be -1, is " + lst.get(0).getCounter();

        q = morphium.createQueryFor(UncachedObject.class);
        q = q.f("value").eq("Random value");
        q = q.sort("-counter");
        UncachedObject uc = q.get();
        assert (uc != null) : "not found?!?";
        assert (uc.getCounter() == 7599) : "Highest value wrong, should be 7599, is " + uc.getCounter();

    }

}
