package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * User: Stephan Bösebeck
 * Date: 11.05.12
 * Time: 23:18
 * <p/>
 */
@Tag("core")
public class SortingTest extends MultiDriverTestBase {
    private void prepare(Morphium morphium) {
        morphium.dropCollection(UncachedObject.class);
        log.info("Writing 5000 random elements...");
        List<UncachedObject> lst = new ArrayList<>(5000);

        for (int i = 0; i < 5000; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setStrValue("Random value");
            uc.setCounter((int) Math.floor(Math.random() * 6000.0));
            uc.setDval(uc.getCounter() % 5);
            lst.add(uc);
        }

        var uc = new UncachedObject();
        uc.setStrValue("Random value");
        uc.setCounter(7599);
        uc.setDval(uc.getCounter() % 5);
        lst.add(uc);
        uc = new UncachedObject();
        uc.setStrValue("Random value");
        uc.setCounter(0);
        uc.setDval(0);
        lst.add(uc);
        log.info("Sending bulk write...");
        morphium.storeList(lst);
        log.info("Wrote it... waiting for batch to be stored");
        TestUtils.waitForConditionToBecomeTrue(1000, "Data not written", ()-> TestUtils.countUC(morphium) == 5002);
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void sortTestMultiKey(Morphium morphium) throws Exception {
        log.info("Running with " + morphium.getDriver().getName());

        try(morphium) {
            prepare(morphium);
            log.info("Sorting objects...");
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("str_value").eq("Random value");
            q = q.sort("dval", "-counter");
            long start = System.currentTimeMillis();
            List<UncachedObject> lst = q.asList();
            long dur = System.currentTimeMillis() - start;
            log.info("Got list in: " + dur + "ms");
            int lastValue = 8888;
            double lastDval = 0;

            for (UncachedObject u : lst) {
                log.info(String.format("-- Values: %f  /  %d", u.getDval(), u.getCounter()));

                if (u.getDval() == lastDval) {
                    assertThat(lastValue).describedAs("Counter not smaller, last %d now: %d", lastValue,
                                                      u.getCounter()).isGreaterThanOrEqualTo(u.getCounter()); // >= u.getCounter()) : "Counter not smaller, last: " + lastValue + " now:" + u.getCounter();
                } else {
                    assertThat(lastDval).describedAs("Dval").isLessThanOrEqualTo(u.getDval());
                    lastDval = u.getDval();
                }

                lastValue = u.getCounter();
            }
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void sortTestDescending(Morphium morphium) throws Exception {
        log.info("Running with " + morphium.getDriver().getName());

        try(morphium) {
            prepare(morphium);
            Thread.sleep(100);
            log.info("Sorting objects...");
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("str_value").eq("Random value");
            q = q.sort("-counter");
            long start = System.currentTimeMillis();
            List<UncachedObject> lst = q.asList();
            long dur = System.currentTimeMillis() - start;
            log.info("Got list in: " + dur + "ms");
            int lastValue = 8888;

            for (UncachedObject u : lst) {
                assertThat(lastValue).describedAs("Counter not smaller, last %d now: %d", lastValue,
                                                  u.getCounter()).isGreaterThanOrEqualTo(u.getCounter()); // >= u.getCounter()) : "Counter not smaller, last: " + lastValue + " now:" + u.getCounter();
                lastValue = u.getCounter();
            }

            assertThat(lastValue).describedAs("Last value").isEqualTo(0);
            q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("str_value").eq("Random value");
            Map<String, Integer> order = new HashMap<>();
            order.put("counter", -1);
            q = q.sort(order);
            lst = q.asList();
            lastValue = 8888;

            if (lst.size() > 5002) {
                log.warn("Got some old elements?");
            } else {
                assertThat(lst.size()).describedAs("result size").isEqualTo(5002);
            }

            for (UncachedObject u : lst) {
                assertThat(lastValue).describedAs("Counter should be smaller, last %d now: %d", lastValue, u.getCounter()).isGreaterThanOrEqualTo(u.getCounter());
                lastValue = u.getCounter();
            }

            assertEquals(0, lastValue, "LastValue wrong");
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void sortTestAscending(Morphium morphium) throws Exception {
        log.info("Running with " + morphium.getDriver().getName());

        try(morphium) {
            prepare(morphium);
            Thread.sleep(1000);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("str_value").eq("Random value");
            q = q.sort("counter");
            List<UncachedObject> lst = q.asList();

            if (lst.size() == 0) {
                Thread.sleep(1000);
                lst = q.asList();
            }

            int lastValue = -1;

            for (UncachedObject u : lst) {
                assert(lastValue <= u.getCounter()) : "Counter not greater, last: " + lastValue + " now:" + u.getCounter();
                lastValue = u.getCounter();
            }

            assert(lastValue == 7599) : "Last value wrong: " + lastValue;
            q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("str_value").eq("Random value");
            Map<String, Integer> order = new HashMap<>();
            order.put("counter", 1);
            q = q.sort(order);
            lst = q.asList();
            lastValue = -1;

            for (UncachedObject u : lst) {
                assert(lastValue <= u.getCounter()) : "Counter not smaller, last: " + lastValue + " now:" + u.getCounter();
                lastValue = u.getCounter();
            }

            assert(lastValue == 7599) : "Last value wrong: " + lastValue;
        }
    }


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void sortTestLimit(Morphium morphium) throws Exception {
        log.info("Running with " + morphium.getDriver().getName());

        try(morphium) {
            prepare(morphium);
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("str_value").eq("Random value");
            q = q.sort("counter");
            q.limit(1);
            Thread.sleep(1000);
            List<UncachedObject> lst = q.asList();
            assertEquals(1, lst.size());
            assertEquals(0, lst.get(0).getCounter(), "Smalest value wrong, should be 0");
            q = morphium.createQueryFor(UncachedObject.class);
            q = q.f("strValue").eq("Random value");
            q = q.sort("-counter");
            UncachedObject uc = q.get();
            assertNotNull(uc, "not found?!?");
            assertEquals(7599, uc.getCounter(), "Highest value wrong, should be 7599");
        }
    }

}
