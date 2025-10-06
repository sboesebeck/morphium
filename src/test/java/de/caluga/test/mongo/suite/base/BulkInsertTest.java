package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.Person;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * User: Stephan Bösebeck
 * Date: 01.07.12
 * Time: 11:34
 * <p/>
 */
@SuppressWarnings("AssertWithSideEffects")
@Tag("core")
public class BulkInsertTest extends MultiDriverTestBase {
    private boolean asyncSuccess = true;
    private boolean asyncCall = false;

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void maxWriteBatchTest(Morphium morphium) throws Exception {
        try (morphium) {
            //logSeparator("Using driver: " + morphium.getDriver().getClass().getName());
            morphium.clearCollection(UncachedObject.class);

            List<UncachedObject> lst = new ArrayList<>();
            for (int i = 0; i < 4212; i++) {
                UncachedObject u = new UncachedObject();
                u.setStrValue("V" + i);
                u.setCounter(i);
                lst.add(u);
            }
            morphium.storeList(lst);
            Thread.sleep(1000);
            long l = TestUtils.countUC(morphium);
            assert (l == 4212) : "Count wrong: " + l;

            for (UncachedObject u : lst) {
                u.setCounter(u.getCounter() + 1000);
            }
            for (int i = 0; i < 100; i++) {
                UncachedObject u = new UncachedObject();
                u.setStrValue("O" + i);
                u.setCounter(i + 1200);
                lst.add(u);
            }
            morphium.storeList(lst);


        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void bulkInsert(Morphium morphium) throws Exception {
        try (morphium) {
            //logSeparator("Using driver: " + morphium.getDriver().getClass().getName());
            morphium.clearCollection(UncachedObject.class);
            log.info("Start storing single");
            long start = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                UncachedObject uc = new UncachedObject();
                uc.setCounter(i + 1);
                uc.setStrValue("nix " + i);
                morphium.store(uc);
            }
            long dur = System.currentTimeMillis() - start;
            log.info("storing objects one by one took " + dur + " ms");
            morphium.clearCollection(UncachedObject.class);
            log.info("Start storing list");
            List<UncachedObject> lst = new ArrayList<>();
            start = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                UncachedObject uc = new UncachedObject();
                uc.setCounter(i + 1);
                uc.setStrValue("nix " + i);
                lst.add(uc);
            }
            log.info("List prepared...");
            morphium.storeList(lst);
            assertNotNull(lst.get(0).getMorphiumId());
            ;
            dur = System.currentTimeMillis() - start;
            if ((morphium.getBufferedWriterBufferCount() != 0)) {
                throw new AssertionError("WriteBufferCount not 0!? Buffered:" + morphium.getBufferedWriterBufferCount());
            }
            log.info("storing objects one by one took " + dur + " ms");
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
            assert (q.countAll() == 100) : "Assert not all stored yet????";

        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    // @MethodSource("getInMemInstanceOnly")
    public void bulkInsertAsync(Morphium morphium) throws Exception {
        try (morphium) {
            log.info("-------------------> Using driver: " + morphium.getDriver().getClass().getName());
            morphium.dropCollection(UncachedObject.class);
            TestUtils.waitForCollectionToBeDeleted(morphium, UncachedObject.class);
            log.info("Start storing single");
            long start = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                UncachedObject uc = new UncachedObject();
                uc.setCounter(i + 1);
                uc.setStrValue("nix " + i);
                morphium.store(uc, new AsyncOperationCallback<UncachedObject>() {
                    @Override
                    public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                        // log.info("Operation succeeded");
                        asyncCall = true;
                    }

                    @Override
                    public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
                        log.error("Got async error - should not be!!!", t);
                        asyncSuccess = false;
                    }
                });
            }
            TestUtils.waitForWrites(morphium, log);
            long dur = System.currentTimeMillis() - start;
            log.info("storing objects one by one async took " + dur + " ms");
            Thread.sleep(500);
            assertEquals(100, TestUtils.countUC(morphium), "Write wrong!");
            assertTrue (asyncSuccess, "Async call failed");
            assertTrue (asyncCall, "Async callback not called");

            morphium.clearCollection(UncachedObject.class);

            log.info("Start storing list");
            List<UncachedObject> lst = new ArrayList<>();
            start = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                UncachedObject uc = new UncachedObject();
                uc.setCounter(i + 1);
                uc.setStrValue("nix " + i);
                lst.add(uc);
            }
            morphium.storeList(lst);
            dur = System.currentTimeMillis() - start;
            assertEquals (0, morphium.getWriteBufferCount(), "WriteBufferCount not 0!? Buffered:" + morphium.getBufferedWriterBufferCount());
            log.info("storing objects one by one took " + dur + " ms");
            Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
            q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
            assertEquals (1000, q.countAll(), "Not all stored yet????");
            log.info("Test finished!");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void bulkInsertNonId(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(Person.class);
            List<Person> prs = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Person p = new Person();
                p.setBirthday(new Date());
                p.setName("" + i);
                prs.add(p);
            }
            morphium.storeList(prs);

            Thread.sleep(1000);
            assertNotNull(prs.get(0).getId());
            ;
            long cnt = morphium.createQueryFor(Person.class).countAll();
            assert (cnt == 100);
        }
    }
}
