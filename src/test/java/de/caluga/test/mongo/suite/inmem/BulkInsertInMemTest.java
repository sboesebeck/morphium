package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.async.AsyncCallbackAdapter;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.Person;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


/**
 * User: Stephan Bösebeck
 * Date: 01.07.12
 * Time: 11:34
 * <p/>
 */
@SuppressWarnings("AssertWithSideEffects")

public class BulkInsertInMemTest extends MorphiumInMemTestBase {
    private boolean asyncSuccess = true;
    private boolean asyncCall = false;

    @Test
    public void maxWriteBatchTest() throws Exception {
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
        long l = morphium.createQueryFor(UncachedObject.class).countAll();
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

    @Test
    public void bulkInsert() {
        //logSeparator("Using driver: " + morphium.getDriver().getClass().getName());
        morphium.clearCollection(UncachedObject.class);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        log.info("Start storing single");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i + 1);
            uc.setStrValue("nix " + i);
            morphium.store(uc);
            assertNotNull(uc.getMorphiumId());
        }
        long dur = System.currentTimeMillis() - start;
        log.info("storing objects one by one took " + dur + " ms");
        assertEquals(1000, q.countAll());
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
        log.info("List prepared...");
        morphium.storeList(lst);
        assertNotNull(lst.get(0).getMorphiumId());
        ;
        dur = System.currentTimeMillis() - start;
        if ((morphium.getWriteBufferCount() != 0)) {
            throw new AssertionError("WriteBufferCount not 0!? Buffered:" + morphium.getBufferedWriterBufferCount());
        }
        log.info("storing objects as list took " + dur + " ms");

        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);

        assertEquals(1000, q.countAll());

    }

    @Test
    public void bulkInsertAsync() throws Exception {
        //logSeparator("Using driver: " + morphium.getDriver().getClass().getName());
        morphium.clearCollection(UncachedObject.class);
        log.info("Start storing single");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i + 1);
            uc.setStrValue("nix " + i);
            morphium.store(uc, new AsyncCallbackAdapter<UncachedObject>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                    asyncCall = true;
                }

                @Override
                public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
                    log.error("Got async error - should not be!!!", t);
                    asyncSuccess = false;
                }
            });
        }
        waitForWrites();
        long dur = System.currentTimeMillis() - start;
        log.info("storing objects one by one async took " + dur + " ms");
        Thread.sleep(1000);
        assert (asyncSuccess);
        assert (asyncCall);

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
        assert (morphium.getWriteBufferCount() == 0) : "WriteBufferCount not 0!? Buffered:" + morphium.getBufferedWriterBufferCount();
        log.info("storing objects one by one took " + dur + " ms");
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        Thread.sleep(100);
        assertEquals(1000, q.countAll());

    }

    @Test
    public void bulkInsertNonId() throws Exception {
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