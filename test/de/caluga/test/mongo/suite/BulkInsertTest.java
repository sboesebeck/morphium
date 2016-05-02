package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.Person;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 01.07.12
 * Time: 11:34
 * <p/>
 */
@SuppressWarnings("AssertWithSideEffects")
@RunWith(Theories.class)
public class BulkInsertTest extends MongoTest {
    private boolean asyncSuccess = true;
    private boolean asyncCall = false;

    @DataPoints
    public static final Morphium[] morphiums = new Morphium[]{morphiumInMemeory, morphiumSingleConnectThreadded, morphiumMeta, morphiumMongodb};//getMorphiums().toArray(new Morphium[getMorphiums().size()]);


    @Theory
    public void maxWriteBatchTest(Morphium morphium) throws Exception {
        logSeparator("Using driver: " + morphium.getDriver().getClass().getName());
        morphium.clearCollection(UncachedObject.class);

        List<UncachedObject> lst = new ArrayList<>();
        for (int i = 0; i < 4212; i++) {
            UncachedObject u = new UncachedObject();
            u.setValue("V" + i);
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
            u.setValue("O" + i);
            u.setCounter(i + 1200);
            lst.add(u);
        }
        morphium.storeList(lst);


    }

    @Theory
    public void bulkInsert(Morphium morphium) throws Exception {
        logSeparator("Using driver: " + morphium.getDriver().getClass().getName());
        morphium.clearCollection(UncachedObject.class);
        log.info("Start storing single");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i + 1);
            uc.setValue("nix " + i);
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
            uc.setValue("nix " + i);
            lst.add(uc);
        }
        log.info("List prepared...");
        morphium.storeList(lst);
        assert (lst.get(0).getMorphiumId() != null);
        dur = System.currentTimeMillis() - start;
        if ((morphium.getWriteBufferCount() != 0)) {
            throw new AssertionError("WriteBufferCount not 0!? Buffered:" + morphium.getBufferedWriterBufferCount());
        }
        log.info("storing objects one by one took " + dur + " ms");
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        assert (q.countAll() == 100) : "Assert not all stored yet????";

    }

    @Theory
    public void bulkInsertAsync(Morphium morphium) throws Exception {
        logSeparator("Using driver: " + morphium.getDriver().getClass().getName());
        morphium.clearCollection(UncachedObject.class);
        log.info("Start storing single");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i + 1);
            uc.setValue("nix " + i);
            morphium.store(uc, new AsyncOperationCallback<UncachedObject>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                    asyncCall = true;
                }

                @Override
                public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
                    log.error("Got async error - should not be!!!");
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
            uc.setValue("nix " + i);
            lst.add(uc);
        }
        morphium.storeList(lst);
        dur = System.currentTimeMillis() - start;
        assert (morphium.getWriteBufferCount() == 0) : "WriteBufferCount not 0!? Buffered:" + morphium.getBufferedWriterBufferCount();
        log.info("storing objects one by one took " + dur + " ms");
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class);
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        assert (q.countAll() == 1000) : "Assert not all stored yet????";

    }


    @Test(expected = IllegalArgumentException.class)
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
    }
}
