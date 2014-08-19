package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.async.AsyncOperationType;
import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 01.07.12
 * Time: 11:34
 * <p/>
 */
public class BulkInsertTest extends MongoTest {
    private boolean asyncSuccess = true;
    private boolean asyncCall = false;

    @Test
    public void maxWriteBatchTest() throws Exception {
        MorphiumSingleton.get().clearCollection(UncachedObject.class);

        List<UncachedObject> lst = new ArrayList<>();
        for (int i = 0; i < 4212; i++) {
            UncachedObject u = new UncachedObject();
            u.setValue("V" + i);
            u.setCounter(i);
            lst.add(u);
        }
        MorphiumSingleton.get().storeList(lst);
        assert (MorphiumSingleton.get().createQueryFor(UncachedObject.class).countAll() == 4212);

        for (UncachedObject u : lst) {
            u.setCounter(u.getCounter() + 1000);
        }
        for (int i = 0; i < 100; i++) {
            UncachedObject u = new UncachedObject();
            u.setValue("O" + i);
            u.setCounter(i + 1200);
            lst.add(u);
        }
        MorphiumSingleton.get().storeList(lst);


    }

    @Test
    public void bulkInsert() throws Exception {
        MorphiumSingleton.get().clearCollection(UncachedObject.class);
        log.info("Start storing single");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i + 1);
            uc.setValue("nix " + i);
            MorphiumSingleton.get().store(uc);
        }
        long dur = System.currentTimeMillis() - start;
        log.info("storing objects one by one took " + dur + " ms");
        MorphiumSingleton.get().clearCollection(UncachedObject.class);
        log.info("Start storing list");
        List<UncachedObject> lst = new ArrayList<UncachedObject>();
        start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i + 1);
            uc.setValue("nix " + i);
            lst.add(uc);
        }
        log.info("List prepared...");
        MorphiumSingleton.get().storeList(lst);
        assert (lst.get(0).getMongoId() != null);
        dur = System.currentTimeMillis() - start;
        if ((MorphiumSingleton.get().getWriteBufferCount() != 0)) {
            throw new AssertionError("WriteBufferCount not 0!? Buffered:" + MorphiumSingleton.get().getBufferedWriterBufferCount());
        }
        log.info("storing objects one by one took " + dur + " ms");
        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        assert (q.countAll() == 100) : "Assert not all stored yet????";

    }

    @Test
    public void bulkInsertAsync() throws Exception {
        MorphiumSingleton.get().clearCollection(UncachedObject.class);
        log.info("Start storing single");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i + 1);
            uc.setValue("nix " + i);
            MorphiumSingleton.get().store(uc, new AsyncOperationCallback<UncachedObject>() {
                @Override
                public void onOperationSucceeded(AsyncOperationType type, Query<UncachedObject> q, long duration, List<UncachedObject> result, UncachedObject entity, Object... param) {
                    asyncCall = true;
                }

                @Override
                public void onOperationError(AsyncOperationType type, Query<UncachedObject> q, long duration, String error, Throwable t, UncachedObject entity, Object... param) {
                    asyncSuccess = false;
                }
            });
        }
        waitForWrites();
        long dur = System.currentTimeMillis() - start;
        log.info("storing objects one by one async took " + dur + " ms");
        assert (asyncSuccess);
        assert (asyncCall);

        MorphiumSingleton.get().clearCollection(UncachedObject.class);

        log.info("Start storing list");
        List<UncachedObject> lst = new ArrayList<UncachedObject>();
        start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i + 1);
            uc.setValue("nix " + i);
            lst.add(uc);
        }
        MorphiumSingleton.get().storeList(lst);
        dur = System.currentTimeMillis() - start;
        assert (MorphiumSingleton.get().getWriteBufferCount() == 0) : "WriteBufferCount not 0!? Buffered:" + MorphiumSingleton.get().getBufferedWriterBufferCount();
        log.info("storing objects one by one took " + dur + " ms");
        Query<UncachedObject> q = MorphiumSingleton.get().createQueryFor(UncachedObject.class);
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        assert (q.countAll() == 1000) : "Assert not all stored yet????";

    }


    @Test(expected = IllegalArgumentException.class)
    public void bulkInsertNonId() throws Exception {
        MorphiumSingleton.get().dropCollection(Person.class);
        List<Person> prs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Person p = new Person();
            p.setBirthday(new Date());
            p.setName("" + i);
            prs.add(p);
        }
        MorphiumSingleton.get().storeList(prs);
    }
}
