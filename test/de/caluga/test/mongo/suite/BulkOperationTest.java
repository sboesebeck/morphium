package de.caluga.test.mongo.suite;

import com.mongodb.BulkWriteResult;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.MorphiumStorageAdapter;
import de.caluga.morphium.MorphiumStorageListener;
import de.caluga.morphium.bulk.BulkOperationContext;
import de.caluga.morphium.bulk.BulkRequestWrapper;
import de.caluga.morphium.query.Query;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: stephan
 * Date: 29.04.14
 * Time: 08:12
 * To change this template use File | Settings | File Templates.
 */
public class BulkOperationTest extends MongoTest {
    private boolean preRemove, postRemove;
    private boolean preUpdate, postUpdate;

    @Test
    public void bulkTest() throws Exception {
        MorphiumSingleton.get().dropCollection(UncachedObject.class);

        createUncachedObjects(100);
        waitForWrites();

        BulkOperationContext c = new BulkOperationContext(MorphiumSingleton.get(), false);
        BulkRequestWrapper wrapper = c.addFind(MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").gte(0));
        wrapper.set("counter", 999, true);
        wrapper.upsert();
        c.execute();
        Thread.sleep(500);

        for (UncachedObject o : MorphiumSingleton.get().createQueryFor(UncachedObject.class).asList()) {
            assert (o.getCounter() == 999) : "Counter is " + o.getCounter();
        }

    }

    @Test
    public void bulkUpsertTest() throws Exception {
        MorphiumSingleton.get().dropCollection(UncachedObject.class);
        BulkOperationContext c = new BulkOperationContext(MorphiumSingleton.get(), false);
        BulkRequestWrapper wrapper = c.addFind(MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").eq(50));
        wrapper = wrapper.upsert();
        wrapper.inc("counter", 1, true);

        BulkWriteResult res = c.execute();
        assert (res.getUpserts().size() == 1);
        Thread.sleep(100);
        long l = MorphiumSingleton.get().createQueryFor(UncachedObject.class).countAll();
        assert (l == 1) : "Count is " + l;
        assert (MorphiumSingleton.get().createQueryFor(UncachedObject.class).get().getCounter() == 51);




    }

    @Test
    public void bulkMultiUpsertTest() throws Exception {
        MorphiumSingleton.get().dropCollection(UncachedObject.class);
        BulkOperationContext c = new BulkOperationContext(MorphiumSingleton.get(), true); //needs to be ordered
        BulkRequestWrapper wrapper = c.addFind(MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").eq(50));
        wrapper = wrapper.upsert();
        wrapper.inc("counter", 1, false);

        wrapper = c.addFind(MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("dval").eq(60).f("counter").eq(10));
        wrapper = wrapper.upsert();
        wrapper.dec("dval", 2.6, false);

        wrapper = c.addFind(MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").eq(10));
        wrapper = wrapper.upsert();
        wrapper.inc("counter", 10, false);

        wrapper = c.addFind(MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").eq(100));
        wrapper = wrapper.upsert();
        wrapper.set("counter", 1, false);

        wrapper = c.addFind(MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").eq(700));
        wrapper = wrapper.upsert();
        wrapper.max("counter", 1000, false);

        BulkWriteResult res = c.execute();
        assert (res.getUpserts().size() == 4);
        Thread.sleep(100);
        long l = MorphiumSingleton.get().createQueryFor(UncachedObject.class).countAll();
        assert (l == 4) : "Count is " + l;
        for (UncachedObject uc : MorphiumSingleton.get().createQueryFor(UncachedObject.class).asList()) {
            log.info("Counter is: " + uc.getCounter() + " dval: " + uc.getDval());
        }

    }


    @Test
    public void bulkInsertTest() throws Exception {

        MorphiumSingleton.get().dropCollection(UncachedObject.class);

        for (int j = 0; j < 2; j++) {
            BulkOperationContext ctx = new BulkOperationContext(MorphiumSingleton.get(), false);
            for (int i = 0; i < 5000; i++) {
                UncachedObject o = new UncachedObject();
                o.setCounter(i);
                o.setValue("Bulk");
                ctx.insert(o);
            }
            BulkWriteResult res = ctx.execute();
            assert (res.getInsertedCount() == 5000);
            assert (res.isAcknowledged());
        }
        while (true) {
            long l = MorphiumSingleton.get().createQueryFor(UncachedObject.class).countAll();
            log.info("Stored: " + l);
            if (l == 10000) break;
        }
    }


    @Test
    public void multipleFindBulkTest() throws Exception {
        MorphiumSingleton.get().dropCollection(UncachedObject.class);

        createUncachedObjects(100);

        BulkOperationContext c = new BulkOperationContext(MorphiumSingleton.get(), false);
        BulkRequestWrapper wrapper = c.addFind(MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").gte(50));
        wrapper.set("counter", 999, true);

        wrapper = c.addFind(MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").lt(40));
        wrapper.set("counter", 999, true);
        c.execute();
        Thread.sleep(500);
        for (UncachedObject o : MorphiumSingleton.get().createQueryFor(UncachedObject.class).asList()) {
            assert (o.getCounter() == 999 || (o.getCounter() >= 40 && o.getCounter() < 50)) : "Counter is: " + o.getCounter();
        }

    }


    @Test
    public void incTest() throws Exception {
        MorphiumSingleton.get().dropCollection(UncachedObject.class);
        createUncachedObjects(100);

        BulkOperationContext c = new BulkOperationContext(MorphiumSingleton.get(), false);
        BulkRequestWrapper wrapper = c.addFind(MorphiumSingleton.get().createQueryFor(UncachedObject.class).f("counter").gte(0));
        wrapper.inc("counter", 1000, true);
        c.execute();
        Thread.sleep(500);

        for (UncachedObject o : MorphiumSingleton.get().createQueryFor(UncachedObject.class).asList()) {
            assert (o.getCounter() > 1000) : "Counter is " + o.getCounter();
        }
    }


    @Test
    public void callbackTest() throws Exception {
        MorphiumSingleton.get().dropCollection(UncachedObject.class);

        MorphiumStorageListener<UncachedObject> listener = new MorphiumStorageAdapter<UncachedObject>() {
            @Override
            public void preRemove(Morphium m, Query<UncachedObject> q) {
                preRemove = true;
            }

            @Override
            public void postRemove(Morphium m, Query<UncachedObject> q) {
                postRemove = true;
            }

            @Override
            public void preUpdate(Morphium m, Class<? extends UncachedObject> cls, Enum updateType) {
                preUpdate = true;
            }

            @Override
            public void postUpdate(Morphium m, Class<? extends UncachedObject> cls, Enum updateType) {
                postUpdate = true;
            }
        };

        MorphiumSingleton.get().addListener(listener);
        preUpdate = postUpdate = preRemove = postRemove = false;
        incTest();
        assert (preUpdate);
        assert (postUpdate);
        MorphiumSingleton.get().removeListener(listener);
    }

}
