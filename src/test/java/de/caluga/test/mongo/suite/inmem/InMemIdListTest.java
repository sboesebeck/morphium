package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.changestream.ChangeStreamMonitor;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.ListContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class InMemIdListTest extends MorphiumInMemTestBase {

    @Test
    public void inMemIdList() throws Exception {
        createUncachedObjects(100);
        Query<UncachedObject> q = morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.counter).gt(10);
        List<Object> lst = q.idList();
        assertEquals(90, lst.size());
        assertTrue(lst.get(0) instanceof MorphiumId);
        assertTrue(morphium.findById(UncachedObject.class, lst.get(0)) instanceof UncachedObject);
        assertNotNull(morphium.findById(UncachedObject.class, lst.get(0)));
    }

    @Test
    public void removeTest() throws Exception {
        UncachedObject o = new UncachedObject("val1", 42);
        morphium.store(o);
        assertEquals(1, morphium.createQueryFor(UncachedObject.class).countAll());
        morphium.remove(o);
        assertEquals(0, morphium.createQueryFor(UncachedObject.class).countAll());
    }

    @Test
    public void inMemListTest() throws Exception {
        ListContainer lc = new ListContainer();
        lc.addString("string");
        lc.addString("other");
        morphium.store(lc, "lc_test", null);
        var cnt = morphium.createQueryFor(ListContainer.class, "lc_test").f(ListContainer.Fields.stringList).eq("other")
         .countAll();
        assertEquals(1, cnt);
        lc = new ListContainer();
        lc.addString("other");
        morphium.store(lc, "lc_test", null);
        cnt = morphium.createQueryFor(ListContainer.class, "lc_test").f(ListContainer.Fields.stringList).eq("other")
         .countAll();
        assertEquals(2, cnt);
        cnt = morphium.createQueryFor(ListContainer.class, "lc_test").f(ListContainer.Fields.stringList).ne("string")
         .countAll();
        assertEquals(1, cnt);
        lc = morphium.createQueryFor(ListContainer.class, "lc_test").f(ListContainer.Fields.stringList).ne("string")
         .get();
        assertNotNull(lc);
        morphium.delete(lc, "lc_test");
        lc = morphium.createQueryFor(ListContainer.class, "lc_test").f(ListContainer.Fields.stringList).ne("string")
         .get();
        assertNull(lc);
        lc = morphium.createQueryFor(ListContainer.class, "lc_test").f(ListContainer.Fields.stringList).eq("string")
         .get();
        assertNotNull(lc);
        assertEquals(1, morphium.createQueryFor(ListContainer.class, "lc_test").countAll());
    }

    @Test
    public void changeStreamTest() throws Exception {
        morphium.dropCollection(UncachedObject.class);
        ChangeStreamMonitor m = new ChangeStreamMonitor(morphium, "uncached_object",true);
        m.start();
        final AtomicInteger cnt = new AtomicInteger(0);
        m.addListener(evt->{
            log.info("Incoming event! "+evt.getOperationType());
            cnt.incrementAndGet();
            return true;
        });
        Thread.sleep(1000);

        for (int i = 0; i < 100; i++) {
            morphium.store(new UncachedObject("value " + i, i));
        }
        Thread.sleep(1000);
        log.info("Number of events: "+cnt.get());
        assertTrue(cnt.get() >= 100 && cnt.get() <= 101);
        cnt.set(0);

        morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("value "+42).remove();
        Thread.sleep(200);
        assertEquals(1,cnt.get());
        morphium.delete(morphium.createQueryFor(UncachedObject.class).f(UncachedObject.Fields.strValue).eq("value "+32).get());
        Thread.sleep(200);
        assertEquals(2,cnt.get());
        m.terminate();
        morphium.store(new UncachedObject("killing", 0));
    }
}
