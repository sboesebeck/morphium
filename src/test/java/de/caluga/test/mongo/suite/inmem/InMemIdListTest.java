package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.ListContainer;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;

import java.util.List;

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
    public void inMemListTest() throws Exception {
        ListContainer lc=new ListContainer();
        lc.addString("string");
        lc.addString("other");
        morphium.store(lc);

        var cnt=morphium.createQueryFor(ListContainer.class).f(ListContainer.Fields.stringList).eq("other").countAll();
        assertEquals(1,cnt);

        lc=new ListContainer();
        lc.addString("other");
        morphium.store(lc);
        cnt=morphium.createQueryFor(ListContainer.class).f(ListContainer.Fields.stringList).eq("other").countAll();
        assertEquals(2,cnt);

        cnt=morphium.createQueryFor(ListContainer.class).f(ListContainer.Fields.stringList).ne("string").countAll();
        assertEquals(1,cnt);

    }
}
