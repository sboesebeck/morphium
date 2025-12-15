package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.annotations.lifecycle.Lifecycle;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.InsertMongoCommand;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * User: Stephan Bösebeck
 * Date: 03.06.12
 * Time: 00:33
 * <p>
 */
@SuppressWarnings("AssertWithSideEffects")
@Tag("core")
public class ReferenceTest extends MorphiumTestBase {

    private final boolean didDeref = false;
    private final boolean wouldDeref = false;

    @Test
    public void storeReferenceTest() throws InterruptedException {
        morphium.dropCollection(ReferenceContainer.class);
        UncachedObject uc1 = new UncachedObject();
        uc1.setCounter(1);
        uc1.setStrValue("Uncached 1");
        morphium.store(uc1);
        UncachedObject uc2 = new UncachedObject();
        uc2.setCounter(1);
        uc2.setStrValue("Uncached 2");
        morphium.store(uc2);
        CachedObject co = new CachedObject();
        co.setCounter(3);
        co.setValue("Cached 3");
        morphium.storeNoCache(co);
        //Making sure it's stored yet
        ReferenceContainer rc = new ReferenceContainer();
        rc.setCo(co);
        rc.setLazyUc(uc2);
        rc.setUc(uc1);
        List<UncachedObject> lst = new ArrayList<>();
        UncachedObject toSearchFor = null;

        for (int i = 0; i < 10; i++) {
            //creating uncached Objects
            UncachedObject uc = new UncachedObject();
            uc.setStrValue("list value " + i);
            uc.setCounter(i);

            if (i == 4) {
                toSearchFor = uc;
            }

            lst.add(uc);
        }

        morphium.storeList(lst);
        rc.setLst(lst);
        UncachedObject toSearchFor2 = null;
        lst = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            //creating uncached Objects
            UncachedObject uc = new UncachedObject();
            uc.setStrValue("list value " + i);
            uc.setCounter(i);
            lst.add(uc);

            if (i == 4) {
                toSearchFor2 = uc;
            }
        }

        rc.setLzyLst(lst);
        morphium.store(rc); //stored
        ReferenceContainer rc2 = new ReferenceContainer();
        rc2.setLst(new ArrayList<>());
        rc2.setUc(toSearchFor);
        morphium.store(rc2);
        Thread.sleep(500);
        //read from db....
        Query<ReferenceContainer> q = morphium.createQueryFor(ReferenceContainer.class);
        q.f("uc").eq(uc1);
        ReferenceContainer rcRead = q.get(); //should only be one...
        assert(rcRead.getId().equals(rc.getId())) : "ID's different?!?!?";
        assert(rcRead.getUc().getMorphiumId().equals(rc.getUc().getMorphiumId())) : "Uc's Id's different?!?!";
        assert(rcRead.getCo().getId().equals(rc.getCo().getId())) : "Co's id's different";
        assert(rcRead.getLazyUc().getMorphiumId().equals(rc.getLazyUc().getMorphiumId())) : "lazy refs Ids differ";
        assert(rcRead.getLst().size() == rc.getLst().size()) : "Size of lists differ?";
        assert(rcRead.getLzyLst().get(0).getClass().getName().contains("$EnhancerByCGLIB$")) : "List not lazy?";
        assert(rcRead.getLzyLst().get(0).getCounter() == rc.getLzyLst().get(0).getCounter()) : "Counter different?!?";
        q = morphium.createQueryFor(ReferenceContainer.class).f("lst").eq(toSearchFor);
        rcRead = q.get();
        assertNotNull(rcRead);
        ;
        assert(rcRead.getUc().getCounter() != (toSearchFor != null ? toSearchFor.getCounter() : 0));
        assertNotNull(rcRead.getCo());
        ;
        assert(rcRead.getId().equals(rc.getId()));
        q = morphium.createQueryFor(ReferenceContainer.class).f("lzyLst").eq(toSearchFor2);
        rcRead = q.get();
        assertNotNull(rcRead);
        ;
        assert(rcRead.getUc().getCounter() != (toSearchFor2 != null ? toSearchFor2.getCounter() : 0));
        assertNotNull(rcRead.getCo());
        ;
        assert(rcRead.getId().equals(rc.getId()));
    }

    @Test
    public void backwardCompatibilityTest() throws Exception {
        morphium.dropCollection(ReferenceContainer.class);
        createUncachedObjects(100);
        Thread.sleep(200);
        UncachedObject referenced = morphium.createQueryFor(UncachedObject.class).get();
        Map<String, Object> reference = new HashMap<>();
        reference.put("referenced_class_name", UncachedObject.class.getName());
        reference.put("collection_name", "uncached_object");
        reference.put("refid", referenced.getMorphiumId());
        Map<String, Object> rc = new HashMap<>();
        rc.put("uc", reference);
        List<Map<String, Object >> lst = new ArrayList<>();
        lst.add(rc);
        InsertMongoCommand cmd = new InsertMongoCommand(morphium.getDriver().getPrimaryConnection(null)).setColl("reference_container")
        .setDb(morphium.getDatabase()).setDocuments(lst);
        cmd.execute();
        cmd.releaseConnection();
        Thread.sleep(1000);
        assert(morphium.createQueryFor(ReferenceContainer.class).countAll() == 1);
        ReferenceContainer container = morphium.createQueryFor(ReferenceContainer.class).get();
        assertNotNull(container.uc);
        ;
        assert(container.uc.getMorphiumId().equals(referenced.getMorphiumId()));
        assert(container.uc.getCounter() == referenced.getCounter());
    }


    @Test
    public void testSimpleDoublyLinkedStructure() throws InterruptedException {
        Morphium m = morphium;
        m.clearCollection(SimpleDoublyLinkedEntity.class);
        SimpleDoublyLinkedEntity e1 = new SimpleDoublyLinkedEntity(1);
        SimpleDoublyLinkedEntity e2 = new SimpleDoublyLinkedEntity(2);
        SimpleDoublyLinkedEntity e3 = new SimpleDoublyLinkedEntity(3);
        // store wihtout links to get Ids
        m.store(e1);
        m.store(e2);
        m.store(e3);
        // set links
        e2.setPrev(e1);
        e3.setPrev(e2);
        // update
        m.store(e1);
        m.store(e2);
        m.store(e3);
        m.clearCachefor(SimpleDoublyLinkedEntity.class);
        Thread.sleep(100);
        e2 = m.createQueryFor(SimpleDoublyLinkedEntity.class).getById(e2.id);
        e1 = m.createQueryFor(SimpleDoublyLinkedEntity.class).getById(e1.id);
        assert(e1.getValue() == e2.getPrev().getValue());
        assert(e2.getValue() == e1.getNext().getValue());
    }


    @Test
    public void mapReferenceTest() throws Exception {
        morphium.clearCollection(ReferenceContainer.class);
        ReferenceContainer c = new ReferenceContainer();
        Map<String, UncachedObject> m = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            UncachedObject uc = new UncachedObject();
            uc.setCounter(i);
            uc.setStrValue("" + i);
            m.put("" + i, uc);
        }

        c.map = m;
        morphium.store(c);
        Thread.sleep(150);
        ReferenceContainer cont = morphium.createQueryFor(ReferenceContainer.class).get();
        assert(cont.id.equals(c.id));

        for (int i = 0; i < 10; i++) {
            assert(cont.map.get("" + i).getCounter() == i);
            assert(cont.map.get("" + i).getStrValue().equals("" + i));
            assert(cont.map.get("" + i).getMorphiumId().equals(c.map.get("" + i).getMorphiumId()));
        }
    }

    @Lifecycle
    @Entity
    public static class SimpleDoublyLinkedEntity {
        @Id
        MorphiumId id;
        @Reference(lazyLoading = true, automaticStore = false)
        SimpleDoublyLinkedEntity prev, next;
        int value;

        public SimpleDoublyLinkedEntity() {
        }

        public SimpleDoublyLinkedEntity(int value) {
            this.value = value;
        }

        public SimpleDoublyLinkedEntity getPrev() {
            return prev;
        }

        public void setPrev(SimpleDoublyLinkedEntity prev) {
            this.prev = prev;
            prev.next = this;
        }

        public SimpleDoublyLinkedEntity getNext() {
            return next;
        }

        public int getValue() {
            return value;
        }
    }


    @Entity
    @WriteSafety(waitForJournalCommit = false, level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
    @NoCache
    public static class ReferenceContainer {
        @Reference
        ArrayList<UncachedObject> lst;
        @Reference
        Map<String, UncachedObject> map;

        @Reference(lazyLoading = true)
        List<UncachedObject> lzyLst;
        @Id
        private MorphiumId id;
        @Reference
        private UncachedObject uc;
        @Reference
        private CachedObject co;
        @Reference(lazyLoading = true)
        private UncachedObject lazyUc;

        public List<UncachedObject> getLzyLst() {
            return lzyLst;
        }

        public Map<String, UncachedObject> getMap() {
            return map;
        }

        public void setMap(Map<String, UncachedObject> map) {
            this.map = map;
        }

        public void setLzyLst(List<UncachedObject> lzyLst) {
            this.lzyLst = lzyLst;
        }

        public MorphiumId getId() {
            return id;
        }

        public void setId(MorphiumId id) {
            this.id = id;
        }

        public UncachedObject getUc() {
            return uc;
        }

        public void setUc(UncachedObject uc) {
            this.uc = uc;
        }

        public CachedObject getCo() {
            return co;
        }

        public void setCo(CachedObject co) {
            this.co = co;
        }

        public List<UncachedObject> getLst() {
            return lst;
        }

        public void setLst(List<UncachedObject> lst) {
            this.lst = (ArrayList) lst;
        }

        public UncachedObject getLazyUc() {
            return lazyUc;
        }

        public void setLazyUc(UncachedObject lazyUc) {
            this.lazyUc = lazyUc;
        }
    }
}
