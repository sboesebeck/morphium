package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.query.Query;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 03.06.12
 * Time: 00:33
 * <p/>
 */
public class ReferenceTest extends MongoTest {

    @Test
    public void storeReferenceTest() throws Exception {
        MorphiumSingleton.get().dropCollection(ReferenceContainer.class);
        Thread.sleep(250);
        UncachedObject uc1 = new UncachedObject();
        uc1.setCounter(1);
        uc1.setValue("Uncached 1");
        MorphiumSingleton.get().store(uc1);

        UncachedObject uc2 = new UncachedObject();
        uc2.setCounter(1);
        uc2.setValue("Uncached 2");
        MorphiumSingleton.get().store(uc2);


        CachedObject co = new CachedObject();
        co.setCounter(3);
        co.setValue("Cached 3");
        MorphiumSingleton.get().storeNoCache(co);
        //Making sure it's stored yet

        ReferenceContainer rc = new ReferenceContainer();
        rc.setCo(co);
        rc.setLazyUc(uc2);
        rc.setUc(uc1);

        List<UncachedObject> lst = new ArrayList<UncachedObject>();
        UncachedObject toSearchFor = null;
        for (int i = 0; i < 10; i++) {
            //creating uncached Objects
            UncachedObject uc = new UncachedObject();
            uc.setValue("list value " + i);
            uc.setCounter(i);
            if (i == 4)
                toSearchFor = uc;
            lst.add(uc);
        }
        MorphiumSingleton.get().storeList(lst);
        rc.setLst(lst);

        UncachedObject toSearchFor2 = null;
        lst = new ArrayList<UncachedObject>();
        for (int i = 0; i < 10; i++) {
            //creating uncached Objects
            UncachedObject uc = new UncachedObject();
            uc.setValue("list value " + i);
            uc.setCounter(i);
            lst.add(uc);
            if (i == 4) {
                toSearchFor2 = uc;
            }
        }

        rc.setLzyLst(lst);


        MorphiumSingleton.get().store(rc); //stored


        ReferenceContainer rc2 = new ReferenceContainer();
        rc2.setLst(new ArrayList<UncachedObject>());
        rc2.setUc(toSearchFor);
        MorphiumSingleton.get().store(rc2);

        //read from db....

        Query<ReferenceContainer> q = MorphiumSingleton.get().createQueryFor(ReferenceContainer.class);
        q.f("uc").eq(uc1);
        ReferenceContainer rcRead = q.get(); //should only be one...

        assert (rcRead.getId().equals(rc.getId())) : "ID's different?!?!?";
        assert (rcRead.getUc().getMongoId().equals(rc.getUc().getMongoId())) : "Uc's Id's different?!?!";
        assert (rcRead.getCo().getId().equals(rc.getCo().getId())) : "Co's id's different";
        assert (rcRead.getLazyUc().getMongoId().equals(rc.getLazyUc().getMongoId())) : "lazy refs Ids differ";
        assert (rcRead.getLst().size() == rc.getLst().size()) : "Size of lists differ?";
        assert (rcRead.getLzyLst().get(0).getClass().getName().contains("$EnhancerByCGLIB$")) : "List not lazy?";
        assert (rcRead.getLzyLst().get(0).getCounter() == rc.getLzyLst().get(0).getCounter()) : "Counter different?!?";

        q = MorphiumSingleton.get().createQueryFor(ReferenceContainer.class).f("lst").eq(toSearchFor);
        rcRead = q.get();
        assert (rcRead != null);
        assert (rcRead.getUc().getCounter() != toSearchFor.getCounter());
        assert (rcRead.getCo() != null);
        assert (rcRead.getId().equals(rc.getId()));

        q = MorphiumSingleton.get().createQueryFor(ReferenceContainer.class).f("lzyLst").eq(toSearchFor2);
        rcRead = q.get();
        assert (rcRead != null);
        assert (rcRead.getUc().getCounter() != toSearchFor2.getCounter());
        assert (rcRead.getCo() != null);
        assert (rcRead.getId().equals(rc.getId()));
    }


    @Entity
    @WriteSafety(waitForSync = true, waitForJournalCommit = true, level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
    @NoCache
    public static class ReferenceContainer {
        @Id
        private ObjectId id;

        @Reference
        private UncachedObject uc;
        @Reference
        private CachedObject co;

        @Reference
        ArrayList<UncachedObject> lst;

        @Reference(lazyLoading = true)
        List<UncachedObject> lzyLst;

        @Reference(lazyLoading = true)
        private UncachedObject lazyUc;

        public List<UncachedObject> getLzyLst() {
            return lzyLst;
        }

        public void setLzyLst(List<UncachedObject> lzyLst) {
            this.lzyLst = lzyLst;
        }

        public ObjectId getId() {
            return id;
        }

        public void setId(ObjectId id) {
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
