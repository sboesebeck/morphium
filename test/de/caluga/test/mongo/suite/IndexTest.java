package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.test.mongo.suite.data.CachedObject;
import de.caluga.test.mongo.suite.data.CappedCol;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 20.06.12
 * Time: 11:18
 * <p/>
 */
public class IndexTest extends MorphiumTestBase {
    @Test
    public void createIndexMapFromTest() {
        List<Map<String, Object>> idx = morphium.createIndexMapFrom(new String[]{"-timer , -namne", "bla, fasel, blub"});
        assert (idx.size() == 2) : "Created indexes: " + idx.size();
        assert (idx.get(0).get("timer").equals(-1));
        assert (idx.get(0).get("namne").equals(-1));

        assert (idx.get(1).get("bla").equals(1));
        assert (idx.get(1).get("fasel").equals(1));
        assert (idx.get(1).get("blub").equals(1));
    }

    @Test
    public void checkIndexTest() throws Exception {
        morphium.dropCollection(CachedObject.class);
        morphium.dropCollection(UncachedObject.class);
        morphium.dropCollection(IndexedObject.class);
        morphium.dropCollection(CappedCol.class);
        Thread.sleep(1500);
        morphium.getConfig().setIndexCappedCheck(MorphiumConfig.IndexCappedCheck.NO_CHECK);
        morphium.storeNoCache(new CachedObject("value", 12));
        Thread.sleep(100);
        morphium.getConfig().setIndexCappedCheck(MorphiumConfig.IndexCappedCheck.CREATE_ON_WRITE_NEW_COL);
        morphium.store(new UncachedObject("value", 123));
        morphium.store(new IndexedObject("name", 123));

        Map<Class<?>, List<Map<String, Object>>> missing = morphium.checkIndices();
        assert (missing.size() != 0);
        assert (missing.get(UncachedObject.class) == null);
        assert (missing.get(IndexedObject.class) == null);
        assert (missing.get(CachedObject.class) != null);
        assert (missing.get(CappedCol.class) != null);
        morphium.ensureIndex(CachedObject.class, missing.get(CachedObject.class).get(0));
        Thread.sleep(1500);
        List<Map<String, Object>> missingForCached = morphium.getMissingIndicesFor(CachedObject.class);
        assert (missingForCached.size() < missing.get(CachedObject.class).size());
        //look for capping info
        boolean found = false;
        for (Map<String, Object> info : missing.get(CappedCol.class)) {
            if (info.containsKey("__capped_size") && info.containsKey("__capped_entries")) {
                found = true;
                break;
            }
        }
        assert (found);

        morphium.ensureIndicesFor(CachedObject.class);
        Thread.sleep(1600); //waiting for writebuffer and mongodb
        assert (morphium.getMissingIndicesFor(CachedObject.class).isEmpty());

    }

    @Test
    public void indexOnNewCollTest() throws Exception {
        morphium.dropCollection(IndexedObject.class);
        while (morphium.getDriver().exists(morphium.getConfig().getDatabase(), morphium.getMapper().getCollectionName(IndexedObject.class))) {
            log.info("Collection still exists... waiting");
            Thread.sleep(100);
        }
        IndexedObject obj = new IndexedObject("test", 101);
        morphium.store(obj);
        //waiting for indices to be created
        Thread.sleep(1000);

        //index should now be available
        List<Map<String, Object>> idx = morphium.getDriver().getIndexes(morphium.getConfig().getDatabase(), "indexed_object");
        boolean foundId = false;
        boolean foundTimerName = false;
        boolean foundTimerName2 = false;
        boolean foundTimer = false;
        boolean foundName = false;
        boolean foundLst = false;

        //        assert (false);
        for (Map<String, Object> i : idx) {
            System.out.println(i.toString());
            Map<String, Object> key = (Map<String, Object>) i.get("key");
            if (key.get("_id") != null && key.get("_id").equals(1)) {
                foundId = true;
                assert (i.get("unique") == null || !(Boolean) i.get("unique"));
            } else if (key.get("name") != null && key.get("name").equals(1) && key.get("timer") == null) {
                foundName = true;
                assert (i.get("unique") == null || !(Boolean) i.get("unique"));
            } else if (key.get("timer") != null && key.get("timer").equals(-1) && key.get("name") == null) {
                foundTimer = true;
                assert (i.get("unique") == null || !(Boolean) i.get("unique"));
            } else if (key.get("lst") != null) {
                foundLst = true;
                assert (i.get("unique") == null || !(Boolean) i.get("unique"));
            } else if (key.get("timer") != null && key.get("timer").equals(-1) && key.get("name") != null && key.get("name").equals(-1)) {
                foundTimerName2 = true;
                assert (i.get("unique") == null || !(Boolean) i.get("unique"));
            } else if (key.get("timer") != null && key.get("timer").equals(1) && key.get("name") != null && key.get("name").equals(-1)) {
                foundTimerName = true;
                assert ((Boolean) i.get("unique"));

            }
        }
        log.info("Found indices id:" + foundId + " timer: " + foundTimer + " TimerName: " + foundTimerName + " name: " + foundName + " TimerName2: " + foundTimerName2);
        assert (foundId && foundTimer && foundTimerName && foundName && foundTimerName2 && foundLst);
    }


    @SuppressWarnings("ConstantConditions")
    @Test
    public void ensureIndexHierarchyTest() throws Exception {
        morphium.dropCollection(IndexedSubObject.class);
        Thread.sleep(250);
        morphium.ensureIndicesFor(IndexedSubObject.class);
        List<Map<String, Object>> idx = morphium.getDriver().getIndexes(morphium.getConfig().getDatabase(), "indexed_sub_object");
        boolean foundnew1 = false;
        boolean foundnew2 = false;

        boolean foundId = false;
        boolean foundTimerName = false;
        boolean foundTimerName2 = false;
        boolean foundTimer = false;
        boolean foundName = false;
        boolean foundLst = false;
        for (Map<String, Object> i : idx) {
            Map<String, Object> key = (Map<String, Object>) i.get("key");
            if (key.get("_id") != null && key.get("_id").equals(1)) {
                foundId = true;
                assert (i.get("unique") == null || !(Boolean) i.get("unique"));
            } else if (key.get("name") != null && key.get("something") == null && key.get("name").equals(1) && key.get("timer") == null) {
                foundName = true;
                assert (i.get("unique") == null || !(Boolean) i.get("unique"));
            } else if (key.get("timer") != null && key.get("timer").equals(-1) && key.get("name") == null) {
                foundTimer = true;
                assert (i.get("unique") == null || !(Boolean) i.get("unique"));
            } else if (key.get("lst") != null) {
                foundLst = true;
                assert (i.get("unique") == null || !(Boolean) i.get("unique"));
            } else if (key.get("timer") != null && key.get("timer").equals(-1) && key.get("name") != null && key.get("name").equals(-1)) {
                foundTimerName2 = true;
                assert (i.get("unique") == null || !(Boolean) i.get("unique"));
            } else if (key.get("timer") != null && key.get("timer").equals(1) && key.get("name") != null && key.get("name").equals(-1)) {
                foundTimerName = true;
                assert ((Boolean) i.get("unique"));
            } else if (key.get("something") != null && key.get("some_other") != null && key.get("something").equals(1) && key.get("some_other").equals(1)) {
                foundnew1 = true;
            } else if (key.get("name") != null && key.get("something") != null && key.get("name").equals(1) && key.get("something").equals(-1)) {
                foundnew2 = true;

            }
        }
        log.info("Found indices id:" + foundId + " timer: " + foundTimer + " TimerName: " + foundTimerName + " name: " + foundName + " TimerName2: " + foundTimerName2 + " SubIndex1: " + foundnew1 + " subIndex2: " + foundnew2);
        assert (foundnew1 && foundnew2 && foundId && foundTimer && foundTimerName && foundName && foundTimerName2 && foundLst);
    }

    @Index({"name,-something", "something,some_other"})
    public static class IndexedSubObject extends IndexedObject {
        @Index(options = {"unique:1"})
        private String idxFld;
        private String something;

        public String getSomething() {
            return something;
        }

        public void setSomething(String something) {
            this.something = something;
        }
    }

    @Entity
    @Index(value = {"-name, timer", "-name, -timer", "lst:2d", "name:text"}, options = {"unique:1", "", "", ""})
    public static class IndexedObject {
        @Id
        MorphiumId id;
        @Property
        @Index(decrement = true)
        private int timer;
        @Index
        private String name;
        private String someOther;
        //Index defined up
        private List<Integer> lst;

        public IndexedObject() {
        }

        public IndexedObject(String name, int timer) {
            this.name = name;
            this.timer = timer;
        }

        public int getTimer() {
            return timer;
        }

        public void setTimer(int timer) {
            this.timer = timer;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public MorphiumId getId() {
            return id;
        }

        public void setId(MorphiumId id) {
            this.id = id;
        }

        public void addLst(Integer i) {
            if (lst == null) {
                lst = new ArrayList<>();
            }
            lst.add(i);
        }

        public List<Integer> getLst() {
            return lst;
        }

        public void setLst(List<Integer> lst) {
            this.lst = lst;
        }
    }
}
