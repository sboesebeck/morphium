package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Index;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.test.mongo.suite.data.CappedCol;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


/**
 * User: Stephan Bösebeck
 * Date: 20.06.12
 * Time: 11:18
 * <p>
 */
@Tag("core")
public class IndexTest extends MultiDriverTestBase {


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void createIndexMapFromTest(Morphium morphium) {
        try (morphium) {
            List<Map<String, Object>> idx = morphium.createIndexKeyMapFrom(new String[] {"-timer , -namne", "bla, fasel, blub"});
            assert(idx.size() == 2) : "Created indexes: " + idx.size();
            assert(idx.get(0).get("timer").equals(-1));
            assert(idx.get(0).get("namne").equals(-1));
            assert(idx.get(1).get("bla").equals(1));
            assert(idx.get(1).get("fasel").equals(1));
            assert(idx.get(1).get("blub").equals(1));
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void checkIndexTest(Morphium morphium) throws Exception {
        try (morphium) {
            log.info("Starting check");
            Thread.sleep(100);
            morphium.dropCollection(UncachedObject.class);
            morphium.dropCollection(IndexedObject.class);
            morphium.dropCollection(CappedCol.class);
            morphium.getConfig().setIndexCheck(de.caluga.morphium.config.CollectionCheckSettings.IndexCheck.NO_CHECK);
            // morphium.createIndex(UncachedObject.class,"uncached_object",new IndexDescription().setKey(Doc.of("counter",-1)).setName("test1"),null);
            morphium.storeNoCache(new UncachedObject("value", 12));
            Thread.sleep(100);
            morphium.getConfig().setIndexCheck(de.caluga.morphium.config.CollectionCheckSettings.IndexCheck.CREATE_ON_WRITE_NEW_COL);
            morphium.store(new IndexedObject("name", 123));
            Map<Class<?>, List<IndexDescription >> missing = morphium.checkIndices();
            log.info("Got indexes");
            assertThat(missing.size()).isNotEqualTo(0);
            assertNotNull(missing.get(UncachedObject.class));

            // All drivers including InMemoryDriver now support text indexes
            assertThat(missing.get(IndexedObject.class)).isNull();
            assertThat(missing.get(CappedCol.class)).isNull(); //all indices created
            String collectionName = morphium.getMapper().getCollectionName(UncachedObject.class);
            morphium.createIndex(UncachedObject.class, collectionName, missing.get(UncachedObject.class).get(0), null);
            Thread.sleep(1600);
            List<IndexDescription> newMissingIndexLists = morphium.getMissingIndicesFor(UncachedObject.class);
            assertThat(newMissingIndexLists.size()).isLessThan(missing.get(UncachedObject.class).size());
            //look for capping info
            morphium.ensureIndicesFor(UncachedObject.class);
            Thread.sleep(1600); //waiting for writebuffer and mongodb
            assertThat(morphium.getMissingIndicesFor(UncachedObject.class)).isEmpty();
            var lst = morphium.getIndexesFromMongo(UncachedObject.class);
            var lst2 = morphium.getIndexesFromEntity(UncachedObject.class);
            assertEquals(lst2.size() + 1, lst.size());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void indexOnNewCollTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.getConfig().setIndexCheck(de.caluga.morphium.config.CollectionCheckSettings.IndexCheck.CREATE_ON_WRITE_NEW_COL);
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
            var idx = morphium.getIndexesFromMongo(IndexedObject.class);
            boolean foundId = false;
            boolean foundTimerName = false;
            boolean foundTimerName2 = false;
            boolean foundTimer = false;
            boolean foundName = false;
            boolean foundLst = false;

            //        assert (false);
            for (var i : idx) {
                System.out.println(i.toString());
                Map<String, Object> key = (Map<String, Object>) i.getKey();

                if (key.get("_id") != null && key.get("_id").equals(1)) {
                    foundId = true;
                    assert(i.getUnique() == null || !(Boolean) i.getUnique());
                } else if (key.get("name") != null && key.get("name").equals(1) && key.get("timer") == null) {
                    foundName = true;
                    assert(i.getUnique() == null || !(Boolean) i.getUnique());
                } else if (key.get("timer") != null && key.get("timer").equals(-1) && key.get("name") == null) {
                    foundTimer = true;
                    assert(i.getUnique() == null || !(Boolean) i.getUnique());
                } else if (key.get("lst") != null) {
                    foundLst = true;
                    assert(i.getUnique() == null || !(Boolean) i.getUnique());
                } else if (key.get("timer") != null && key.get("timer").equals(-1) && key.get("name") != null && key.get("name").equals(-1)) {
                    foundTimerName2 = true;
                    assert(i.getUnique() == null || !(Boolean) i.getUnique());
                } else if (key.get("timer") != null && key.get("timer").equals(1) && key.get("name") != null && key.get("name").equals(-1)) {
                    foundTimerName = true;
                    assert(i.getUnique() != null && (Boolean) i.getUnique());
                }
            }

            log.info("Found indices id:" + foundId + " timer: " + foundTimer + " TimerName: " + foundTimerName + " name: " + foundName + " TimerName2: " + foundTimerName2);
            assert(foundId && foundTimer && foundTimerName && foundName && foundTimerName2 && foundLst);
        }
    }


    @SuppressWarnings("ConstantConditions")
    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void ensureIndexHierarchyTest(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(IndexedSubObject.class);
            Thread.sleep(500);
            morphium.ensureIndicesFor(IndexedSubObject.class);
            Thread.sleep(500); // Wait for indices to be fully created on replica sets
            var idx = morphium.getIndexesFromMongo(IndexedSubObject.class);
            // Log all indices for debugging
            for (IndexDescription i : idx) {
                log.info("Index found: " + i.getKey());
            }
            boolean foundnew1 = false;
            boolean foundnew2 = false;
            boolean foundId = false;
            boolean foundTimerName = false;
            boolean foundTimerName2 = false;
            boolean foundTimer = false;
            boolean foundName = false;
            boolean foundLst = false;

            for (IndexDescription i : idx) {
                Map<String, Object> key = (Map<String, Object>) i.getKey();

                if (key.get("_id") != null && key.get("_id").equals(1)) {
                    foundId = true;
                    assert(i.getUnique() == null || !(Boolean) i.getUnique());
                } else if (key.get("name") != null && key.get("something") == null && key.get("name").equals(1) && key.get("timer") == null) {
                    foundName = true;
                    assert(i.getUnique() == null || !(Boolean) i.getUnique());
                } else if (key.get("timer") != null && key.get("timer").equals(-1) && key.get("name") == null) {
                    foundTimer = true;
                    assert(i.getUnique() == null || !(Boolean) i.getUnique());
                } else if (key.get("lst") != null) {
                    foundLst = true;
                    assert(i.getUnique() == null || !(Boolean) i.getUnique());
                } else if (key.get("timer") != null && key.get("timer").equals(-1) && key.get("name") != null && key.get("name").equals(-1)) {
                    foundTimerName2 = true;
                    assert(i.getUnique() == null || !(Boolean) i.getUnique());
                } else if (key.get("timer") != null && key.get("timer").equals(1) && key.get("name") != null && key.get("name").equals(-1)) {
                    foundTimerName = true;
                    assert(i.getUnique() == null || (Boolean) i.getUnique());
                } else if (key.get("something") != null && key.get("some_other") != null && key.get("something").equals(1) && key.get("some_other").equals(1)) {
                    foundnew1 = true;
                } else if (key.get("name") != null && key.get("something") != null && key.get("name").equals(1) && key.get("something").equals(-1)) {
                    foundnew2 = true;
                }
            }

            log.info("Found indices id:" + foundId + " timer: " + foundTimer + " TimerName: " + foundTimerName + " name: " + foundName + " TimerName2: " + foundTimerName2 + " lst: " + foundLst + " SubIndex1: " + foundnew1 +
                     " subIndex2: " + foundnew2);
            assert(foundnew1 && foundnew2 && foundId && foundTimer && foundTimerName && foundName && foundTimerName2 && foundLst);
        }
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
        @Index(options = {"unique:true"})
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
