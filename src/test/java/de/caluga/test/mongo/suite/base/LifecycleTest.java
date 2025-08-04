package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.annotations.lifecycle.*;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * User: Stephan Bösebeck
 * Date: 29.03.12
 * Time: 22:59
 * <p/>
 */
public class LifecycleTest extends MorphiumTestBase {
    private static boolean preStore = false;
    private static boolean postStore = false;
    private static boolean preRemove = false;
    private static boolean postRemove = false;
    private static boolean postLoad = false;
    private static boolean preUpdate = false;
    private static boolean postUpdate = false;


    @Test
    public void lifecycleTest() {
        LfTestObj obj = new LfTestObj();
        obj.setValue("Ein Test");
        morphium.store(obj);
        assertTrue(preStore, "Something went wrong: Prestore");
        assertTrue(postStore, "Something went wrong: poststore");
        Query<LfTestObj> q = morphium.createQueryFor(LfTestObj.class);
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        q.f("value").eq("Ein Test");
        obj = q.get(); //should trigger
        assertTrue(postLoad, "Something went wrong: postload");
        morphium.setInEntity(obj, "value", "test beendet");
        TestUtils.waitForWrites(morphium, log);
        assertTrue(preUpdate);
        assertTrue(postUpdate);
        morphium.delete(obj);
        assertTrue(preRemove, "Pre remove not called");
        assertTrue(postRemove, "Post remove not called");
        preUpdate = false;
        postUpdate = false;
        morphium.set(q, "value", "a test - lifecycle won't be called");
        assertFalse(preUpdate);
        assertFalse(postUpdate);
    }

    @Test
    public void testLazyLoading() throws Exception {
        Morphium m = morphium;
        m.clearCollection(EntityPostLoad.class);
        EntityPostLoad e = new EntityPostLoad("test");
        e.emb = new EmbeddedPostLoad("testEmb");
        m.store(e);
        Thread.sleep(150);
        EntityPostLoad eFetched = m.createQueryFor(EntityPostLoad.class).get();
        assertEquals("test", eFetched.value, "Value");
        assertEquals("test", eFetched.value, "value:");
        assertEquals("OK", eFetched.testPostLoad, "post load:");
        assertEquals(eFetched.value, eFetched.testPostLoadValue, "post load: fields initiated:");
        EmbeddedPostLoad emb = eFetched.getEmb();
        assertEquals("testEmb", emb.value, "embedded: value:");
        assertEquals("OK", emb.testPostLoad, "embedded: post load:");
        assertEquals(emb.value, emb.testPostLoadValue, "embedded: post load: fields initiated:");
        eFetched.value = "newVal";
        m.store(eFetched);
        assertEquals("OK", eFetched.getEmb().testPreStore, "Embedded: preStore");
        assertEquals("OK", eFetched.getEmb().testPostStore, "Embedded: postStore");
        m.delete(eFetched);
        assertEquals("OK", eFetched.getEmb().testPreRemove, "Embedded: preDel");
        assertEquals("OK", eFetched.getEmb().testPostRemove, "Embedded: postDel");
    }

    @Entity
    @NoCache
    @Lifecycle
    public static class LfTestObj {
        @Id
        private MorphiumId id;
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public MorphiumId getId() {
            return id;
        }

        public void setId(MorphiumId id) {
            this.id = id;
        }

        @PreStore
        public void preStore() {
            System.out.println("Object is about to be stored");
            preStore = true;
        }

        @PostStore
        public void postStore() {
            System.out.println("Object was stored");
            postStore = true;
        }

        @PostLoad
        public void postLoad() {
            System.out.println("object was loaded");
            postLoad = true;
        }

        @PreRemove
        public void preRemove() {
            System.out.println("Object is about to be removed");
            preRemove = true;
        }

        @PostRemove
        public void postRemove() {
            System.out.println("Object was deleted!");
            postRemove = true;
            id = null;
        }

        @PostUpdate
        public void postUpdate() {
            postUpdate = true;
        }

        @PreUpdate
        public void preUpdate() {
            preUpdate = true;
        }
    }

    @Entity
    @Lifecycle
    public static class EntityPostLoad {
        @Transient
        public String testPostLoad, testPostLoadValue;
        public EmbeddedPostLoad emb;
        public String value;
        @Id
        MorphiumId id;

        public EntityPostLoad(String value) {
            this.value = value;
        }

        public EmbeddedPostLoad getEmb() {
            return emb;
        }

        @PostLoad
        public void postLoad() {
            testPostLoad = "OK";
            testPostLoadValue = value;
        }
    }

    @Embedded
    @Lifecycle
    public static class EmbeddedPostLoad {
        @Transient
        public String testPostLoad, testPostLoadValue;
        @Transient
        public String testPreRemove, testPreStore, testPostStore;
        public String value;
        @Transient
        private String testPostRemove;

        //Update for embedded objects not possible - only using a query, but then there is no object to call the lifecycle method on


        public EmbeddedPostLoad(String value) {
            this.value = value;
        }

        @PostLoad
        public void postLoad() {
            testPostLoad = "OK";
            testPostLoadValue = value;
        }

        @PreStore
        public void preStore() {
            testPreStore = "OK";
        }

        @PostStore
        public void postStore() {
            testPostStore = "OK";
        }

        @PreRemove
        public void preRemove() {
            testPreRemove = "OK";
        }

        @PostRemove
        public void postRemove() {
            testPostRemove = "OK";
        }
    }

}
