package de.caluga.test.mongo.suite;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.annotations.lifecycle.*;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.morphium.query.Query;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 29.03.12
 * Time: 22:59
 * <p/>
 */
public class LifecycleTest extends MongoTest {
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
        assert (preStore) : "Something went wrong: Prestore";
        assert (postStore) : "Something went wrong: poststore";

        Query<LfTestObj> q = morphium.createQueryFor(LfTestObj.class);
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        q.f("value").eq("Ein Test");
        obj = q.get(); //should trigger

        assert (postLoad) : "Something went wrong: postload";

        morphium.set(obj, "value", "test beendet");
        waitForWrites();
        assert (preUpdate);
        assert (postUpdate);
        morphium.delete(obj);
        assert (preRemove) : "Pre remove not called";
        assert (postRemove) : "Post remove not called";

        preUpdate = false;
        postUpdate = false;
        morphium.set(q, "value", "a test - lifecycle won't be called");
        assert (!preUpdate);
        assert (!postUpdate);

    }

    @Test
    public void testLazyLoading() {

        Morphium m = morphium;
        m.clearCollection(EntityPostLoad.class);

        EntityPostLoad e = new EntityPostLoad("test");
        e.emb = new EmbeddedPostLoad("testEmb");
        m.store(e);

        EntityPostLoad eFetched = m.createQueryFor(EntityPostLoad.class).get();

        assertEquals("value:", "test", eFetched.value);
        assertEquals("post load:", "OK", eFetched.testPostLoad);
        assertEquals("post load: fields initiated:", eFetched.value, eFetched.testPostLoadValue);

        EmbeddedPostLoad emb = eFetched.getEmb();
        assertEquals("embedded: value:", "testEmb", emb.value);
        assertEquals("embedded: post load:", "OK", emb.testPostLoad);
        assertEquals("embedded: post load: fields initiated:", emb.value, emb.testPostLoadValue);

        eFetched.value = "newVal";
        m.store(eFetched);
        assertEquals("Embedded: preStore", eFetched.getEmb().testPreStore, "OK");
        assertEquals("Embedded: postStore", eFetched.getEmb().testPostStore, "OK");

        m.delete(eFetched);
        assertEquals("Embedded: preDel", eFetched.getEmb().testPreRemove, "OK");
        assertEquals("Embedded: postDel", eFetched.getEmb().testPostRemove, "OK");


    }

    private void assertEquals(String s, String test, String value) {
        assert (test.equals(value)) : s;
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
