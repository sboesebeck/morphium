package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.ReadPreferenceLevel;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.annotations.lifecycle.*;
import de.caluga.morphium.query.Query;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * User: Stephan Bösebeck
 * Date: 29.03.12
 * Time: 22:59
 * <p/>
 * TODO: Add documentation here
 */
public class LifecycleTest extends MongoTest {
    private static boolean preStore = false;
    private static boolean postStore = false;
    private static boolean preRemove = false;
    private static boolean postRemove = false;
    private static boolean postLoad = false;


    @Test
    public void lifecycleTest() {
        LfTestObj obj = new LfTestObj();
        obj.setValue("Ein Test");
        MorphiumSingleton.get().store(obj);
        assert (preStore) : "Something went wrong: Prestore";
        assert (postStore) : "Something went wrong: poststore";

        Query<LfTestObj> q = MorphiumSingleton.get().createQueryFor(LfTestObj.class);
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        q.f("value").eq("Ein Test");
        obj = q.get(); //should trigger

        assert (postLoad) : "Something went wrong: postload";

        MorphiumSingleton.get().delete(obj);
        assert (preRemove) : "Pre remove not called";
        assert (postRemove) : "Post remove not called";
    }

    @Entity
    @NoCache
    @Lifecycle
    public static class LfTestObj {
        @Id
        private ObjectId id;
        private String value;

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public ObjectId getId() {
            return id;
        }

        public void setId(ObjectId id) {
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
    }
}
