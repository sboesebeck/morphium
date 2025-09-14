package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.async.AsyncOperationCallback;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.Query;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Tag;

/**
 * User: Stephan Bösebeck
 * Date: 14.01.16
 * Time: 14:55
 * <p>
 * TODO: Add documentation here
 */
@Tag("core")
public class CustomCollectionNameTest extends MorphiumTestBase {


    @Test
    public void testUpdateInOtherCollection() throws Exception {
        Morphium m = morphium;
        String collectionName = "entity_collection_name_update";
        m.clearCollection(EntityCollectionName.class, collectionName);
        EntityCollectionName e = new EntityCollectionName(1);
        m.storeNoCache(e, collectionName);
        Thread.sleep(100);
        Query<EntityCollectionName> q = m.createQueryFor(EntityCollectionName.class).f("value").eq(1);
        q.setCollectionName(collectionName);
        EntityCollectionName eFetched = q.get();
assert eFetched != null : "fetched before update";
assert eFetched.value == 1 : "fetched s2:";
        e.value = 2;
        m.updateUsingFields(e, collectionName, null, new String[] {"value"});
        Query<EntityCollectionName> q2 = m.createQueryFor(EntityCollectionName.class).f("value").eq(2);
        q2.setCollectionName(collectionName);
        EntityCollectionName eFetched2 = q2.get();
        assertNotNull(eFetched2, "fetchedd after update");
    }

    @Test
    public void testDeleteInOtherCollection() throws Exception {
        Morphium m = morphium;
        String collectionName = "entity_collection_name_delete";
        m.clearCollection(EntityCollectionName.class, collectionName);
        EntityCollectionName e = new EntityCollectionName(1);
        m.storeNoCache(e, collectionName);
        Thread.sleep(150);
        Query<EntityCollectionName> q = m.createQueryFor(EntityCollectionName.class).f("value").eq(1);
        q.setCollectionName(collectionName);
        EntityCollectionName eFetched = q.get();
assert eFetched != null : "fetched before delete";
        m.delete(q, (AsyncOperationCallback<EntityCollectionName>) null);
        EntityCollectionName eFetched2 = q.get();
assert eFetched2 == null : "fetched after delete";
    }


    @Entity
    public static class EntityCollectionName {
        public int value;
        @Id
        MorphiumId id;

        public EntityCollectionName(int value) {
            this.value = value;
        }
    }
}
