package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.PartiallyUpdateable;
import de.caluga.morphium.Query;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 14.06.12
 * Time: 22:38
 * <p/>
 * TODO: Add documentation here
 */
public class PartialUpdateTests extends MongoTest {
    public static final int NO_OBJECTS = 100;

    /**
     * test checking for partial updates
     */
    @Test
    public void testPartialUpdates() {
        UncachedObject uo = new UncachedObject();
        uo = MorphiumSingleton.get().createPartiallyUpdateableEntity(uo);
        assert (uo instanceof PartiallyUpdateable) : "Created proxy incorrect";
        uo.setValue("A TEST");
        List<String> alteredFields = ((PartiallyUpdateable) uo).getAlteredFields();
        for (String f : alteredFields) {
            log.info("Field altered: " + f);
        }

        assert (alteredFields.contains("value")) : "Field not set?";

    }

    @Test
    public void partialUpdateTest() throws Exception {
        MorphiumSingleton.get().clearCollection(PartUpdTestObj.class);
        PartUpdTestObj o = new PartUpdTestObj();
        o.setName("1st");
        o.setValue(5);
        MorphiumSingleton.get().store(o);
        waitForWrites();

        Query<PartUpdTestObj> q = MorphiumSingleton.get().createQueryFor(PartUpdTestObj.class);
        q = q.f("value").eq(5);
        PartUpdTestObj po = q.get();
        assert (po.getValue() == o.getValue()) : "Values different?";
        assert (po.getName().equals(o.getName())) : "Names different?";
        assert (po instanceof PartiallyUpdateable) : "No partial updateable?";
        po.inc();
        assert (((PartiallyUpdateable) po).getAlteredFields().contains("value")) : "Value not changed?";
        MorphiumSingleton.get().store(po);

        po.setName("neuer Name");
        assert (!((PartiallyUpdateable) po).getAlteredFields().contains("value")) : "Value still in altered fields?";
        assert (((PartiallyUpdateable) po).getAlteredFields().contains("name")) : "Name not changed?";
        MorphiumSingleton.get().store(po);

        po = (PartUpdTestObj) q.q().f("value").eq(6).get();
        assert (po.getName().equals("neuer Name")) : "Name not changed?";
    }


    @Entity
    @Cache
    @PartialUpdate
    @WriteSafety(level = SafetyLevel.WAIT_FOR_SLAVE)
    public static class PartUpdTestObj {
        private String name;
        private int value;

        @Id
        private ObjectId id;

        @PartialUpdate("value")
        public void inc() {
            value++;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }


}
