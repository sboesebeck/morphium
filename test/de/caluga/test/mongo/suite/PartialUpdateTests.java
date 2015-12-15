package de.caluga.test.mongo.suite;

import de.caluga.morphium.PartiallyUpdateable;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.annotations.caching.Cache;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.morphium.query.Query;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 14.06.12
 * Time: 22:38
 * <p/>
 */
public class PartialUpdateTests extends MongoTest {
    public static final int NO_OBJECTS = 100;

    /**
     * test checking for partial updates
     */
    @Test
    public void testPartialUpdates() {
        UncachedObject uo = new UncachedObject();
        uo = morphium.createPartiallyUpdateableEntity(uo);
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
        morphium.clearCollection(PartUpdTestObj.class);
        PartUpdTestObj o = new PartUpdTestObj();
        o.setName("1st");
        o.setValue(5);
        morphium.store(o);
        waitForWrites();

        Query<PartUpdTestObj> q = morphium.createQueryFor(PartUpdTestObj.class);
        q = q.f("value").eq(5);
        PartUpdTestObj po = q.get();
        assert (po.getValue() == o.getValue()) : "Values different?";
        assert (po.getName().equals(o.getName())) : "Names different?";
        assert (po instanceof PartiallyUpdateable) : "No partial updateable?";
        po.inc();
        assert (((PartiallyUpdateable) po).getAlteredFields().contains("value")) : "Value not changed?";
        morphium.store(po);

        po.setName("neuer Name");
        assert (!((PartiallyUpdateable) po).getAlteredFields().contains("value")) : "Value still in altered fields?";
        assert (((PartiallyUpdateable) po).getAlteredFields().contains("name")) : "Name not changed?";
        morphium.store(po);
        waitForWrites();
        q.setReadPreferenceLevel(ReadPreferenceLevel.PRIMARY);
        po = q.q().f("value").eq(6).get();
        assert (po.getName().equals("neuer Name")) : "Name not changed?";
    }


    @Entity
    @Cache
    @PartialUpdate
    @WriteSafety(level = SafetyLevel.WAIT_FOR_ALL_SLAVES)
    public static class PartUpdTestObj {
        private String name;
        private int value;

        @Id
        private MorphiumId id;

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
