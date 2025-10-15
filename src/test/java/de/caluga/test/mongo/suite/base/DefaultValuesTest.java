package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("core")
public class DefaultValuesTest extends MorphiumTestBase {

    @Test
    public void defaultValuesTest() throws Exception {
        DefaultsTestEntitiy e = new DefaultsTestEntitiy();
        morphium.store(e);
        TestUtils.waitForConditionToBecomeTrue(2000, "DefaultsTestEntitiy not persisted",
            () -> morphium.findById(DefaultsTestEntitiy.class, e.id) != null);
        DefaultsTestEntitiy read = morphium.findById(DefaultsTestEntitiy.class, e.id);
        assertNull(read.bool);
        assertEquals(read.v, e.v);
        assertEquals("value", read.value);
        assertEquals("value2", read.value2);
        morphium.setInEntity(read, "value", (Object) null);
        morphium.unsetInEntity(read, "value2");
        final MorphiumId entityId = e.id;
        TestUtils.waitForConditionToBecomeTrue(2000, "setInEntity/unsetInEntity operations not persisted",
            () -> {
                var obj = morphium.findById(DefaultsTestEntitiy.class, entityId);
                return obj != null && obj.value == null;
            });

        var m = morphium.createQueryFor(DefaultsTestEntitiy.class).f("_id").eq(e.id).asMapList().get(0);
        for (var en : m.entrySet()) {
            log.info("Got key: {} = {}", en.getKey(), en.getValue());
        }

        read = morphium.findById(DefaultsTestEntitiy.class, e.id);
        assertNull(read.value);
        assertEquals("value2", read.value2);
        assertNull(read.bool);
    }


    @Entity
    public static class DefaultsTestEntitiy {
        @Id
        public MorphiumId id;
        // Removed @UseIfNull - default behavior now accepts nulls
        public String value = "value";
        public String value2 = "value2";
        public int v = 12;
        public Boolean bool = null;
    }
}
