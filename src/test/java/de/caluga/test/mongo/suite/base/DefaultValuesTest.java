package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.MorphiumId;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("core")
public class DefaultValuesTest extends MorphiumTestBase {

    @Test
    public void defaultValuesTest() throws Exception {
        DefaultsTestEntitiy e = new DefaultsTestEntitiy();
        morphium.store(e);
        Thread.sleep(150);
        DefaultsTestEntitiy read = morphium.findById(DefaultsTestEntitiy.class, e.id);
        assert(read.bool == null);
        assert(read.v == e.v);
        assert(read.value.equals("value"));
        assert(read.value2.equals("value2"));
        morphium.setInEntity(read, "value", (Object) null);
        morphium.unsetInEntity(read, "value2");
        Thread.sleep(500);

        read = morphium.findById(DefaultsTestEntitiy.class, e.id);
        assert(read.value == null);
        assert(read.value2.equals("value2"));
        assert(read.bool == null);
    }


    @Entity
    public static class DefaultsTestEntitiy {
        @Id
        public MorphiumId id;
        public String value = "value";
        public String value2 = "value2";
        public int v = 12;
        public Boolean bool = null;
    }
}
