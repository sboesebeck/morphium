package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import org.junit.Test;

@SuppressWarnings("AssertWithSideEffects")
public class FieldShadowingTest extends MongoTest {

    @Test
    public void shadowFieldTest() throws Exception {
        Shadowed it = new Shadowed();
        it.value = "A test";
        String marshall = MorphiumSingleton.get().getMapper().marshall(it).toString();
        log.info(marshall);
        assert (marshall.contains("A test"));

        assert (MorphiumSingleton.get().getMapper().unmarshall(Shadowed.class, marshall).value != null);

        ReShadowed rs = new ReShadowed();
        rs.value = "A 2nd test";
        marshall = MorphiumSingleton.get().getMapper().marshall(rs).toString();
        assert (marshall.contains("A 2nd test"));

    }


    public static class Shadowed extends UncachedObject {
        private String value;

        public void setValue(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public static class ReShadowed extends Shadowed {
        private String value;

        public ReShadowed() {
            super.setValue("shadowed");

        }

        public String getSuperValue() {
            return super.getValue();
        }

    }


}
