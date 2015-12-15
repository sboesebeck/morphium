package de.caluga.test.mongo.suite;

import de.caluga.morphium.Utils;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

@SuppressWarnings("AssertWithSideEffects")
public class FieldShadowingTest extends MongoTest {

    @Test
    public void shadowFieldTest() throws Exception {
        Shadowed it = new Shadowed();
        it.value = "A test";
        String marshall = Utils.toJsonString(morphium.getMapper().marshall(it));
        log.info(marshall);
        assert (marshall.contains("A test"));

        assert (morphium.getMapper().unmarshall(Shadowed.class, marshall).value != null);
        assert (morphium.getMapper().unmarshall(Shadowed.class, marshall).value.equals("A test"));

        ReShadowed rs = new ReShadowed();
        rs.value = "A 2nd test";
        marshall = Utils.toJsonString(morphium.getMapper().marshall(rs));
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
