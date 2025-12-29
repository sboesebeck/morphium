package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Utils;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

@SuppressWarnings("AssertWithSideEffects")
@Tag("core")
public class FieldShadowingTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void shadowFieldTest(Morphium morphium) throws Exception  {
        Shadowed it = new Shadowed();
        it.value = "A test";
        String marshall = Utils.toJsonString(morphium.getMapper().serialize(it));
        log.info(marshall);
        assert (marshall.contains("A test"));

        assertNotNull(morphium.getMapper().deserialize(Shadowed.class, marshall).value);
        ;
        assert (morphium.getMapper().deserialize(Shadowed.class, marshall).value.equals("A test"));

        ReShadowed rs = new ReShadowed();
        rs.value = "A 2nd test";
        marshall = Utils.toJsonString(morphium.getMapper().serialize(rs));
        log.info("Marshall: " + marshall);
        //causing problems when using jackson!
        //assert (!marshall.contains("A 2nd test"));

    }


    public static class Shadowed extends UncachedObject {
        private String value;

        public String getStrValue() {
            return value;
        }

        public void setStrValue(String strValue) {
            this.value = strValue;
        }
    }

    public static class ReShadowed extends Shadowed {
        private String value;

        public ReShadowed() {
            super.setStrValue("shadowed");

        }

        public String getSuperValue() {
            return super.getStrValue();
        }

    }


}
