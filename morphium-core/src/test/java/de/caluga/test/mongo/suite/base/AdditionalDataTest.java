package de.caluga.test.mongo.suite.base;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.test.mongo.suite.data.AdditionalDataEntity;
import de.caluga.test.mongo.suite.data.UncachedObject;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * User: Stephan Bösebeck
 * Date: 22.08.12
 * Time: 15:54
 * <p>
 */
@Tag("core")
public class AdditionalDataTest extends MultiDriverTestBase {


    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void additionalData(Morphium morphium) throws Exception {
        try (morphium) {
            WriteConcern w = morphium.getWriteConcernForClass(AdditionalDataEntity.class);
            System.out.println("W: " + w);
            AdditionalDataEntity d = new AdditionalDataEntity();
            d.setCounter(999);
            Map<String, Object> additional = new HashMap<>();
            additional.put("102-92-93", 3234);
            Map<String, String> dat = new HashMap<>();
            dat.put("tst", "tst");
            dat.put("tst2", "tst2");
            additional.put("test", dat);
            d.setAdditionals(additional);
            morphium.store(d);
            System.out.println("Stored some additional data!");
            AdditionalDataEntity d2 = TestUtils.waitForObject( () -> morphium.findById(AdditionalDataEntity.class, d.getMorphiumId()));
            assertNotNull(d2.getAdditionals());
            assert (d2.getAdditionals().get("102-92-93").equals(3234));
            assert (((Map) d2.getAdditionals().get("test")).get("tst").equals("tst"));
            assert (d2.getAdditionals().get("_id") == null);
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void additionalDataCompex(Morphium morphium) throws Exception {
        try (morphium) {
            morphium.dropCollection(AdditionalDataEntity.class);
            WriteConcern w = morphium.getWriteConcernForClass(AdditionalDataEntity.class);
            System.out.println("W: " + w);
            AdditionalDataEntity d = new AdditionalDataEntity();
            d.setCounter(999);
            Map<String, Object> additional = new HashMap<>();
            additional.put("102-92-93", 3234);
            Map<String, String> dat = new HashMap<>();
            dat.put("tst", "tst");
            dat.put("tst2", "tst2");
            additional.put("test", dat);
            additional.put("object", new UncachedObject());
            AdditionalDataEntity d2 = new AdditionalDataEntity();
            d2.setStrValue("inner value");
            Map<String, Object> additional2 = new HashMap<>();
            additional2.put("a lot", "of things");
            additional2.put("object2", new UncachedObject());
            AdditionalDataEntity ad3 = new AdditionalDataEntity();
            ad3.setStrValue("sagich nicht");
            additional2.put("addit2", ad3);

            d2.setAdditionals(additional2);
            additional.put("addit", d2);
            d.setAdditionals(additional);

            String str = morphium.getMapper().serialize(d).toString();
            morphium.store(d);

            log.info(str);
            d2 = TestUtils.waitForObject( () -> morphium.findById(AdditionalDataEntity.class, d.getMorphiumId()));

            assertNotNull(d2.getAdditionals());
            assertNotNull(d2.getAdditionals().get("object"));

        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void additionalDataNullTest(Morphium morphium) throws Exception {
        try (morphium) {
            AdditionalDataEntity d = new AdditionalDataEntity();
            d.setCounter(999);
            d.setAdditionals(null);
            morphium.store(d);
            AdditionalDataEntity d2 = TestUtils.waitForObject( () -> morphium.findById(AdditionalDataEntity.class, d.getMorphiumId()));
            assertNotNull(d2);
            assert (d2.getAdditionals() == null || d2.getAdditionals().isEmpty());
        }


    }
}
