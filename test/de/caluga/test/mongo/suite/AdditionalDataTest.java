package de.caluga.test.mongo.suite;

import de.caluga.morphium.driver.WriteConcern;
import de.caluga.test.mongo.suite.data.AdditionalDataEntity;
import de.caluga.test.mongo.suite.data.UncachedObject;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 22.08.12
 * Time: 15:54
 * <p/>
 */
public class AdditionalDataTest extends MorphiumTestBase {

    @Test
    public void additionalData() throws Exception {
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
        d.setAdditionals(additional);
        morphium.store(d);
        Thread.sleep(2000);
        System.out.println("Stored some additional data!");
        AdditionalDataEntity d2 = morphium.findById(AdditionalDataEntity.class, d.getMorphiumId());
        assert (d2.getAdditionals() != null);
        assert (d2.getAdditionals().get("102-92-93").equals(3234));
        assert (((Map) d2.getAdditionals().get("test")).get("tst").equals("tst"));
        assert (d2.getAdditionals().get("_id") == null);

    }

    @Test
    public void additionalDateCompex() throws Exception {
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
        d2.setValue("inner value");
        Map<String, Object> additional2 = new HashMap<>();
        additional2.put("a lot", "of things");
        additional2.put("object2", new UncachedObject());
        AdditionalDataEntity ad3 = new AdditionalDataEntity();
        ad3.setValue("sagich nicht");
        additional2.put("addit2", ad3);

        d2.setAdditionals(additional2);
        additional.put("addit", d2);
        d.setAdditionals(additional);

        String str = morphium.getMapper().serialize(d).toString();
        morphium.store(d);

        log.info(str);
        Thread.sleep(2000);
        d2 = morphium.findById(AdditionalDataEntity.class, d.getMorphiumId());

        assert (d2.getAdditionals() != null);
        assert (d2.getAdditionals().get("object") != null);


    }

    @Test
    public void additionalDataNullTest() throws Exception {
        AdditionalDataEntity d = new AdditionalDataEntity();
        d.setCounter(999);
        d.setAdditionals(null);
        morphium.store(d);
        Thread.sleep(500);
        AdditionalDataEntity d2 = morphium.findById(AdditionalDataEntity.class, d.getMorphiumId());
        assert (d2 != null);
        assert (d2.getAdditionals() == null || d2.getAdditionals().isEmpty());
    }


}
