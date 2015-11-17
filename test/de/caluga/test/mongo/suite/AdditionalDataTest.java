package de.caluga.test.mongo.suite;

import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.AdditionalData;
import de.caluga.morphium.driver.WriteConcern;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 22.08.12
 * Time: 15:54
 * <p/>
 */
public class AdditionalDataTest extends MongoTest {

    @Test
    public void additionalData() throws Exception {
        MorphiumSingleton.get().dropCollection(AddDat.class);
        WriteConcern w = MorphiumSingleton.get().getWriteConcernForClass(AddDat.class);
        System.out.println("W: " + w);
        AddDat d = new AddDat();
        d.setCounter(999);
        Map<String, Object> additional = new HashMap<>();
        additional.put("102-92-93", 3234);
        Map<String, String> dat = new HashMap<>();
        dat.put("tst", "tst");
        dat.put("tst2", "tst2");
        additional.put("test", dat);
        d.setAdditionals(additional);
        MorphiumSingleton.get().store(d);
        System.out.println("Stored some additional data!");
        AddDat d2 = MorphiumSingleton.get().findById(AddDat.class, d.getMorphiumId());
        assert (d2.additionals != null);
        assert (d2.additionals.get("102-92-93").equals(3234));
        assert (((Map) d2.additionals.get("test")).get("tst").equals("tst"));
        assert (d2.additionals.get("_id") == null);

    }

    @Test
    public void additionalDateCompex() throws Exception {
        MorphiumSingleton.get().dropCollection(AddDat.class);
        WriteConcern w = MorphiumSingleton.get().getWriteConcernForClass(AddDat.class);
        System.out.println("W: " + w);
        AddDat d = new AddDat();
        d.setCounter(999);
        Map<String, Object> additional = new HashMap<>();
        additional.put("102-92-93", 3234);
        Map<String, String> dat = new HashMap<>();
        dat.put("tst", "tst");
        dat.put("tst2", "tst2");
        additional.put("test", dat);
        additional.put("object", new UncachedObject());
        AddDat d2 = new AddDat();
        d2.setValue("inner value");
        Map<String, Object> additional2 = new HashMap<>();
        additional2.put("a lot", "of things");
        additional2.put("object2", new UncachedObject());
        AddDat ad3 = new AddDat();
        ad3.setValue("sagich nicht");
        additional2.put("addit2", ad3);

        d2.setAdditionals(additional2);
        additional.put("addit", d2);
        d.setAdditionals(additional);

        String str = MorphiumSingleton.get().getMapper().marshall(d).toString();
        MorphiumSingleton.get().store(d);

        log.info(str);
        Thread.sleep(1000);
        d2 = MorphiumSingleton.get().findById(AddDat.class, d.getMorphiumId());

        assert (d2.additionals != null);
        assert (d2.additionals.get("object") != null);


    }

    @Test
    public void additionalDataNullTest() throws Exception {
        AddDat d = new AddDat();
        d.setCounter(999);
        d.setAdditionals(null);
        MorphiumSingleton.get().store(d);

    }

    public static class AddDat extends UncachedObject {
        @AdditionalData(readOnly = false)
        Map<String, Object> additionals;

        public Map<String, Object> getAdditionals() {
            return additionals;
        }

        public void setAdditionals(Map<String, Object> additionals) {
            this.additionals = additionals;
        }
    }


}
