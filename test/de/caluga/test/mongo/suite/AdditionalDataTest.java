package de.caluga.test.mongo.suite;

import com.mongodb.WriteConcern;
import de.caluga.morphium.MorphiumSingleton;
import de.caluga.morphium.annotations.AdditionalData;
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
        WriteConcern w = MorphiumSingleton.get().getWriteConcernForClass(AddDat.class);
//        assert(w.getW()>1);
//        assert(w.getWtimeout()>1000);
        System.out.println("W: " + w);
        AddDat d = new AddDat();
        d.setCounter(999);
        Map<String, Object> additional = new HashMap<String, Object>();
        additional.put("102-92-93", new Integer(3234));
        Map<String, String> dat = new HashMap<String, String>();
        dat.put("tst", "tst");
        dat.put("tst2", "tst2");
        additional.put("test", dat);
        d.setAdditionals(additional);
        MorphiumSingleton.get().store(d);
        System.out.println("Stored some additional data!");
        AddDat d2 = MorphiumSingleton.get().findById(AddDat.class, d.getMongoId());
        assert (d2.additionals != null);
        assert (d2.additionals.get("102-92-93").equals(new Integer(3234)));
        assert (((Map) d2.additionals.get("test")).get("tst").equals("tst"));
        assert (d2.additionals.get("_id") == null);

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
