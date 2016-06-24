package de.caluga.test.mongo.suite.bson;/**
 * Created by stephan on 28.10.15.
 */

import de.caluga.morphium.Logger;
import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.bson.BsonDecoder;
import de.caluga.morphium.driver.bson.BsonEncoder;
import de.caluga.morphium.driver.bson.MorphiumId;
import org.junit.Test;

import java.util.*;


/**
 * TODO: Add Documentation here
 **/
public class BsonTest extends BaseTest {

    private static Logger log = new Logger(BsonTest.class);


    @Test
    public void encodeDecodeTest() throws Exception {

        Map<String, Object> doc = new HashMap<>();

        doc.put("_id", new MorphiumId());
        doc.put("counter", 123);
        doc.put("value", "a value");

        Map<String, Object> subDoc = new HashMap<>();
        subDoc.put("some", 1223.2);
        subDoc.put("bool", true);
        subDoc.put("created", new Date());

        doc.put("sub", subDoc);


        byte[] bytes = BsonEncoder.encodeDocument(doc);

        log.error(Utils.getHex(bytes));
        //        System.err.println(Utils.getHex(bytes));

        BsonDecoder dec = new BsonDecoder();
        Map<String, Object> aDoc = dec.decodeDocument(bytes);
        assert (aDoc.equals(doc));
    }


    @Test
    public void mongoIdTest() throws Exception {
        List<MorphiumId> lst = new ArrayList<>();
        log.info("Creating...");
        for (int i = 0; i < 10000; i++) {
            if (i % 1000 == 0) {
                log.info("Created " + i);
            }
            MorphiumId id = new MorphiumId();
            assert (!lst.contains(id));
            lst.add(id);
        }
        log.info("done");
    }


    @Test
    public void encodeTest() throws Exception {
        Map<String, Object> doc = new HashMap<>();

        doc.put("_id", new MorphiumId());
        doc.put("counter", 123);
        doc.put("value", "a value");

        Map<String, Object> subDoc = new HashMap<>();
        subDoc.put("some", 1223.2);
        subDoc.put("bool", true);
        subDoc.put("created", new Date());

        doc.put("sub", subDoc);


        byte[] bytes = BsonEncoder.encodeDocument(doc);

        log.error(Utils.getHex(bytes));
        System.err.println(Utils.getHex(bytes));

    }


}
