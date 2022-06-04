package de.caluga.test.morphium.driver.bson;/**
 * Created by stephan on 28.10.15.
 */

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.bson.BsonDecoder;
import de.caluga.morphium.driver.bson.BsonEncoder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * TODO: Add Documentation here
 **/
public class BsonTest extends BaseTest {

    private static Logger log = LoggerFactory.getLogger(BsonTest.class);


    @Test
    public void encodeDecodeTest() throws Exception {

        Doc doc = Doc.of();

        doc.put("_id", new MorphiumId());
        doc.put("counter", 123);
        doc.put("value", "a value");

        Doc subDoc = Doc.of();
        subDoc.put("some", 1223.2);
        subDoc.put("bool", true);
        subDoc.put("created", new Date());

        doc.put("sub", subDoc);


        byte[] bytes = BsonEncoder.encodeDocument(doc);

        log.error(Utils.getHex(bytes));
        //        System.err.println(Utils.getHex(bytes));

        BsonDecoder dec = new BsonDecoder();
        Doc aDoc = dec.decodeDocument(bytes);
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
        Doc doc = Doc.of();

        doc.put("_id", new MorphiumId());
        doc.put("counter", 123);
        doc.put("value", "a value");

        Doc subDoc = Doc.of();
        subDoc.put("some", 1223.2);
        subDoc.put("bool", true);
        subDoc.put("created", new Date());

        doc.put("sub", subDoc);


        byte[] bytes = BsonEncoder.encodeDocument(doc);

        log.error(Utils.getHex(bytes));
        System.err.println(Utils.getHex(bytes));

    }


}
