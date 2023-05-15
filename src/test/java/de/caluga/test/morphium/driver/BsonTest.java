package de.caluga.test.morphium.driver;/**
 * Created by stephan on 28.10.15.
 */

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.bson.BsonDecoder;
import de.caluga.morphium.driver.bson.BsonEncoder;
import de.caluga.morphium.driver.bson.UUIDRepresentation;
import de.caluga.test.morphium.driver.connection.BaseTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;


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


    @Test
    public void uuidDecodeTest() throws Exception {
        UUID uuid = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        var d = Doc.of("uuid_value", uuid);
        byte[] ret = BsonEncoder.encodeDocument(d);

        var res = BsonDecoder.decodeDocument(ret);
        assertNotNull(res);
        assertFalse(res.isEmpty());
        assertTrue(res.get("uuid_value") instanceof UUID);
        assertEquals(uuid, res.get("uuid_value"));

        ret = BsonEncoder.encodeDocument(d, UUIDRepresentation.JAVA_LEGACY);
        res = BsonDecoder.decodeDocument(ret);
        assertNotNull(res);
        assertNotNull(res);
        assertFalse(res.isEmpty());
        assertTrue(res.get("uuid_value") instanceof UUID);
        assertEquals(uuid, res.get("uuid_value"));
    }

    @Test
    public void uuidTest() throws Exception {
        //Standard representation
        UUID u = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        BsonEncoder enc = new BsonEncoder();
        enc.encodeObject("test", u);
        log.info("\n" + Utils.getHex(enc.getBytes()));
        assertEquals(Utils.getHex(enc.getBytes()), "00000000:  05 74 65 73 74 00 10 00 00 00 04 00 11 22 33 44     .test-.---.-...D\n" +
                "00000010:  55 66 77 88 99 AA BB CC DD EE FF                    Ufw........\n");
        //LEGACY_JAVA
        enc = new BsonEncoder();
        enc.setUuidRepresentation(UUIDRepresentation.JAVA_LEGACY);
        enc.encodeObject("test", u);
        log.info("\n" + Utils.getHex(enc.getBytes()));
        assertEquals(Utils.getHex(enc.getBytes()), "00000000:  05 74 65 73 74 00 10 00 00 00 03 77 66 55 44 33     .test-.---.wfUD.\n" +
                "00000010:  22 11 00 FF EE DD CC BB AA 99 88                    ..-........\n");

        //PYTHON_LEGACY
        enc = new BsonEncoder();
        enc.setUuidRepresentation(UUIDRepresentation.PYTHON_LEGACY);
        enc.encodeObject("test", u);
        log.info("\n" + Utils.getHex(enc.getBytes()));
        assertEquals(Utils.getHex(enc.getBytes()), "00000000:  05 74 65 73 74 00 10 00 00 00 03 00 11 22 33 44     .test-.---.-...D\n" +
                "00000010:  55 66 77 88 99 AA BB CC DD EE FF                    Ufw........\n");

        //CSHarp
        enc = new BsonEncoder();
        enc.setUuidRepresentation(UUIDRepresentation.C_SHARP_LEGACY);
        enc.encodeObject("test", u);
        log.info("\n" + Utils.getHex(enc.getBytes()));
        assertEquals(Utils.getHex(enc.getBytes()), "00000000:  05 74 65 73 74 00 10 00 00 00 03 33 22 11 00 55     .test-.---....-U\n" +
                "00000010:  44 77 66 88 99 AA BB CC DD EE FF                    Dwf........\n");


    }

}
