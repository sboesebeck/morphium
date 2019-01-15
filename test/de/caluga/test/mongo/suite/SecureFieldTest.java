package de.caluga.test.mongo.suite;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.Property;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.security.Encryption;
import org.junit.Test;

import java.util.Map;

public class SecureFieldTest extends MongoTest{

    @Test
    public void encryptedFieldTest() throws Exception {
        EncObject e=new EncObject();
        e.id=new MorphiumId();
        e.text="A text";
        Map<String, Object> map = morphium.getMapper().serialize(e);
        assert(!(map.get("text") instanceof String));

        EncObject e2=morphium.getMapper().deserialize(EncObject.class,map);
        assert(e2!=null);
    }


    @Entity
    public static class EncObject {
        @Id
        MorphiumId id;

        @Property(encryption = Encryption.AES256,passphrase = "pass")
        String text;


    }
}
