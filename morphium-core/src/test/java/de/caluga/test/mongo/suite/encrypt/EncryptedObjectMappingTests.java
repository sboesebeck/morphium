package de.caluga.test.mongo.suite.encrypt;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.encryption.Encrypted;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.encryption.AESEncryptionProvider;
import de.caluga.morphium.objectmapping.MorphiumObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import de.caluga.morphium.Morphium;

@Tag("encryption")
public class EncryptedObjectMappingTests extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstancesNoSingle")
    public void objectMapperTest(Morphium morphium) throws Exception  {
        morphium.getEncryptionKeyProvider().setEncryptionKey("key", "1234567890abcdef".getBytes());
        morphium.getEncryptionKeyProvider().setDecryptionKey("key", "1234567890abcdef".getBytes());
        MorphiumObjectMapper om = morphium.getMapper();
        EncryptedEntity ent = new EncryptedEntity();
        ent.enc = "Text to be encrypted";
        ent.text = "plain text";
        ent.intValue = 42;
        ent.floatValue = 42.3f;
        ent.listOfStrings = new ArrayList<>();
        ent.listOfStrings.add("Test1");
        ent.listOfStrings.add("Test2");
        ent.listOfStrings.add("Test3");

        ent.sub = new Subdoc();
        ent.sub.intVal = 42;
        ent.sub.strVal = "42";
        ent.sub.name = "name of the document";

        Map<String, Object> serialized = om.serialize(ent);
        assert (!ent.enc.equals(serialized.get("enc")));

        EncryptedEntity deserialized = om.deserialize(EncryptedEntity.class, serialized);
        assert (deserialized.enc.equals(ent.enc));
        assert (ent.intValue.equals(deserialized.intValue));
        assert (ent.floatValue.equals(deserialized.floatValue));
        assert (ent.listOfStrings.equals(deserialized.listOfStrings));
    }


    @Entity
    public static class EncryptedEntity {
        @Id
        public MorphiumId id;

        @Encrypted(provider = AESEncryptionProvider.class, keyName = "key")
        public String enc;

        @Encrypted(provider = AESEncryptionProvider.class, keyName = "key")
        public Integer intValue;

        @Encrypted(provider = AESEncryptionProvider.class, keyName = "key")
        public Float floatValue;

        @Encrypted(provider = AESEncryptionProvider.class, keyName = "key")
        public List<String> listOfStrings;

        @Encrypted(provider = AESEncryptionProvider.class, keyName = "key")
        public Subdoc sub;


        public String text;
    }


    @Embedded
    public static class Subdoc {
        public String name;
        public String strVal;
        public Integer intVal;
    }
}
