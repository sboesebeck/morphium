package de.caluga.test.mongo.suite.encrypt;

import de.caluga.morphium.encryption.AESEncryptionProvider;
import de.caluga.morphium.encryption.PropertyEncryptionKeyProvider;
import de.caluga.morphium.encryption.RSAEncryptionProvider;
import de.caluga.rsa.AES;
import de.caluga.rsa.RSA;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;

@Tag("encryption")
public class EncryptionTest {

    @Test
    public void propertyKeyProviderTest() {
        PropertyEncryptionKeyProvider encProvider = new PropertyEncryptionKeyProvider();

        Properties p = new Properties();
        p.setProperty("key1", "1234");
        p.setProperty("key2.enc", "12345");
        p.setProperty("key2.dec", "123456");

        encProvider.readFromProperties(p, null, null, false);
        byte[] ek = encProvider.getDecryptionKey("key1");
        assert (Arrays.equals(ek, p.getProperty("key1").getBytes()));
        ek = encProvider.getEncryptionKey("key1");
        assert (Arrays.equals(ek, p.getProperty("key1").getBytes()));

        ek = encProvider.getEncryptionKey("key2");
        assert (Arrays.equals(ek, p.getProperty("key2.enc").getBytes()));

        ek = encProvider.getDecryptionKey("key2");
        assert (Arrays.equals(ek, p.getProperty("key2.dec").getBytes()));
    }

    @Test
    public void propertyKeyProviderEncryptedTest() {
        PropertyEncryptionKeyProvider encProvider = new PropertyEncryptionKeyProvider();
        String encryptionKey = "1234567890abcdef";
        AES a = new AES();
        a.setEncryptionKey(encryptionKey.getBytes());

        Properties p = new Properties();
        p.setProperty("key1", Base64.getEncoder().encodeToString(a.encrypt("12345".getBytes())));
        p.setProperty("key2.enc", Base64.getEncoder().encodeToString(a.encrypt("12345")));
        p.setProperty("key2.dec", Base64.getEncoder().encodeToString(a.encrypt("123456")));

        encProvider.readFromProperties(p, null, encryptionKey, true);
        byte[] ek = encProvider.getDecryptionKey("key1");
        assert (Arrays.equals(ek, "12345".getBytes()));
        ek = encProvider.getEncryptionKey("key1");
        assert (Arrays.equals(ek, "12345".getBytes()));

        ek = encProvider.getEncryptionKey("key2");
        assert (Arrays.equals(ek, "12345".getBytes()));

        ek = encProvider.getDecryptionKey("key2");
        assert (Arrays.equals(ek, "123456".getBytes()));
    }


    @Test
    public void aesEncryptionProviderTest() {
        AESEncryptionProvider aes = new AESEncryptionProvider();
        aes.setEncryptionKey("1234567890abcdef".getBytes());

        String original = "This is the text";

        byte[] encrypted = aes.encrypt(original.getBytes());

        byte[] decrypted = aes.decrypt(encrypted);
        assert (Arrays.equals(original.getBytes(), decrypted));
    }

    @Test
    public void rsaEncryptionProviderTest() {
        RSA rsa = new RSA(512); //generates key...
        byte[] privateKey = rsa.getPrivateKeyBytes();
        byte[] publicKey = rsa.getPublicKeyBytes();

        RSAEncryptionProvider provider = new RSAEncryptionProvider();
        provider.setEncryptionKey(publicKey);
        provider.setDecryptionKey(privateKey);

        String originalData = "this is a test";
        byte[] enc = provider.encrypt(originalData.getBytes());
        byte[] dec = provider.decrypt(enc);

        assert (Arrays.equals(dec, originalData.getBytes()));


    }
}
