package de.caluga.morphium.encryption;

import de.caluga.rsa.AES;

import java.util.Base64;

/**
 * AES-based implementation of {@link ValueEncryptionProvider}.
 */
public class AESEncryptionProvider implements ValueEncryptionProvider {
    @SuppressWarnings({"CanBeFinal", "FieldMayBeFinal"})
    private AES aes;

    /** Creates a new AESEncryptionProvider with a default AES instance. */
    public AESEncryptionProvider() {
        aes = new AES();
    }

    @Override
    public void setEncryptionKey(byte[] key) {
        aes.setEncryptionKey(key);
    }

    @Override
    public void setEncryptionKeyBase64(String key) {
        setEncryptionKey(Base64.getDecoder().decode(key));
    }

    @Override
    public void setDecryptionKey(byte[] key) {
        setEncryptionKey(key);
    }

    @Override
    public void sedDecryptionKeyBase64(String key) {
        setEncryptionKeyBase64(key);
    }

    @Override
    public byte[] encrypt(byte[] input) {
        return aes.encrypt(input);
    }

    @Override
    public byte[] decrypt(byte[] input) {
        return aes.decrypt(input);
    }
}
