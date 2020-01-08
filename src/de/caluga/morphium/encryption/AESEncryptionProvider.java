package de.caluga.morphium.encryption;

import de.caluga.rsa.AES;

import java.util.Base64;

public class AESEncryptionProvider implements ValueEncryptionProvider {
    private AES aes;

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
