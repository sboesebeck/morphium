package de.caluga.morphium.encryption;

public interface ValueEncryptionProvider {
    void setEncryptionKey(byte[] key);

    void setEncryptionKeyBase64(String key);

    void setDecryptionKey(byte[] key);

    void sedDecryptionKeyBase64(String key);

    byte[] encrypt(byte[] input);

    byte[] decrypt(byte[] input);

}
