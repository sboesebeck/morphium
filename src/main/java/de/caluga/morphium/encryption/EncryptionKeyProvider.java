package de.caluga.morphium.encryption;

public interface EncryptionKeyProvider {
    void setEncryptionKey(String name, byte[] key);

    void setDecryptionKey(String name, byte[] key);

    byte[] getEncryptionKey(String name);

    byte[] getDecryptionKey(String name);

}
