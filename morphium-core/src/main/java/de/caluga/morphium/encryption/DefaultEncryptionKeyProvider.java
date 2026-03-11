package de.caluga.morphium.encryption;

import java.util.HashMap;

public class DefaultEncryptionKeyProvider implements EncryptionKeyProvider {
    @SuppressWarnings({"CanBeFinal", "FieldMayBeFinal"})
    private HashMap<String, byte[]> encKeys = new HashMap<>();
    @SuppressWarnings({"CanBeFinal", "FieldMayBeFinal"})
    private HashMap<String, byte[]> decKeys = new HashMap<>();

    @Override
    public void setEncryptionKey(String name, byte[] key) {
        encKeys.put(name, key);
    }

    @Override
    public void setDecryptionKey(String name, byte[] key) {
        decKeys.put(name, key);
    }

    @Override
    public byte[] getEncryptionKey(String name) {
        return encKeys.get(name);
    }

    @Override
    public byte[] getDecryptionKey(String name) {
        return decKeys.get(name);
    }
}
