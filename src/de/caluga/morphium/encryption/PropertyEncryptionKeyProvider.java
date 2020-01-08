package de.caluga.morphium.encryption;

import de.caluga.rsa.AES;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertyEncryptionKeyProvider implements EncryptionKeyProvider {
    private Map<String, byte[]> keys = new HashMap<>();

    public PropertyEncryptionKeyProvider() {
    }

    public PropertyEncryptionKeyProvider(Properties p, String prefix, String encryptionKeyForReading, boolean useBase64) {
        readFromProperties(p, prefix, encryptionKeyForReading, useBase64);
    }

    public PropertyEncryptionKeyProvider(Properties p, String prefix, String encryptionKeyForReading) {
        readFromProperties(p, prefix, encryptionKeyForReading, encryptionKeyForReading != null);
    }

    public void readFromProperties(Properties p, String prefix, String encryptionKeyForReading, boolean useBase64) {
        AES a = new AES();
        if (encryptionKeyForReading != null) {
            a.setKey(encryptionKeyForReading);
        }
        int idx = 0;
        if (prefix != null) idx = prefix.length();
        for (String n : p.stringPropertyNames()) {
            if (prefix != null && !n.startsWith(prefix)) {
                continue;
            }
            byte[] v;
            if (useBase64) {
                v = Base64.getDecoder().decode(p.getProperty(n));
            } else {
                v = p.getProperty(n).getBytes();
            }
            if (encryptionKeyForReading != null) {
                v = a.decrypt(v);
            }
            keys.put(n.substring(idx), v);
        }
    }

    @Override
    public void setEncryptionKey(String name, byte[] key) {
        keys.put(name + ".enc", key);
    }

    @Override
    public void setDecryptionKey(String name, byte[] key) {
        keys.put(name + ".dec", key);
    }

    @Override
    public byte[] getEncryptionKey(String name) {
        if (keys.containsKey(name + ".enc")) {
            return keys.get(name + ".enc");
        }
        return keys.get(name);
    }

    @Override
    public byte[] getDecryptionKey(String name) {
        if (keys.containsKey(name + ".dec")) {
            return keys.get(name + ".dec");
        }
        return keys.get(name);
    }
}
