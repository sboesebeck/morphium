package de.caluga.morphium.encryption;

import de.caluga.rsa.RSA;

import java.util.Base64;

public class RSAEncryptionProvider implements ValueEncryptionProvider {

    private byte[] encKey;
    private byte[] decKey;

    @Override
    public void setEncryptionKey(byte[] key) {
        encKey = key;
    }

    @Override
    public void setEncryptionKeyBase64(String key) {
        encKey = Base64.getDecoder().decode(key);
    }

    @Override
    public void setDecryptionKey(byte[] key) {
        decKey = key;
    }

    @Override
    public void sedDecryptionKeyBase64(String key) {
        decKey = Base64.getDecoder().decode(key);
    }

    @Override
    public byte[] encrypt(byte[] input) {
        RSA rsa = new RSA();
        rsa.setPublicKeyBytes(encKey);
        return rsa.encrypt(input);
    }

    @Override
    public byte[] decrypt(byte[] input) {
        RSA rsa = new RSA();
        rsa.setPrivateKeyBytes(decKey);
        return rsa.decrypt(input);
    }
}
