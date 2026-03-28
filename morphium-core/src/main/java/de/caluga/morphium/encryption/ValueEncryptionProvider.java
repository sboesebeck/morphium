package de.caluga.morphium.encryption;

/**
 * Interface for value encryption and decryption providers.
 */
public interface ValueEncryptionProvider {
    /** Sets the encryption key.
     * @param key the key bytes */
    void setEncryptionKey(byte[] key);

    /** Sets the encryption key from a Base64-encoded string.
     * @param key the Base64-encoded key */
    void setEncryptionKeyBase64(String key);

    /** Sets the decryption key.
     * @param key the key bytes */
    void setDecryptionKey(byte[] key);

    /** Sets the decryption key from a Base64-encoded string.
     * @param key the Base64-encoded key */
    void sedDecryptionKeyBase64(String key);

    /** Encrypts the given input bytes.
     * @param input the data to encrypt
     * @return the encrypted data */
    byte[] encrypt(byte[] input);

    /** Decrypts the given input bytes.
     * @param input the data to decrypt
     * @return the decrypted data */
    byte[] decrypt(byte[] input);

}
