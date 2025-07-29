package de.caluga.morphium.config;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Transient;
import de.caluga.morphium.encryption.AESEncryptionProvider;
import de.caluga.morphium.encryption.DefaultEncryptionKeyProvider;
import de.caluga.morphium.encryption.EncryptionKeyProvider;
import de.caluga.morphium.encryption.ValueEncryptionProvider;

@Embedded
public class EncryptionSettings {
    @Transient
    private Class<? extends EncryptionKeyProvider> encryptionKeyProviderClass = DefaultEncryptionKeyProvider.class;
    @Transient
    private Class<? extends ValueEncryptionProvider> valueEncryptionProviderClass = AESEncryptionProvider.class;
    /**
     * login credentials for MongoDB - if necessary. If null, don't authenticate
     */
    @Transient
    private String credentialsEncryptionKey;
    @Transient
    private String credentialsDecryptionKey;
    private Boolean credentialsEncrypted;

}
