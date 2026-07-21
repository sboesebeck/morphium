package de.caluga.morphium.driver.inmem.auth;

import com.ongres.scram.common.stringprep.StringPreparations;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * SCRAM (RFC 5802) credential material for one mechanism, derived and stored
 * the way mongod does it: only iterationCount/salt/storedKey/serverKey are
 * persisted - never the password or the salted password - so a stolen
 * system.users dump does not allow client impersonation.
 */
public final class ScramCredentials {

    public enum Mechanism {
        SCRAM_SHA_1("SCRAM-SHA-1", "SHA-1", "HmacSHA1", "PBKDF2WithHmacSHA1"),
        SCRAM_SHA_256("SCRAM-SHA-256", "SHA-256", "HmacSHA256", "PBKDF2WithHmacSHA256");

        private final String mechanismName;
        private final String digestAlgorithm;
        private final String hmacAlgorithm;
        private final String pbkdf2Algorithm;

        Mechanism(String mechanismName, String digestAlgorithm, String hmacAlgorithm, String pbkdf2Algorithm) {
            this.mechanismName = mechanismName;
            this.digestAlgorithm = digestAlgorithm;
            this.hmacAlgorithm = hmacAlgorithm;
            this.pbkdf2Algorithm = pbkdf2Algorithm;
        }

        public String mechanismName() {
            return mechanismName;
        }

        public static Mechanism fromName(String name) {
            for (Mechanism m : values()) {
                if (m.mechanismName.equals(name)) {
                    return m;
                }
            }
            throw new IllegalArgumentException("Unsupported SCRAM mechanism: " + name);
        }
    }

    private final Mechanism mechanism;
    private final int iterationCount;
    private final byte[] salt;
    private final byte[] storedKey;
    private final byte[] serverKey;

    private ScramCredentials(Mechanism mechanism, int iterationCount, byte[] salt, byte[] storedKey,
                             byte[] serverKey) {
        this.mechanism = mechanism;
        this.iterationCount = iterationCount;
        this.salt = salt;
        this.storedKey = storedKey;
        this.serverKey = serverKey;
    }

    /**
     * Derives storedKey/serverKey per RFC 5802. MongoDB compatibility detail:
     * for SCRAM-SHA-1 the "password" fed into PBKDF2 is md5Hex(user + ":mongo:" +
     * password) - a leftover from MONGODB-CR - while SCRAM-SHA-256 uses the
     * SASLprep'd password (RFC 7677; identity for ASCII passwords). The same
     * preparation is used by SaslAuthCommand on the client side, so both ends agree.
     */
    public static ScramCredentials derive(Mechanism mechanism, String user, String rawPassword, byte[] salt,
                                          int iterationCount) {
        Objects.requireNonNull(mechanism, "mechanism");
        try {
            String password = mechanism == Mechanism.SCRAM_SHA_1
                              ? md5Hex(user + ":mongo:" + rawPassword)
                              : StringPreparations.SASL_PREPARATION.normalize(rawPassword);
            return deriveRaw(mechanism, password, salt, iterationCount);
        } catch (IllegalStateException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            // MD5 is a mandatory JCE algorithm - failure here is a broken JVM
            throw new IllegalStateException("SCRAM key derivation failed for " + mechanism.mechanismName(), e);
        }
    }

    /**
     * Derives from the password exactly as it enters the SCRAM algorithm (RFC 5802
     * section 3), skipping the mongo-specific mangling above - needed for the RFC
     * test vectors and for callers that already hold the prepared password.
     */
    public static ScramCredentials deriveRaw(Mechanism mechanism, String scramPassword, byte[] salt,
                                             int iterationCount) {
        Objects.requireNonNull(mechanism, "mechanism");
        Objects.requireNonNull(salt, "salt");
        try {
            byte[] saltedPassword = pbkdf2(mechanism, scramPassword, salt, iterationCount);
            byte[] clientKey = hmac(mechanism, saltedPassword, "Client Key");
            byte[] storedKey = MessageDigest.getInstance(mechanism.digestAlgorithm).digest(clientKey);
            byte[] serverKey = hmac(mechanism, saltedPassword, "Server Key");
            return new ScramCredentials(mechanism, iterationCount, salt.clone(), storedKey, serverKey);
        } catch (Exception e) {
            // SHA-1/SHA-256/PBKDF2 are mandatory JCE algorithms - failure here is a broken JVM
            throw new IllegalStateException("SCRAM key derivation failed for " + mechanism.mechanismName(), e);
        }
    }

    /**
     * The persisted shape inside admin.system.users: base64 strings, not
     * byte[] - the wire layer serializes this map as-is and mongod stores
     * strings here, so BinData would break compatibility.
     */
    public Map<String, Object> toStoredDocument() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("iterationCount", iterationCount);
        doc.put("salt", Base64.getEncoder().encodeToString(salt));
        doc.put("storedKey", Base64.getEncoder().encodeToString(storedKey));
        doc.put("serverKey", Base64.getEncoder().encodeToString(serverKey));
        return doc;
    }

    public static ScramCredentials fromStoredDocument(Mechanism mechanism, Map<String, Object> doc) {
        Objects.requireNonNull(mechanism, "mechanism");
        Objects.requireNonNull(doc, "doc");
        int iterations = ((Number) doc.get("iterationCount")).intValue();
        byte[] salt = Base64.getDecoder().decode((String) doc.get("salt"));
        byte[] storedKey = Base64.getDecoder().decode((String) doc.get("storedKey"));
        byte[] serverKey = Base64.getDecoder().decode((String) doc.get("serverKey"));
        return new ScramCredentials(mechanism, iterations, salt, storedKey, serverKey);
    }

    public Mechanism getMechanism() {
        return mechanism;
    }

    public int getIterationCount() {
        return iterationCount;
    }

    public byte[] getSalt() {
        return salt.clone();
    }

    public byte[] getStoredKey() {
        return storedKey.clone();
    }

    public byte[] getServerKey() {
        return serverKey.clone();
    }

    private static byte[] pbkdf2(Mechanism mechanism, String password, byte[] salt, int iterationCount)
    throws Exception {
        int keyLengthBits = MessageDigest.getInstance(mechanism.digestAlgorithm).getDigestLength() * 8;
        PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterationCount, keyLengthBits);
        try {
            return SecretKeyFactory.getInstance(mechanism.pbkdf2Algorithm).generateSecret(spec).getEncoded();
        } finally {
            spec.clearPassword();
        }
    }

    private static byte[] hmac(Mechanism mechanism, byte[] key, String data) throws Exception {
        Mac mac = Mac.getInstance(mechanism.hmacAlgorithm);
        mac.init(new SecretKeySpec(key, mechanism.hmacAlgorithm));
        return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    }

    private static String md5Hex(String s) throws NoSuchAlgorithmException {
        byte[] digest = MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder(digest.length * 2);
        for (byte b : digest) {
            sb.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ScramCredentials other)) {
            return false;
        }
        return iterationCount == other.iterationCount && mechanism == other.mechanism
               && Arrays.equals(salt, other.salt) && Arrays.equals(storedKey, other.storedKey)
               && Arrays.equals(serverKey, other.serverKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mechanism, iterationCount, Arrays.hashCode(salt), Arrays.hashCode(storedKey),
                Arrays.hashCode(serverKey));
    }
}
