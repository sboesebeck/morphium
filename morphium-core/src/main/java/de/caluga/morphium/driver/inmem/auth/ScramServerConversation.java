package de.caluga.morphium.driver.inmem.auth;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Server side of one SCRAM authentication attempt (RFC 5802 / RFC 7677), the counterpart to
 * SaslAuthCommand's ongres ScramClient. One instance per saslStart conversation - the nonce
 * binds both messages together, so instances must never be reused or shared.
 *
 * The caller looks up the user's credentials via {@link #extractUser(String)} BEFORE
 * constructing the conversation; this class deliberately receives them up front so the
 * crypto core stays free of any user-store dependency.
 */
public final class ScramServerConversation {

    /** Any client-caused failure: malformed message, nonce mismatch, bad proof. */
    public static class AuthenticationFailedException extends Exception {
        public AuthenticationFailedException(String message) {
            super(message);
        }
    }

    private enum State { EXPECT_CLIENT_FIRST, EXPECT_CLIENT_FINAL, COMPLETE, FAILED }

    // base64 of "n,," - the client-final channel-binding attribute must echo the gs2 header
    private static final String GS2_NO_CHANNEL_BINDING_B64 = "biws";
    private static final SecureRandom RANDOM = new SecureRandom();

    private final ScramCredentials credentials;
    private State state = State.EXPECT_CLIENT_FIRST;
    private String user;
    private String clientFirstBare;
    private String serverFirstMessage;
    private String combinedNonce;
    private String serverNonceSuffix;

    public ScramServerConversation(ScramCredentials credentials) {
        this.credentials = credentials;
    }

    /** For tests only: a fixed suffix makes the server-first message reproducible (RFC vectors). */
    ScramServerConversation withServerNonceSuffix(String suffix) {
        this.serverNonceSuffix = suffix;
        return this;
    }

    /**
     * Consumes "n,,n=&lt;user&gt;,r=&lt;clientNonce&gt;" and produces
     * "r=&lt;combinedNonce&gt;,s=&lt;base64 salt&gt;,i=&lt;iterations&gt;". The server nonce
     * extends the client's - the client verifies its own prefix survived, which prevents an
     * attacker from splicing two conversations together.
     */
    public String handleClientFirst(String clientFirstMessage) throws AuthenticationFailedException {
        if (state != State.EXPECT_CLIENT_FIRST) {
            throw new IllegalStateException("handleClientFirst called in state " + state);
        }
        state = State.FAILED; // only a fully parsed message moves the state forward
        String[] parsed = parseClientFirst(clientFirstMessage);
        user = parsed[0];
        String clientNonce = parsed[1];
        clientFirstBare = parsed[2];

        String suffix = serverNonceSuffix != null ? serverNonceSuffix : generateNonceSuffix();
        combinedNonce = clientNonce + suffix;
        serverFirstMessage = "r=" + combinedNonce
                + ",s=" + Base64.getEncoder().encodeToString(credentials.getSalt())
                + ",i=" + credentials.getIterationCount();
        state = State.EXPECT_CLIENT_FINAL;
        return serverFirstMessage;
    }

    /**
     * Consumes "c=biws,r=&lt;nonce&gt;,p=&lt;base64 proof&gt;" and produces
     * "v=&lt;base64 serverSignature&gt;". The proof only proves knowledge of clientKey when
     * H(proof XOR HMAC(storedKey, authMessage)) matches storedKey - constant-time compare,
     * so a byte-wise timing oracle cannot leak key material.
     */
    public String handleClientFinal(String clientFinalMessage) throws AuthenticationFailedException {
        if (state != State.EXPECT_CLIENT_FINAL) {
            throw new IllegalStateException("handleClientFinal called in state " + state);
        }
        state = State.FAILED; // stays failed unless the proof verifies
        if (clientFinalMessage == null) {
            throw new AuthenticationFailedException("empty client-final message");
        }
        // p must be the last attribute; the proof's base64 may contain ',' never, but split on
        // the attribute marker instead of blind indexOf(',') to keep extensions before p intact
        int proofIdx = clientFinalMessage.lastIndexOf(",p=");
        if (proofIdx < 0) {
            throw new AuthenticationFailedException("client-final message has no proof");
        }
        String withoutProof = clientFinalMessage.substring(0, proofIdx);
        String proofB64 = clientFinalMessage.substring(proofIdx + 3);

        String[] attrs = withoutProof.split(",");
        if (attrs.length < 2 || !attrs[0].startsWith("c=") || !attrs[1].startsWith("r=")) {
            throw new AuthenticationFailedException("malformed client-final message");
        }
        if (!GS2_NO_CHANNEL_BINDING_B64.equals(attrs[0].substring(2))) {
            throw new AuthenticationFailedException("channel binding mismatch");
        }
        // full combined nonce required - a replayed client-final from another conversation
        // carries that conversation's server suffix and must be rejected
        if (!combinedNonce.equals(attrs[1].substring(2))) {
            throw new AuthenticationFailedException("nonce mismatch");
        }

        byte[] proof;
        try {
            proof = Base64.getDecoder().decode(proofB64);
        } catch (IllegalArgumentException e) {
            throw new AuthenticationFailedException("proof is not valid base64");
        }

        String authMessage = clientFirstBare + "," + serverFirstMessage + "," + withoutProof;
        byte[] clientSignature = hmac(credentials.getStoredKey(), authMessage);
        if (proof.length != clientSignature.length) {
            throw new AuthenticationFailedException("authentication failed");
        }
        byte[] clientKey = new byte[proof.length];
        for (int i = 0; i < proof.length; i++) {
            clientKey[i] = (byte) (proof[i] ^ clientSignature[i]);
        }
        if (!MessageDigest.isEqual(digest(clientKey), credentials.getStoredKey())) {
            throw new AuthenticationFailedException("authentication failed");
        }

        state = State.COMPLETE;
        return "v=" + Base64.getEncoder().encodeToString(hmac(credentials.getServerKey(), authMessage));
    }

    public boolean isComplete() {
        return state == State.COMPLETE;
    }

    /** The n= value from client-first, RFC-unescaped; null before handleClientFirst. */
    public String getUser() {
        return user;
    }

    /**
     * Pulls the username out of a client-first message without any credentials - the driver
     * needs it to look up the user document before a conversation can be constructed.
     */
    public static String extractUser(String clientFirstMessage) throws AuthenticationFailedException {
        return parseClientFirst(clientFirstMessage)[0];
    }

    // returns {user, clientNonce, clientFirstBare}
    private static String[] parseClientFirst(String msg) throws AuthenticationFailedException {
        if (msg == null || !msg.startsWith("n,")) {
            // "p=..." would request channel binding, "y," claims CB support - neither is
            // possible without TLS channel data, and mongo clients always send "n,,"
            throw new AuthenticationFailedException("unsupported or malformed gs2 header");
        }
        int bareStart = msg.indexOf(',', 2);
        if (bareStart < 0) {
            throw new AuthenticationFailedException("malformed client-first message");
        }
        // an authzid between the commas ("a=...") is ignored, as mongod does
        String bare = msg.substring(bareStart + 1);
        String[] attrs = bare.split(",");
        if (attrs.length < 2 || !attrs[0].startsWith("n=") || !attrs[1].startsWith("r=")) {
            // a leading "m=" would be a mandatory extension we do not implement - covered
            // here because it makes attrs[0] not start with "n="
            throw new AuthenticationFailedException("malformed client-first message");
        }
        String clientNonce = attrs[1].substring(2);
        if (clientNonce.isEmpty()) {
            throw new AuthenticationFailedException("empty client nonce");
        }
        return new String[] {unescapeUsername(attrs[0].substring(2)), clientNonce, bare};
    }

    // RFC 5802: "," and "=" cannot appear raw in a saslname; they arrive as =2C / =3D
    private static String unescapeUsername(String name) throws AuthenticationFailedException {
        if (name.isEmpty()) {
            throw new AuthenticationFailedException("empty username");
        }
        StringBuilder sb = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c != '=') {
                sb.append(c);
                continue;
            }
            if (name.regionMatches(i, "=2C", 0, 3)) {
                sb.append(',');
            } else if (name.regionMatches(i, "=3D", 0, 3)) {
                sb.append('=');
            } else {
                throw new AuthenticationFailedException("invalid escape sequence in username");
            }
            i += 2;
        }
        return sb.toString();
    }

    // 18 random bytes -> 24 base64 chars, no padding; matches mongod's nonce length
    private static String generateNonceSuffix() {
        byte[] bytes = new byte[18];
        RANDOM.nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private byte[] hmac(byte[] key, String data) {
        String algorithm = credentials.getMechanism() == ScramCredentials.Mechanism.SCRAM_SHA_1
                           ? "HmacSHA1" : "HmacSHA256";
        try {
            Mac mac = Mac.getInstance(algorithm);
            mac.init(new SecretKeySpec(key, algorithm));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            // HmacSHA1/HmacSHA256 are mandatory JCE algorithms - failure here is a broken JVM
            throw new IllegalStateException("SCRAM crypto unavailable", e);
        }
    }

    private byte[] digest(byte[] data) {
        String algorithm = credentials.getMechanism() == ScramCredentials.Mechanism.SCRAM_SHA_1
                           ? "SHA-1" : "SHA-256";
        try {
            return MessageDigest.getInstance(algorithm).digest(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("SCRAM crypto unavailable", e);
        }
    }
}
