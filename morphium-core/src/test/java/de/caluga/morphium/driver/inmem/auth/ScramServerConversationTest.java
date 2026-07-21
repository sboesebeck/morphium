package de.caluga.morphium.driver.inmem.auth;

import com.ongres.scram.client.ScramClient;
import com.ongres.scram.client.ScramSession;
import com.ongres.scram.common.ScramMechanisms;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;

import de.caluga.morphium.driver.inmem.auth.ScramCredentials.Mechanism;
import de.caluga.morphium.driver.inmem.auth.ScramServerConversation.AuthenticationFailedException;

import static com.ongres.scram.common.stringprep.StringPreparations.NO_PREPARATION;
import static com.ongres.scram.common.stringprep.StringPreparations.SASL_PREPARATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Server-side SCRAM conversation for the InMemoryDriver/PoppyDB wire server. The counterpart
 * is SaslAuthCommand, which drives the ongres ScramClient - so the round-trip tests here use
 * exactly that client setup (including MongoDB's md5-mangled password for SCRAM-SHA-1).
 *
 * The RFC vectors pin the exact wire messages: deriveRaw() bypasses the mongo-specific
 * password mangling because the RFCs feed "pencil" directly into the SCRAM algorithm.
 */
@Tag("core")
public class ScramServerConversationTest {

    // RFC 5802 section 5: full example conversation for SCRAM-SHA-1
    private static final String RFC5802_CLIENT_FIRST = "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL";
    private static final String RFC5802_SERVER_FIRST = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
    private static final String RFC5802_CLIENT_FINAL = "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=";
    private static final String RFC5802_SERVER_FINAL = "v=rmF9pqV8S7suAoZWja4dJRkFsKQ=";

    // RFC 7677 section 3: full example conversation for SCRAM-SHA-256
    private static final String RFC7677_CLIENT_FIRST = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
    private static final String RFC7677_SERVER_FIRST = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    private static final String RFC7677_CLIENT_FINAL = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
    private static final String RFC7677_SERVER_FINAL = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";

    private ScramCredentials rfc5802Credentials() {
        return ScramCredentials.deriveRaw(Mechanism.SCRAM_SHA_1, "pencil",
                Base64.getDecoder().decode("QSXCR+Q6sek8bf92"), 4096);
    }

    private ScramCredentials rfc7677Credentials() {
        return ScramCredentials.deriveRaw(Mechanism.SCRAM_SHA_256, "pencil",
                Base64.getDecoder().decode("W22ZaJ0SNY7soEsUEjb6gQ=="), 4096);
    }

    @Test
    public void rfc5802TestVectorSha1() throws Exception {
        ScramServerConversation conv = new ScramServerConversation(rfc5802Credentials())
                .withServerNonceSuffix("3rfcNHYJY1ZVvWVs7j");
        assertThat(conv.handleClientFirst(RFC5802_CLIENT_FIRST)).isEqualTo(RFC5802_SERVER_FIRST);
        assertThat(conv.getUser()).isEqualTo("user");
        assertThat(conv.isComplete()).isFalse();
        assertThat(conv.handleClientFinal(RFC5802_CLIENT_FINAL)).isEqualTo(RFC5802_SERVER_FINAL);
        assertThat(conv.isComplete()).isTrue();
    }

    @Test
    public void rfc7677TestVectorSha256() throws Exception {
        ScramServerConversation conv = new ScramServerConversation(rfc7677Credentials())
                .withServerNonceSuffix("%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0");
        assertThat(conv.handleClientFirst(RFC7677_CLIENT_FIRST)).isEqualTo(RFC7677_SERVER_FIRST);
        assertThat(conv.getUser()).isEqualTo("user");
        assertThat(conv.handleClientFinal(RFC7677_CLIENT_FINAL)).isEqualTo(RFC7677_SERVER_FINAL);
        assertThat(conv.isComplete()).isTrue();
    }

    // Same client construction as SaslAuthCommand.execute(), including the mongo password mangling.
    private ScramClient buildClient(Mechanism mechanism) {
        if (mechanism == Mechanism.SCRAM_SHA_1) {
            return ScramClient.channelBinding(ScramClient.ChannelBinding.NO)
                    .stringPreparation(NO_PREPARATION)
                    .selectClientMechanism(ScramMechanisms.SCRAM_SHA_1)
                    .setup();
        }
        return ScramClient.channelBinding(ScramClient.ChannelBinding.NO)
                .stringPreparation(SASL_PREPARATION)
                .selectClientMechanism(ScramMechanisms.SCRAM_SHA_256)
                .setup();
    }

    private String clientPassword(Mechanism mechanism, String user, String rawPassword) throws Exception {
        if (mechanism != Mechanism.SCRAM_SHA_1) {
            return rawPassword;
        }
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] md5 = md.digest((user + ":mongo:" + rawPassword).getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder();
        for (byte b : md5) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }

    private void roundTrip(Mechanism mechanism, ScramCredentials credentials, String user, String rawPassword) throws Exception {
        ScramServerConversation server = new ScramServerConversation(credentials);
        ScramSession session = buildClient(mechanism).scramSession(user);
        String serverFirst = server.handleClientFirst(session.clientFirstMessage());
        ScramSession.ClientFinalProcessor finalProcessor = session.receiveServerFirstMessage(serverFirst)
                .clientFinalProcessor(clientPassword(mechanism, user, rawPassword));
        String serverFinal = server.handleClientFinal(finalProcessor.clientFinalMessage());
        // throws ScramInvalidServerSignatureException if our serverKey-side math is off
        finalProcessor.receiveServerFinalMessage(serverFinal);
        assertThat(server.isComplete()).isTrue();
        assertThat(server.getUser()).isEqualTo(user);
    }

    @Test
    public void roundTripAgainstOngresClientSha1() throws Exception {
        byte[] salt = new byte[16];
        new SecureRandom().nextBytes(salt);
        ScramCredentials creds = ScramCredentials.derive(Mechanism.SCRAM_SHA_1, "testuser", "s3cr3t!", salt, 4096);
        roundTrip(Mechanism.SCRAM_SHA_1, creds, "testuser", "s3cr3t!");
    }

    @Test
    public void roundTripAgainstOngresClientSha256() throws Exception {
        byte[] salt = new byte[16];
        new SecureRandom().nextBytes(salt);
        ScramCredentials creds = ScramCredentials.derive(Mechanism.SCRAM_SHA_256, "testuser", "s3cr3t!", salt, 15000);
        roundTrip(Mechanism.SCRAM_SHA_256, creds, "testuser", "s3cr3t!");
    }

    @Test
    public void wrongPasswordFailsWithoutServerSignature() throws Exception {
        byte[] salt = new byte[16];
        new SecureRandom().nextBytes(salt);
        ScramCredentials creds = ScramCredentials.derive(Mechanism.SCRAM_SHA_256, "testuser", "correct", salt, 4096);
        ScramServerConversation server = new ScramServerConversation(creds);
        ScramSession session = buildClient(Mechanism.SCRAM_SHA_256).scramSession("testuser");
        String serverFirst = server.handleClientFirst(session.clientFirstMessage());
        String clientFinal = session.receiveServerFirstMessage(serverFirst)
                .clientFinalProcessor("wrong").clientFinalMessage();
        assertThatThrownBy(() -> server.handleClientFinal(clientFinal))
                .isInstanceOf(AuthenticationFailedException.class);
        assertThat(server.isComplete()).isFalse();
    }

    @Test
    public void tamperedNonceInClientFinalFails() throws Exception {
        ScramServerConversation conv = new ScramServerConversation(rfc5802Credentials())
                .withServerNonceSuffix("3rfcNHYJY1ZVvWVs7j");
        conv.handleClientFirst(RFC5802_CLIENT_FIRST);
        // replayed/foreign nonce: proof is valid for a different conversation's nonce
        String tampered = RFC5802_CLIENT_FINAL.replace("3rfcNHYJY1ZVvWVs7j", "XrfcNHYJY1ZVvWVs7X");
        assertThatThrownBy(() -> conv.handleClientFinal(tampered))
                .isInstanceOf(AuthenticationFailedException.class);
        assertThat(conv.isComplete()).isFalse();
    }

    @Test
    public void extractUserHandlesRfcEscaping() throws Exception {
        assertThat(ScramServerConversation.extractUser("n,,n=u=2Cser,r=abcdef")).isEqualTo("u,ser");
        assertThat(ScramServerConversation.extractUser("n,,n=a=3Db,r=abcdef")).isEqualTo("a=b");
        assertThat(ScramServerConversation.extractUser(RFC5802_CLIENT_FIRST)).isEqualTo("user");
    }

    @Test
    public void escapedUserSurvivesFullConversation() throws Exception {
        ScramServerConversation conv = new ScramServerConversation(rfc5802Credentials())
                .withServerNonceSuffix("sfx");
        conv.handleClientFirst("n,,n=u=2Cser,r=clientnonce");
        assertThat(conv.getUser()).isEqualTo("u,ser");
    }

    @Test
    public void storedDocumentRoundTripPreservesVerification() throws Exception {
        ScramCredentials original = rfc5802Credentials();
        Map<String, Object> doc = original.toStoredDocument();
        // exactly the shape mongod keeps in admin.system.users
        assertThat(doc.get("iterationCount")).isEqualTo(4096);
        assertThat(doc.get("salt")).isEqualTo("QSXCR+Q6sek8bf92");
        assertThat(doc.get("storedKey")).isInstanceOf(String.class);
        assertThat(doc.get("serverKey")).isInstanceOf(String.class);

        ScramCredentials restored = ScramCredentials.fromStoredDocument(Mechanism.SCRAM_SHA_1, doc);
        ScramServerConversation conv = new ScramServerConversation(restored)
                .withServerNonceSuffix("3rfcNHYJY1ZVvWVs7j");
        assertThat(conv.handleClientFirst(RFC5802_CLIENT_FIRST)).isEqualTo(RFC5802_SERVER_FIRST);
        assertThat(conv.handleClientFinal(RFC5802_CLIENT_FINAL)).isEqualTo(RFC5802_SERVER_FINAL);
        assertThat(conv.isComplete()).isTrue();
    }

    @Test
    public void mechanismNameMapping() {
        assertThat(Mechanism.SCRAM_SHA_1.mechanismName()).isEqualTo("SCRAM-SHA-1");
        assertThat(Mechanism.SCRAM_SHA_256.mechanismName()).isEqualTo("SCRAM-SHA-256");
        assertThat(Mechanism.fromName("SCRAM-SHA-1")).isEqualTo(Mechanism.SCRAM_SHA_1);
        assertThat(Mechanism.fromName("SCRAM-SHA-256")).isEqualTo(Mechanism.SCRAM_SHA_256);
    }

    @Test
    public void malformedClientFirstFails() {
        ScramServerConversation conv = new ScramServerConversation(rfc5802Credentials());
        assertThatThrownBy(() -> conv.handleClientFirst("garbage"))
                .isInstanceOf(AuthenticationFailedException.class);
    }

    @Test
    public void wrongStateThrowsIllegalState() throws Exception {
        ScramServerConversation conv = new ScramServerConversation(rfc5802Credentials())
                .withServerNonceSuffix("3rfcNHYJY1ZVvWVs7j");
        assertThatThrownBy(() -> conv.handleClientFinal(RFC5802_CLIENT_FINAL))
                .isInstanceOf(IllegalStateException.class);
        conv.handleClientFirst(RFC5802_CLIENT_FIRST);
        assertThatThrownBy(() -> conv.handleClientFirst(RFC5802_CLIENT_FIRST))
                .isInstanceOf(IllegalStateException.class);
    }
}
