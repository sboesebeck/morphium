package de.caluga.poppydb.netty;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.auth.CreateUserAdminCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wireprotocol.OpMsg;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.ongres.scram.client.ScramClient;
import com.ongres.scram.common.ScramMechanisms;

import static com.ongres.scram.common.stringprep.StringPreparations.SASL_PREPARATION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Auth enforcement at the wire level (--auth): with enforcement enabled, a connection may only
 * run pre-auth commands (handshake, SASL, ping, buildInfo) until a SCRAM exchange succeeded;
 * everything else is rejected with code 13 Unauthorized. Enforcement is strictly opt-in -
 * the default stays open (test/dev usage unchanged).
 *
 * Uses an EmbeddedChannel around the real MongoCommandHandler, so the full wire dispatch
 * including the SASL conversation against the InMemoryDriver runs - no network needed.
 */
public class AuthEnforcementTest {

    private InMemoryDriver drv;
    private final AtomicInteger msgId = new AtomicInteger(1);

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
    }

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    private MongoCommandHandler handler(boolean authRequired) {
        MongoCommandHandler h = new MongoCommandHandler(drv, null, null, null, new AtomicInteger(1),
                "localhost", 17017, "rs0", List.of("localhost:17017"), true, "localhost:17017",
                0, () -> null);
        h.setAuthRequired(authRequired);
        return h;
    }

    private Map<String, Object> send(EmbeddedChannel ch, Map<String, Object> cmd) {
        OpMsg msg = new OpMsg();
        msg.setMessageId(msgId.incrementAndGet());
        msg.setFirstDoc(cmd);
        ch.writeInbound(msg);
        OpMsg reply = ch.readOutbound();
        assertThat(reply).as("no reply for " + cmd.keySet().iterator().next()).isNotNull();
        return reply.getFirstDoc();
    }

    private void createUser(String user, String pwd) throws Exception {
        CreateUserAdminCommand cmd = new CreateUserAdminCommand(null).setUserName(user).setPwd(pwd);
        cmd.setDb("admin");
        Map<String, Object> res = drv.readSingleAnswer(drv.runCommand(cmd));
        assertThat(res.get("ok")).isEqualTo(1.0);
    }

    /** Full SCRAM-SHA-256 exchange over the wire handler; returns the final reply. */
    private Map<String, Object> authenticate(EmbeddedChannel ch, String user, String pwd) throws Exception {
        ScramClient client = ScramClient.channelBinding(ScramClient.ChannelBinding.NO)
                .stringPreparation(SASL_PREPARATION)
                .selectClientMechanism(ScramMechanisms.SCRAM_SHA_256)
                .setup();
        var session = client.scramSession(user);

        Map<String, Object> first = send(ch, Doc.of("saslStart", 1, "mechanism", "SCRAM-SHA-256",
                "payload", session.clientFirstMessage().getBytes(StandardCharsets.UTF_8),
                "options", Doc.of("skipEmptyExchange", true), "$db", "admin"));
        assertThat(first.get("ok")).as("saslStart: " + first).isEqualTo(1.0);

        var serverFirst = session.receiveServerFirstMessage(
                new String((byte[]) first.get("payload"), StandardCharsets.UTF_8));
        var clientFinal = serverFirst.clientFinalProcessor(pwd);

        return send(ch, Doc.of("saslContinue", 1, "conversationId", first.get("conversationId"),
                "payload", clientFinal.clientFinalMessage().getBytes(StandardCharsets.UTF_8), "$db", "admin"));
    }

    private Map<String, Object> findCmd() {
        return Doc.of("find", "somecoll", "filter", Doc.of(), "$db", "authtestdb");
    }

    @Test
    public void withoutEnforcementEverythingWorksUnauthenticated() {
        EmbeddedChannel ch = new EmbeddedChannel(handler(false));

        Map<String, Object> reply = send(ch, findCmd());

        assertThat(reply.get("ok")).as("default stays open: " + reply).isEqualTo(1.0);
    }

    @Test
    public void enforcementRejectsUnauthenticatedCommands() {
        EmbeddedChannel ch = new EmbeddedChannel(handler(true));

        Map<String, Object> reply = send(ch, findCmd());

        assertThat(reply.get("ok")).isEqualTo(0.0);
        assertThat((Integer) reply.get("code")).isEqualTo(13);
        assertThat(reply.get("errmsg").toString()).contains("requires authentication");
    }

    @Test
    public void preAuthCommandsStayAvailable() {
        EmbeddedChannel ch = new EmbeddedChannel(handler(true));

        assertThat(send(ch, Doc.of("hello", 1, "$db", "admin")).get("ok")).isEqualTo(1.0);
        assertThat(send(ch, Doc.of("ping", 1, "$db", "admin")).get("ok")).isEqualTo(1.0);
        assertThat(send(ch, Doc.of("buildInfo", 1, "$db", "admin")).get("ok")).isEqualTo(1.0);

        // saslStart must reach the SASL machinery (auth error 18), not the enforcement gate (13)
        Map<String, Object> sasl = send(ch, Doc.of("saslStart", 1, "mechanism", "SCRAM-SHA-256",
                "payload", "n,,n=ghost,r=nonce12345".getBytes(StandardCharsets.UTF_8), "$db", "admin"));
        assertThat((Integer) sasl.get("code")).isEqualTo(18);
    }

    @Test
    public void successfulScramExchangeUnlocksTheConnection() throws Exception {
        createUser("wireuser", "wirepass");
        EmbeddedChannel ch = new EmbeddedChannel(handler(true));

        Map<String, Object> authReply = authenticate(ch, "wireuser", "wirepass");
        assertThat(authReply.get("ok")).as("SCRAM exchange: " + authReply).isEqualTo(1.0);
        assertThat(authReply.get("done")).isEqualTo(true);

        Map<String, Object> reply = send(ch, findCmd());
        assertThat(reply.get("ok")).as("after auth the command must run: " + reply).isEqualTo(1.0);
    }

    @Test
    public void failedScramExchangeDoesNotUnlock() throws Exception {
        createUser("wireuser2", "rightpass");
        EmbeddedChannel ch = new EmbeddedChannel(handler(true));

        Map<String, Object> authReply = authenticate(ch, "wireuser2", "wrongpass");
        assertThat(authReply.get("ok")).isEqualTo(0.0);

        Map<String, Object> reply = send(ch, findCmd());
        assertThat((Integer) reply.get("code")).as("connection must stay locked").isEqualTo(13);
    }

    @Test
    public void logoutLocksTheConnectionAgain() throws Exception {
        createUser("wireuser3", "pw");
        EmbeddedChannel ch = new EmbeddedChannel(handler(true));
        authenticate(ch, "wireuser3", "pw");
        assertThat(send(ch, findCmd()).get("ok")).isEqualTo(1.0);

        assertThat(send(ch, Doc.of("logout", 1, "$db", "admin")).get("ok")).isEqualTo(1.0);

        assertThat((Integer) send(ch, findCmd()).get("code")).isEqualTo(13);
    }
}
