package de.caluga.test.morphium.driver;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;

import com.ongres.scram.client.ScramClient;
import com.ongres.scram.common.ScramMechanisms;

import de.caluga.morphium.Utils;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.auth.CreateUserAdminCommand;
import de.caluga.morphium.driver.commands.auth.SaslAuthCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

import static com.ongres.scram.common.stringprep.StringPreparations.NO_PREPARATION;
import static com.ongres.scram.common.stringprep.StringPreparations.SASL_PREPARATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * End-to-end SCRAM authentication against the in-memory server: a user created via createUser
 * (stored mongod-shaped in admin.system.users) can authenticate with morphium's OWN production
 * SCRAM client (SaslAuthCommand) - the same code that authenticates against real MongoDB.
 * Verification only, no enforcement yet: unauthenticated commands still work.
 */
@Tag("inmemory")
public class InMemScramAuthTest {

    private InMemoryDriver drv;

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

    private void createUser(String user, String pwd) throws Exception {
        CreateUserAdminCommand cmd = new CreateUserAdminCommand(null)
                .setUserName(user)
                .setPwd(pwd);
        cmd.setDb("admin");
        Map<String, Object> result = drv.readSingleAnswer(drv.runCommand(cmd));
        assertThat(result.get("ok")).as("createUser result: " + result).isEqualTo(1.0);
    }

    private void authenticate(String user, String pwd, String mechanism) throws Exception {
        SaslAuthCommand auth = new SaslAuthCommand(drv.getPrimaryConnection(null));
        auth.setUser(user).setPassword(pwd).setMechanism(mechanism).setDb("admin");
        auth.execute();
    }

    @Test
    public void createdUserAuthenticatesWithScramSha256() throws Exception {
        createUser("alice", "s3cr3t");
        authenticate("alice", "s3cr3t", "SCRAM-SHA-256");
    }

    @Test
    public void createdUserAuthenticatesWithScramSha1() throws Exception {
        createUser("bob", "hunter2");
        authenticate("bob", "hunter2", "SCRAM-SHA-1");
    }

    @Test
    public void wrongPasswordIsRejected() throws Exception {
        createUser("carol", "rightpass");

        Throwable t = catchThrowable(() -> authenticate("carol", "wrongpass", "SCRAM-SHA-256"));

        assertThat(t).as("wrong password must fail authentication").isNotNull();
        assertThat(t.getMessage()).contains("Authentication failed");
    }

    @Test
    public void unknownUserIsRejectedWithTheSameError() throws Exception {
        Throwable t = catchThrowable(() -> authenticate("nobody", "whatever", "SCRAM-SHA-256"));

        assertThat(t).isNotNull();
        assertThat(t.getMessage())
                .as("unknown user and wrong password must be indistinguishable (no enumeration)")
                .contains("Authentication failed");
    }

    @Test
    public void duplicateCreateUserIsRejected() throws Exception {
        createUser("dave", "pw1");

        CreateUserAdminCommand cmd = new CreateUserAdminCommand(null).setUserName("dave").setPwd("pw2");
        cmd.setDb("admin");
        Map<String, Object> result = drv.readSingleAnswer(drv.runCommand(cmd));

        assertThat(result.get("ok")).isEqualTo(0.0);
        assertThat((Integer) result.get("code")).isEqualTo(51003);
        assertThat(result.get("errmsg").toString()).contains("already exists");
    }

    @Test
    public void userDocumentIsStoredInSystemUsersInMongodShape() throws Exception {
        createUser("eve", "pw");

        var found = drv.findByFieldValue("admin", "system.users", "_id", "admin.eve");
        assertThat(found).hasSize(1);
        @SuppressWarnings("unchecked")
        Map<String, Object> credentials = (Map<String, Object>) found.get(0).get("credentials");
        assertThat(credentials).containsKeys("SCRAM-SHA-1", "SCRAM-SHA-256");
    }

    @Test
    public void mongoshStyleThreeStepExchangeWithoutSkipEmptyExchange() throws Exception {
        // clients that do NOT request skipEmptyExchange (e.g. mongosh) expect done:false with the
        // server-final payload on the second step and done:true on a third, empty saslContinue
        createUser("frank", "pw123");

        ScramClient scramClient = ScramClient.channelBinding(ScramClient.ChannelBinding.NO)
                .stringPreparation(SASL_PREPARATION)
                .selectClientMechanism(ScramMechanisms.SCRAM_SHA_256)
                .setup();
        var session = scramClient.scramSession("frank");

        var con = drv.getPrimaryConnection(null);
        GenericCommand start = new GenericCommand(con);
        start.setCommandName("saslStart");
        start.setCmdData(Doc.of("saslStart", 1, "mechanism", "SCRAM-SHA-256",
                "payload", session.clientFirstMessage().getBytes(StandardCharsets.UTF_8)));
        start.setDb("admin");
        Map<String, Object> reply = con.readSingleAnswer(con.sendCommand(start));

        assertThat(reply.get("ok")).isEqualTo(1.0);
        assertThat(reply.get("done")).as("first step is never done").isEqualTo(false);
        int conversationId = (Integer) reply.get("conversationId");

        var serverFirst = session.receiveServerFirstMessage(new String((byte[]) reply.get("payload"), StandardCharsets.UTF_8));
        var clientFinal = serverFirst.clientFinalProcessor("pw123");

        GenericCommand cont = new GenericCommand(con);
        cont.setCommandName("saslContinue");
        cont.setCmdData(Doc.of("saslContinue", 1, "conversationId", conversationId,
                "payload", clientFinal.clientFinalMessage().getBytes(StandardCharsets.UTF_8)));
        cont.setDb("admin");
        reply = con.readSingleAnswer(con.sendCommand(cont));

        assertThat(reply.get("ok")).isEqualTo(1.0);
        assertThat(reply.get("done")).as("without skipEmptyExchange the second step is not done").isEqualTo(false);
        clientFinal.receiveServerFinalMessage(new String((byte[]) reply.get("payload"), StandardCharsets.UTF_8));

        GenericCommand fin = new GenericCommand(con);
        fin.setCommandName("saslContinue");
        fin.setCmdData(Doc.of("saslContinue", 1, "conversationId", conversationId, "payload", new byte[0]));
        fin.setDb("admin");
        reply = con.readSingleAnswer(con.sendCommand(fin));

        assertThat(reply.get("ok")).isEqualTo(1.0);
        assertThat(reply.get("done")).as("empty third step completes the exchange").isEqualTo(true);
    }

    @Test
    public void staleConversationIdIsAProtocolError() throws Exception {
        var con = drv.getPrimaryConnection(null);
        GenericCommand cont = new GenericCommand(con);
        cont.setCommandName("saslContinue");
        cont.setCmdData(Doc.of("saslContinue", 1, "conversationId", 999_999, "payload", new byte[0]));
        cont.setDb("admin");
        Map<String, Object> reply = con.readSingleAnswer(con.sendCommand(cont));

        assertThat(reply.get("ok")).isEqualTo(0.0);
        assertThat((Integer) reply.get("code")).isEqualTo(17);
    }
}
