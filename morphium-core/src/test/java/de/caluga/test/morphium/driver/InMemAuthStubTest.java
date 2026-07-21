package de.caluga.test.morphium.driver;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.auth.CreateRoleAdminCommand;
import de.caluga.morphium.driver.commands.auth.CreateUserAdminCommand;
import de.caluga.morphium.driver.commands.auth.SaslAuthCommand;
import de.caluga.morphium.driver.commands.auth.X509AuthCommand;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #245: the server-side auth surface of InMemoryDriver used to consist of empty stubs that
 * queued no result - which the dispatch machinery resolved to {ok:1.0}: every client
 * "authenticated" successfully with any or no credentials. SCRAM verification and createUser
 * are real now (see InMemScramAuthTest); everything still unimplemented (X.509, createRole)
 * or invalid (bad createUser, auth for unknown users) must FAIL LOUDLY - never resolve to
 * the silent dispatch-default success again.
 */
@Tag("inmemory")
public class InMemAuthStubTest {

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

    private Map<String, Object> resultOf(int requestId, String cmdName) throws Exception {
        assertThat(requestId).as(cmdName + " must queue a real result (stub returned 0)").isGreaterThan(0);

        Map<String, Object> result = drv.readSingleAnswer(requestId);
        assertThat(result).as(cmdName + " result").isNotNull();
        assertThat(result.get("ok")).as(cmdName + " must not report success").isEqualTo(0.0);
        assertThat((Integer) result.get("code")).as(cmdName + " must carry an error code").isNotNull();
        return result;
    }

    private void assertHonestFailure(int requestId, String cmdName) throws Exception {
        assertThat(resultOf(requestId, cmdName).get("errmsg").toString().toLowerCase())
                .as(cmdName + " error must be unmistakable")
                .containsAnyOf("not supported", "not implemented");
    }

    @Test
    public void directTypedSaslStartPointsToTheWirePath() throws Exception {
        // the real conversation runs via the generic dispatch (raw payload needed) - a direct
        // typed call cannot carry it and must fail with a pointer, never silently succeed
        Map<String, Object> result = resultOf(drv.runCommand(new SaslAuthCommand(null).setDb("admin")), "saslStart (typed)");
        assertThat(result.get("errmsg").toString()).contains("generic");
    }

    @Test
    public void x509AuthenticateFailsLoudly() throws Exception {
        assertHonestFailure(drv.runCommand(new X509AuthCommand(null).setDb("$external")), "authenticate");
    }

    @Test
    public void createUserWithoutPasswordFailsLoudly() throws Exception {
        Map<String, Object> result = resultOf(
                drv.runCommand(new CreateUserAdminCommand(null).setUserName("nopwd").setDb("admin")), "createUser");
        assertThat(result.get("errmsg").toString()).contains("pwd");
    }

    @Test
    public void createRoleFailsLoudly() throws Exception {
        assertHonestFailure(drv.runCommand(new CreateRoleAdminCommand(null).setDb("admin")), "createRole");
    }

    @Test
    public void saslStartForUnknownUserFailsWithAuthenticationFailed() throws Exception {
        // the path PoppyDB's wire handler takes: generic command with the raw SASL payload
        GenericCommand cmd = new GenericCommand(drv);
        cmd.fromMap(Doc.of("saslStart", 1, "mechanism", "SCRAM-SHA-256",
                "payload", "n,,n=ghost,r=clientnonce123".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "$db", "admin"));

        Map<String, Object> result = resultOf(drv.runCommand(cmd), "saslStart (generic dispatch)");
        assertThat((Integer) result.get("code")).isEqualTo(18);
        assertThat(result.get("errmsg").toString()).contains("Authentication failed");
    }
}
