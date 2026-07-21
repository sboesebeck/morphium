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
 * #245: the entire server-side auth surface of InMemoryDriver (saslStart, X.509 authenticate,
 * createUser, createRole) consisted of empty stubs that queued no result - which the dispatch
 * machinery resolved to {ok:1.0}. Every client "authenticated" successfully with any or no
 * credentials, and createUser/createRole reported success while creating nothing.
 *
 * Until real authentication lands (phase D), these commands must FAIL LOUDLY: ok:0 with an
 * unmistakable error, so nobody mistakes the dispatch default for working auth.
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

    private void assertHonestFailure(int requestId, String cmdName) throws Exception {
        assertThat(requestId).as(cmdName + " must queue a real result (stub returned 0)").isGreaterThan(0);

        Map<String, Object> result = drv.readSingleAnswer(requestId);
        assertThat(result).as(cmdName + " result").isNotNull();
        assertThat(result.get("ok")).as(cmdName + " must not report success").isEqualTo(0.0);
        assertThat((Integer) result.get("code")).as(cmdName + " must carry an error code").isNotNull();
        assertThat(result.get("errmsg").toString().toLowerCase())
                .as(cmdName + " error must be unmistakable")
                .containsAnyOf("not supported", "not implemented");
    }

    @Test
    public void saslStartFailsLoudly() throws Exception {
        assertHonestFailure(drv.runCommand(new SaslAuthCommand(null).setDb("admin")), "saslStart");
    }

    @Test
    public void x509AuthenticateFailsLoudly() throws Exception {
        assertHonestFailure(drv.runCommand(new X509AuthCommand(null).setDb("$external")), "authenticate");
    }

    @Test
    public void createUserFailsLoudly() throws Exception {
        assertHonestFailure(drv.runCommand(new CreateUserAdminCommand(null).setDb("admin")), "createUser");
    }

    @Test
    public void createRoleFailsLoudly() throws Exception {
        assertHonestFailure(drv.runCommand(new CreateRoleAdminCommand(null).setDb("admin")), "createRole");
    }

    @Test
    public void saslStartViaGenericDispatchFailsLoudly() throws Exception {
        // the path PoppyDB's wire handler takes: generic command resolved via the command cache
        GenericCommand cmd = new GenericCommand(drv);
        cmd.fromMap(Doc.of("saslStart", 1, "mechanism", "SCRAM-SHA-256", "$db", "admin"));

        assertHonestFailure(drv.runCommand(cmd), "saslStart (generic dispatch)");
    }
}
