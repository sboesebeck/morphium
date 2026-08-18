package de.caluga.test.morphium.driver;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code usersInfo} is what {@code db.getUsers()} sends, so without it mongosh cannot list
 * users on a PoppyDB instance at all ("no such command: 'usersInfo'"). It reads the same
 * {@code admin.system.users} documents createUser writes, and must never hand out the stored
 * credentials unless explicitly asked for them.
 */
@Tag("inmemory")
public class InMemUsersInfoTest {

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

    private void createUser(String db, String user, String... roles) throws Exception {
        GenericCommand cmd = new GenericCommand(null);
        cmd.setDb(db);
        cmd.setCmdData(Doc.of("createUser", user, "$db", db, "pwd", "secret",
                "roles", List.of(roles.length == 0 ? "readWrite" : roles[0])));
        int msg = drv.runCommand(cmd);
        Map<String, Object> res = drv.readSingleAnswer(msg);
        assertThat(res.get("ok")).as("createUser %s@%s", user, db).isEqualTo(1.0);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> usersInfo(String db, Object argument, Object... extra) throws Exception {
        Doc data = Doc.of("usersInfo", argument, "$db", db);

        for (int i = 0; i + 1 < extra.length; i += 2) {
            data.put((String) extra[i], extra[i + 1]);
        }

        GenericCommand cmd = new GenericCommand(null);
        cmd.setDb(db);
        cmd.setCmdData(data);
        int msg = drv.runCommand(cmd);
        return drv.readSingleAnswer(msg);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> usersOf(Map<String, Object> result) {
        assertThat(result.get("ok")).as("usersInfo must succeed").isEqualTo(1.0);
        return (List<Map<String, Object>>) result.get("users");
    }

    @Test
    public void listsAllUsersOfTheCurrentDatabase() throws Exception {
        createUser("admin", "adm");
        createUser("testdb", "alice");
        createUser("testdb", "bob");

        List<Map<String, Object>> users = usersOf(usersInfo("testdb", 1));

        assertThat(users).as("only the current database's users").hasSize(2);
        assertThat(users).extracting(u -> u.get("user")).containsExactlyInAnyOrder("alice", "bob");
        assertThat(users).extracting(u -> u.get("db")).containsOnly("testdb");
        assertThat(users).extracting(u -> u.get("_id")).contains("testdb.alice", "testdb.bob");
    }

    @Test
    public void doesNotExposeCredentialsUnlessAskedFor() throws Exception {
        createUser("testdb", "alice");

        Map<String, Object> user = usersOf(usersInfo("testdb", 1)).get(0);
        assertThat(user).as("credentials must not leak into a plain usersInfo").doesNotContainKey("credentials");
        assertThat(user.get("mechanisms")).as("the mechanisms themselves are not secret").isNotNull();

        Map<String, Object> withCredentials = usersOf(usersInfo("testdb", 1, "showCredentials", true)).get(0);
        assertThat(withCredentials).as("showCredentials was requested").containsKey("credentials");
    }

    @Test
    public void looksUpASingleUserByName() throws Exception {
        createUser("testdb", "alice");
        createUser("testdb", "bob");

        List<Map<String, Object>> users = usersOf(usersInfo("testdb", "alice"));

        assertThat(users).hasSize(1);
        assertThat(users.get(0).get("user")).isEqualTo("alice");
    }

    @Test
    public void looksUpAUserOfAnotherDatabaseByDocument() throws Exception {
        createUser("otherdb", "carol");

        List<Map<String, Object>> users = usersOf(usersInfo("admin", Doc.of("user", "carol", "db", "otherdb")));

        assertThat(users).hasSize(1);
        assertThat(users.get(0).get("db")).isEqualTo("otherdb");
    }

    @Test
    public void returnsAnEmptyListForAnUnknownUser() throws Exception {
        createUser("testdb", "alice");

        assertThat(usersOf(usersInfo("testdb", "nobody")))
                .as("an unknown user is not an error - mongod returns an empty list")
                .isEmpty();
    }

    @Test
    public void forAllDBsCrossesDatabaseBoundaries() throws Exception {
        createUser("admin", "adm");
        createUser("testdb", "alice");

        List<Map<String, Object>> users = usersOf(usersInfo("admin", 1, "forAllDBs", true));

        assertThat(users).extracting(u -> u.get("user")).contains("adm", "alice");
    }

    @Test
    public void reportsNoUsersOnAFreshInstanceInsteadOfFailing() throws Exception {
        assertThat(usersOf(usersInfo("admin", 1)))
                .as("a server without any users must answer with an empty list, not an error")
                .isEmpty();
    }
}
