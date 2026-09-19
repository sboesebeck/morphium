package de.caluga.test.morphium.driver.command;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.ReadPreferenceType;
import de.caluga.morphium.driver.commands.FindCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.test.ConnectionMock;

/**
 * The read preference a caller asks for has to end up in the command that is sent to the server.
 * On a replica set the driver picks the node itself and mongod ignores $readPreference, but a
 * mongos routes reads by exactly this field - so without it, all read preference configuration
 * (MorphiumConfig.defaultReadPreference, @DefaultReadPreference) is silently dropped on a
 * sharded cluster.
 */
@Tag("driver")
public class MongoCommandReadPreferenceTest {

    @Test
    public void readPreferenceSetOnTheCommandIsSent() {
        FindCommand cmd = new FindCommand(new ConnectionMock()).setColl("test");
        cmd.setReadPreference(ReadPreference.primary());

        assertThat(modeOf(cmd)).isEqualTo("primary");
    }

    @Test
    public void readPreferenceOfTheConnectionIsSentWhenTheCommandHasNone() {
        ConnectionMock connection = new ConnectionMock();
        connection.setEffectiveReadPreference(ReadPreference.secondaryPreferred());
        FindCommand cmd = new FindCommand(connection).setColl("test");

        assertThat(modeOf(cmd)).isEqualTo("secondaryPreferred");
    }

    @Test
    public void readPreferenceOfTheCommandWinsOverTheOneOfTheConnection() {
        ConnectionMock connection = new ConnectionMock();
        connection.setEffectiveReadPreference(ReadPreference.nearest());
        FindCommand cmd = new FindCommand(connection).setColl("test");
        cmd.setReadPreference(ReadPreference.primary());

        assertThat(modeOf(cmd)).isEqualTo("primary");
    }

    @Test
    public void withoutAnyReadPreferenceThePreviousDefaultIsKept() {
        FindCommand cmd = new FindCommand(new ConnectionMock()).setColl("test");

        assertThat(modeOf(cmd)).isEqualTo("primaryPreferred");
    }

    @ParameterizedTest
    @CsvSource({
        "PRIMARY,primary",
        "PRIMARY_PREFERRED,primaryPreferred",
        "SECONDARY,secondary",
        "SECONDARY_PREFERRED,secondaryPreferred",
        "NEAREST,nearest"
    })
    public void everyReadPreferenceTypeUsesItsWireProtocolName(ReadPreferenceType type, String expectedMode) {
        ReadPreference readPreference = new ReadPreference();
        readPreference.setType(type);
        FindCommand cmd = new FindCommand(new ConnectionMock()).setColl("test");
        cmd.setReadPreference(readPreference);

        assertThat(modeOf(cmd)).isEqualTo(expectedMode);
    }

    @Test
    public void tagSetIsSentAlongWithTheMode() {
        ReadPreference readPreference = ReadPreference.secondary();
        readPreference.addTag("dc", "muc");
        FindCommand cmd = new FindCommand(new ConnectionMock()).setColl("test");
        cmd.setReadPreference(readPreference);

        assertThat(readPreferenceOf(cmd)).containsEntry("mode", "secondary");
        // the wire protocol expects a list of tag documents
        assertThat(readPreferenceOf(cmd).get("tags")).isEqualTo(List.of(Map.of("dc", "muc")));
    }

    /**
     * The other half of the chain: a connection handed out for a read preference has to carry it,
     * so that commands built on it are sent with it without every call site having to pass it on.
     */
    @Test
    public void connectionHandedOutForAReadPreferenceCarriesItIntoTheCommand() {
        InMemoryDriver driver = new InMemoryDriver();
        driver.connect();
        MongoConnection connection = driver.getReadConnection(ReadPreference.secondaryPreferred());

        try {
            assertThat(connection.getEffectiveReadPreference()).isNotNull();
            assertThat(connection.getEffectiveReadPreference().getType())
                .isEqualTo(ReadPreferenceType.SECONDARY_PREFERRED);
            assertThat(modeOf(new FindCommand(connection).setColl("test"))).isEqualTo("secondaryPreferred");
        } finally {
            driver.releaseConnection(connection);
            driver.close();
        }
    }

    @Test
    public void connectionForWritesIsMarkedAsPrimary() {
        InMemoryDriver driver = new InMemoryDriver();
        driver.connect();
        MongoConnection connection = driver.getPrimaryConnection(null);

        try {
            assertThat(connection.getEffectiveReadPreference()).isNotNull();
            assertThat(connection.getEffectiveReadPreference().getType()).isEqualTo(ReadPreferenceType.PRIMARY);
        } finally {
            driver.releaseConnection(connection);
            driver.close();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readPreferenceOf(FindCommand cmd) {
        return (Map<String, Object>) cmd.asMap().get("$readPreference");
    }

    private String modeOf(FindCommand cmd) {
        return (String) readPreferenceOf(cmd).get("mode");
    }
}