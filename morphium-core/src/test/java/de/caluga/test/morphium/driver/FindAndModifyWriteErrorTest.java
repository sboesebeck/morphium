package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.FindAndModifyMongoCommand;
import de.caluga.morphium.driver.wire.MongoConnection;
import de.caluga.test.ConnectionMock;
import de.caluga.test.DriverMock;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that {@link FindAndModifyMongoCommand#execute()} throws
 * {@link MorphiumDriverException} (not {@code RuntimeException}) and
 * attaches structured writeErrors when the server response contains them.
 */
@Tag("inmemory")
class FindAndModifyWriteErrorTest {

    /**
     * ConnectionMock that returns a canned writeResult with writeErrors
     * from {@code readSingleAnswer}.
     */
    static class WriteErrorConnectionMock extends ConnectionMock {
        private final Map<String, Object> cannedResult;

        WriteErrorConnectionMock(Map<String, Object> cannedResult) {
            this.cannedResult = cannedResult;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public int sendCommand(de.caluga.morphium.driver.commands.MongoCommand cmd) throws MorphiumDriverException {
            return 42;
        }

        @Override
        public Map<String, Object> readSingleAnswer(int id) throws MorphiumDriverException {
            return cannedResult;
        }

        @Override
        public de.caluga.morphium.driver.MorphiumDriver getDriver() {
            return new DriverMock();
        }
    }

    @Test
    void writeErrors_throwsMorphiumDriverException_withStructuredErrors() {
        List<Map<String, Object>> writeErrors = List.of(
            Doc.of("index", 0, "code", 11000, "errmsg", "E11000 duplicate key error")
        );
        Map<String, Object> serverResponse = Doc.of(
            "ok", 1,
            "n", 0,
            "writeErrors", writeErrors
        );

        MongoConnection con = new WriteErrorConnectionMock(serverResponse);
        FindAndModifyMongoCommand cmd = new FindAndModifyMongoCommand(con);
        cmd.setDb("testdb").setColl("testcoll");
        cmd.setQuery(Doc.of("_id", "abc"));
        cmd.setUpdate(Doc.of("$set", Doc.of("name", "test")));

        assertThatThrownBy(cmd::execute)
            .isInstanceOf(MorphiumDriverException.class)
            .satisfies(ex -> {
                MorphiumDriverException mde = (MorphiumDriverException) ex;
                assertThat(mde.getWriteErrors()).isNotNull();
                assertThat(mde.getWriteErrors()).hasSize(1);
                assertThat(mde.getWriteErrors().get(0).get("code")).isEqualTo(11000);
            })
            .hasMessageContaining("11000")
            .hasMessageContaining("E11000 duplicate key error");
    }

    @Test
    void noWriteErrors_returnsValueNormally() throws MorphiumDriverException {
        Map<String, Object> value = Doc.of("_id", "abc", "name", "found");
        Map<String, Object> serverResponse = Doc.of(
            "ok", 1,
            "n", 1,
            "value", value
        );

        MongoConnection con = new WriteErrorConnectionMock(serverResponse);
        FindAndModifyMongoCommand cmd = new FindAndModifyMongoCommand(con);
        cmd.setDb("testdb").setColl("testcoll");
        cmd.setQuery(Doc.of("_id", "abc"));
        cmd.setUpdate(Doc.of("$set", Doc.of("name", "updated")));

        Map<String, Object> result = cmd.execute();
        assertThat(result).containsEntry("_id", "abc");
        assertThat(result).containsEntry("name", "found");
    }
}
