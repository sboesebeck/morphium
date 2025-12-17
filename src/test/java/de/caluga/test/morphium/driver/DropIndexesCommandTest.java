package de.caluga.test.morphium.driver;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.DropIndexesCommand;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.ListIndexesCommand;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for DropIndexesCommand functionality.
 * Tests run against all configured drivers (InMemory, Pooled, Single).
 */
@Tag("core")
public class DropIndexesCommandTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testDropSingleIndex(Morphium morphium) throws Exception {
        try (morphium) {
            String db = morphium.getConfig().connectionSettings().getDatabase();
            String coll = "drop_index_test_" + System.currentTimeMillis();

            // Create an index using the driver
            var conn = morphium.getDriver().getPrimaryConnection(null);
            try {
                // Create collection by inserting a document
                morphium.getDriver().insert(db, coll, List.of(Doc.of("_id", "test1", "myfield", "value")), null);

                // Create an index
                var createCmd = new de.caluga.morphium.driver.commands.CreateIndexesCommand(conn)
                    .setDb(db)
                    .setColl(coll)
                    .addIndex(Doc.of("myfield", 1), Doc.of("name", "myfield_1"));
                createCmd.execute();

                // Verify index exists
                var listCmd = new ListIndexesCommand(conn).setDb(db).setColl(coll);
                var indexes = listCmd.execute();
                boolean found = indexes.stream().anyMatch(idx -> "myfield_1".equals(idx.getName()));
                assertThat(found).as("Index should exist after creation").isTrue();

                // Drop the index using DropIndexesCommand via GenericCommand
                var dropCmd = new DropIndexesCommand(conn)
                    .setDb(db)
                    .setColl(coll)
                    .setIndex("myfield_1");
                int msgId = morphium.getDriver().runCommand(new GenericCommand(conn).fromMap(dropCmd.asMap()));
                var result = morphium.getDriver().readSingleAnswer(msgId);
                assertThat(result.get("ok")).isIn(1, 1.0);

                // Verify index no longer exists
                listCmd = new ListIndexesCommand(conn).setDb(db).setColl(coll);
                indexes = listCmd.execute();
                found = indexes.stream().anyMatch(idx -> "myfield_1".equals(idx.getName()));
                assertThat(found).as("Index should not exist after drop").isFalse();
            } finally {
                conn.release();
                // Cleanup
                morphium.getDriver().drop(db, coll, null);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testDropAllIndexes(Morphium morphium) throws Exception {
        try (morphium) {
            String db = morphium.getConfig().connectionSettings().getDatabase();
            String coll = "drop_all_index_test_" + System.currentTimeMillis();

            var conn = morphium.getDriver().getPrimaryConnection(null);
            try {
                // Create collection by inserting a document
                morphium.getDriver().insert(db, coll, List.of(Doc.of("_id", "test1", "field1", 1, "field2", 2, "field3", 3)), null);

                // Create multiple indexes
                var createCmd1 = new de.caluga.morphium.driver.commands.CreateIndexesCommand(conn)
                    .setDb(db).setColl(coll).addIndex(Doc.of("field1", 1), Doc.of("name", "field1_1"));
                createCmd1.execute();

                var createCmd2 = new de.caluga.morphium.driver.commands.CreateIndexesCommand(conn)
                    .setDb(db).setColl(coll).addIndex(Doc.of("field2", -1), Doc.of("name", "field2_-1"));
                createCmd2.execute();

                var createCmd3 = new de.caluga.morphium.driver.commands.CreateIndexesCommand(conn)
                    .setDb(db).setColl(coll).addIndex(Doc.of("field3", 1), Doc.of("name", "field3_1"));
                createCmd3.execute();

                // Verify indexes created (should have _id + 3 created = 4 indexes)
                var listCmd = new ListIndexesCommand(conn).setDb(db).setColl(coll);
                var indexes = listCmd.execute();
                assertThat(indexes.size()).isGreaterThanOrEqualTo(4);

                // Drop all indexes using "*"
                var dropCmd = new DropIndexesCommand(conn)
                    .setDb(db)
                    .setColl(coll)
                    .setIndex("*");
                int msgId = morphium.getDriver().runCommand(new GenericCommand(conn).fromMap(dropCmd.asMap()));
                var result = morphium.getDriver().readSingleAnswer(msgId);
                assertThat(result.get("ok")).isIn(1, 1.0);

                // Verify only _id index remains
                listCmd = new ListIndexesCommand(conn).setDb(db).setColl(coll);
                indexes = listCmd.execute();
                // Should only have _id_ index
                for (var idx : indexes) {
                    String name = idx.getName();
                    assertThat(name).isIn("_id_", "_id_1");
                }
            } finally {
                conn.release();
                morphium.getDriver().drop(db, coll, null);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testDropNonExistentIndex(Morphium morphium) throws Exception {
        try (morphium) {
            String db = morphium.getConfig().connectionSettings().getDatabase();
            String coll = "drop_nonexistent_test_" + System.currentTimeMillis();

            var conn = morphium.getDriver().getPrimaryConnection(null);
            try {
                // Create collection with one index
                morphium.getDriver().insert(db, coll, List.of(Doc.of("_id", "test1", "field1", 1)), null);

                var createCmd = new de.caluga.morphium.driver.commands.CreateIndexesCommand(conn)
                    .setDb(db).setColl(coll).addIndex(Doc.of("field1", 1), Doc.of("name", "field1_1"));
                createCmd.execute();

                // Try to drop a non-existent index
                var dropCmd = new DropIndexesCommand(conn)
                    .setDb(db)
                    .setColl(coll)
                    .setIndex("nonexistent_index");
                int msgId = morphium.getDriver().runCommand(new GenericCommand(conn).fromMap(dropCmd.asMap()));
                var result = morphium.getDriver().readSingleAnswer(msgId);

                // Should return error (ok=0)
                assertThat(result.get("ok")).isIn(0, 0.0);
                assertThat(result.get("errmsg")).isNotNull();
            } finally {
                conn.release();
                morphium.getDriver().drop(db, coll, null);
            }
        }
    }
}
