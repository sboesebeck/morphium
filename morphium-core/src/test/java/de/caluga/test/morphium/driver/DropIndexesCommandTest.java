package de.caluga.test.morphium.driver;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.DropIndexesCommand;
import de.caluga.morphium.driver.commands.GenericCommand;
import de.caluga.morphium.driver.commands.ListIndexesCommand;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for DropIndexesCommand functionality.
 * Tests run against all configured drivers (InMemory, Pooled, Single).
 */
@Tag("core")
public class DropIndexesCommandTest extends MultiDriverTestBase {

    @Entity
    public static class TestDoc {
        @Id
        public String id;
        public String myfield;
        public int field1;
        public int field2;
        public int field3;
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testDropSingleIndex(Morphium morphium) throws Exception {
        try (morphium) {
            String db = morphium.getConfig().connectionSettings().getDatabase();
            String coll = "drop_index_test_" + System.currentTimeMillis();

            // Create collection by storing a document
            TestDoc doc = new TestDoc();
            doc.id = "test1";
            doc.myfield = "value";
            morphium.storeNoCache(doc, coll, null);
            Thread.sleep(100);

            // Create an index using the connection
            var conn = morphium.getDriver().getPrimaryConnection(null);
            try {
                var createCmd = new CreateIndexesCommand(conn)
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
                int msgId = conn.sendCommand(new GenericCommand(conn).fromMap(dropCmd.asMap()));
                var result = conn.readSingleAnswer(msgId);
                assertThat(result.get("ok")).isIn(1, 1.0);

                // Release connection before verification to avoid connection issues
                conn.close();
                Thread.sleep(100);
                conn = morphium.getDriver().getPrimaryConnection(null);

                // Verify index no longer exists
                listCmd = new ListIndexesCommand(conn).setDb(db).setColl(coll);
                indexes = listCmd.execute();
                found = indexes.stream().anyMatch(idx -> "myfield_1".equals(idx.getName()));
                assertThat(found).as("Index should not exist after drop").isFalse();
            } finally {
                conn.close();
                // Cleanup
                morphium.dropCollection(TestDoc.class, coll, null);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testDropAllIndexes(Morphium morphium) throws Exception {
        try (morphium) {
            String db = morphium.getConfig().connectionSettings().getDatabase();
            String coll = "drop_all_index_test_" + System.currentTimeMillis();

            // Create collection by storing a document
            TestDoc doc = new TestDoc();
            doc.id = "test1";
            doc.field1 = 1;
            doc.field2 = 2;
            doc.field3 = 3;
            morphium.storeNoCache(doc, coll, null);
            Thread.sleep(100);

            var conn = morphium.getDriver().getPrimaryConnection(null);
            try {
                // Create multiple indexes
                new CreateIndexesCommand(conn).setDb(db).setColl(coll)
                .addIndex(Doc.of("field1", 1), Doc.of("name", "field1_1")).execute();
                new CreateIndexesCommand(conn).setDb(db).setColl(coll)
                .addIndex(Doc.of("field2", -1), Doc.of("name", "field2_-1")).execute();
                new CreateIndexesCommand(conn).setDb(db).setColl(coll)
                .addIndex(Doc.of("field3", 1), Doc.of("name", "field3_1")).execute();

                // Verify indexes created (should have _id + 3 created = 4 indexes)
                var listCmd = new ListIndexesCommand(conn).setDb(db).setColl(coll);
                var indexes = listCmd.execute();
                assertThat(indexes.size()).isGreaterThanOrEqualTo(4);

                // Drop all indexes using "*"
                var dropCmd = new DropIndexesCommand(conn)
                .setDb(db)
                .setColl(coll)
                .setIndex("*");
                int msgId = conn.sendCommand(new GenericCommand(conn).fromMap(dropCmd.asMap()));
                var result = conn.readSingleAnswer(msgId);
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
                conn.close();
                morphium.dropCollection(TestDoc.class, coll, null);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void testDropNonExistentIndex(Morphium morphium) throws Exception {
        try (morphium) {
            String db = morphium.getConfig().connectionSettings().getDatabase();
            String coll = "drop_nonexistent_test_" + System.currentTimeMillis();

            // Create collection by storing a document
            TestDoc doc = new TestDoc();
            doc.id = "test1";
            doc.field1 = 1;
            morphium.storeNoCache(doc, coll, null);
            Thread.sleep(100);

            var conn = morphium.getDriver().getPrimaryConnection(null);
            try {
                new CreateIndexesCommand(conn).setDb(db).setColl(coll)
                .addIndex(Doc.of("field1", 1), Doc.of("name", "field1_1")).execute();

                // Try to drop a non-existent index - should throw exception or return error
                var dropCmd = new DropIndexesCommand(conn)
                .setDb(db)
                .setColl(coll)
                .setIndex("nonexistent_index");
                int msgId = conn.sendCommand(new GenericCommand(conn).fromMap(dropCmd.asMap()));
                try {
                    var result = conn.readSingleAnswer(msgId);
                    // If we get here without exception, verify error response
                    assertThat(result.get("ok")).isIn(0, 0.0);
                    assertThat(result.get("errmsg")).isNotNull();
                } catch (MorphiumDriverException e) {
                    // Driver throws exception for error responses - this is also acceptable
                    assertThat(e.getMessage()).contains("index not found");
                }
            } finally {
                conn.close();
                morphium.dropCollection(TestDoc.class, coll, null);
            }
        }
    }
}
