package de.caluga.test.morphium.driver;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.commands.ListIndexesCommand;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * listIndexes must report the index options it was created with - anything it drops silently
 * (like partialFilterExpression used to be) breaks every listIndexes/createIndexes round trip,
 * most prominently PoppyDB's index replication (#258), which recreates secondary indexes from
 * exactly this output.
 */
@Tag("core")
public class ListIndexesFidelityTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void listIndexesReportsPartialFilterExpression(Morphium morphium) throws Exception {
        try (morphium) {
            String db = morphium.getConfig().connectionSettings().getDatabase();
            String coll = "list_indexes_fidelity";
            Map<String, Object> partialFilter = Doc.of("archived", Doc.of("$eq", false));

            var conn = morphium.getDriver().getPrimaryConnection(null);
            try {
                new CreateIndexesCommand(conn).setDb(db).setColl(coll)
                    .addIndex(Doc.of("user_id", 1),
                              Doc.of("name", "partial_idx", "unique", true,
                                     "partialFilterExpression", partialFilter))
                    .execute();
            } finally {
                morphium.getDriver().releaseConnection(conn);
            }

            conn = morphium.getDriver().getPrimaryConnection(null);
            List<IndexDescription> indexes = new ListIndexesCommand(conn).setDb(db).setColl(coll).execute();

            IndexDescription idx = indexes.stream()
                .filter(i -> "partial_idx".equals(i.getName()))
                .findFirst().orElse(null);
            assertNotNull(idx, "created index must appear in listIndexes: " + indexes);
            assertEquals(Boolean.TRUE, idx.getUnique());
            assertEquals(partialFilter, idx.getPartialFilterExpression(),
                "partialFilterExpression must round-trip through listIndexes");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void listIndexesReportsCollation(Morphium morphium) throws Exception {
        try (morphium) {
            String db = morphium.getConfig().connectionSettings().getDatabase();
            String coll = "list_indexes_fidelity_collation";

            var conn = morphium.getDriver().getPrimaryConnection(null);
            try {
                new CreateIndexesCommand(conn).setDb(db).setColl(coll)
                    .addIndex(Doc.of("name", 1),
                              Doc.of("name", "collation_idx",
                                     "collation", Doc.of("locale", "de", "strength", 2)))
                    .execute();
            } finally {
                morphium.getDriver().releaseConnection(conn);
            }

            conn = morphium.getDriver().getPrimaryConnection(null);
            List<IndexDescription> indexes = new ListIndexesCommand(conn).setDb(db).setColl(coll).execute();

            IndexDescription idx = indexes.stream()
                .filter(i -> "collation_idx".equals(i.getName()))
                .findFirst().orElse(null);
            assertNotNull(idx, "created index must appear in listIndexes: " + indexes);
            assertNotNull(idx.getCollation(), "collation must round-trip through listIndexes");
            assertEquals("de", idx.getCollation().get("locale"));
            assertEquals(2, ((Number) idx.getCollation().get("strength")).intValue());
        }
    }
}
