package de.caluga.test.morphium.driver.inmem;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.CreateIndexesCommand;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * MOR-097: $inc, $mul and $currentDate below a scalar value are rejected like $set always was: PathNotViable (28), the document stays as it was. (The
 * multikey unique index, where $push of a value that another document holds must be a duplicate key, is MOR-098.)
 */
@Tag("inmemory")
public class UpdateRejectionTest {

    private static final String DB = "updrejdb";
    private static final String COLL = "docs";

    private InMemoryDriver drv;

    @BeforeEach
    public void setup() throws Exception {
        drv = new InMemoryDriver();
        drv.connect();
        new CreateIndexesCommand(drv).setDb(DB).setColl(COLL)
            .addIndex(new IndexDescription().setKey(Doc.of("tag_list", 1)).setUnique(true))
            .execute();
        drv.insert(DB, COLL, List.of(
            Doc.of("_id", "a", "scalar_value", 7, "tag_list", new ArrayList<>(List.of("a"))),
            Doc.of("_id", "b", "scalar_value", 7, "tag_list", new ArrayList<>(List.of("b")))), null, true);
    }

    @AfterEach
    public void tearDown() {
        if (drv != null) {
            drv.close();
        }
    }

    /** runs the update on document a and returns the code of the rejection; fails if the update is applied */
    @SuppressWarnings("unchecked")
    private int rejectionCode(Map<String, Object> update) throws Exception {
        try {
            Map<String, Object> result = drv.update(DB, COLL, Doc.of("_id", "a"), null, update, false, false, null, null, null);

            if (result != null && result.get("writeErrors") instanceof List<?> errs && !errs.isEmpty()) {
                return ((Number) ((Map<String, Object>) errs.get(0)).get("code")).intValue();
            }
        } catch (MorphiumDriverException e) {
            if (e.getMongoCode() instanceof Number n) {
                return n.intValue();
            }

            if (e.getWriteErrors() != null && !e.getWriteErrors().isEmpty()) {
                return ((Number) e.getWriteErrors().get(0).get("code")).intValue();
            }
        }

        throw new AssertionError("the update was applied: " + a());
    }

    private Map<String, Object> a() throws Exception {
        List<Map<String, Object>> res = drv.find(DB, COLL, Doc.of("_id", "a"), null, null, 0, 1);
        assertNotNull(res);
        return res.get(0);
    }

    private void assertUnchanged() throws Exception {
        assertEquals(7, a().get("scalar_value"));
        assertEquals(List.of("a"), a().get("tag_list"));
    }

    @Test
    public void setBelowAScalarIsPathNotViable() throws Exception {
        assertEquals(28, rejectionCode(Doc.of("$set", Doc.of("scalar_value.sub", 1))));
        assertUnchanged();
    }

    @Test
    public void incBelowAScalarIsPathNotViable() throws Exception {
        assertEquals(28, rejectionCode(Doc.of("$inc", Doc.of("scalar_value.sub", 1))));
        assertUnchanged();
    }

    @Test
    public void mulBelowAScalarIsPathNotViable() throws Exception {
        assertEquals(28, rejectionCode(Doc.of("$mul", Doc.of("scalar_value.sub", 2))));
        assertUnchanged();
    }

    @Test
    public void incBelowAnExistingDocumentStillWorks() throws Exception {
        drv.update(DB, COLL, Doc.of("_id", "a"), null, Doc.of("$set", Doc.of("box", Doc.of("n", 1))), false, false, null, null, null);
        drv.update(DB, COLL, Doc.of("_id", "a"), null, Doc.of("$inc", Doc.of("box.n", 2, "fresh.n", 5)), false, false, null, null, null);
        assertEquals(3, ((Number) ((Map<?, ?>) a().get("box")).get("n")).intValue());
        assertEquals(5, ((Number) ((Map<?, ?>) a().get("fresh")).get("n")).intValue());
    }

    @Test
    public void currentDateBelowAScalarIsPathNotViable() throws Exception {
        assertEquals(28, rejectionCode(Doc.of("$currentDate", Doc.of("scalar_value.sub", true))));
        assertUnchanged();
    }
}
