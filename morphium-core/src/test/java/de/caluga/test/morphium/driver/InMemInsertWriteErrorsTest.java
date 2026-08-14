package de.caluga.test.morphium.driver;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * writeErrors.index must refer to the client's original batch positions. The insert path
 * removes failed documents from its working list between its error-detection loops, so a
 * later loop's index silently shifted once an earlier loop had removed a document.
 */
@Tag("inmemory")
public class InMemInsertWriteErrorsTest {
    private final String db = "bulkerr";
    private final String coll = "docs";

    @Test
    public void unorderedWriteErrorIndexesReferToOriginalBatchPositions() throws Exception {
        var drv = new InMemoryDriver();
        drv.connect();
        MorphiumId committed = new MorphiumId();
        drv.insert(db, coll, List.of(Doc.of("_id", committed, "seed", true)), null);

        MorphiumId y = new MorphiumId();
        List<Map<String, Object>> batch = new ArrayList<>(List.of(
            Doc.of("_id", committed, "n", 0),      // duplicate vs committed doc -> error at 0
            Doc.of("_id", y, "n", 1),
            Doc.of("_id", y, "n", 2),              // intra-batch duplicate -> error at 2
            Doc.of("_id", new MorphiumId(), "n", 3)));

        var writeErrors = drv.insert(db, coll, batch, null, false);

        assertEquals(2, writeErrors.size());
        assertEquals(0, ((Number) writeErrors.get(0).get("index")).intValue(),
            "first error is the duplicate against the committed document, at batch index 0");
        assertEquals(2, ((Number) writeErrors.get(1).get("index")).intValue(),
            "the intra-batch duplicate sits at batch index 2 - the index must not shift because index 0 was removed from the working list");
    }
}
