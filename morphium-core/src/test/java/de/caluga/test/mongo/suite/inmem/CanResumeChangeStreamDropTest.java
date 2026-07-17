package de.caluga.test.mongo.suite.inmem;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.inmem.InMemoryDriver;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for the resume-across-a-drop window-loss bug.
 *
 * <p>{@code drop()} purges every buffered event for the dropped namespace from the replay
 * buffer INCLUDING the drop notification itself. When other namespaces keep the buffer
 * non-empty and contiguous with a disconnected consumer's resume token, {@code
 * canResumeChangeStream} used to see a gap-free window and allowed a resume that replayed
 * right across the drop — a disconnected secondary would never learn the collection was
 * dropped and would keep serving it forever.
 *
 * <p>The fix tracks the sequence of the most recent purge-causing drop and refuses a resume
 * whose token predates it, turning the drop-in-the-gap into an explicit window-lost → re-sync.
 */
@Tag("inmemory")
public class CanResumeChangeStreamDropTest {

    private static final String DB = "resumedrop";

    private static Map<String, Object> doc(int i) {
        return Doc.of("_id", "d" + i, "v", i);
    }

    @Test
    public void resumeAcrossCollectionDropIsRefused() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            // Interleave writes to two collections so that after collA's events are purged by
            // the drop, the remaining collB events are still contiguous with the resume token.
            drv.store(DB, "collA", List.of(doc(1)), null);        // token t1
            drv.store(DB, "collB", List.of(doc(2)), null);        // token t2
            long resumeToken = drv.getChangeStreamSequence();      // consumer caught up to t2
            drv.store(DB, "collA", List.of(doc(3)), null);        // token t3 (collA)
            drv.store(DB, "collB", List.of(doc(4)), null);        // token t4 (collB)

            // Sanity: without a drop the consumer can resume from its token.
            assertTrue(drv.canResumeChangeStream(resumeToken),
                    "a contiguous buffer must be resumable before any drop");

            // Drop collA. This purges collA's buffered events (t1, t3) and the drop notification,
            // leaving [t2, t4] (collB) which is still contiguous with resumeToken (t2).
            drv.drop(DB, "collA", null);

            // The consumer sitting at t2 missed collA's drop entirely. It must NOT be allowed to
            // resume, otherwise it would replay [t4] and keep the dropped collA forever.
            assertFalse(drv.canResumeChangeStream(resumeToken),
                    "a resume token predating a namespace drop must be refused (window lost)");

            // A consumer already past the drop boundary can still resume.
            assertTrue(drv.canResumeChangeStream(drv.getChangeStreamSequence()),
                    "a fully-caught-up consumer past the drop can resume");
        } finally {
            drv.close();
        }
    }

    @Test
    public void resumeAcrossDatabaseDropIsRefused() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            drv.store(DB, "collA", List.of(doc(1)), null);
            drv.store("otherdb", "collB", List.of(doc(2)), null);
            long resumeToken = drv.getChangeStreamSequence();
            drv.store("otherdb", "collB", List.of(doc(3)), null);

            drv.drop(DB, null);

            assertFalse(drv.canResumeChangeStream(resumeToken),
                    "a resume token predating a database drop must be refused (window lost)");
        } finally {
            drv.close();
        }
    }
}
