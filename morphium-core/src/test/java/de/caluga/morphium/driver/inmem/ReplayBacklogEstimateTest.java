package de.caluga.morphium.driver.inmem;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;

/**
 * The wire gate for a secondary's replication resume (MongoCommandHandler's
 * {@code poppyResumeSequence} handling) must not only know whether the resume window is still
 * contiguous ({@link InMemoryDriver#canResumeChangeStream}) - it must also know how LARGE the
 * replay backlog is. A resume deep inside the window whose backlog exceeds the watch cursor's
 * byte budget is doomed: the replay fills the cursor queue past its budget, the cursor is killed
 * mid-replay, and the consumer retries the same token - a livelock where a full re-sync would
 * have cost seconds. {@link InMemoryDriver#estimateReplayBacklogBytes(long, long)} is that size,
 * so the gate can answer HistoryLost up front instead of admitting the doomed replay.
 */
@Tag("core")
public class ReplayBacklogEstimateTest {

    private static final String DB = "replay_est_db";
    private static final String COLL = "probe";

    @Test
    public void estimateBacklogBytesIsTheSumOfEventsAfterTheToken() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            long before = drv.getChangeStreamSequence();
            drv.store(DB, COLL, List.of(Doc.of("_id", 1, "payload", "x".repeat(1024))), null);
            drv.store(DB, COLL, List.of(Doc.of("_id", 2, "payload", "x".repeat(1024))), null);
            drv.store(DB, COLL, List.of(Doc.of("_id", 3, "payload", "x".repeat(1024))), null);
            long after = drv.getChangeStreamSequence();

            assertThat(after).as("three writes produce three sequence increments").isEqualTo(before + 3);

            long allThree = drv.estimateReplayBacklogBytes(before, 0);
            assertThat(allThree).as("backlog after 'before' covers all three events").isGreaterThan(0L);

            long lastTwo = drv.estimateReplayBacklogBytes(before + 1, 0);
            assertThat(lastTwo)
                .as("advancing the resume point shrinks the backlog")
                .isGreaterThan(0L).isLessThan(allThree);

            long caughtUp = drv.estimateReplayBacklogBytes(after, 0);
            assertThat(caughtUp).as("resuming at the newest sequence has nothing to replay").isEqualTo(0L);

            long beyondNewest = drv.estimateReplayBacklogBytes(after + 5, 0);
            assertThat(beyondNewest).as("a future sequence has nothing to replay").isEqualTo(0L);
        } finally {
            drv.close();
        }
    }

    @Test
    public void earlyStopReturnsAsSoonAsTheBacklogPassesTheBound() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        try {
            long before = drv.getChangeStreamSequence();
            drv.store(DB, COLL, List.of(Doc.of("_id", 1, "payload", "x".repeat(2048))), null);
            drv.store(DB, COLL, List.of(Doc.of("_id", 2, "payload", "x".repeat(2048))), null);
            drv.store(DB, COLL, List.of(Doc.of("_id", 3, "payload", "x".repeat(2048))), null);

            long full = drv.estimateReplayBacklogBytes(before, 0);
            long stopped = drv.estimateReplayBacklogBytes(before, 1);

            assertThat(stopped)
                .as("early stop returns before the whole backlog is summed")
                .isGreaterThan(1L).isLessThan(full);
        } finally {
            drv.close();
        }
    }
}