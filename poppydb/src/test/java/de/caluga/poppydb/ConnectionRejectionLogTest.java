package de.caluga.poppydb;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #530: a connection storm logs one WARN for the first rejection, then at most one summary line
 * per 10 s, and one INFO once connections are accepted after a quiet period - instead of one WARN
 * per rejection.
 */
public class ConnectionRejectionLogTest {

    private static final long INTERVAL = ConnectionRejectionLog.SUMMARY_INTERVAL_MS;

    @Test
    public void anEpisodeLogsFirstLineSummariesAndRecovery() {
        ConnectionRejectionLog rejections = new ConnectionRejectionLog("client connection limit");

        assertThat(rejections.accepted(0)).as("no episode, nothing to report").isNull();

        assertThat(rejections.rejected(1_000, () -> "first")).isEqualTo("first");

        // a storm: 500 rejections within the summary interval produce no further line
        for (int i = 1; i <= 500; i++) {
            assertThat(rejections.rejected(1_000 + i * 10, () -> "first again")).isNull();
        }

        String summary = rejections.rejected(1_000 + INTERVAL, () -> "first again");
        assertThat(summary).isNotNull().contains("501 more connections rejected").doesNotContain("first");
        assertThat(rejections.rejected(1_000 + INTERVAL + 1, () -> "x")).isNull();
        long lastRejection = 1_000 + INTERVAL + 1;

        assertThat(rejections.accepted(lastRejection + INTERVAL - 1)).as("not quiet long enough yet").isNull();

        // the duration runs from the first to the LAST rejection, not to the (much later) accept
        String recovery = rejections.accepted(lastRejection + 3_600_000);
        assertThat(recovery).contains("503 connections rejected in " + (lastRejection - 1_000) + " ms");
        assertThat(rejections.accepted(lastRejection + 3_600_001)).as("reported once").isNull();

        // a new episode starts with a full first line again
        assertThat(rejections.rejected(lastRejection + 3_700_000, () -> "first of episode 2"))
                .isEqualTo("first of episode 2");
    }

    /** Churn at the limit: every freed slot is taken by one connect, the next one is rejected. */
    @Test
    public void alternatingAcceptsAndRejectionsDoNotFloodTheLog() {
        ConnectionRejectionLog rejections = new ConnectionRejectionLog("client connection limit");
        int lines = 0;

        for (int i = 0; i < 100; i++) {
            long now = 1_000 + i * 10L; // 100 pairs within one second
            if (rejections.rejected(now, () -> "first") != null) {
                lines++;
            }
            if (rejections.accepted(now + 5) != null) {
                lines++;
            }
        }

        assertThat(lines).as("one first line for the whole churn").isEqualTo(1);

        String recovery = rejections.accepted(1_000 + 990 + INTERVAL);
        assertThat(recovery).contains("100 connections rejected in 990 ms");
    }

    /** A quiet period without any accepted connection also ends the episode. */
    @Test
    public void aRejectionAfterAQuietPeriodStartsANewEpisode() {
        ConnectionRejectionLog rejections = new ConnectionRejectionLog("client connection limit");
        rejections.rejected(0, () -> "first");
        rejections.rejected(500, () -> "x");

        String line = rejections.rejected(500 + INTERVAL, () -> "first of episode 2");
        assertThat(line).startsWith("first of episode 2").contains("previous episode: 2 connections rejected in 500 ms");
    }
}
