package de.caluga.poppydb;

import java.util.function.Supplier;

/**
 * Rate limit for the log of one connection limit (#530). A connection storm used to log one WARN
 * per rejected connection - hundreds of identical lines per second. Now an episode logs:
 * <ul>
 *     <li>the first rejection after a clean period in full,</li>
 *     <li>while rejections continue, at most one summary line per {@link #SUMMARY_INTERVAL_MS},</li>
 *     <li>the end of the episode with its total and duration (first to last rejection): reported
 *     by the first accepted connection once no rejection happened for {@link #SUMMARY_INTERVAL_MS}.
 *     Churn at the limit - a slot frees, one connect is accepted, the next is rejected - therefore
 *     keeps the episode open instead of logging a recovery and a new first line per freed slot.</li>
 * </ul>
 * The methods return the line to log (or null) instead of logging, so the caller logs outside the
 * lock and a test can check the rate limit without capturing the log.
 */
final class ConnectionRejectionLog {

    static final long SUMMARY_INTERVAL_MS = 10_000;

    private final String limitName;
    // Read without the lock on every accepted connection - the fast path when nothing is rejected.
    private volatile boolean inEpisode = false;
    private long episodeStartMs;
    private long lastRejectionMs;
    private long lastLineMs;
    private long rejectedSinceLine;
    private long rejectedInEpisode;

    ConnectionRejectionLog(String limitName) {
        this.limitName = limitName;
    }

    /** A connection was rejected: the line to WARN, or null while the episode is rate-limited. */
    synchronized String rejected(long nowMs, Supplier<String> firstLine) {
        if (inEpisode && nowMs - lastRejectionMs >= SUMMARY_INTERVAL_MS) {
            // A quiet period without an accepted connection ended the old episode all the same.
            String previous = " (previous episode: " + episodeSummary() + ")";
            return startEpisode(nowMs, firstLine) + previous;
        }

        if (!inEpisode) {
            return startEpisode(nowMs, firstLine);
        }

        lastRejectionMs = nowMs;
        rejectedInEpisode++;
        rejectedSinceLine++;

        if (nowMs - lastLineMs < SUMMARY_INTERVAL_MS) {
            return null;
        }

        String line = limitName + " still reached: " + rejectedSinceLine + " more connections rejected in the last "
                + (nowMs - lastLineMs) / 1000 + " s";
        lastLineMs = nowMs;
        rejectedSinceLine = 0;
        return line;
    }

    /**
     * A connection was accepted: the line to INFO if this ends a rejection episode - only after
     * {@link #SUMMARY_INTERVAL_MS} without a rejection - else null.
     */
    String accepted(long nowMs) {
        if (!inEpisode) {
            return null;
        }

        synchronized (this) {
            if (!inEpisode || nowMs - lastRejectionMs < SUMMARY_INTERVAL_MS) {
                return null;
            }

            inEpisode = false;
            return "Accepting connections again (" + limitName + "): " + episodeSummary();
        }
    }

    private String startEpisode(long nowMs, Supplier<String> firstLine) {
        inEpisode = true;
        episodeStartMs = nowMs;
        lastRejectionMs = nowMs;
        lastLineMs = nowMs;
        rejectedSinceLine = 0;
        rejectedInEpisode = 1;
        return firstLine.get();
    }

    private String episodeSummary() {
        return rejectedInEpisode + " connections rejected in " + (lastRejectionMs - episodeStartMs) + " ms";
    }
}
