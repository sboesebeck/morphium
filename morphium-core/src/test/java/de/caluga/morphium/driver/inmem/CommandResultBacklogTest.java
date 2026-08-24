package de.caluga.morphium.driver.inmem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriver.DriverStatsKey;
import de.caluga.morphium.driver.commands.GenericCommand;

/**
 * Defense-in-depth for the by-id command-result store: a caller that runs commands but never
 * fetches the answers (the bug class behind the PoppyDB secondary leak) must not be able to
 * grow the store without bound. Results whose command id is more than a full window of ids in
 * the past are abandoned by definition -- a legitimate caller fetches its answer synchronously
 * in the same call stack -- and get evicted once the store exceeds the window size. resetData()
 * clears the store completely.
 */
@Tag("inmemory")
public class CommandResultBacklogTest {

    private long pendingReplies(InMemoryDriver drv) {
        Double d = drv.getDriverStats().get(DriverStatsKey.REPLY_IN_MEM);
        return d == null ? 0 : d.longValue();
    }

    /** Issues one update-upsert command and discards the returned message id. */
    private int leakOneCommand(InMemoryDriver drv, int i) {
        GenericCommand cmd = new GenericCommand(drv);
        cmd.setDb("backlogdb");
        cmd.setColl("coll");
        cmd.setCmdData(Doc.of(
            "update", "coll",
            "$db", "backlogdb",
            "updates", List.of(Doc.of(
                "q", Doc.of("_id", i % 10),
                "u", Doc.of("_id", i % 10, "value", i),
                "upsert", true
            ))
        ));
        return drv.runCommand(cmd);
    }

    @Test
    public void unfetchedResultsAreBoundedAndFreshResultsStayFetchable() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            final int n = 12_000;

            for (int i = 0; i < n; i++) {
                leakOneCommand(drv, i);
            }

            long pending = pendingReplies(drv);
            assertTrue(pending < n,
                "unfetched command results must be bounded, but all " + pending + " of " + n + " are still held");

            // A result produced NOW must still be fetchable despite the backlog: eviction may
            // only ever hit abandoned entries, never one a real caller is about to read.
            int msgId = leakOneCommand(drv, n);
            Map<String, Object> answer = drv.readSingleAnswer(msgId);
            assertNotNull(answer, "a fresh command result must survive backlog eviction");
            assertEquals(1.0, ((Number) answer.get("ok")).doubleValue(), 0.0001);
        } finally {
            drv.close();
        }
    }

    @Test
    public void resetDataClearsUnfetchedResults() throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();

        try {
            for (int i = 0; i < 50; i++) {
                leakOneCommand(drv, i);
            }

            assertTrue(pendingReplies(drv) >= 50);
            drv.resetData();
            assertEquals(0, pendingReplies(drv), "resetData() must drop unfetched command results");
        } finally {
            drv.close();
        }
    }
}
