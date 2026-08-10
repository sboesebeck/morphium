package de.caluga.morphium.driver.inmem;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.driver.Doc;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * InMem change-stream pipelines with a top-level $or: the requeue branch (update events
 * whose updateDescription shows processed_by cleared to an empty array) must pass both
 * QueryHelper matching and the subscription delivery path - alongside regular inserts.
 */
@Tag("core")
public class ChangeStreamOrPipelineTest {

    @Test
    public void orWithRequeueBranchMatchesUpdateEvent() {
        Map<String, Object> match = new LinkedHashMap<>();
        Map<String, Object> in = new LinkedHashMap<>();
        in.put("$in", Arrays.asList("insert", "lock_released"));
        match.put("operationType", in);
        Map<String, Object> requeue = new LinkedHashMap<>();
        requeue.put("operationType", "update");
        requeue.put("updateDescription.updatedFields.processed_by", UtilsMap.of("$size", 0));
        Map<String, Object> or = UtilsMap.of("$or", Arrays.asList(match, requeue));

        Map<String, Object> event = Doc.of(
                "operationType", "update",
                "updateDescription", Doc.of(
                        "updatedFields", Doc.of("processed_by", List.of()),
                        "removedFields", List.of()));

        assertThat(QueryHelper.matchesQuery(or, event, null)).isTrue();
    }

    @Test
    public void monitorWithOrPipelineReceivesRequeueUpdateEvents() throws Exception {
        de.caluga.morphium.MorphiumConfig cfg = new de.caluga.morphium.MorphiumConfig();
        cfg.driverSettings().setDriverName("InMemDriver");
        cfg.connectionSettings().setDatabase("scratch_or_pipeline");
        cfg.clusterSettings().setHostSeed(new java.util.ArrayList<>());
        try (de.caluga.morphium.Morphium morphium = new de.caluga.morphium.Morphium(cfg)) {
            Map<String, Object> match = new LinkedHashMap<>();
            match.put("operationType", UtilsMap.of("$in", Arrays.asList("insert", "lock_released")));
            Map<String, Object> requeue = new LinkedHashMap<>();
            requeue.put("operationType", "update");
            requeue.put("updateDescription.updatedFields.processed_by", UtilsMap.of("$size", 0));
            List<Map<String, Object>> pipeline =
                    List.of(UtilsMap.of("$match", UtilsMap.of("$or", Arrays.asList(match, requeue))));

            var received = new java.util.concurrent.CopyOnWriteArrayList<String>();
            var monitor = new de.caluga.morphium.changestream.ChangeStreamMonitor(morphium, "scratch_coll", false, 250, pipeline);
            monitor.addListener(evt -> {
                received.add(evt.getOperationType());
                return true;
            });
            monitor.start();

            try {
                var drv = (InMemoryDriver) morphium.getDriver();
                Map<String, Object> doc = Doc.of("k", 1, "processed_by", List.of("someone"));
                new de.caluga.morphium.driver.commands.InsertMongoCommand(drv.getPrimaryConnection(null))
                        .setDb("scratch_or_pipeline").setColl("scratch_coll")
                        .setDocuments(new java.util.ArrayList<>(List.of(doc))).execute();
                Thread.sleep(300);
                drv.update("scratch_or_pipeline", "scratch_coll",
                        Doc.of("k", 1), null, Doc.of("$set", Doc.of("processed_by", List.of())), false, false, null, null);
                Thread.sleep(700);

                assertThat(received).as("insert AND requeue-update must pass the $or pipeline")
                        .contains("insert", "update");
            } finally {
                monitor.terminate();
            }
        }
    }
}
