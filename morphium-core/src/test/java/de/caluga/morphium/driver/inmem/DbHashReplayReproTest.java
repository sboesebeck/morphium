package de.caluga.morphium.driver.inmem;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.driver.commands.GenericCommand;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Repro for the 2026-08-30 EmptyNodeRestartWipeTest failure: two nodes holding the SAME
 * logical documents must produce the SAME dbHash, no matter whether a document was
 * materialized by a plain insert (snapshot / live replication) or by the idempotent
 * replay path (replace-style upsert of the identical full document, as
 * ReplicationManager.applyInsertIdempotent does after a benign duplicate-_id sync race).
 *
 * <p>Before the fix, InMemoryDriver's replacement-update path reordered the document to
 * "_id first" (obj.clear(); obj.put("_id", ...); obj.putAll(u)) while the insert path kept
 * the client's field order - so a replayed document became byte-different from the same
 * document applied via insert, dbHash diverged, and the next consistencyShortcut forced a
 * destructive full sync (drop + resnapshot) on a perfectly healthy data-bearing follower.
 */
@Tag("core")
public class DbHashReplayReproTest {

    private static final String DB = "dbhashrepro";
    private static final String COLL = "objs";

    private String dbHashOf(InMemoryDriver drv) throws Exception {
        GenericCommand cmd = new GenericCommand(drv);
        cmd.setCmdData(Doc.of("dbHash", 1, "collections", List.of(COLL), "$db", DB));
        int id = drv.runCommand(cmd);
        Map<String, Object> res = drv.readSingleAnswer(id);
        assertThat(res.get("ok")).isEqualTo(1.0);
        @SuppressWarnings("unchecked")
        Map<String, Object> colls = (Map<String, Object>) res.get("collections");
        return (String) colls.get(COLL);
    }

    @Test
    public void replayedIdenticalDocumentMustNotChangeDbHash() throws Exception {
        MorphiumId oid = new MorphiumId();
        // field order deliberately puts _id NOT first - morphium's ObjectMapperImpl builds a
        // plain HashMap, so "_id first" is not guaranteed for real client documents either
        Map<String, Object> doc = Doc.of("str_value", "pre-1", "counter", 1, "dval", 0.0, "_id", oid);

        InMemoryDriver viaInsert = new InMemoryDriver();
        viaInsert.connect();
        InMemoryDriver viaReplay = new InMemoryDriver();
        viaReplay.connect();

        try {
            viaInsert.store(DB, COLL, List.of(Doc.of(doc)), null);

            // the replay node first got the same document via snapshot insert ...
            viaReplay.store(DB, COLL, List.of(Doc.of(doc)), null);
            // ... and then re-applied the identical insert event through the idempotent
            // replay (exact command shape of ReplicationManager.applyInsertIdempotent)
            GenericCommand upsert = new GenericCommand(viaReplay);
            upsert.setCmdData(Doc.of(
                "update", COLL,
                "$db", DB,
                "updates", List.of(Doc.of(
                    "q", Doc.of("_id", oid),
                    "u", Doc.of(doc),
                    "upsert", true))));
            int msgId = viaReplay.runCommand(upsert);
            Map<String, Object> res = viaReplay.readSingleAnswer(msgId);
            assertThat(res.get("ok")).isEqualTo(1.0);

            Map<String, Object> insertDoc = viaInsert.find(DB, COLL, Doc.of(), null, null, 0, 0).get(0);
            Map<String, Object> replayDoc = viaReplay.find(DB, COLL, Doc.of(), null, null, 0, 0).get(0);
            System.out.println("insert node field order: " + insertDoc.keySet());
            System.out.println("replay node field order: " + replayDoc.keySet());

            // logically identical ...
            assertThat(replayDoc).isEqualTo(insertDoc);
            // ... and therefore the consistency check must see identical hashes
            assertThat(dbHashOf(viaReplay))
                .as("dbHash after an idempotent replay of the identical document must match "
                    + "the plain-insert hash - otherwise the consistency shortcut wrongly "
                    + "triggers a destructive full sync on a healthy follower")
                .isEqualTo(dbHashOf(viaInsert));
        } finally {
            viaInsert.close();
            viaReplay.close();
        }
    }
}
