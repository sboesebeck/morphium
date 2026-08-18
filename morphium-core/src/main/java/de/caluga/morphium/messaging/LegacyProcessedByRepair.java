package de.caluga.morphium.messaging;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.MorphiumDriverException;
import de.caluga.morphium.driver.commands.UpdateMongoCommand;

/**
 * Repair for legacy/foreign message documents whose {@code processed_by} is an explicit
 * {@code null} (#291). Morphium senders initialize the field via Msg's {@code @PreStore}, but
 * writers outside that lifecycle (other applications mapping the same collection without the
 * guard, raw driver writers, restored dumps) store explicit nulls - and mongod rejects
 * {@code $addToSet} on such a field ("Cannot apply $addToSet to non-array field ... has
 * non-array type null"). Since exclusive messages must be marked BEFORE the listener runs, a
 * failing mark means hard non-delivery, so every marking site falls back to this repair when its
 * {@code $addToSet} did not take effect.
 *
 * <p>The repair is race-safe: the update is guarded by {@code {field: null}}, so it matches only
 * the broken legacy shape - never an existing array, whose marks must not be clobbered. Running
 * it only AFTER a failed {@code $addToSet} also rules out the pathological array-containing-null
 * match: the field was non-array the moment the mark failed. If a concurrent instance repaired
 * first, this update matches nothing and the caller's retried/rechecked {@code $addToSet} path
 * takes over ($addToSet on the now-existing array is idempotent).
 */
final class LegacyProcessedByRepair {

    private static final Logger log = LoggerFactory.getLogger(LegacyProcessedByRepair.class);

    private LegacyProcessedByRepair() {
    }

    /**
     * Attempts {@code {_id: queryId, fieldName: null} -> {$set: {fieldName: [instanceId]}}} on
     * {@code collection}. Returns {@code true} iff THIS call repaired the document - the instance
     * id is then already contained in the fresh array, no further {@code $addToSet} needed.
     */
    static boolean repairNullField(Morphium morphium, String collection, Object queryId,
                                   String fieldName, String instanceId) {
        if (morphium == null || morphium.getDriver() == null || morphium.getConfig() == null) {
            return false;
        }

        UpdateMongoCommand cmd = null;

        try {
            cmd = new UpdateMongoCommand(
                            morphium.getDriver().getPrimaryConnection(morphium.getWriteConcernForClass(Msg.class)));
            cmd.setColl(collection).setDb(morphium.getDatabase());
            Map<String, Object> query = Doc.of("_id", queryId);
            query.put(fieldName, null);
            cmd.addUpdate(query, Doc.of("$set", Doc.of(fieldName, List.of(instanceId))),
                          null, false, false, null, null, null);
            Map<String, Object> ret = cmd.execute();
            cmd.releaseConnection();
            cmd = null;
            Object modified = ret.get("nModified") != null ? ret.get("nModified") : ret.get("modified");
            boolean repaired = modified instanceof Number && ((Number) modified).intValue() > 0;

            if (repaired) {
                log.info("{}: repaired legacy null {} on message {} in {} (#291)", instanceId, fieldName, queryId, collection);
            }

            return repaired;
        } catch (MorphiumDriverException e) {
            log.warn("{}: could not repair legacy null {} on message {}", instanceId, fieldName, queryId, e);
            return false;
        } finally {
            if (cmd != null) {
                cmd.releaseConnection();
            }
        }
    }
}
