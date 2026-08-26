package de.caluga.morphium.driver.inmem;

import de.caluga.morphium.IndexDescription;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.commands.ListIndexesCommand;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Regression tests for the #340 follow-up regression (ACC outage after a full cluster
 * restart): the dump restore recreated TTL indexes with {@code expireAfterSeconds} as
 * {@code Long} (the JSON parser's number type) instead of {@code Integer}. The restore itself
 * ran fine - the damage surfaced only when ANOTHER node asked for the index over the wire:
 * {@code ListIndexesCommand.execute} feeds the result into {@link IndexDescription#fromMap},
 * whose reflective {@code fld.set} threw {@code IllegalArgumentException} for the Integer
 * field, which failed the initial sync on every peer, forever - two of three nodes never left
 * recovery.
 *
 * <p>No test caught it because the restore and the server restart were each covered alone,
 * but not the combination "restored from a dump, then queried by a peer". The tests here pin
 * exactly that combination at driver level, plus both halves separately:
 * <ul>
 *   <li>the receiving side ({@code fromMap}) must coerce numeric fields tolerantly - that is
 *       the line where a harmless type difference used to become a node that never comes back
 *       up, and it also protects against dumps written by unfixed versions;</li>
 *   <li>the restore side must register the spec with the correct wrapper types, so the wire
 *       serves Int32 - which keeps OLDER peers (without the fromMap hardening) alive in a
 *       mixed-version replica set.</li>
 * </ul>
 */
@Tag("inmemory")
public class RestoredIndexWireCompatTest {
    private static final String DB = "idx_wire_compat_db";
    private static final String COLL = "ttl_coll";

    /** Fixture literal, exactly what the JSON parser sees in a #340 dump: every number a Long. */
    private static File writeTtlDumpFixture(Path tmp) throws Exception {
        String json = "{ \"_id\" : 1723891234567, \"db\" : \"" + DB + "\", \"data\" : { \"" + COLL + "\" : [ "
                + "{ \"_id\" : \"a\", \"ended_on\" : { \"class_name\" : \"java.util.Date\", \"value\" : 1723891234567 } } ] }, "
                + "\"indexes\" : { \"" + COLL + "\" : [ "
                + "{ \"v\" : 2.0, \"key\" : { \"ended_on\" : 1 }, \"name\" : \"ended_on_ttl\", \"expireAfterSeconds\" : 259200 } ] } }";
        File f = new File(tmp.toFile(), DB + ".morphium.gz");
        try (GZIPOutputStream gz = new GZIPOutputStream(new FileOutputStream(f))) {
            gz.write(json.getBytes(StandardCharsets.UTF_8));
        }
        return f;
    }

    /**
     * THE missing combination: restore from a dump, then list the indexes the way a syncing
     * peer does - ListIndexesCommand parses every result through IndexDescription.fromMap.
     * Before the fix this threw IllegalArgumentException (Long into the Integer field
     * expireAfterSeconds) and with it failed every peer's initial sync against this node.
     */
    @Test
    public void restoredTtlIndexSurvivesTheListIndexesRoundTrip(@TempDir Path tmp) throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.connect();
        drv.restoreFromFile(writeTtlDumpFixture(tmp));

        var con = drv.getPrimaryConnection(null);
        List<IndexDescription> indexes;
        try {
            indexes = new ListIndexesCommand(con).setDb(DB).setColl(COLL).execute();
        } finally {
            drv.releaseConnection(con);
        }

        IndexDescription ttl = indexes.stream().filter(i -> "ended_on_ttl".equals(i.getName())).findFirst().orElse(null);
        assertNotNull(ttl, "the restored TTL index must be listed, got: " + indexes);
        assertEquals(259200, ttl.getExpireAfterSeconds(),
                "expireAfterSeconds must survive the restore -> listIndexes -> fromMap round trip");
    }

    /**
     * The restore side alone: the recreated index must be REGISTERED with Integer-typed
     * numeric options, not just survive our own fromMap. A mixed-version replica set can
     * contain peers without the fromMap hardening - they crash on an Int64 expireAfterSeconds,
     * so the wire representation this node serves must be Int32.
     */
    @Test
    public void restoredIndexOptionsAreRegisteredWithInt32Types(@TempDir Path tmp) throws Exception {
        InMemoryDriver drv = new InMemoryDriver();
        drv.restoreFromFile(writeTtlDumpFixture(tmp));

        Map<String, Object> raw = null;
        for (Map<String, Object> idx : drv.getIndexes(DB, COLL)) {
            Map<?, ?> opts = (Map<?, ?>) idx.get("$options");
            if (opts != null && "ended_on_ttl".equals(opts.get("name"))) {
                raw = idx;
            }
        }
        assertNotNull(raw, "restored TTL index must exist");
        Object expire = ((Map<?, ?>) raw.get("$options")).get("expireAfterSeconds");
        assertInstanceOf(Integer.class, expire,
                "expireAfterSeconds must be registered as Integer (wire Int32) - a Long here crashes "
                + "every peer WITHOUT the fromMap hardening during its initial sync, got: "
                + expire.getClass().getName());
    }

    /**
     * The receiving side alone: fromMap must coerce numeric values against the declared field
     * type instead of letting the reflective set throw. Every Integer field of
     * IndexDescription has the same trap, and the JSON/wire side can deliver Long (dump
     * restore, Int64) or Double (mongosh sends plain numbers as doubles) for any of them.
     */
    @Test
    public void fromMapCoercesNumericTypesForEveryIntegerField() {
        Doc longSpec = Doc.of("key", Doc.of("ended_on", 1), "name", "ended_on_ttl");
        longSpec.put("expireAfterSeconds", 259200L);
        longSpec.put("textIndexVersion", 3L);
        longSpec.put("2dsphereIndexVersion", 3L);
        longSpec.put("bits", 26L);
        longSpec.put("min", -180L);
        longSpec.put("max", 180L);
        IndexDescription fromLongs = IndexDescription.fromMap(longSpec);
        assertEquals(259200, fromLongs.getExpireAfterSeconds(), "Long -> Integer coercion for expireAfterSeconds");
        assertEquals(3, fromLongs.getTextIndexVersion(), "Long -> Integer coercion for textIndexVersion");
        assertEquals(3, fromLongs.get_2dsphereIndexVersion(), "Long -> Integer coercion for 2dsphereIndexVersion");
        assertEquals(26, fromLongs.getBits(), "Long -> Integer coercion for bits");
        assertEquals(-180, fromLongs.getMin(), "Long -> Integer coercion for min");
        assertEquals(180, fromLongs.getMax(), "Long -> Integer coercion for max");

        Doc doubleSpec = Doc.of("key", Doc.of("ended_on", 1), "name", "ended_on_ttl");
        doubleSpec.put("expireAfterSeconds", 259200.0d);
        IndexDescription fromDouble = IndexDescription.fromMap(doubleSpec);
        assertEquals(259200, fromDouble.getExpireAfterSeconds(), "Double -> Integer coercion (mongosh numbers)");
    }

    /** Pre-existing tolerance that must not regress: wire Int32 1/0 for Boolean fields. */
    @Test
    public void fromMapKeepsIntegerToBooleanTolerance() {
        Doc boolSpec = Doc.of("key", Doc.of("email", 1), "name", "uniq");
        boolSpec.put("unique", 1);
        IndexDescription idx = IndexDescription.fromMap(boolSpec);
        assertEquals(Boolean.TRUE, idx.getUnique(), "Integer 1 must still coerce to Boolean TRUE");
    }
}
