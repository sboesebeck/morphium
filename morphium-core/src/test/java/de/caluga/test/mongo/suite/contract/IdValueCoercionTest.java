package de.caluga.test.mongo.suite.contract;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.NoCache;
import de.caluga.morphium.driver.MorphiumId;
import de.caluga.morphium.query.FindAndModifyOptions;
import de.caluga.morphium.query.ReturnDocument;
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * MOR-045: the key {@code id} of an update becomes {@code _id}; a String value for an entity whose {@code @Id} is a {@code MorphiumId} was written as a String {@code _id}
 * that the mapper cannot read back. A String that is a valid id is an id, any other String is refused.
 */
@Tag("core")
public class IdValueCoercionTest extends MultiDriverTestBase {

    @Entity
    @NoCache
    public static class WithObjectId {
        @Id
        public MorphiumId id;
        public String value;
    }

    private static final String HEX = "5f1e2d3c4b5a69788796a5b4";

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aStringIdOfAnObjectIdEntityIsStoredAsTheId(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(WithObjectId.class);
            WithObjectId r = m.createQueryFor(WithObjectId.class).f("value").eq("v")
                    .findOneAndUpdate(Map.of("$setOnInsert", Map.of("id", HEX), "$set", Map.of("value", "v")),
                                      FindAndModifyOptions.options().upsert(true).returnDocument(ReturnDocument.AFTER));
            assertNotNull(r);
            assertEquals(HEX, r.id.toString());

            Map<String, Object> raw = ModifyFixturesAccess.rawDocs(m.createQueryFor(WithObjectId.class)).get(0);
            assertFalse(raw.get("_id") instanceof String, "the _id is an id, not a String: " + raw.get("_id").getClass());

            WithObjectId read = m.createQueryFor(WithObjectId.class).f("value").eq("v").get();
            assertNotNull(read, "the mapper reads the document back");
            assertEquals(HEX, read.id.toString());
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void aStringThatIsNoIdIsRefused(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(WithObjectId.class);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> m.createQueryFor(WithObjectId.class).f("value").eq("v")
                    .findOneAndUpdate(Map.of("$setOnInsert", Map.of("id", "not-an-id"), "$set", Map.of("value", "v")),
                                      FindAndModifyOptions.options().upsert(true).returnDocument(ReturnDocument.AFTER)));
            assertNotNull(e.getMessage());
            assertEquals(0, m.createQueryFor(WithObjectId.class).countAll(), "nothing was written");
        }
    }

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")
    public void anIdThatIsAlreadyAnIdIsLeftAlone(Morphium m) throws Exception {
        try (m) {
            m.dropCollection(WithObjectId.class);
            MorphiumId id = new MorphiumId();
            WithObjectId r = m.createQueryFor(WithObjectId.class).f("value").eq("v")
                    .findOneAndUpdate(Map.of("$setOnInsert", Map.of("id", id), "$set", Map.of("value", "v")),
                                      FindAndModifyOptions.options().upsert(true).returnDocument(ReturnDocument.AFTER));
            assertEquals(id, r.id);
        }
    }
}
