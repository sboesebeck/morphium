package de.caluga.morphium.query;

import de.caluga.morphium.query.UpdateOutcome.Kind;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("core")
public class UpdateOutcomeTest {

    @Test
    public void anInsertCarriesTheUpsertedIdAndMayHaveNoDocument() {
        UpdateOutcome<String> o = new UpdateOutcome<>(Kind.INSERTED, null, "id-1");
        assertEquals(Kind.INSERTED, o.getKind());
        assertNull(o.getDocument(), "an insert has no pre-image");
        assertEquals("id-1", o.getUpsertedId());
        assertTrue(o.wasApplied());
        assertTrue(o.wasInserted());
        assertFalse(o.isDuplicateKey());
    }

    @Test
    public void anUpdateIsAppliedButNotInserted() {
        UpdateOutcome<String> o = new UpdateOutcome<>(Kind.UPDATED, "doc", null);
        assertEquals("doc", o.getDocument());
        assertTrue(o.wasApplied());
        assertFalse(o.wasInserted());
        assertNull(o.getUpsertedId());
    }

    @Test
    public void noMatchAndDuplicateKeyAreNotApplied() {
        assertFalse(new UpdateOutcome<String>(Kind.NOT_MATCHED, null, null).wasApplied());
        UpdateOutcome<String> lost = UpdateOutcome.duplicateKey();
        assertEquals(Kind.DUPLICATE_KEY, lost.getKind());
        assertFalse(lost.wasApplied());
        assertTrue(lost.isDuplicateKey());
        assertNull(lost.getDocument());
    }

    @Test
    public void theKindIsRequiredAndConsistent() {
        assertThrows(IllegalArgumentException.class, () -> new UpdateOutcome<String>(null, null, null));
        assertThrows(IllegalArgumentException.class, () -> new UpdateOutcome<>(Kind.UPDATED, "d", "id"), "only an insert has an upserted id");
        assertThrows(IllegalArgumentException.class, () -> new UpdateOutcome<>(Kind.INSERTED, null, null), "an insert has an id");
        assertThrows(IllegalArgumentException.class, () -> new UpdateOutcome<>(Kind.NOT_MATCHED, "d", null), "no match, no document");
        assertThrows(IllegalArgumentException.class, () -> new UpdateOutcome<>(Kind.DUPLICATE_KEY, "d", null), "a lost upsert has no document");
    }

    @Test
    public void equalsHashCodeAndToString() {
        UpdateOutcome<String> a = new UpdateOutcome<>(Kind.INSERTED, "d", "i");
        assertEquals(a, new UpdateOutcome<>(Kind.INSERTED, "d", "i"));
        assertEquals(a.hashCode(), new UpdateOutcome<>(Kind.INSERTED, "d", "i").hashCode());
        assertNotEquals(a, new UpdateOutcome<>(Kind.INSERTED, "e", "i"));
        assertNotEquals(a, new UpdateOutcome<>(Kind.INSERTED, "d", "j"));
        assertNotEquals(a, UpdateOutcome.duplicateKey());
        assertTrue(a.toString().contains("INSERTED") && a.toString().contains("i"), a.toString());
    }
}
