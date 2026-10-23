package de.caluga.morphium.query;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("core")
public class UpdateResultTest {

    @Test
    public void anAcknowledgedResultCarriesTheCounts() {
        UpdateResult r = new UpdateResult(3, 2, "id-1", true);
        assertEquals(3, r.getMatchedCount());
        assertEquals(2, r.getModifiedCount());
        assertEquals("id-1", r.getUpsertedId());
        assertTrue(r.isAcknowledged());
        assertTrue(r.wasUpserted());
    }

    @Test
    public void noUpsertedIdMeansNothingWasInserted() {
        UpdateResult r = new UpdateResult(1, 0, null, true);
        assertNull(r.getUpsertedId());
        assertFalse(r.wasUpserted());
    }

    @Test
    public void anUnacknowledgedResultSaysSoInsteadOfPretendingToHaveMatchedNothing() {
        UpdateResult r = UpdateResult.unacknowledged();
        assertFalse(r.isAcknowledged());
        assertEquals(0, r.getMatchedCount());
        assertTrue(r.toString().contains("unacknowledged"), r.toString());
    }

    @Test
    public void equalsHashCodeAndToStringUseAllFields() {
        UpdateResult a = new UpdateResult(1, 1, "x", true);
        assertEquals(a, new UpdateResult(1, 1, "x", true));
        assertEquals(a.hashCode(), new UpdateResult(1, 1, "x", true).hashCode());
        assertNotEquals(a, new UpdateResult(1, 0, "x", true));
        assertNotEquals(a, new UpdateResult(1, 1, "y", true));
        assertNotEquals(a, new UpdateResult(1, 1, "x", false));
        assertTrue(a.toString().contains("matched=1") && a.toString().contains("modified=1") && a.toString().contains("upsertedId=x"), a.toString());
    }

    @Test
    public void negativeCountsAreRejected() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> new UpdateResult(-1, 0, null, true));
    }
}
