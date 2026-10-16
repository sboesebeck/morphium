package de.caluga.morphium.query;

import de.caluga.morphium.query.LeaseResult.Status;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("core")
public class LeaseResultTest {

    @Test
    public void anAcquiredLeaseCarriesTheDocument() {
        LeaseResult<String> r = LeaseResult.acquired("doc");
        assertEquals(Status.ACQUIRED, r.getStatus());
        assertTrue(r.isAcquired());
        assertEquals("doc", r.getLease());
    }

    @Test
    public void aHeldLeaseHasNoDocument() {
        LeaseResult<String> r = LeaseResult.held();
        assertEquals(Status.HELD, r.getStatus());
        assertFalse(r.isAcquired());
        assertNull(r.getLease());
    }

    @Test
    public void anAcquiredLeaseNeedsADocument() {
        assertThrows(IllegalArgumentException.class, () -> LeaseResult.acquired(null));
    }

    @Test
    public void equalsAndToString() {
        assertEquals(LeaseResult.acquired("a"), LeaseResult.acquired("a"));
        assertEquals(LeaseResult.acquired("a").hashCode(), LeaseResult.acquired("a").hashCode());
        assertNotEquals(LeaseResult.acquired("a"), LeaseResult.acquired("b"));
        assertNotEquals(LeaseResult.acquired("a"), LeaseResult.held());
        assertEquals(LeaseResult.held(), LeaseResult.held());
        assertTrue(LeaseResult.held().toString().contains("HELD"));
    }
}
