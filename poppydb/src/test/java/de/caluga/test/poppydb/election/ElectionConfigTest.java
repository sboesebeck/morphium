package de.caluga.test.poppydb.election;

import de.caluga.poppydb.election.ElectionConfig;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * #387: the election timeout must ORDER candidates by priority, not merely bias them. Two nodes
 * whose priorities are at least one {@code electionPriorityStep} apart must never draw
 * overlapping timeouts - every timeout of the higher-priority node is shorter than every timeout
 * of the lower-priority one - while nodes of equal priority keep randomizing within their slot.
 *
 * <p>Before the fix the formula was {@code random(min..max) + range * (1 - priority/100)}: the
 * random span was as wide as the whole priority delay, so any two priorities less than 100 apart
 * overlapped (100 vs 90: 2.0-4.0s against 2.2-4.2s) and the lower one campaigned first often
 * enough to matter - one election in five for 50 vs 10, far more for 100 vs 90.
 */
public class ElectionConfigTest {

    private static final int DRAWS = 2000;

    private static ElectionConfig config(int min, int max, int priority) {
        return new ElectionConfig()
                .setElectionTimeoutMinMs(min)
                .setElectionTimeoutMaxMs(max)
                .setElectionPriority(priority);
    }

    private static int[] draw(ElectionConfig config) {
        int[] minMax = {Integer.MAX_VALUE, Integer.MIN_VALUE};
        for (int i = 0; i < DRAWS; i++) {
            int t = config.randomElectionTimeout();
            minMax[0] = Math.min(minMax[0], t);
            minMax[1] = Math.max(minMax[1], t);
        }
        return minMax;
    }

    private static void assertOrdered(int min, int max, int step, int higher, int lower) {
        ElectionConfig high = config(min, max, higher).setElectionPriorityStep(step);
        ElectionConfig low = config(min, max, lower).setElectionPriorityStep(step);
        int[] highWindow = draw(high);
        int[] lowWindow = draw(low);
        assertTrue(highWindow[1] < lowWindow[0],
                "priority " + higher + " (window " + highWindow[0] + ".." + highWindow[1] + "ms) must always time out before priority "
                        + lower + " (window " + lowWindow[0] + ".." + lowWindow[1] + "ms) with range " + min + ".." + max + " and step " + step);
    }

    @Test
    void higherPriorityAlwaysTimesOutFirstWithDefaultStep() {
        // the default step is 10: adjacent steps must not overlap, larger gaps neither
        assertOrdered(2000, 4000, 10, 100, 90);
        assertOrdered(2000, 4000, 10, 50, 40);
        assertOrdered(2000, 4000, 10, 100, 50);
        assertOrdered(2000, 4000, 10, 50, 10);
        assertOrdered(2000, 4000, 10, 20, 10);
        // 10 and 1 are only nine apart - less than a step - and share a slot; that is by design
        // the acceptance bus runs 100/50/25
        assertOrdered(2000, 4000, 10, 50, 25);
    }

    @Test
    void orderingScalesWithTheConfiguredRange() {
        // the test runner's configuration
        assertOrdered(5000, 10000, 10, 100, 90);
        assertOrdered(5000, 10000, 10, 50, 25);
        // a tiny test range
        assertOrdered(100, 200, 10, 100, 50);
    }

    @Test
    void stepIsConfigurable() {
        // step 5: 100 and 95 are one step apart and must be ordered
        assertOrdered(2000, 4000, 5, 100, 95);
        // step 25: 100 and 75 are one step apart and must be ordered
        assertOrdered(2000, 4000, 25, 100, 75);
    }

    @Test
    void equalPrioritiesStillRandomize() {
        ElectionConfig config = config(2000, 4000, 50);
        Set<Integer> distinct = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            distinct.add(config.randomElectionTimeout());
        }
        assertTrue(distinct.size() > 10, "equal priorities must spread within their slot, got " + distinct.size() + " distinct values");
    }

    @Test
    void timeoutsStayWithinTheConfiguredRange() {
        for (int priority : new int[]{100, 75, 50, 25, 10, 1}) {
            int[] window = draw(config(2000, 4000, priority));
            assertTrue(window[0] >= 2000, "priority " + priority + " must not time out before the minimum: " + window[0]);
            assertTrue(window[1] < 4000, "priority " + priority + " must time out before the maximum: " + window[1]);
        }
    }

    @Test
    void highestPriorityStartsAtTheMinimum() {
        // priority 100 owns the first slot - it must not wait a priority delay of its own
        int[] window = draw(config(2000, 4000, 100));
        assertEquals(2000, window[0]);
    }

    @Test
    void invalidStepIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ElectionConfig().setElectionPriorityStep(0));
        assertThrows(IllegalArgumentException.class, () -> new ElectionConfig().setElectionPriorityStep(101));
    }
}
