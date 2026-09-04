package de.caluga.poppydb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * A node that binds to an address the seed list does not spell out literally must still recognize
 * ITSELF in that seed list. On the acceptance message bus every node ran with {@code bind=<its IP>}
 * while the seeds are host names - the identity fallback in {@link PoppyDB#configureReplicaSet}
 * only fires when exactly one seed carries the port, so with three seeds on 27017 it never did.
 *
 * <p>Two consequences, both silent: the per-host priorities never reach the node (every node ran at
 * the default 50, so a 100/50/25 config elected whoever won the race), and self is not filtered out
 * of the election peer list - {@code totalNodes = peers + 1} counts one node too many and the node
 * answers its own vote request over the network, casting a second vote under its seed name. With an
 * odd node count the two errors cancel; with an even one they do not, and two of four nodes can
 * elect a leader.
 */
@Tag("server")
public class ReplicaSetIdentityTest {

    /** RFC 5737 documentation addresses: routable nowhere, never a local interface. */
    private static final String PEER_A = "192.0.2.10:27017";
    private static final String PEER_B = "198.51.100.10:27017";
    private static final String SELF_SEED = "localhost:27017";

    private PoppyDB db;

    @AfterEach
    public void tearDown() {
        if (db != null) {
            try {
                db.shutdown();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Binding by IP while the seeds are names: the node must adopt the seed that resolves to its
     * own bind address as its identity, and must not keep itself in the peer list.
     */
    @Test
    public void bindByIpAdoptsResolvingSeedAsIdentity() {
        db = configuredNode("127.0.0.1");

        assertEquals(SELF_SEED, db.getElectionManager().getMyAddress());
        assertFalse(db.getElectionManager().getPeerAddresses().contains(SELF_SEED),
                    "self must not be its own election peer");
        assertEquals(2, db.getElectionManager().getPeerAddresses().size(),
                     "a three-node set has two peers - counting self inflates the majority");
    }

    /**
     * The symptom on acceptance: a 100/50/25 config in which every node silently ran at the default
     * 50, so the lowest-priority node held the primary role.
     */
    @Test
    public void bindByIpAppliesSeedPriority() {
        db = configuredNode("127.0.0.1");

        assertEquals(100, db.getElectionManager().getPriority());
    }

    /**
     * If no seed can be shown to be this node, every quorum this node computes is wrong: it counts
     * one member too many and casts a second vote under the seed name it failed to claim. That is a
     * refusal to start, not a warning to scroll past.
     */
    @Test
    public void unidentifiableNodeRefusesToStart() {
        db = new PoppyDB(27017, "127.0.0.1", 100, 60);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> db.configureReplicaSet("rs0", List.of(PEER_A, PEER_B, "203.0.113.10:27017"),
                                             Map.of(PEER_A, 100, PEER_B, 50, "203.0.113.10:27017", 25),
                                             true, null));

        assertTrue(e.getMessage().contains("127.0.0.1:27017"),
                   "the message must name the bind address that could not be matched: " + e.getMessage());
    }

    /**
     * A wildcard bind has no address of its own to compare against - the seed to claim is the one
     * pointing at any of this machine's interfaces. This is the case the old fallback was written
     * for, so refusing to start here would break setups that work today.
     */
    @Test
    public void wildcardBindAdoptsLocalSeedAsIdentity() {
        db = configuredNode("0.0.0.0");

        assertEquals(SELF_SEED, db.getElectionManager().getMyAddress());
        assertEquals(100, db.getElectionManager().getPriority());
    }

    private PoppyDB configuredNode(String bind) {
        PoppyDB node = new PoppyDB(27017, bind, 100, 60);
        node.configureReplicaSet("rs0", List.of(SELF_SEED, PEER_A, PEER_B),
                                 Map.of(SELF_SEED, 100, PEER_A, 50, PEER_B, 25), true, null);
        return node;
    }
}
